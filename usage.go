package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// S3 usage. The web pod never holds the bucket credential, so the size of the
// bucket is measured by a short Job in the Velero namespace (where that
// credential lives), running this same binary as `backup-ui s3-usage`. The
// dispatcher starts it on an interval or on request, reads its result from the
// pod log and stores it in the usageConfigMap, which the web pod reads.
//
// True per-backup S3 size does not exist: Kopia deduplicates across every backup
// of a namespace, so what a backup "costs" depends on what the others share. The
// report therefore sizes FOLDERS (each namespace's Kopia repository, each
// backup's metadata), and the backups table shows volume data before dedup.

const (
	usageConfigMap    = "s3-usage"
	usageJobLabel     = "homelab.smugiwara.com/usage"
	usageRequestLabel = "homelab.smugiwara.com/usage-request"
	usageMarker       = "S3USAGE "
)

// UsageGroup is one folder (or set of loose files) in the bucket.
type UsageGroup struct {
	Kind    string // "backup" (Velero metadata), "kopia" (volume data), "other"
	Name    string // backup name, namespace, or folder/file name
	Bytes   int64
	Objects int
	Oldest  time.Time
	Newest  time.Time
	// Set by the web pod, not the scan: a backup folder with no Backup object.
	Orphan bool `json:"-"`
}

// UsageSection is one BackupStorageLocation's prefix, or everything else
// under the scanned prefix that Velero does not own.
type UsageSection struct {
	Title   string
	Prefix  string
	Managed bool
	Bytes   int64
	Objects int
	Groups  []UsageGroup
}

type UsageReport struct {
	Bucket   string
	Prefix   string
	Scanned  time.Time
	Bytes    int64
	Objects  int
	Sections []UsageSection
	// Unfinished multipart uploads: billed, invisible to a plain listing.
	Uploads       int
	UploadsOldest time.Time
	UploadsError  string `json:",omitempty"`
}

// bslPrefix is one BackupStorageLocation as the scan needs it.
type bslPrefix struct{ Name, Prefix string }

// parseBSLPrefixes reads "name=prefix;name=prefix".
func parseBSLPrefixes(s string) []bslPrefix {
	var out []bslPrefix
	for _, p := range strings.Split(s, ";") {
		name, prefix, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || name == "" {
			continue
		}
		out = append(out, bslPrefix{name, dirPrefix(prefix)})
	}
	// Longest first, so a nested prefix wins over its parent.
	sort.Slice(out, func(i, j int) bool { return len(out[i].Prefix) > len(out[j].Prefix) })
	return out
}

// dirPrefix normalises "a/b" and "/a/b/" to "a/b/" ("" stays "").
func dirPrefix(p string) string {
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// usageAccumulator groups objects as they stream in, so a large bucket costs
// memory per folder, not per object.
type usageAccumulator struct {
	scan     string
	bsls     []bslPrefix
	sections map[string]*UsageSection
	groups   map[string]map[string]*UsageGroup
	total    UsageReport
}

func newUsageAccumulator(scan string, bsls []bslPrefix) *usageAccumulator {
	a := &usageAccumulator{scan: scan, bsls: bsls, sections: map[string]*UsageSection{}, groups: map[string]map[string]*UsageGroup{}}
	for _, b := range bsls {
		a.section(b.Name, "Velero location "+b.Name, b.Prefix, true)
	}
	return a
}

func (a *usageAccumulator) section(id, title, prefix string, managed bool) *UsageSection {
	if s, ok := a.sections[id]; ok {
		return s
	}
	s := &UsageSection{Title: title, Prefix: prefix, Managed: managed}
	a.sections[id] = s
	a.groups[id] = map[string]*UsageGroup{}
	return s
}

func (a *usageAccumulator) add(o s3Object) {
	sid, kind, name := a.classify(o.Key)
	sec := a.sections[sid]
	if sec == nil {
		sec = a.section(sid, "Not managed by Velero", a.scan, false)
	}
	key := kind + "/" + name
	g := a.groups[sid][key]
	if g == nil {
		g = &UsageGroup{Kind: kind, Name: name}
		a.groups[sid][key] = g
	}
	g.Bytes += o.Size
	g.Objects++
	if g.Oldest.IsZero() || o.LastModified.Before(g.Oldest) {
		g.Oldest = o.LastModified
	}
	if o.LastModified.After(g.Newest) {
		g.Newest = o.LastModified
	}
	sec.Bytes += o.Size
	sec.Objects++
	a.total.Bytes += o.Size
	a.total.Objects++
}

// classify maps a key to (section, group kind, group name). Inside a Velero
// prefix, backups/<name>/ is one backup's metadata and kopia/<ns>/ is one
// namespace's volume-data repository (layout of Velero's object store).
func (a *usageAccumulator) classify(key string) (string, string, string) {
	for _, b := range a.bsls {
		if !strings.HasPrefix(key, b.Prefix) {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(key, b.Prefix), "/", 3)
		if len(parts) >= 3 {
			switch parts[0] {
			case "backups":
				return b.Name, "backup", parts[1]
			case "kopia":
				return b.Name, "kopia", parts[1]
			}
		}
		if len(parts) == 1 {
			return b.Name, "other", "(files)"
		}
		return b.Name, "other", parts[0] + "/"
	}
	rest := strings.TrimPrefix(key, a.scan)
	if dir, _, ok := strings.Cut(rest, "/"); ok {
		// A folder that merely CONTAINS a Velero prefix (automatic/k3s_backups/
		// around scheduled/ and manual/) is reported by its own name.
		return "", "other", dir + "/"
	}
	return "", "other", "(loose files)"
}

func (a *usageAccumulator) report(bucket string, now time.Time) UsageReport {
	r := a.total
	r.Bucket, r.Prefix, r.Scanned = bucket, a.scan, now
	ids := make([]string, 0, len(a.sections))
	for id := range a.sections {
		ids = append(ids, id)
	}
	// Velero locations first (in name order), the unmanaged rest last.
	sort.Slice(ids, func(i, j int) bool {
		if (ids[i] == "") != (ids[j] == "") {
			return ids[j] == ""
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		s := *a.sections[id]
		for _, g := range a.groups[id] {
			s.Groups = append(s.Groups, *g)
		}
		sort.Slice(s.Groups, func(i, j int) bool { return s.Groups[i].Bytes > s.Groups[j].Bytes })
		r.Sections = append(r.Sections, s)
	}
	return r
}

// runS3Usage is the `s3-usage` subcommand, run by the usage Job. It prints the
// report on one marked line; anything else in the log is diagnostics.
func runS3Usage() {
	bucket, region := os.Getenv("BUCKET"), env("REGION", "us-east-1")
	scan := dirPrefix(os.Getenv("SCAN_PREFIX"))
	creds, err := loadAWSCreds(env("AWS_SHARED_CREDENTIALS_FILE", "/creds/cloud"), env("AWS_PROFILE", "default"))
	if err != nil {
		log.Fatalf("credentials: %v", err)
	}
	if bucket == "" {
		log.Fatal("BUCKET is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	s3 := newS3Client(bucket, region, os.Getenv("S3_URL"), creds)
	acc := newUsageAccumulator(scan, parseBSLPrefixes(os.Getenv("BSL_PREFIXES")))
	if err := s3.listObjects(ctx, scan, acc.add); err != nil {
		log.Fatalf("listing s3://%s/%s: %v", bucket, scan, err)
	}
	r := acc.report(bucket, time.Now().UTC())
	ups, err := s3.listUploads(ctx, scan)
	if err != nil {
		// Commonly not granted (s3:ListBucketMultipartUploads); report, don't fail.
		r.UploadsError = err.Error()
	}
	r.Uploads = len(ups)
	for _, u := range ups {
		if r.UploadsOldest.IsZero() || u.Initiated.Before(r.UploadsOldest) {
			r.UploadsOldest = u.Initiated
		}
	}
	b, _ := json.Marshal(r)
	fmt.Println(usageMarker + string(b))
	log.Printf("scanned s3://%s/%s: %d objects, %d bytes", bucket, scan, r.Objects, r.Bytes)
}

// parseUsageLog finds the report line in the Job's log.
func parseUsageLog(logText string) (UsageReport, error) {
	var r UsageReport
	lines := strings.Split(logText, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if rest, ok := strings.CutPrefix(lines[i], usageMarker); ok {
			return r, json.Unmarshal([]byte(rest), &r)
		}
	}
	return r, fmt.Errorf("no report in the scan log")
}

// ---- dispatcher side --------------------------------------------------------

// usageState is the usageConfigMap's data.
type usageState struct {
	Report  string // JSON UsageReport of the last successful scan
	Error   string // why the last scan failed, if it did
	Job     string // the scan Job being waited on, if any
	Started time.Time
	Checked time.Time // when the last scan finished (success or not)
}

func readUsageState(data map[string]string) usageState {
	s := usageState{Report: data["report"], Error: data["error"], Job: data["job"]}
	s.Started, _ = time.Parse(time.RFC3339, data["started"])
	s.Checked, _ = time.Parse(time.RFC3339, data["checked"])
	return s
}

func (s usageState) data() map[string]string {
	ts := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	}
	return map[string]string{"report": s.Report, "error": s.Error, "job": s.Job, "started": ts(s.Started), "checked": ts(s.Checked)}
}

func saveUsageState(ctx context.Context, k *kubeClient, dc DispatchConfig, s usageState, exists bool) error {
	if exists {
		patch := map[string]any{"data": s.data()}
		return k.do(ctx, http.MethodPatch, cmBase(dc)+"/"+usageConfigMap, "application/merge-patch+json", patch, nil)
	}
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": usageConfigMap, "namespace": dc.StateNS},
		"data":     s.data(),
	}
	return k.do(ctx, http.MethodPost, cmBase(dc), "", cm, nil)
}

// reconcileUsage is one dispatcher poll of the usage scan: harvest a finished
// Job, or start one if the last scan is stale or the operator asked.
func reconcileUsage(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	if dc.UsageInterval <= 0 || dc.SelfImage == "" {
		return nil
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	exists := true
	if err := k.get(ctx, cmBase(dc)+"/"+usageConfigMap, &cm); err != nil {
		if !isNotFound(err) {
			return err
		}
		exists = false
	}
	st := readUsageState(cm.Data)
	jobs := "/apis/batch/v1/namespaces/" + dc.VeleroNS + "/jobs"

	if st.Job != "" {
		var js jobStatus
		err := k.get(ctx, jobs+"/"+st.Job, &js)
		switch {
		case isNotFound(err):
			st.Error = "the scan Job " + st.Job + " disappeared before it finished"
		case err != nil:
			return err
		default:
			ok, failed, msg := js.done()
			if !ok && !failed {
				if time.Since(st.Started) < 45*time.Minute {
					return nil // still running
				}
				st.Error = "the scan Job " + st.Job + " has been running for over 45 minutes"
				deleteJob(ctx, k, dc.VeleroNS, st.Job)
			} else {
				logText, lerr := jobLog(ctx, k, dc.VeleroNS, st.Job)
				if ok && lerr == nil {
					if r, perr := parseUsageLog(logText); perr == nil {
						b, _ := json.Marshal(r)
						st.Report, st.Error = string(b), ""
					} else {
						st.Error = perr.Error()
					}
				} else {
					st.Error = strings.TrimSpace("scan failed: " + msg + " " + lastLines(logText, 3))
					if lerr != nil {
						st.Error += " (log unavailable: " + lerr.Error() + ")"
					}
				}
			}
		}
		st.Job, st.Checked = "", time.Now()
		log.Printf("s3 usage scan finished (error: %q)", st.Error)
		return saveUsageState(ctx, k, dc, st, exists)
	}

	asked, err := takeUsageRequests(ctx, k, dc)
	if err != nil {
		return err
	}
	if !asked && !st.Checked.IsZero() && time.Since(st.Checked) < dc.UsageInterval {
		return nil
	}
	job, err := buildUsageJob(ctx, k, dc, "s3-usage-"+time.Now().UTC().Format("20060102-150405"))
	if err != nil {
		st.Error, st.Checked = "could not start the scan: "+err.Error(), time.Now()
		return saveUsageState(ctx, k, dc, st, exists)
	}
	if err := k.do(ctx, http.MethodPost, jobs, "", job, nil); err != nil {
		return err
	}
	st.Job, st.Started = dig(job, "metadata", "name").(string), time.Now()
	log.Printf("started s3 usage scan %s", st.Job)
	return saveUsageState(ctx, k, dc, st, exists)
}

// takeUsageRequests marks every pending "rescan" request as handled and
// reports whether there was one. Old requests are cleaned up.
func takeUsageRequests(ctx context.Context, k *kubeClient, dc DispatchConfig) (bool, error) {
	var l configMapList
	base := cmBase(dc)
	if err := k.get(ctx, base+"?labelSelector="+url.QueryEscape(usageRequestLabel+"=true"), &l); err != nil {
		return false, err
	}
	asked := false
	for _, it := range l.Items {
		if time.Since(it.Metadata.CreationTimestamp) > dc.RequestMaxAge {
			_ = k.do(ctx, http.MethodDelete, base+"/"+it.Metadata.Name, "", nil, nil)
			continue
		}
		if it.Metadata.Annotations[annPhase] != "" {
			continue
		}
		asked = true
		if err := annotate(ctx, k, base, it.Metadata.Name, "Started", "S3 usage scan started"); err != nil {
			return asked, err
		}
	}
	return asked, nil
}

// buildUsageJob points the scan at the default BSL's bucket and credential.
// It scans the default BSL prefix's top-level folder (e.g. "automatic/"), not
// just Velero's own prefixes, so anything else stored there shows up too —
// that is usually where stale data hides.
func buildUsageJob(ctx context.Context, k *kubeClient, dc DispatchConfig, name string) (map[string]any, error) {
	var l struct {
		Items []struct {
			Metadata meta `json:"metadata"`
			Spec     struct {
				Default       bool `json:"default"`
				ObjectStorage struct {
					Bucket string `json:"bucket"`
					Prefix string `json:"prefix"`
				} `json:"objectStorage"`
				Config     map[string]string `json:"config"`
				Credential struct {
					Name string `json:"name"`
					Key  string `json:"key"`
				} `json:"credential"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backupstoragelocations", &l); err != nil {
		return nil, err
	}
	if len(l.Items) == 0 {
		return nil, fmt.Errorf("no BackupStorageLocations")
	}
	def := l.Items[0]
	for _, b := range l.Items {
		if b.Spec.Default {
			def = b
		}
	}
	bucket := def.Spec.ObjectStorage.Bucket
	var prefixes []string
	for _, b := range l.Items {
		if b.Spec.ObjectStorage.Bucket == bucket {
			prefixes = append(prefixes, b.Metadata.Name+"="+b.Spec.ObjectStorage.Prefix)
		}
	}
	scan := dc.UsageScanPrefix
	if scan == "" {
		scan, _, _ = strings.Cut(strings.Trim(def.Spec.ObjectStorage.Prefix, "/"), "/")
	}
	credName, credKey := def.Spec.Credential.Name, def.Spec.Credential.Key
	if credName == "" {
		credName, credKey = dc.CredSecret, "cloud"
	}
	envs := []any{
		map[string]any{"name": "BUCKET", "value": bucket},
		map[string]any{"name": "REGION", "value": def.Spec.Config["region"]},
		map[string]any{"name": "S3_URL", "value": def.Spec.Config["s3Url"]},
		map[string]any{"name": "AWS_PROFILE", "value": orDefault(def.Spec.Config["profile"], "default")},
		map[string]any{"name": "SCAN_PREFIX", "value": dirPrefix(scan)},
		map[string]any{"name": "BSL_PREFIXES", "value": strings.Join(prefixes, ";")},
		map[string]any{"name": "AWS_SHARED_CREDENTIALS_FILE", "value": "/creds/cloud"},
	}
	return map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": name, "namespace": dc.VeleroNS, "labels": map[string]string{usageJobLabel: "job"}},
		"spec": map[string]any{
			"backoffLimit": 0, "ttlSecondsAfterFinished": 3600, "activeDeadlineSeconds": 1800,
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]string{usageJobLabel: "job"}},
				"spec": map[string]any{
					"restartPolicy": "Never",
					// Read-only S3 listing: no API access, not root.
					"automountServiceAccountToken": false,
					"securityContext": map[string]any{
						"runAsNonRoot": true, "runAsUser": 65532, "seccompProfile": map[string]any{"type": "RuntimeDefault"},
					},
					"containers": []any{map[string]any{
						"name": "s3-usage", "image": dc.SelfImage, "args": []any{"s3-usage"},
						"env": envs,
						"securityContext": map[string]any{
							"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
							"capabilities": map[string]any{"drop": []any{"ALL"}},
						},
						"resources":    map[string]any{"requests": map[string]any{"cpu": "10m", "memory": "32Mi"}, "limits": map[string]any{"memory": "128Mi"}},
						"volumeMounts": []any{map[string]any{"name": "creds", "mountPath": "/creds", "readOnly": true}},
					}},
					"volumes": []any{map[string]any{"name": "creds", "secret": map[string]any{
						"secretName": credName, "items": []any{map[string]any{"key": credKey, "path": "cloud"}},
					}}},
				},
			},
		},
	}, nil
}

func orDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// jobLog returns the log of a Job's (only) pod.
func jobLog(ctx context.Context, k *kubeClient, ns, job string) (string, error) {
	var pods struct {
		Items []struct {
			Metadata meta `json:"metadata"`
		} `json:"items"`
	}
	sel := url.QueryEscape("job-name=" + job)
	if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/pods?labelSelector="+sel, &pods); err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pod for job %s", job)
	}
	return k.text(ctx, "/api/v1/namespaces/"+ns+"/pods/"+pods.Items[0].Metadata.Name+"/log")
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " / ")
}

// ---- web side ---------------------------------------------------------------

// UsageView is what the page shows about the bucket.
type UsageView struct {
	Report  *UsageReport
	Error   string
	Running bool
	Started time.Time
	Checked time.Time
	// Totals for the card.
	Managed, Unmanaged int64
}

func loadUsage(ctx context.Context, k *kubeClient, ns string, backups []Run) (UsageView, error) {
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/configmaps/"+usageConfigMap, &cm); err != nil {
		if isNotFound(err) {
			return UsageView{}, nil
		}
		return UsageView{}, err
	}
	st := readUsageState(cm.Data)
	v := UsageView{Error: st.Error, Running: st.Job != "", Started: st.Started, Checked: st.Checked}
	if st.Report == "" {
		return v, nil
	}
	var r UsageReport
	if err := json.Unmarshal([]byte(st.Report), &r); err != nil {
		return v, fmt.Errorf("stored usage report: %w", err)
	}
	known := map[string]bool{}
	for _, b := range backups {
		known[b.Name] = true
	}
	for i := range r.Sections {
		s := &r.Sections[i]
		if s.Managed {
			v.Managed += s.Bytes
		} else {
			v.Unmanaged += s.Bytes
		}
		for j := range s.Groups {
			g := &s.Groups[j]
			g.Orphan = s.Managed && g.Kind == "backup" && !known[g.Name]
		}
	}
	v.Report = &r
	return v, nil
}

func usageCard(v UsageView) Card {
	c := Card{Title: "S3 bucket usage"}
	switch {
	case v.Report == nil && v.Running:
		c.Health, c.Big, c.Detail = HealthUnknown, "scanning…", "First scan of the bucket is running."
		return c
	case v.Report == nil:
		c.Health, c.Big, c.Detail = HealthUnknown, "unknown", "Not scanned yet."
		if v.Error != "" {
			c.Health, c.Detail = HealthWarn, v.Error
		}
		return c
	}
	r := v.Report
	c.Health, c.Big = HealthOK, humanBytes(r.Bytes)
	c.Detail = fmt.Sprintf("Velero %s · other %s · in s3://%s/%s · scanned %s",
		humanBytes(v.Managed), humanBytes(v.Unmanaged), r.Bucket, r.Prefix, humanAgo(time.Since(r.Scanned)))
	if r.Uploads > 0 {
		c.Health = HealthWarn
		c.Detail += fmt.Sprintf(" · %d unfinished multipart upload(s), still billed", r.Uploads)
	}
	if v.Error != "" {
		c.Health = HealthWarn
		c.Detail += " · last scan failed: " + v.Error
	}
	return c
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// createUsageRequest asks the dispatcher for a rescan.
func createUsageRequest(ctx context.Context, k *kubeClient, ns string) (string, error) {
	name := "usg-" + newRequestName(time.Now())[4:]
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{usageRequestLabel: "true"}},
	}
	return name, k.do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", "", cm, nil)
}
