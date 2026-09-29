package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Health is the traffic-light state shown on each card.
type Health string

const (
	HealthOK      Health = "ok"
	HealthWarn    Health = "warn"
	HealthBad     Health = "bad"
	HealthUnknown Health = "unknown"
)

// Run is one backup attempt, from either engine, in a common shape so the UI
// can show Velero backups and restic Jobs in one list.
type Run struct {
	Source   string // "cloud" or "disk"
	Name     string
	Phase    string // Completed, PartiallyFailed, Failed, InProgress, ...
	Started  time.Time
	Finished time.Time
	Expires  time.Time
	Errors   int
	Warnings int
	Items    int
	Schedule string
	Manual   bool
	Location string // storage folder: "scheduled" / "manual"
}

func (r Run) Duration() time.Duration {
	if r.Started.IsZero() {
		return 0
	}
	end := r.Finished
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(r.Started)
}

// Card is one headline tile.
type Card struct {
	Title  string
	Health Health
	Big    string
	Detail string
}

type Schedule struct {
	Name, Cron, Phase string
	LastBackup        time.Time
}

type Status struct {
	Generated time.Time
	Cards     []Card
	Cloud     []Run
	Disk      []Run
	Watchdog  []Run
	Schedules []Schedule
	BSL       []string
	Errors    []string // collector failures, shown rather than hidden

	Categories []Category
	Requests   []RequestView
}

type Config struct {
	VeleroNS       string
	LocalNS        string
	LocalCronJob   string
	WatchdogCron   string
	SealedCertURL  string
	StateNS        string
	StateConfigMap string
	CloudMaxAge    time.Duration
	DiskMaxAge     time.Duration
}

type meta struct {
	Name              string            `json:"name"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	OwnerReferences   []struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"ownerReferences"`
}

type veleroBackupList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			StorageLocation string `json:"storageLocation"`
		} `json:"spec"`
		Status struct {
			Phase               string    `json:"phase"`
			StartTimestamp      time.Time `json:"startTimestamp"`
			CompletionTimestamp time.Time `json:"completionTimestamp"`
			Expiration          time.Time `json:"expiration"`
			Errors              int       `json:"errors"`
			Warnings            int       `json:"warnings"`
			Progress            struct {
				ItemsBackedUp int `json:"itemsBackedUp"`
			} `json:"progress"`
		} `json:"status"`
	} `json:"items"`
}

type veleroScheduleList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			Schedule string `json:"schedule"`
		} `json:"spec"`
		Status struct {
			Phase      string    `json:"phase"`
			LastBackup time.Time `json:"lastBackup"`
		} `json:"status"`
	} `json:"items"`
}

type veleroBSLList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			ObjectStorage struct {
				Bucket string `json:"bucket"`
				Prefix string `json:"prefix"`
			} `json:"objectStorage"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

type jobList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Status   struct {
			StartTime      time.Time `json:"startTime"`
			CompletionTime time.Time `json:"completionTime"`
			Active         int       `json:"active"`
			Succeeded      int       `json:"succeeded"`
			Failed         int       `json:"failed"`
			Conditions     []struct {
				Type               string    `json:"type"`
				Status             string    `json:"status"`
				LastTransitionTime time.Time `json:"lastTransitionTime"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

func collect(ctx context.Context, k *kubeClient, cfg Config, cats []Category) Status {
	st := Status{Generated: time.Now(), Categories: cats}
	fail := func(what string, err error) { st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", what, err)) }

	var bl veleroBackupList
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+cfg.VeleroNS+"/backups", &bl); err != nil {
		fail("velero backups", err)
	}
	st.Cloud = veleroRuns(bl)

	var sl veleroScheduleList
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+cfg.VeleroNS+"/schedules", &sl); err != nil {
		fail("velero schedules", err)
	}
	for _, s := range sl.Items {
		st.Schedules = append(st.Schedules, Schedule{s.Metadata.Name, s.Spec.Schedule, s.Status.Phase, s.Status.LastBackup})
	}

	var bsl veleroBSLList
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+cfg.VeleroNS+"/backupstoragelocations", &bsl); err != nil {
		fail("velero storage locations", err)
	}
	for _, b := range bsl.Items {
		st.BSL = append(st.BSL, fmt.Sprintf("%s: %s (s3://%s/%s)", b.Metadata.Name, b.Status.Phase, b.Spec.ObjectStorage.Bucket, b.Spec.ObjectStorage.Prefix))
	}

	var jl jobList
	if err := k.get(ctx, "/apis/batch/v1/namespaces/"+cfg.LocalNS+"/jobs", &jl); err != nil {
		fail("backup jobs", err)
	}
	st.Disk = jobRuns(jl, "disk", cfg.LocalCronJob)
	st.Watchdog = jobRuns(jl, "watchdog", cfg.WatchdogCron)

	reqs, err := listRequests(ctx, k, cfg.StateNS)
	if err != nil {
		fail("backup requests", err)
	}
	st.Requests = reqs

	// The freshness card watches the SCHEDULED backups only: a manual backup
	// is not expected on any cadence and must not mask a missed weekly run.
	var scheduled []Run
	for _, r := range st.Cloud {
		if !r.Manual {
			scheduled = append(scheduled, r)
		}
	}
	st.Cards = append(st.Cards,
		cloudCard(scheduled, st.BSL, cfg.CloudMaxAge),
		diskCard(st.Disk, cfg.DiskMaxAge),
		watchdogCard(st.Watchdog),
		keyCard(ctx, k, cfg, fail),
	)
	return st
}

func veleroRuns(bl veleroBackupList) []Run {
	var out []Run
	for _, b := range bl.Items {
		phase := b.Status.Phase
		if phase == "" {
			phase = "New"
		}
		out = append(out, Run{
			Source: "cloud", Name: b.Metadata.Name, Phase: phase,
			Started: b.Status.StartTimestamp, Finished: b.Status.CompletionTimestamp,
			Expires: b.Status.Expiration, Errors: b.Status.Errors, Warnings: b.Status.Warnings,
			Items: b.Status.Progress.ItemsBackedUp, Schedule: b.Metadata.Labels["velero.io/schedule-name"],
			Manual:   b.Metadata.Labels["velero.io/schedule-name"] == "",
			Location: strings.TrimPrefix(b.Spec.StorageLocation, "cloud-"),
		})
	}
	sortNewest(out)
	return out
}

// jobRuns keeps the Jobs owned by one CronJob. Jobs made with
// `kubectl create job --from=cronjob/x` carry the same ownerReference, so
// manual runs are included and flagged via the instantiate annotation.
func jobRuns(jl jobList, source, cronjob string) []Run {
	var out []Run
	for _, j := range jl.Items {
		owned := false
		for _, o := range j.Metadata.OwnerReferences {
			if o.Kind == "CronJob" && o.Name == cronjob {
				owned = true
			}
		}
		if !owned {
			continue
		}
		phase := "Pending"
		fin := j.Status.CompletionTime
		for _, c := range j.Status.Conditions {
			if c.Status != "True" {
				continue
			}
			switch c.Type {
			case "Complete":
				phase = "Completed"
			case "Failed":
				phase = "Failed"
				fin = c.LastTransitionTime
			}
		}
		if phase == "Pending" && j.Status.Active > 0 {
			phase = "InProgress"
		}
		start := j.Status.StartTime
		if start.IsZero() {
			start = j.Metadata.CreationTimestamp
		}
		out = append(out, Run{
			Source: source, Name: j.Metadata.Name, Phase: phase, Started: start, Finished: fin,
			Errors: j.Status.Failed, Schedule: cronjob,
			Manual:   j.Metadata.Annotations["cronjob.kubernetes.io/instantiate"] == "manual",
			Location: jobLocation(j.Metadata.Annotations),
		})
	}
	sortNewest(out)
	return out
}

func sortNewest(r []Run) {
	sort.Slice(r, func(i, j int) bool { return r[i].Started.After(r[j].Started) })
}

// lastGood returns the newest run that fully succeeded.
func lastGood(runs []Run) (Run, bool) {
	for _, r := range runs {
		if r.Phase == "Completed" {
			return r, true
		}
	}
	return Run{}, false
}

func cloudCard(runs []Run, bsl []string, maxAge time.Duration) Card {
	c := Card{Title: "Cloud backup (Velero → S3)"}
	if len(runs) == 0 {
		c.Health, c.Big, c.Detail = HealthBad, "no backups", "Velero has no Backup objects."
		return c
	}
	newest := runs[0]
	good, ok := lastGood(runs)
	switch {
	case newest.Phase == "Failed" || newest.Phase == "FailedValidation":
		c.Health = HealthBad
	case newest.Phase == "PartiallyFailed":
		c.Health = HealthWarn
	case newest.Phase == "InProgress":
		c.Health = HealthOK
	case newest.Phase != "Completed":
		c.Health = HealthWarn
	default:
		c.Health = HealthOK
	}
	if !ok {
		c.Health = HealthBad
		c.Big = "no successful backup"
	} else {
		age := time.Since(good.Finished)
		c.Big = humanAgo(age)
		if age > maxAge {
			c.Health = HealthBad
		}
		c.Detail = fmt.Sprintf("last full success: %s", good.Name)
	}
	c.Detail += fmt.Sprintf(" · latest run %s (%s)", newest.Phase, newest.Name)
	for _, b := range bsl {
		if !strings.Contains(b, ": Available") {
			c.Health = HealthBad
			c.Detail += " · storage location: " + b
		}
	}
	return c
}

func diskCard(runs []Run, maxAge time.Duration) Card {
	c := Card{Title: "Disk backup (restic → munin)"}
	if len(runs) == 0 {
		c.Health, c.Big, c.Detail = HealthUnknown, "no backups yet", "Disk backups are manual-only. Use the form below to start one."
		return c
	}
	newest := runs[0]
	good, ok := lastGood(runs)
	if !ok {
		c.Health, c.Big = HealthBad, "no successful run"
	} else {
		age := time.Since(good.Finished)
		c.Big = humanAgo(age)
		c.Health = HealthOK
		// maxAge == 0 means "manual-only": nothing is due, so age is not a fault.
		if maxAge > 0 && age > maxAge {
			c.Health = HealthBad
		}
		c.Detail = "last success: " + good.Name
	}
	switch newest.Phase {
	case "Failed":
		c.Health = HealthBad
	case "InProgress":
		if newest.Duration() > 12*time.Hour && c.Health == HealthOK {
			c.Health = HealthWarn
		}
	}
	c.Detail += fmt.Sprintf(" · latest run %s (%s, %s)", newest.Phase, newest.Name, humanDur(newest.Duration()))
	return c
}

func watchdogCard(runs []Run) Card {
	c := Card{Title: "Watchdog (healthchecks.io ping)"}
	if len(runs) == 0 {
		c.Health, c.Big, c.Detail = HealthWarn, "no runs", "No backup-watchdog Jobs found."
		return c
	}
	consecutive := 0
	for _, r := range runs {
		if r.Phase == "Failed" {
			consecutive++
			continue
		}
		break
	}
	if consecutive > 0 {
		c.Health = HealthBad
		c.Big = fmt.Sprintf("%d failed in a row", consecutive)
		c.Detail = "The watchdog is withholding its healthchecks.io ping — normally because the newest Completed backup is older than its limit (a PartiallyFailed run does not count)."
		return c
	}
	c.Health, c.Big = HealthOK, runs[0].Phase
	c.Detail = "latest: " + runs[0].Name
	return c
}

// keyCard reports the age of the newest Sealed Secrets signing key and whether
// the vault export predates it. Key age comes from the controller's PUBLIC
// certificate (its NotBefore is the key's creation time), so this service never
// needs RBAC on Secrets — the signing key stays unreadable to it.
func keyCard(ctx context.Context, k *kubeClient, cfg Config, fail func(string, error)) Card {
	c := Card{Title: "Secrets key (Sealed Secrets → your vault)"}
	created, err := fetchKeyCreated(ctx, cfg.SealedCertURL)
	if err != nil {
		fail("sealed-secrets certificate", err)
		c.Health, c.Big, c.Detail = HealthUnknown, "unknown", "Could not read the controller's public certificate."
		return c
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	exported := time.Time{}
	if err := k.get(ctx, "/api/v1/namespaces/"+cfg.StateNS+"/configmaps/"+cfg.StateConfigMap, &cm); err == nil {
		exported, _ = time.Parse(time.RFC3339, cm.Data["exportedAt"])
	}
	c.Big = "key " + strings.TrimSuffix(humanAgo(time.Since(created)), " ago") + " old"
	switch {
	case exported.IsZero():
		c.Health = HealthWarn
		c.Detail = "No vault export recorded. Run scripts/k8s-secrets/copy_secrets.sh."
	case exported.Before(created):
		c.Health = HealthBad
		c.Detail = fmt.Sprintf("Newest key (%s) is newer than your last vault export (%s). Re-run copy_secrets.sh.",
			created.Format("2006-01-02"), exported.Format("2006-01-02"))
	default:
		c.Health = HealthOK
		c.Detail = fmt.Sprintf("Vault export %s covers the newest key (%s).", exported.Format("2006-01-02"), created.Format("2006-01-02"))
	}
	return c
}

func fetchKeyCreated(ctx context.Context, url string) (time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return time.Time{}, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("%s: %s", url, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return time.Time{}, err
	}
	return parseCertNotBefore(b)
}

func parseCertNotBefore(pemBytes []byte) (time.Time, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return time.Time{}, fmt.Errorf("no PEM block in certificate response")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, err
	}
	return cert.NotBefore, nil
}

func humanAgo(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func humanDur(d time.Duration) string {
	switch {
	case d <= 0:
		return "—"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%02dh", int(d.Hours()/24), int(d.Hours())%24)
	}
}

// jobLocation names the folder a disk run wrote to. Only manual runs write into
// homelab-backups/manual; a scheduled CronJob run (if the schedule is ever
// un-suspended) uses the same repo but is labelled by trigger.
func jobLocation(ann map[string]string) string {
	if ann["cronjob.kubernetes.io/instantiate"] == "manual" {
		return "manual"
	}
	return "scheduled"
}
