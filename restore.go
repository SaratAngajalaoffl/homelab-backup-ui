package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Restore flow, end to end:
//
//  1. The operator picks an active backup, one of its volumes (PVC) and the disk
//     the replacement should live on. The web pod writes a restore ConfigMap.
//  2. The dispatcher creates a NEW PVC on that disk's StorageClass, restores the
//     backup's data into it, then repoints every Deployment that mounted the old
//     PVC. The old PVC is kept and labelled "orphaned".
//  3. The operator verifies the app, then either deletes the orphan (finalize)
//     or switches back to it (rollback). Switching is symmetric, so a rollback
//     can itself be undone until the orphan is deleted.
//
// ArgoCD's selfHeal would revert the repoint, so the dispatcher pauses the
// owning Application's auto-sync while a restore is live; see restore_dispatch.go.
const (
	restoreLabel       = "homelab.smugiwara.com/restore"
	restoreActionLabel = "homelab.smugiwara.com/restore-action"
	// stateLabel is set on the PVCs of a restore: "active" or "orphaned".
	stateLabel  = "homelab.smugiwara.com/restore-state"
	annRecord   = "homelab.smugiwara.com/restore-record"
	annOrphaned = "homelab.smugiwara.com/orphaned-at"
	annState    = "homelab.smugiwara.com/state"
	annSince    = "homelab.smugiwara.com/phase-since"
)

// Restore phases. In-progress ones are advanced by the dispatcher; terminal ones
// are only ever changed by an operator action.
const (
	PhaseValidating = "Validating"
	PhaseBinding    = "Binding"
	PhaseRestoring  = "Restoring"
	PhaseActive     = "Active"
	PhaseFinalized  = "Finalized"
	PhaseFailed     = "Failed"
	PhaseRejected   = "Rejected"
	PhaseDiscarded  = "Discarded"
)

// Disk is a place a restored volume can live, expressed as the StorageClass
// that provisions onto it. HostRoot is the directory that class provisions
// under; the dispatcher refuses to mount any PV path outside the configured roots.
type Disk struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	StorageClass string `json:"storageClass"`
	HostRoot     string `json:"hostRoot"`
}

func loadDisks(path string) ([]Disk, error) {
	var disks []Disk
	if err := loadJSONFile(path, &disks); err != nil {
		return nil, err
	}
	for _, d := range disks {
		if d.ID == "" || d.StorageClass == "" || d.HostRoot == "" {
			return nil, fmt.Errorf("%s: disk %q needs id, storageClass and hostRoot", path, d.ID)
		}
	}
	return disks, nil
}

// RestoreRequest is what the operator submits. Everything in it is re-checked
// by the dispatcher; the web pod only fails fast.
type RestoreRequest struct {
	Source    string // "cloud" or "disk"
	Backup    string // Velero Backup name, or the restic Job name that took the snapshot
	Namespace string
	Claim     string
	Disk      string
}

// RestoreSource is one active backup the operator can restore from.
type RestoreSource struct {
	Source     string
	Name       string
	Started    time.Time
	Namespaces []string
	Partial    bool
}

// Volume is one PVC in a namespace that can be backed up.
type Volume struct {
	Namespace    string
	Name         string
	Size         string
	StorageClass string
	Phase        string
	State        string // restore-state label: "", "active" or "orphaned"
}

type workloadRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// restoreState is the dispatcher's working memory for one restore, kept as one
// JSON annotation on the record so a restarted dispatcher resumes where it was.
type restoreState struct {
	NewClaim   string        `json:"newClaim"`
	Active     string        `json:"active"`
	Orphan     string        `json:"orphan"`
	Workloads  []workloadRef `json:"workloads,omitempty"`
	Volume     string        `json:"volume,omitempty"` // pod volume name the old PVC was mounted as
	PVCUID     string        `json:"pvcUid,omitempty"`
	BinderJob  string        `json:"binderJob,omitempty"`
	RestoreJob string        `json:"restoreJob,omitempty"`
	Dest       string        `json:"dest,omitempty"`
	Snapshot   string        `json:"snapshot,omitempty"`
	// ArgoPaused maps an Application to the auto-sync policy it had before the
	// restore, so resuming puts back exactly what was there. A null value means
	// the Application had no auto-sync to begin with.
	ArgoPaused map[string]json.RawMessage `json:"argoPaused,omitempty"`
}

func (s restoreState) paused() bool { return len(s.ArgoPaused) > 0 }

func (s restoreState) argoApps() string {
	var n []string
	for a := range s.ArgoPaused {
		n = append(n, a)
	}
	sort.Strings(n)
	return strings.Join(n, ", ")
}

// RestoreView is one row of the restores table.
type RestoreView struct {
	Name, Source, Backup, Namespace, Claim, Disk string
	Phase, Message                               string
	NewClaim, Active, Orphan                     string
	ArgoApps                                     string
	ArgoPaused                                   bool
	Created                                      time.Time

	CanSwitch, CanDeleteOrphan, CanResume, CanDiscard bool
	SwitchLabel                                       string
}

var (
	dnsName      = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	claimSuffix  = regexp.MustCompile(`-r\d{10}$`)
	restoreNames = map[string]bool{"cloud": true, "disk": true}
)

func allowedNamespaces(cats []Category) map[string]bool {
	m := map[string]bool{}
	for _, c := range cats {
		for _, n := range c.Namespaces {
			m[n] = true
		}
	}
	return m
}

func findDisk(disks []Disk, id string) (Disk, bool) {
	for _, d := range disks {
		if d.ID == id {
			return d, true
		}
	}
	return Disk{}, false
}

// validateRestoreRequest is the single validation point for the shape of a
// restore request, used by the web handler (fail fast) and the dispatcher
// (authoritative). Whether the backup is still active and the claim exists is
// checked by the dispatcher against the cluster.
func validateRestoreRequest(cats []Category, disks []Disk, r RestoreRequest) (Disk, error) {
	if !restoreNames[r.Source] {
		return Disk{}, fmt.Errorf("unknown backup source %q", r.Source)
	}
	if len(r.Backup) > 253 || !dnsName.MatchString(r.Backup) {
		return Disk{}, fmt.Errorf("invalid backup name %q", r.Backup)
	}
	if !allowedNamespaces(cats)[r.Namespace] {
		return Disk{}, fmt.Errorf("namespace %q is not in any backup category", r.Namespace)
	}
	if len(r.Claim) > 253 || !dnsName.MatchString(r.Claim) {
		return Disk{}, fmt.Errorf("invalid volume name %q", r.Claim)
	}
	d, ok := findDisk(disks, r.Disk)
	if !ok {
		return Disk{}, fmt.Errorf("unknown disk %q", r.Disk)
	}
	return d, nil
}

// activeSources lists the backups a restore may start from: finished, and not
// yet expired. PartiallyFailed Velero backups are offered but flagged, since
// they usually still hold the volume the operator wants.
func activeSources(cloud, disk []Run, cats []Category, now time.Time) []RestoreSource {
	allowed := allowedNamespaces(cats)
	var out []RestoreSource
	add := func(r Run, partial bool) {
		out = append(out, RestoreSource{
			Source: r.Source, Name: r.Name, Started: r.Started, Partial: partial,
			Namespaces: coveredNamespaces(r.Namespaces, allowed),
		})
	}
	for _, r := range cloud {
		if r.Phase != "Completed" && r.Phase != "PartiallyFailed" {
			continue
		}
		if !r.Expires.IsZero() && !r.Expires.After(now) {
			continue
		}
		add(r, r.Phase == "PartiallyFailed")
	}
	for _, r := range disk {
		if r.Phase == "Completed" {
			add(r, false)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started.After(out[j].Started) })
	return out
}

// coveredNamespaces narrows a run's namespaces to the allow-list; a run with no
// explicit list (or "*") covered everything.
func coveredNamespaces(ns []string, allowed map[string]bool) []string {
	var out []string
	all := len(ns) == 0
	for _, n := range ns {
		if n == "*" {
			all = true
		}
	}
	if all {
		for n := range allowed {
			out = append(out, n)
		}
	} else {
		for _, n := range ns {
			if allowed[n] {
				out = append(out, n)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (s RestoreSource) covers(ns string) bool {
	for _, n := range s.Namespaces {
		if n == ns {
			return true
		}
	}
	return false
}

func findSource(srcs []RestoreSource, source, name string) (RestoreSource, bool) {
	for _, s := range srcs {
		if s.Source == source && s.Name == name {
			return s, true
		}
	}
	return RestoreSource{}, false
}

// newClaimName derives the replacement PVC's name. A previous restore's suffix
// is stripped first so repeated restores do not grow the name.
func newClaimName(claim string, now time.Time) string {
	base := claimSuffix.ReplaceAllString(claim, "")
	if len(base) > 230 {
		base = base[:230]
	}
	return base + "-r" + now.UTC().Format("0601021504")
}

func newRestoreName(now time.Time) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "rst-" + now.UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
}

type pvcList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			StorageClassName string   `json:"storageClassName"`
			AccessModes      []string `json:"accessModes"`
			VolumeName       string   `json:"volumeName"`
			Resources        struct {
				Requests map[string]string `json:"requests"`
			} `json:"resources"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

func listVolumes(ctx context.Context, k *kubeClient, namespaces []string, fail func(string, error)) []Volume {
	var out []Volume
	for _, ns := range namespaces {
		var l pvcList
		if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/persistentvolumeclaims", &l); err != nil {
			fail("volumes in "+ns, err)
			continue
		}
		for _, p := range l.Items {
			out = append(out, Volume{
				Namespace: ns, Name: p.Metadata.Name, Size: p.Spec.Resources.Requests["storage"],
				StorageClass: p.Spec.StorageClassName, Phase: p.Status.Phase,
				State: p.Metadata.Labels[stateLabel],
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedNamespaces(cats []Category) []string {
	var out []string
	for n := range allowedNamespaces(cats) {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func parseState(ann map[string]string) restoreState {
	var s restoreState
	_ = json.Unmarshal([]byte(ann[annState]), &s)
	return s
}

func restoreView(name string, data, ann map[string]string, created time.Time) RestoreView {
	st := parseState(ann)
	v := RestoreView{
		Name: name, Source: data["source"], Backup: data["backup"], Namespace: data["namespace"],
		Claim: data["claim"], Disk: data["disk"], Phase: ann[annPhase], Message: ann[annMessage],
		NewClaim: st.NewClaim, Active: st.Active, Orphan: st.Orphan,
		ArgoApps: st.argoApps(), ArgoPaused: st.paused(), Created: created,
	}
	if v.Phase == "" {
		v.Phase = "Queued"
	}
	if v.Phase == PhaseActive && st.Orphan != "" {
		v.CanSwitch, v.CanDeleteOrphan = true, true
		if st.Active == st.NewClaim {
			v.SwitchLabel = "Roll back to " + st.Orphan
		} else {
			v.SwitchLabel = "Switch to " + st.NewClaim
		}
	}
	// Resuming is only offered once the orphan is gone: until then the operator
	// may still roll back, and a resumed sync would revert the repoint anyway.
	v.CanResume = (v.Phase == PhaseFinalized) && st.paused()
	v.CanDiscard = v.Phase == PhaseFailed && st.NewClaim != ""
	return v
}

func listRestores(ctx context.Context, k *kubeClient, ns string) ([]RestoreView, error) {
	var l configMapList
	sel := url.QueryEscape(restoreLabel + "=true")
	if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/configmaps?labelSelector="+sel, &l); err != nil {
		return nil, err
	}
	var out []RestoreView
	for _, it := range l.Items {
		out = append(out, restoreView(it.Metadata.Name, it.Data, it.Metadata.Annotations, it.Metadata.CreationTimestamp))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })
	if len(out) > 20 {
		out = out[:20]
	}
	return out, nil
}

// createRestore and createRestoreAction are the web process's only restore
// writes: ConfigMaps, like backup requests.
func createRestore(ctx context.Context, k *kubeClient, ns string, r RestoreRequest) (string, error) {
	name := newRestoreName(time.Now())
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{restoreLabel: "true"}},
		"data": map[string]string{
			"source": r.Source, "backup": r.Backup, "namespace": r.Namespace, "claim": r.Claim, "disk": r.Disk,
		},
	}
	return name, k.do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", "", cm, nil)
}

var restoreActions = map[string]bool{"switch": true, "delete-orphan": true, "resume-sync": true, "discard": true}

func createRestoreAction(ctx context.Context, k *kubeClient, ns, restore, action string) (string, error) {
	name := "act-" + newRestoreName(time.Now())[4:]
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{restoreActionLabel: "true"}},
		"data":     map[string]string{"restore": restore, "action": action},
	}
	return name, k.do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", "", cm, nil)
}
