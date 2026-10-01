package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	bindTimeout     = 10 * time.Minute
	validateTimeout = 5 * time.Minute
	terminalKeep    = 30 * 24 * time.Hour
)

func cmBase(dc DispatchConfig) string { return "/api/v1/namespaces/" + dc.StateNS + "/configmaps" }

// restoreRec is one restore ConfigMap plus the decoded working state.
type restoreRec struct {
	name    string
	data    map[string]string
	phase   string
	st      restoreState
	since   time.Time
	created time.Time
}

func (r *restoreRec) request() RestoreRequest {
	return RestoreRequest{
		Source: r.data["source"], Backup: r.data["backup"], Namespace: r.data["namespace"],
		Claim: r.data["claim"], Disk: r.data["disk"],
	}
}

// id is the record's unique suffix, reused to name the Jobs it owns.
func (r *restoreRec) id() string { return strings.TrimPrefix(r.name, "rst-") }

func (r *restoreRec) save(ctx context.Context, k *kubeClient, dc DispatchConfig, phase, msg string) error {
	r.phase = phase
	state, _ := json.Marshal(r.st)
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{
		annPhase: phase, annMessage: msg, annState: string(state), annSince: time.Now().UTC().Format(time.RFC3339),
	}}}
	return k.do(ctx, http.MethodPatch, cmBase(dc)+"/"+r.name, "application/merge-patch+json", patch, nil)
}

func loadRestoreRecs(ctx context.Context, k *kubeClient, dc DispatchConfig) ([]*restoreRec, error) {
	var l configMapList
	sel := url.QueryEscape(restoreLabel + "=true")
	if err := k.get(ctx, cmBase(dc)+"?labelSelector="+sel, &l); err != nil {
		return nil, err
	}
	var out []*restoreRec
	for _, it := range l.Items {
		r := &restoreRec{
			name: it.Metadata.Name, data: it.Data, phase: it.Metadata.Annotations[annPhase],
			st: parseState(it.Metadata.Annotations), created: it.Metadata.CreationTimestamp,
		}
		r.since, _ = time.Parse(time.RFC3339, it.Metadata.Annotations[annSince])
		out = append(out, r)
	}
	return out, nil
}

// reconcileRestores advances every in-flight restore by one step. Each step is
// non-blocking: it creates or inspects objects and records the outcome, so a
// restore that takes hours costs the dispatcher nothing between polls.
func reconcileRestores(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	if len(dc.Disks) == 0 {
		return nil
	}
	recs, err := loadRestoreRecs(ctx, k, dc)
	if err != nil {
		return err
	}
	for _, r := range recs {
		switch r.phase {
		case "":
			// Mark Validating BEFORE acting, as for backups: a crash leaves a
			// visible stuck record rather than a retry that makes a second PVC.
			if err := r.save(ctx, k, dc, PhaseValidating, ""); err != nil {
				return err
			}
			startRestore(ctx, k, dc, r, recs)
		case PhaseValidating:
			if time.Since(r.since) > validateTimeout {
				failRestore(ctx, k, dc, r, "the dispatcher was interrupted while starting this restore; check for a leftover volume and discard it")
			}
		case PhaseBinding:
			advanceBinding(ctx, k, dc, r)
		case PhaseRestoring:
			advanceRestoring(ctx, k, dc, r)
		case PhaseFinalized, PhaseFailed, PhaseRejected, PhaseDiscarded:
			if time.Since(r.created) > terminalKeep && !r.st.paused() {
				_ = k.do(ctx, http.MethodDelete, cmBase(dc)+"/"+r.name, "", nil, nil)
			}
		}
	}
	return nil
}

func rejectRestore(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec, msg string) {
	log.Printf("restore %s rejected: %s", r.name, msg)
	if err := r.save(ctx, k, dc, PhaseRejected, msg); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
}

func failRestore(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec, msg string) {
	log.Printf("restore %s failed: %s", r.name, msg)
	if r.st.BinderJob != "" {
		deleteJob(ctx, k, r.data["namespace"], r.st.BinderJob)
	}
	if err := r.save(ctx, k, dc, PhaseFailed, msg); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
}

func deleteJob(ctx context.Context, k *kubeClient, ns, name string) {
	_ = k.do(ctx, http.MethodDelete, "/apis/batch/v1/namespaces/"+ns+"/jobs/"+name+"?propagationPolicy=Background", "", nil, nil)
}

// fetchRuns returns the same runs the dashboard shows, for re-checking that a
// requested backup is real and still active.
func fetchRuns(ctx context.Context, k *kubeClient, dc DispatchConfig) (cloud, disk []Run, err error) {
	var bl veleroBackupList
	if err = k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backups", &bl); err != nil {
		return
	}
	var jl jobList
	if err = k.get(ctx, "/apis/batch/v1/namespaces/"+dc.LocalNS+"/jobs", &jl); err != nil {
		return
	}
	return veleroRuns(bl), jobRuns(jl, "disk", dc.LocalCronJob), nil
}

func startRestore(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec, all []*restoreRec) {
	req := r.request()
	disk, err := validateRestoreRequest(dc.Categories, dc.Disks, req)
	if err != nil {
		rejectRestore(ctx, k, dc, r, err.Error())
		return
	}
	// Another live restore of the same volume (under any of its names) would
	// fight over the same workloads.
	for _, o := range all {
		if o.name == r.name || o.data["namespace"] != req.Namespace || isTerminalPhase(o.phase) {
			continue
		}
		for _, c := range []string{o.data["claim"], o.st.NewClaim, o.st.Active, o.st.Orphan} {
			if c != "" && c == req.Claim {
				rejectRestore(ctx, k, dc, r, fmt.Sprintf("restore %s is still open on this volume; finalize or roll it back first", o.name))
				return
			}
		}
	}
	cloud, diskRuns, err := fetchRuns(ctx, k, dc)
	if err != nil {
		rejectRestore(ctx, k, dc, r, "could not list backups: "+err.Error())
		return
	}
	src, ok := findSource(activeSources(cloud, diskRuns, dc.Categories, time.Now()), req.Source, req.Backup)
	if !ok {
		rejectRestore(ctx, k, dc, r, fmt.Sprintf("%s backup %q is not an active backup", req.Source, req.Backup))
		return
	}
	if !src.covers(req.Namespace) {
		rejectRestore(ctx, k, dc, r, fmt.Sprintf("backup %s does not cover namespace %s", req.Backup, req.Namespace))
		return
	}
	pvcBase := "/api/v1/namespaces/" + req.Namespace + "/persistentvolumeclaims"
	var old struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			StorageClassName string   `json:"storageClassName"`
			AccessModes      []string `json:"accessModes"`
			Resources        struct {
				Requests map[string]string `json:"requests"`
			} `json:"resources"`
		} `json:"spec"`
	}
	if err := k.get(ctx, pvcBase+"/"+req.Claim, &old); err != nil {
		rejectRestore(ctx, k, dc, r, "volume not found: "+err.Error())
		return
	}
	if old.Metadata.Labels[stateLabel] == "orphaned" {
		rejectRestore(ctx, k, dc, r, "that volume is an orphan of an earlier restore")
		return
	}
	var sc struct{}
	if err := k.get(ctx, "/apis/storage.k8s.io/v1/storageclasses/"+disk.StorageClass, &sc); err != nil {
		rejectRestore(ctx, k, dc, r, fmt.Sprintf("storage class %q for disk %q is not available: %v", disk.StorageClass, disk.ID, err))
		return
	}
	refs, volName, err := findWorkloads(ctx, k, req.Namespace, req.Claim)
	if err != nil {
		rejectRestore(ctx, k, dc, r, err.Error())
		return
	}
	var snapshot string
	if req.Source == "cloud" {
		snapshot, err = findSnapshot(ctx, k, dc, req.Backup, req.Namespace, old.Metadata.UID, volName)
		if err != nil {
			rejectRestore(ctx, k, dc, r, err.Error())
			return
		}
	}

	now := time.Now()
	newClaim := newClaimName(req.Claim, now)
	r.st = restoreState{
		NewClaim: newClaim, Active: req.Claim, Workloads: refs, Volume: volName, PVCUID: old.Metadata.UID,
		BinderJob: "bind-" + r.id(), Snapshot: snapshot,
	}
	pvc := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		// No labels from the old PVC: its helm/ArgoCD labels would make ArgoCD
		// treat the replacement as one of its own and prune it.
		"metadata": map[string]any{
			"name": newClaim, "namespace": req.Namespace,
			"labels":      map[string]string{stateLabel: "restoring"},
			"annotations": map[string]string{annRecord: r.name},
		},
		"spec": map[string]any{
			"accessModes":      old.Spec.AccessModes,
			"storageClassName": disk.StorageClass,
			"resources":        map[string]any{"requests": old.Spec.Resources.Requests},
		},
	}
	if err := k.do(ctx, http.MethodPost, pvcBase, "", pvc, nil); err != nil {
		rejectRestore(ctx, k, dc, r, "could not create the new volume: "+err.Error())
		return
	}
	// Local-path volumes bind on first use, so a throwaway pod claims it. That
	// provisions the directory we then restore into.
	if err := k.do(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+req.Namespace+"/jobs", "", buildBinderJob(dc, r.st.BinderJob, req.Namespace, newClaim), nil); err != nil {
		_ = k.do(ctx, http.MethodDelete, pvcBase+"/"+newClaim, "", nil, nil)
		rejectRestore(ctx, k, dc, r, "could not start the volume binder: "+err.Error())
		return
	}
	msg := fmt.Sprintf("created %s on %s; waiting for it to bind", newClaim, disk.Name)
	if err := r.save(ctx, k, dc, PhaseBinding, msg); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
	log.Printf("restore %s: %s", r.name, msg)
}

func isTerminalPhase(p string) bool {
	switch p {
	case PhaseFinalized, PhaseFailed, PhaseRejected, PhaseDiscarded:
		return true
	}
	return false
}

func buildBinderJob(dc DispatchConfig, name, ns, claim string) map[string]any {
	return map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{restoreLabel: "binder"}},
		"spec": map[string]any{
			"backoffLimit": 0, "ttlSecondsAfterFinished": 3600,
			"template": map[string]any{"spec": map[string]any{
				"restartPolicy": "Never",
				"securityContext": map[string]any{
					"runAsNonRoot": true, "runAsUser": 65532, "seccompProfile": map[string]any{"type": "RuntimeDefault"},
				},
				"containers": []any{map[string]any{
					"name": "bind", "image": dc.HelperImage, "command": []string{"true"},
					"securityContext": map[string]any{"allowPrivilegeEscalation": false, "capabilities": map[string]any{"drop": []string{"ALL"}}},
					"volumeMounts":    []any{map[string]any{"name": "vol", "mountPath": "/vol"}},
				}},
				"volumes": []any{map[string]any{"name": "vol", "persistentVolumeClaim": map[string]any{"claimName": claim}}},
			}},
		},
	}
}

type jobStatus struct {
	Status struct {
		Succeeded  int `json:"succeeded"`
		Failed     int `json:"failed"`
		Active     int `json:"active"`
		Conditions []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"conditions"`
	} `json:"status"`
}

func (j jobStatus) done() (ok, failed bool, msg string) {
	for _, c := range j.Status.Conditions {
		if c.Status != "True" {
			continue
		}
		switch c.Type {
		case "Complete":
			return true, false, ""
		case "Failed":
			return false, true, c.Message
		}
	}
	return false, false, ""
}

func advanceBinding(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec) {
	req := r.request()
	disk, _ := findDisk(dc.Disks, req.Disk)
	if time.Since(r.since) > bindTimeout {
		failRestore(ctx, k, dc, r, fmt.Sprintf("the new volume did not bind within %s; check that disk %q is available", bindTimeout, disk.Name))
		return
	}
	var js jobStatus
	if err := k.get(ctx, "/apis/batch/v1/namespaces/"+req.Namespace+"/jobs/"+r.st.BinderJob, &js); err == nil {
		if _, failed, msg := js.done(); failed {
			failRestore(ctx, k, dc, r, "the volume binder pod failed: "+msg)
			return
		}
	}
	var pvc struct {
		Spec struct {
			VolumeName string `json:"volumeName"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	}
	if err := k.get(ctx, "/api/v1/namespaces/"+req.Namespace+"/persistentvolumeclaims/"+r.st.NewClaim, &pvc); err != nil {
		failRestore(ctx, k, dc, r, "new volume disappeared: "+err.Error())
		return
	}
	if pvc.Status.Phase != "Bound" {
		return
	}
	var pv struct {
		Spec struct {
			HostPath struct {
				Path string `json:"path"`
			} `json:"hostPath"`
			Local struct {
				Path string `json:"path"`
			} `json:"local"`
		} `json:"spec"`
	}
	if err := k.get(ctx, "/api/v1/persistentvolumes/"+pvc.Spec.VolumeName, &pv); err != nil {
		failRestore(ctx, k, dc, r, "could not read the new volume's PV: "+err.Error())
		return
	}
	dest := pv.Spec.HostPath.Path
	if dest == "" {
		dest = pv.Spec.Local.Path
	}
	// The restore Job mounts this path read-write as root, so never trust it
	// blindly: it must sit under the root configured for the chosen disk.
	if !pathUnder(dest, disk.HostRoot) {
		failRestore(ctx, k, dc, r, fmt.Sprintf("new volume's path %q is not under disk %q's root %q; refusing to mount it", dest, disk.Name, disk.HostRoot))
		return
	}
	r.st.Dest = dest
	r.st.RestoreJob = "restore-" + r.id()
	jobNS, job, err := buildRestoreJob(ctx, k, dc, r)
	if err == nil {
		err = k.do(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+jobNS+"/jobs", "", job, nil)
	}
	if err != nil {
		failRestore(ctx, k, dc, r, "could not start the restore job: "+err.Error())
		return
	}
	deleteJob(ctx, k, req.Namespace, r.st.BinderJob)
	msg := fmt.Sprintf("restoring into %s (job %s/%s)", r.st.NewClaim, jobNS, r.st.RestoreJob)
	if err := r.save(ctx, k, dc, PhaseRestoring, msg); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
	log.Printf("restore %s: %s", r.name, msg)
}

func pathUnder(p, root string) bool {
	if p == "" || root == "" || !path.IsAbs(p) {
		return false
	}
	p, root = path.Clean(p), path.Clean(root)
	return p != root && strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/")
}

func restoreJobNS(dc DispatchConfig, source string) string {
	if source == "cloud" {
		return dc.VeleroNS
	}
	return dc.LocalNS
}

func advanceRestoring(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec) {
	ns := restoreJobNS(dc, r.data["source"])
	var js jobStatus
	if err := k.get(ctx, "/apis/batch/v1/namespaces/"+ns+"/jobs/"+r.st.RestoreJob, &js); err != nil {
		failRestore(ctx, k, dc, r, "restore job disappeared: "+err.Error())
		return
	}
	ok, failed, msg := js.done()
	switch {
	case failed:
		failRestore(ctx, k, dc, r, fmt.Sprintf("restore job failed (%s); logs: kubectl -n %s logs job/%s", msg, ns, r.st.RestoreJob))
	case ok:
		cutover(ctx, k, dc, r)
	}
}

// cutover points the apps at the restored volume and orphans the old one.
// Anything that goes wrong part-way is undone, so a failed cutover leaves the
// apps exactly where they were.
func cutover(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec) {
	req := r.request()
	paused, err := pauseArgo(ctx, k, dc, req.Namespace)
	r.st.ArgoPaused = paused
	if err != nil {
		resumeArgo(ctx, k, dc, r.st.ArgoPaused)
		r.st.ArgoPaused = nil
		failRestore(ctx, k, dc, r, "could not pause ArgoCD auto-sync: "+err.Error())
		return
	}
	// Persist the pause before touching workloads, so it is never lost track of.
	if err := r.save(ctx, k, dc, PhaseRestoring, "cutting over"); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
	if err := repoint(ctx, k, req.Namespace, r.st.Workloads, req.Claim, r.st.NewClaim); err != nil {
		resumeArgo(ctx, k, dc, r.st.ArgoPaused)
		r.st.ArgoPaused = nil
		failRestore(ctx, k, dc, r, "could not repoint the apps: "+err.Error())
		return
	}
	markPVC(ctx, k, req.Namespace, r.st.NewClaim, "active", r.name)
	markPVC(ctx, k, req.Namespace, req.Claim, "orphaned", r.name)
	r.st.Active, r.st.Orphan = r.st.NewClaim, req.Claim
	msg := fmt.Sprintf("apps now use %s; old volume %s is orphaned. Verify, then delete the old volume or roll back", r.st.NewClaim, req.Claim)
	if err := r.save(ctx, k, dc, PhaseActive, msg); err != nil {
		log.Printf("restore %s: %v", r.name, err)
	}
	log.Printf("restore %s: %s", r.name, msg)
}

func markPVC(ctx context.Context, k *kubeClient, ns, claim, state, record string) {
	ann := map[string]any{annRecord: record, annOrphaned: nil}
	if state == "orphaned" {
		ann[annOrphaned] = time.Now().UTC().Format(time.RFC3339)
	}
	patch := map[string]any{"metadata": map[string]any{"labels": map[string]string{stateLabel: state}, "annotations": ann}}
	if err := k.do(ctx, http.MethodPatch, "/api/v1/namespaces/"+ns+"/persistentvolumeclaims/"+claim, "application/merge-patch+json", patch, nil); err != nil {
		log.Printf("label %s/%s as %s: %v", ns, claim, state, err)
	}
}

// ---- workloads --------------------------------------------------------------

type podVolume struct {
	Name                  string `json:"name"`
	PersistentVolumeClaim *struct {
		ClaimName string `json:"claimName"`
	} `json:"persistentVolumeClaim"`
}

type workloadList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			Template struct {
				Spec struct {
					Volumes []podVolume `json:"volumes"`
				} `json:"spec"`
			} `json:"template"`
			VolumeClaimTemplates []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"volumeClaimTemplates"`
		} `json:"spec"`
	} `json:"items"`
}

func workloadURL(ns string, ref workloadRef) string {
	return "/apis/apps/v1/namespaces/" + ns + "/" + strings.ToLower(ref.Kind) + "s/" + ref.Name
}

// findWorkloads returns the Deployments/StatefulSets whose pod template mounts
// the claim, plus the pod-volume name it is mounted as (Velero identifies a
// volume's data by that name). It refuses cases it cannot safely repoint.
func findWorkloads(ctx context.Context, k *kubeClient, ns, claim string) ([]workloadRef, string, error) {
	var refs []workloadRef
	volName := ""
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		var l workloadList
		if err := k.get(ctx, "/apis/apps/v1/namespaces/"+ns+"/"+strings.ToLower(kind)+"s", &l); err != nil {
			return nil, "", fmt.Errorf("could not list %ss: %w", strings.ToLower(kind), err)
		}
		for _, it := range l.Items {
			for _, t := range it.Spec.VolumeClaimTemplates {
				if regexp.MustCompile(`^` + regexp.QuoteMeta(t.Metadata.Name+"-"+it.Metadata.Name) + `-\d+$`).MatchString(claim) {
					return nil, "", fmt.Errorf("%s belongs to StatefulSet %s, whose volume claim templates cannot be repointed; restore its database from the logical dump instead", claim, it.Metadata.Name)
				}
			}
			for _, v := range it.Spec.Template.Spec.Volumes {
				if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim {
					refs = append(refs, workloadRef{Kind: kind, Name: it.Metadata.Name})
					volName = v.Name
				}
			}
		}
	}
	// Anything else mounting the claim (a bare pod, a Job) cannot be repointed.
	var pods struct {
		Items []struct {
			Metadata meta `json:"metadata"`
			Spec     struct {
				Volumes []podVolume `json:"volumes"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/pods", &pods); err != nil {
		return nil, "", fmt.Errorf("could not list pods: %w", err)
	}
	for _, p := range pods.Items {
		uses := false
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim {
				uses = true
			}
		}
		if !uses {
			continue
		}
		owned := false
		for _, o := range p.Metadata.OwnerReferences {
			if o.Kind == "ReplicaSet" || o.Kind == "StatefulSet" {
				owned = true
			}
		}
		if !owned {
			return nil, "", fmt.Errorf("%s is mounted by pod %s, which is not managed by a Deployment or StatefulSet and cannot be repointed", claim, p.Metadata.Name)
		}
	}
	return refs, volName, nil
}

// repoint swaps claimName from -> to in each workload's pod template. Each
// patch is guarded by a JSON-patch "test", and on any failure the workloads
// already changed are put back.
func repoint(ctx context.Context, k *kubeClient, ns string, refs []workloadRef, from, to string) error {
	var done []workloadRef
	for _, ref := range refs {
		if err := patchClaim(ctx, k, ns, ref, from, to); err != nil {
			for _, d := range done {
				if rerr := patchClaim(ctx, k, ns, d, to, from); rerr != nil {
					log.Printf("revert %s/%s: %v", ns, d.Name, rerr)
				}
			}
			return fmt.Errorf("%s %s: %w", ref.Kind, ref.Name, err)
		}
		done = append(done, ref)
	}
	return nil
}

func patchClaim(ctx context.Context, k *kubeClient, ns string, ref workloadRef, from, to string) error {
	obj := struct {
		Spec struct {
			Template struct {
				Spec struct {
					Volumes []podVolume `json:"volumes"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}{}
	if err := k.get(ctx, workloadURL(ns, ref), &obj); err != nil {
		return err
	}
	var ops []map[string]any
	for i, v := range obj.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != from {
			continue
		}
		p := fmt.Sprintf("/spec/template/spec/volumes/%d/persistentVolumeClaim/claimName", i)
		ops = append(ops,
			map[string]any{"op": "test", "path": p, "value": from},
			map[string]any{"op": "replace", "path": p, "value": to})
	}
	if len(ops) == 0 {
		return fmt.Errorf("no volume mounts %s", from)
	}
	return k.do(ctx, http.MethodPatch, workloadURL(ns, ref), "application/json-patch+json", ops, nil)
}

// ---- ArgoCD -----------------------------------------------------------------

type argoAppList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			Destination struct {
				Namespace string `json:"namespace"`
			} `json:"destination"`
			SyncPolicy struct {
				Automated json.RawMessage `json:"automated"`
			} `json:"syncPolicy"`
		} `json:"spec"`
	} `json:"items"`
}

func argoAppURL(dc DispatchConfig, name string) string {
	return "/apis/argoproj.io/v1alpha1/namespaces/" + dc.ArgoNS + "/applications/" + name
}

// pauseArgo turns off auto-sync on every Application deploying into ns and
// returns what each one had, keyed by name. selfHeal would otherwise revert the
// repoint within seconds. Applications without auto-sync are left alone.
func pauseArgo(ctx context.Context, k *kubeClient, dc DispatchConfig, ns string) (map[string]json.RawMessage, error) {
	var l argoAppList
	if err := k.get(ctx, "/apis/argoproj.io/v1alpha1/namespaces/"+dc.ArgoNS+"/applications", &l); err != nil {
		if isNotFound(err) {
			return nil, nil // ArgoCD is not installed: nothing to pause
		}
		return nil, err
	}
	paused := map[string]json.RawMessage{}
	for _, a := range l.Items {
		auto := a.Spec.SyncPolicy.Automated
		if a.Spec.Destination.Namespace != ns || len(auto) == 0 || string(auto) == "null" {
			continue
		}
		patch := map[string]any{"spec": map[string]any{"syncPolicy": map[string]any{"automated": nil}}}
		if err := k.do(ctx, http.MethodPatch, argoAppURL(dc, a.Metadata.Name), "application/merge-patch+json", patch, nil); err != nil {
			return paused, fmt.Errorf("%s: %w", a.Metadata.Name, err)
		}
		paused[a.Metadata.Name] = auto
	}
	return paused, nil
}

func resumeArgo(ctx context.Context, k *kubeClient, dc DispatchConfig, paused map[string]json.RawMessage) {
	for app, prev := range paused {
		patch := map[string]any{"spec": map[string]any{"syncPolicy": map[string]any{"automated": prev}}}
		if err := k.do(ctx, http.MethodPatch, argoAppURL(dc, app), "application/merge-patch+json", patch, nil); err != nil && !isNotFound(err) {
			log.Printf("resume ArgoCD app %s: %v", app, err)
		}
	}
}

// ---- operator actions -------------------------------------------------------

// processRestoreActions runs operator actions (switch / delete-orphan /
// resume-sync / discard) queued as ConfigMaps by the web pod.
func processRestoreActions(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	if len(dc.Disks) == 0 {
		return nil
	}
	var l configMapList
	sel := url.QueryEscape(restoreActionLabel + "=true")
	base := cmBase(dc)
	if err := k.get(ctx, base+"?labelSelector="+sel, &l); err != nil {
		return err
	}
	for _, it := range l.Items {
		name := it.Metadata.Name
		if time.Since(it.Metadata.CreationTimestamp) > dc.RequestMaxAge {
			_ = k.do(ctx, http.MethodDelete, base+"/"+name, "", nil, nil)
			continue
		}
		if it.Metadata.Annotations[annPhase] != "" {
			continue
		}
		if err := annotate(ctx, k, base, name, "Processing", ""); err != nil {
			return err
		}
		phase, msg := runRestoreAction(ctx, k, dc, it.Data["restore"], it.Data["action"])
		if err := annotate(ctx, k, base, name, phase, msg); err != nil {
			return err
		}
		log.Printf("restore action %s %s: %s: %s", it.Data["action"], it.Data["restore"], phase, msg)
	}
	return nil
}

func runRestoreAction(ctx context.Context, k *kubeClient, dc DispatchConfig, restore, action string) (string, string) {
	if !restoreActions[action] {
		return "Rejected", fmt.Sprintf("unknown action %q", action)
	}
	recs, err := loadRestoreRecs(ctx, k, dc)
	if err != nil {
		return "Rejected", err.Error()
	}
	var r *restoreRec
	for _, c := range recs {
		if c.name == restore {
			r = c
		}
	}
	if r == nil {
		return "Rejected", "no such restore " + restore
	}
	ns := r.data["namespace"]
	pvcURL := func(c string) string { return "/api/v1/namespaces/" + ns + "/persistentvolumeclaims/" + c }
	reject := func(format string, a ...any) (string, string) { return "Rejected", fmt.Sprintf(format, a...) }
	done := func(phase, msg string) (string, string) {
		if err := r.save(ctx, k, dc, phase, msg); err != nil {
			return "Rejected", err.Error()
		}
		return "Done", msg
	}

	switch action {
	case "switch":
		if r.phase != PhaseActive || r.st.Orphan == "" {
			return reject("restore %s has no orphaned volume to switch to", r.name)
		}
		var target struct {
			Metadata meta `json:"metadata"`
		}
		if err := k.get(ctx, pvcURL(r.st.Orphan), &target); err != nil {
			return reject("volume %s is gone: %v", r.st.Orphan, err)
		}
		from, to := r.st.Active, r.st.Orphan
		if err := repoint(ctx, k, ns, r.st.Workloads, from, to); err != nil {
			return reject("could not repoint the apps: %v", err)
		}
		markPVC(ctx, k, ns, to, "active", r.name)
		markPVC(ctx, k, ns, from, "orphaned", r.name)
		r.st.Active, r.st.Orphan = to, from
		return done(PhaseActive, fmt.Sprintf("apps now use %s; %s is orphaned", to, from))

	case "delete-orphan":
		if r.phase != PhaseActive || r.st.Orphan == "" {
			return reject("restore %s has no orphaned volume to delete", r.name)
		}
		var o struct {
			Metadata meta `json:"metadata"`
		}
		if err := k.get(ctx, pvcURL(r.st.Orphan), &o); err != nil && !isNotFound(err) {
			return reject("could not read %s: %v", r.st.Orphan, err)
		} else if err == nil {
			// Never delete a volume this restore did not orphan.
			if o.Metadata.Labels[stateLabel] != "orphaned" || o.Metadata.Annotations[annRecord] != r.name {
				return reject("%s is not an orphan of restore %s; refusing to delete it", r.st.Orphan, r.name)
			}
			if err := k.do(ctx, http.MethodDelete, pvcURL(r.st.Orphan), "", nil, nil); err != nil {
				return reject("could not delete %s: %v", r.st.Orphan, err)
			}
		}
		deleted := r.st.Orphan
		r.st.Orphan = ""
		msg := fmt.Sprintf("deleted %s", deleted)
		// When the apps are back on the original volume, git already describes
		// reality and sync can safely resume. Otherwise the chart must first be
		// updated to the new claim, so resuming is left to the operator.
		switch {
		case !r.st.paused():
		case r.st.Active == r.data["claim"]:
			resumeArgo(ctx, k, dc, r.st.ArgoPaused)
			r.st.ArgoPaused = nil
			msg += "; ArgoCD auto-sync resumed"
		default:
			msg += fmt.Sprintf("; ArgoCD auto-sync is still paused for %s. Set the chart to use existing volume %s, push, then resume sync", r.st.argoApps(), r.st.Active)
		}
		return done(PhaseFinalized, msg)

	case "resume-sync":
		if r.phase != PhaseFinalized || !r.st.paused() {
			return reject("restore %s has no paused ArgoCD auto-sync to resume", r.name)
		}
		resumeArgo(ctx, k, dc, r.st.ArgoPaused)
		apps := r.st.argoApps()
		r.st.ArgoPaused = nil
		return done(PhaseFinalized, fmt.Sprintf("ArgoCD auto-sync resumed for %s", apps))

	case "discard":
		if r.phase != PhaseFailed || r.st.NewClaim == "" {
			return reject("restore %s has nothing to discard", r.name)
		}
		var n struct {
			Metadata meta `json:"metadata"`
		}
		if err := k.get(ctx, pvcURL(r.st.NewClaim), &n); err == nil && n.Metadata.Annotations[annRecord] != r.name {
			return reject("%s was not created by restore %s; refusing to delete it", r.st.NewClaim, r.name)
		}
		if err := k.do(ctx, http.MethodDelete, pvcURL(r.st.NewClaim), "", nil, nil); err != nil && !isNotFound(err) {
			return reject("could not delete %s: %v", r.st.NewClaim, err)
		}
		return done(PhaseDiscarded, fmt.Sprintf("deleted the unfinished volume %s", r.st.NewClaim))
	}
	return reject("unhandled action %q", action)
}
