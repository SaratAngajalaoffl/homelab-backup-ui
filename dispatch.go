package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DispatchConfig is what the dispatcher needs on top of Config.
type DispatchConfig struct {
	Config
	Categories     []Category
	ManualLocation string // Velero BackupStorageLocation for manual backups
	ManualTTL      string // Velero backup TTL, e.g. "720h0m0s"
	RequestMaxAge  time.Duration

	// Restore.
	ArgoNS      string // namespace of the ArgoCD Applications
	HelperImage string // image for the throwaway pod that binds a new volume
	KopiaImage  string // image with the kopia CLI, for restoring Velero fs-backups
	CredSecret  string // Secret (in the Velero namespace) holding the BSL's AWS credential
	RepoSecret  string // Secret holding Velero's Kopia repository password (optional)
}

// runDispatcher polls request ConfigMaps and turns valid ones into a Velero
// Backup and/or a restic Job. It is a separate process with its own
// ServiceAccount so that the internet-facing web pod never holds the rights to
// create Backups (whose hooks can exec in pods) or Jobs (which can mount host
// paths).
func runDispatcher(ctx context.Context, k *kubeClient, dc DispatchConfig) {
	log.Printf("dispatcher started (manual location %s)", dc.ManualLocation)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if err := dispatchAll(ctx, k, dc); err != nil {
			log.Printf("dispatch: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func dispatchOnce(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	var l configMapList
	sel := url.QueryEscape(requestLabel + "=true")
	base := "/api/v1/namespaces/" + dc.StateNS + "/configmaps"
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
		// Mark Processing BEFORE acting. If the process dies mid-way the request
		// is stuck (visible, harmless) rather than retried into duplicate backups.
		if err := annotate(ctx, k, base, name, "Processing", ""); err != nil {
			return err
		}
		phase, msg := process(ctx, k, dc, it.Data["categories"], it.Data["targets"])
		if err := annotate(ctx, k, base, name, phase, msg); err != nil {
			return err
		}
		log.Printf("request %s: %s: %s", name, phase, msg)
	}
	return nil
}

// dispatchAll is one poll: backup requests, then restore actions and progress.
// Each part runs even if an earlier one failed.
func dispatchAll(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	var errs []string
	for _, step := range []func(context.Context, *kubeClient, DispatchConfig) error{dispatchOnce, processRestoreActions, reconcileRestores} {
		if err := step(ctx, k, dc); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func annotate(ctx context.Context, k *kubeClient, base, name, phase, msg string) error {
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{annPhase: phase, annMessage: msg}}}
	return k.do(ctx, http.MethodPatch, base+"/"+name, "application/merge-patch+json", patch, nil)
}

// process validates a request and starts whatever it asks for. It returns the
// final phase ("Started" if anything was created, else "Rejected") and a
// human-readable message that the UI shows verbatim.
func process(ctx context.Context, k *kubeClient, dc DispatchConfig, cats, targets string) (string, string) {
	catIDs, tgts := splitCSV(cats), splitCSV(targets)
	ns, err := resolveRequest(dc.Categories, catIDs, tgts)
	if err != nil {
		return "Rejected", err.Error()
	}
	now := time.Now().UTC()
	stamp := now.Format("20060102-150405")
	var started, problems []string
	for _, t := range tgts {
		switch t {
		case "cloud":
			name, err := startVeleroBackup(ctx, k, dc, stamp, catIDs, ns)
			if err != nil {
				problems = append(problems, "S3: "+err.Error())
			} else {
				started = append(started, "S3 backup "+name)
			}
		case "disk":
			name, err := startDiskJob(ctx, k, dc, stamp, catIDs, ns)
			if err != nil {
				problems = append(problems, "Munin: "+err.Error())
			} else {
				started = append(started, "Munin job "+name)
			}
		}
	}
	msg := strings.Join(append(started, problems...), "; ")
	if len(started) == 0 {
		return "Rejected", msg
	}
	return "Started", msg
}

func startVeleroBackup(ctx context.Context, k *kubeClient, dc DispatchConfig, stamp string, catIDs, ns []string) (string, error) {
	var bl veleroBackupList
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backups", &bl); err != nil {
		return "", err
	}
	for _, b := range bl.Items {
		if b.Metadata.Labels[triggerLabel] == "manual" && (b.Status.Phase == "" || b.Status.Phase == "New" || b.Status.Phase == "InProgress") {
			return "", fmt.Errorf("a manual backup (%s) is already running", b.Metadata.Name)
		}
	}
	name := "manual-" + stamp
	obj := buildBackup(dc, name, catIDs, ns)
	return name, k.do(ctx, http.MethodPost, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backups", "", obj, nil)
}

const triggerLabel = "homelab.smugiwara.com/trigger"

func buildBackup(dc DispatchConfig, name string, catIDs, ns []string) map[string]any {
	return map[string]any{
		"apiVersion": "velero.io/v1", "kind": "Backup",
		"metadata": map[string]any{
			"name": name, "namespace": dc.VeleroNS,
			"labels":      map[string]string{triggerLabel: "manual"},
			"annotations": map[string]string{"homelab.smugiwara.com/categories": strings.Join(catIDs, ",")},
		},
		"spec": map[string]any{
			"storageLocation":          dc.ManualLocation,
			"includedNamespaces":       ns,
			"ttl":                      dc.ManualTTL,
			"defaultVolumesToFsBackup": true,
			// Same policy as the scheduled backup: plaintext Secret objects never go to S3.
			"excludedResources": []string{"secrets"},
		},
	}
}

func startDiskJob(ctx context.Context, k *kubeClient, dc DispatchConfig, stamp string, catIDs, ns []string) (string, error) {
	var jl jobList
	base := "/apis/batch/v1/namespaces/" + dc.LocalNS
	if err := k.get(ctx, base+"/jobs", &jl); err != nil {
		return "", err
	}
	for _, j := range jl.Items {
		for _, o := range j.Metadata.OwnerReferences {
			if o.Kind == "CronJob" && o.Name == dc.LocalCronJob && j.Status.Active > 0 {
				return "", fmt.Errorf("a disk backup (%s) is already running", j.Metadata.Name)
			}
		}
	}
	var cj map[string]any
	if err := k.get(ctx, base+"/cronjobs/"+dc.LocalCronJob, &cj); err != nil {
		return "", err
	}
	name := "manual-" + stamp
	job, err := buildJob(cj, dc.LocalNS, name, catIDs, ns)
	if err != nil {
		return "", err
	}
	return name, k.do(ctx, http.MethodPost, base+"/jobs", "", job, nil)
}

// buildJob instantiates the (suspended) backup-local CronJob's own job
// template, so the pod spec — root, read-only PVC mount, drive mount, image —
// stays defined once, in the chart. Only two env vars are injected.
func buildJob(cj map[string]any, ns, name string, catIDs, namespaces []string) (map[string]any, error) {
	spec, _ := dig(cj, "spec", "jobTemplate", "spec").(map[string]any)
	if spec == nil {
		return nil, fmt.Errorf("cronjob has no jobTemplate.spec")
	}
	containers, _ := dig(spec, "template", "spec", "containers").([]any)
	if len(containers) == 0 {
		return nil, fmt.Errorf("cronjob has no containers")
	}
	c := containers[0].(map[string]any)
	env, _ := c["env"].([]any)
	c["env"] = append(env,
		map[string]any{"name": "BACKUP_NAMESPACES", "value": strings.Join(namespaces, ",")},
		map[string]any{"name": "BACKUP_TAGS", "value": strings.Join(catIDs, ",")},
	)
	uid, _ := dig(cj, "metadata", "uid").(string)
	cjName, _ := dig(cj, "metadata", "name").(string)
	return map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{
			"name": name, "namespace": ns,
			"labels":      map[string]string{triggerLabel: "manual"},
			"annotations": map[string]string{"cronjob.kubernetes.io/instantiate": "manual"},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "batch/v1", "kind": "CronJob", "name": cjName, "uid": uid, "controller": true,
			}},
		},
		"spec": spec,
	}, nil
}

func dig(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}
