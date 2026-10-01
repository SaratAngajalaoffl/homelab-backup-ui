package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCluster is a tiny in-memory Kubernetes API: enough GET/list/POST/PATCH
// (merge and JSON patch)/DELETE to drive a whole restore through the real
// dispatcher code. Controllers (binding, Jobs completing) are simulated by the
// test mutating objects between dispatcher steps.
type fakeCluster struct {
	mu   sync.Mutex
	objs map[string]map[string]any
	seq  int
}

var collections = map[string]bool{
	"backups": true, "jobs": true, "configmaps": true, "persistentvolumeclaims": true, "deployments": true,
	"statefulsets": true, "pods": true, "applications": true, "podvolumebackups": true, "cronjobs": true,
}

func newFakeCluster(t *testing.T) (*fakeCluster, *kubeClient) {
	c := &fakeCluster{objs: map[string]map[string]any{}}
	srv := httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(srv.Close)
	return c, &kubeClient{base: srv.URL, http: srv.Client()}
}

func (c *fakeCluster) put(path string, obj map[string]any) {
	// Round-trip through JSON so seeded objects have the same shapes the API serves.
	var norm map[string]any
	b, _ := json.Marshal(obj)
	json.Unmarshal(b, &norm)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objs[path] = norm
}

func (c *fakeCluster) get(path string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.objs[path]
}

func (c *fakeCluster) children(collection string) []string {
	var out []string
	for p := range c.objs {
		if strings.HasPrefix(p, collection+"/") && !strings.Contains(strings.TrimPrefix(p, collection+"/"), "/") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func dig2(m map[string]any, keys ...string) any {
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

func mergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		pm, isMap := v.(map[string]any)
		dm, dstIsMap := dst[k].(map[string]any)
		if isMap && dstIsMap {
			mergePatch(dm, pm)
		} else {
			dst[k] = v
		}
	}
}

func jsonPatch(obj map[string]any, ops []map[string]any) error {
	for _, op := range ops {
		parts := strings.Split(strings.TrimPrefix(op["path"].(string), "/"), "/")
		var cur any = obj
		for _, p := range parts[:len(parts)-1] {
			switch node := cur.(type) {
			case map[string]any:
				cur = node[p]
			case []any:
				i, _ := strconv.Atoi(p)
				cur = node[i]
			}
		}
		last := parts[len(parts)-1]
		m := cur.(map[string]any)
		switch op["op"] {
		case "test":
			if m[last] != op["value"] {
				return fmt.Errorf("test failed at %s: %v != %v", op["path"], m[last], op["value"])
			}
		case "replace":
			m[last] = op["value"]
		}
	}
	return nil
}

func (c *fakeCluster) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path := r.URL.Path
	body, _ := io.ReadAll(r.Body)
	reply := func(v any) { json.NewEncoder(w).Encode(v) }
	switch r.Method {
	case http.MethodGet:
		if collections[path[strings.LastIndex(path, "/")+1:]] {
			sel := r.URL.Query().Get("labelSelector")
			items := []any{}
			for _, p := range c.children(path) {
				if sel != "" {
					k, v, _ := strings.Cut(sel, "=")
					if dig2(c.objs[p], "metadata", "labels", k) != v {
						continue
					}
				}
				items = append(items, c.objs[p])
			}
			reply(map[string]any{"items": items})
			return
		}
		if o, ok := c.objs[path]; ok {
			reply(o)
			return
		}
		http.NotFound(w, r)
	case http.MethodPost:
		var o map[string]any
		json.Unmarshal(body, &o)
		name := dig2(o, "metadata", "name").(string)
		c.seq++
		md := o["metadata"].(map[string]any)
		md["uid"] = fmt.Sprintf("uid-%d", c.seq)
		md["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
		c.objs[path+"/"+name] = o
		reply(o)
	case http.MethodPatch:
		o, ok := c.objs[path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Content-Type") == "application/json-patch+json" {
			var ops []map[string]any
			json.Unmarshal(body, &ops)
			if err := jsonPatch(o, ops); err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
		} else {
			var p map[string]any
			json.Unmarshal(body, &p)
			mergePatch(o, p)
		}
		reply(o)
	case http.MethodDelete:
		if _, ok := c.objs[path]; !ok {
			http.NotFound(w, r)
			return
		}
		delete(c.objs, path)
		reply(map[string]any{})
	}
}

const (
	nsApp   = "homelab-atuin"
	pvcBase = "/api/v1/namespaces/" + nsApp + "/persistentvolumeclaims/"
	depPath = "/apis/apps/v1/namespaces/" + nsApp + "/deployments/atuin-server"
	argoApp = "/apis/argoproj.io/v1alpha1/namespaces/argocd/applications/atuin"
	stateNS = "/api/v1/namespaces/homelab-backup-ui/configmaps"
)

func seedCluster(c *fakeCluster) {
	now := time.Now().UTC().Format(time.RFC3339)
	c.put(pvcBase+"atuin-server-config", map[string]any{
		"metadata": map[string]any{"name": "atuin-server-config", "uid": "pvc-uid-old", "labels": map[string]any{"app.kubernetes.io/instance": "atuin"}},
		"spec": map[string]any{"storageClassName": "local-path", "accessModes": []any{"ReadWriteOnce"},
			"resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}},
		"status": map[string]any{"phase": "Bound"},
	})
	c.put(depPath, deploy("atuin-server", vol("config", "atuin-server-config")))
	c.put("/apis/storage.k8s.io/v1/storageclasses/local-data", map[string]any{"metadata": map[string]any{"name": "local-data"}})
	c.put(argoApp, map[string]any{
		"metadata": map[string]any{"name": "atuin"},
		"spec": map[string]any{"destination": map[string]any{"namespace": nsApp},
			"syncPolicy": map[string]any{"automated": map[string]any{"prune": true, "selfHeal": true}}},
	})
	// A finished disk backup taken by the dispatcher, covering the namespace.
	c.put("/apis/batch/v1/namespaces/homelab-backup-local/jobs/manual-1", map[string]any{
		"metadata": map[string]any{"name": "manual-1", "creationTimestamp": now,
			"ownerReferences": []any{map[string]any{"kind": "CronJob", "name": "backup-local"}},
			"annotations":     map[string]any{"cronjob.kubernetes.io/instantiate": "manual"}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"env": []any{map[string]any{"name": "BACKUP_NAMESPACES", "value": nsApp}}}}}}},
		"status": map[string]any{"startTime": now, "completionTime": now,
			"conditions": []any{map[string]any{"type": "Complete", "status": "True"}}},
	})
	c.put("/apis/batch/v1/namespaces/homelab-backup-local/cronjobs/backup-local", map[string]any{
		"metadata": map[string]any{"name": "backup-local"},
		"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "restic", "env": []any{}, "volumeMounts": []any{}}}, "volumes": []any{}}}}}},
	})
}

func testDC() DispatchConfig {
	return DispatchConfig{
		Config: Config{VeleroNS: "velero", LocalNS: "homelab-backup-local", LocalCronJob: "backup-local",
			StateNS: "homelab-backup-ui", Disks: testDisks},
		Categories:    []Category{{ID: "apps", Namespaces: []string{nsApp}}},
		RequestMaxAge: time.Hour, ArgoNS: "argocd", HelperImage: "busybox",
	}
}

func claimOf(c *fakeCluster) string {
	return dig2(c.get(depPath), "spec", "template", "spec", "volumes").([]any)[0].(map[string]any)["persistentVolumeClaim"].(map[string]any)["claimName"].(string)
}

func ann(c *fakeCluster, path, key string) string {
	s, _ := dig2(c.get(path), "metadata", "annotations", key).(string)
	return s
}

func newClaimIn(t *testing.T, c *fakeCluster) string {
	for _, p := range func() []string {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.children("/api/v1/namespaces/" + nsApp + "/persistentvolumeclaims")
	}() {
		if n := strings.TrimPrefix(p, pvcBase); n != "atuin-server-config" {
			return n
		}
	}
	t.Fatal("no new PVC was created")
	return ""
}

func runAction(t *testing.T, ctx context.Context, k *kubeClient, dc DispatchConfig, c *fakeCluster, rec, action string) string {
	t.Helper()
	name, err := createRestoreAction(ctx, k, "homelab-backup-ui", rec, action)
	if err != nil {
		t.Fatal(err)
	}
	if err := processRestoreActions(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	if got := ann(c, stateNS+"/"+name, annPhase); got != "Done" {
		t.Fatalf("action %s: %s: %s", action, got, ann(c, stateNS+"/"+name, annMessage))
	}
	return ann(c, stateNS+"/"+rec, annMessage)
}

// startAndFinish drives a restore to the Active phase and returns its record path.
func startAndFinish(t *testing.T, ctx context.Context, k *kubeClient, dc DispatchConfig, c *fakeCluster) (recName, recPath string) {
	t.Helper()
	recName, err := createRestore(ctx, k, "homelab-backup-ui",
		RestoreRequest{Source: "disk", Backup: "manual-1", Namespace: nsApp, Claim: "atuin-server-config", Disk: "data"})
	if err != nil {
		t.Fatal(err)
	}
	recPath = stateNS + "/" + recName
	step := func(want string) {
		t.Helper()
		if err := reconcileRestores(ctx, k, dc); err != nil {
			t.Fatal(err)
		}
		if got := ann(c, recPath, annPhase); got != want {
			t.Fatalf("phase = %q (%s), want %q", got, ann(c, recPath, annMessage), want)
		}
	}
	step(PhaseBinding)
	nc := newClaimIn(t, c)
	if sc := dig2(c.get(pvcBase+nc), "spec", "storageClassName"); sc != "local-data" {
		t.Fatalf("new PVC on storage class %v, want the chosen disk's local-data", sc)
	}
	if dig2(c.get(pvcBase+nc), "metadata", "labels", "app.kubernetes.io/instance") != nil {
		t.Fatal("the new PVC must not inherit the old PVC's ArgoCD tracking label")
	}
	if claimOf(c) != "atuin-server-config" {
		t.Fatal("apps must keep using the old volume until the data is restored")
	}

	// Still unbound: nothing should move.
	step(PhaseBinding)
	c.put(pvcBase+nc, mergeInto(c.get(pvcBase+nc), map[string]any{"status": map[string]any{"phase": "Bound"}, "spec": map[string]any{"volumeName": "pv-new"}}))
	c.put("/api/v1/persistentvolumes/pv-new", map[string]any{"spec": map[string]any{"hostPath": map[string]any{"path": "/home/x/storage/k8s/pvc-new_" + nsApp + "_" + nc}}})
	step(PhaseRestoring)
	rj := "/apis/batch/v1/namespaces/homelab-backup-local/jobs/restore-" + strings.TrimPrefix(recName, "rst-")
	if c.get(rj) == nil {
		t.Fatal("restore job was not created")
	}

	// Job still running: no cutover.
	step(PhaseRestoring)
	if claimOf(c) != "atuin-server-config" {
		t.Fatal("cut over before the restore job finished")
	}
	c.put(rj, mergeInto(c.get(rj), map[string]any{"status": map[string]any{"conditions": []any{map[string]any{"type": "Complete", "status": "True"}}}}))
	step(PhaseActive)
	return recName, recPath
}

func mergeInto(o, p map[string]any) map[string]any {
	mergePatch(o, p)
	return o
}

func TestRestoreFlowSwitchDeleteAndResume(t *testing.T) {
	ctx := context.Background()
	c, k := newFakeCluster(t)
	seedCluster(c)
	dc := testDC()
	recName, recPath := startAndFinish(t, ctx, k, dc, c)

	nc := newClaimIn(t, c)
	if claimOf(c) != nc {
		t.Fatalf("deployment uses %q, want the new claim %q", claimOf(c), nc)
	}
	if got := dig2(c.get(pvcBase+"atuin-server-config"), "metadata", "labels", stateLabel); got != "orphaned" {
		t.Errorf("old PVC label = %v, want orphaned", got)
	}
	if got := dig2(c.get(pvcBase+nc), "metadata", "labels", stateLabel); got != "active" {
		t.Errorf("new PVC label = %v, want active", got)
	}
	if dig2(c.get(argoApp), "spec", "syncPolicy", "automated") != nil {
		t.Error("ArgoCD auto-sync should be paused while the repoint is live")
	}

	// Roll back, then forward again: the orphan swaps each time.
	runAction(t, ctx, k, dc, c, recName, "switch")
	if claimOf(c) != "atuin-server-config" {
		t.Fatalf("rollback left the deployment on %q", claimOf(c))
	}
	if dig2(c.get(pvcBase+nc), "metadata", "labels", stateLabel) != "orphaned" {
		t.Error("rolled-back-from volume should now be the orphan")
	}
	runAction(t, ctx, k, dc, c, recName, "switch")
	if claimOf(c) != nc {
		t.Fatal("switching again should return to the restored volume")
	}

	// Finalize: the old volume goes, ArgoCD stays paused (git still names the old claim).
	msg := runAction(t, ctx, k, dc, c, recName, "delete-orphan")
	if c.get(pvcBase+"atuin-server-config") != nil {
		t.Error("the orphan should be deleted")
	}
	if ann(c, recPath, annPhase) != PhaseFinalized || !strings.Contains(msg, "still paused") {
		t.Errorf("phase=%s msg=%q", ann(c, recPath, annPhase), msg)
	}
	if dig2(c.get(argoApp), "spec", "syncPolicy", "automated") != nil {
		t.Error("auto-sync must stay paused until the chart uses the new volume")
	}
	if c.get(pvcBase+nc) == nil {
		t.Fatal("the active volume must never be deleted")
	}

	runAction(t, ctx, k, dc, c, recName, "resume-sync")
	if got := dig2(c.get(argoApp), "spec", "syncPolicy", "automated"); got == nil {
		t.Error("resume-sync should restore the original auto-sync policy")
	}
}

func TestRestoreFlowRollbackThenDeleteResumesSync(t *testing.T) {
	ctx := context.Background()
	c, k := newFakeCluster(t)
	seedCluster(c)
	dc := testDC()
	recName, _ := startAndFinish(t, ctx, k, dc, c)
	nc := newClaimIn(t, c)

	runAction(t, ctx, k, dc, c, recName, "switch") // roll back to the original
	msg := runAction(t, ctx, k, dc, c, recName, "delete-orphan")
	if c.get(pvcBase+nc) != nil {
		t.Error("the discarded restored volume should be deleted")
	}
	if c.get(pvcBase+"atuin-server-config") == nil {
		t.Fatal("the original volume must survive a rollback")
	}
	if !strings.Contains(msg, "resumed") || dig2(c.get(argoApp), "spec", "syncPolicy", "automated") == nil {
		t.Errorf("git matches reality after a rollback, so sync should resume automatically: %q", msg)
	}
}

func TestRestoreRejections(t *testing.T) {
	ctx := context.Background()
	c, k := newFakeCluster(t)
	seedCluster(c)
	dc := testDC()
	try := func(r RestoreRequest) (phase, msg, path string) {
		name, _ := createRestore(ctx, k, "homelab-backup-ui", r)
		if err := reconcileRestores(ctx, k, dc); err != nil {
			t.Fatal(err)
		}
		p := stateNS + "/" + name
		return ann(c, p, annPhase), ann(c, p, annMessage), p
	}
	base := RestoreRequest{Source: "disk", Backup: "manual-1", Namespace: nsApp, Claim: "atuin-server-config", Disk: "data"}

	r := base
	r.Backup = "manual-404"
	if phase, msg, _ := try(r); phase != PhaseRejected || !strings.Contains(msg, "not an active backup") {
		t.Errorf("unknown backup: %s %q", phase, msg)
	}
	r = base
	r.Claim = "no-such-claim"
	if phase, _, _ := try(r); phase != PhaseRejected {
		t.Errorf("missing volume: %s", phase)
	}
	if n := len(c.children("/api/v1/namespaces/" + nsApp + "/persistentvolumeclaims")); n != 1 {
		t.Errorf("a rejected restore must not leave a PVC behind, found %d", n)
	}

	// A second restore of a volume that is mid-restore is refused.
	if phase, _, _ := try(base); phase != PhaseBinding {
		t.Fatalf("first restore: %s", phase)
	}
	if phase, msg, _ := try(base); phase != PhaseRejected || !strings.Contains(msg, "still open") {
		t.Errorf("concurrent restore: %s %q", phase, msg)
	}
}

func TestRestoreFailureLeavesAppsUntouched(t *testing.T) {
	ctx := context.Background()
	c, k := newFakeCluster(t)
	seedCluster(c)
	dc := testDC()
	recName, err := createRestore(ctx, k, "homelab-backup-ui",
		RestoreRequest{Source: "disk", Backup: "manual-1", Namespace: nsApp, Claim: "atuin-server-config", Disk: "data"})
	if err != nil {
		t.Fatal(err)
	}
	recPath := stateNS + "/" + recName
	reconcileRestores(ctx, k, dc)
	nc := newClaimIn(t, c)

	// A PV path outside the disk's root must never be mounted.
	c.put(pvcBase+nc, mergeInto(c.get(pvcBase+nc), map[string]any{"status": map[string]any{"phase": "Bound"}, "spec": map[string]any{"volumeName": "pv-evil"}}))
	c.put("/api/v1/persistentvolumes/pv-evil", map[string]any{"spec": map[string]any{"hostPath": map[string]any{"path": "/etc"}}})
	reconcileRestores(ctx, k, dc)
	if ann(c, recPath, annPhase) != PhaseFailed || !strings.Contains(ann(c, recPath, annMessage), "refusing to mount") {
		t.Fatalf("phase=%s msg=%s", ann(c, recPath, annPhase), ann(c, recPath, annMessage))
	}
	if claimOf(c) != "atuin-server-config" || dig2(c.get(argoApp), "spec", "syncPolicy", "automated") == nil {
		t.Error("a failed restore must leave the app and ArgoCD as they were")
	}

	runAction(t, ctx, k, dc, c, recName, "discard")
	if c.get(pvcBase+nc) != nil {
		t.Error("discard should delete the unfinished volume")
	}
	if c.get(pvcBase+"atuin-server-config") == nil {
		t.Fatal("discard must never touch the original volume")
	}
}
