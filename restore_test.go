package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testDisks = []Disk{
	{ID: "os", Name: "OS disk", StorageClass: "local-path", HostRoot: "/var/lib/rancher/k3s/storage"},
	{ID: "data", Name: "Data disk", StorageClass: "local-data", HostRoot: "/home/x/storage/k8s"},
}

func TestValidateRestoreRequest(t *testing.T) {
	cats := []Category{{ID: "apps", Namespaces: []string{"homelab-atuin"}}}
	ok := RestoreRequest{Source: "cloud", Backup: "manual-20261001-100000", Namespace: "homelab-atuin", Claim: "atuin-server-config", Disk: "data"}
	d, err := validateRestoreRequest(cats, testDisks, ok)
	if err != nil || d.StorageClass != "local-data" {
		t.Fatalf("valid request rejected: %v %+v", err, d)
	}
	for name, mut := range map[string]func(*RestoreRequest){
		"bad source":                  func(r *RestoreRequest) { r.Source = "ftp" },
		"bad backup":                  func(r *RestoreRequest) { r.Backup = "../etc" },
		"namespace not in a category": func(r *RestoreRequest) { r.Namespace = "kube-system" },
		"bad claim":                   func(r *RestoreRequest) { r.Claim = "A b" },
		"unknown disk":                func(r *RestoreRequest) { r.Disk = "usb" },
	} {
		r := ok
		mut(&r)
		if _, err := validateRestoreRequest(cats, testDisks, r); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestActiveSources(t *testing.T) {
	now := time.Now()
	cats := []Category{{Namespaces: []string{"homelab-a", "homelab-b"}}}
	cloud := []Run{
		{Source: "cloud", Name: "ok", Phase: "Completed", Started: now.Add(-time.Hour), Expires: now.Add(time.Hour), Namespaces: []string{"homelab-a", "kube-system"}},
		{Source: "cloud", Name: "expired", Phase: "Completed", Expires: now.Add(-time.Hour)},
		{Source: "cloud", Name: "failed", Phase: "Failed"},
		{Source: "cloud", Name: "partial", Phase: "PartiallyFailed", Started: now.Add(-2 * time.Hour), Namespaces: []string{"*"}},
	}
	disk := []Run{
		{Source: "disk", Name: "d1", Phase: "Completed", Started: now.Add(-3 * time.Hour), Namespaces: []string{"homelab-b"}},
		{Source: "disk", Name: "running", Phase: "InProgress"},
	}
	got := activeSources(cloud, disk, cats, now)
	var names []string
	for _, s := range got {
		names = append(names, s.Name)
	}
	if want := []string{"ok", "partial", "d1"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("active sources = %v, want %v", names, want)
	}
	if !reflect.DeepEqual(got[0].Namespaces, []string{"homelab-a"}) {
		t.Errorf("namespaces not narrowed to the allow-list: %v", got[0].Namespaces)
	}
	if !got[1].Partial || !reflect.DeepEqual(got[1].Namespaces, []string{"homelab-a", "homelab-b"}) {
		t.Errorf("wildcard backup should cover everything and be flagged partial: %+v", got[1])
	}
}

func TestNewClaimName(t *testing.T) {
	at := time.Date(2026, 10, 1, 10, 15, 0, 0, time.UTC)
	first := newClaimName("immich-library", at)
	if first != "immich-library-r2610011015" {
		t.Fatal(first)
	}
	if again := newClaimName(first, at.Add(time.Hour)); again != "immich-library-r2610011115" {
		t.Errorf("suffix should be replaced, not stacked: %s", again)
	}
}

func TestPathUnder(t *testing.T) {
	root := "/var/lib/rancher/k3s/storage"
	for p, want := range map[string]bool{
		root + "/pvc-1_ns_x":                  true,
		root + "/../../etc":                   false,
		root:                                  false,
		"/etc/passwd":                         false,
		"relative/path":                       false,
		"":                                    false,
		"/var/lib/rancher/k3s/storage-evil/x": false,
	} {
		if got := pathUnder(p, root); got != want {
			t.Errorf("pathUnder(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestRestoreViewActions(t *testing.T) {
	st := `{"newClaim":"c-r1","active":"c-r1","orphan":"c","argoPaused":{"atuin":{"selfHeal":true}}}`
	v := restoreView("rst-1", map[string]string{"claim": "c"}, map[string]string{annPhase: PhaseActive, annState: st}, time.Now())
	if !v.CanSwitch || !v.CanDeleteOrphan || v.CanResume || v.SwitchLabel != "Roll back to c" {
		t.Errorf("active restore: %+v", v)
	}
	st = `{"newClaim":"c-r1","active":"c-r1","argoPaused":{"atuin":{"selfHeal":true}}}`
	v = restoreView("rst-1", map[string]string{"claim": "c"}, map[string]string{annPhase: PhaseFinalized, annState: st}, time.Now())
	if v.CanSwitch || !v.CanResume || !v.ArgoPaused {
		t.Errorf("finalized restore with paused sync: %+v", v)
	}
	v = restoreView("rst-2", nil, map[string]string{annPhase: PhaseFailed, annState: `{"newClaim":"c-r2"}`}, time.Now())
	if !v.CanDiscard {
		t.Errorf("failed restore should offer discard: %+v", v)
	}
}

func TestTemplateRenders(t *testing.T) {
	st := Status{
		Generated:  time.Now(),
		CanRestore: true,
		Disks:      testDisks,
		Sources:    []RestoreSource{{Source: "cloud", Name: "manual-1", Started: time.Now(), Namespaces: []string{"homelab-a"}}},
		Volumes:    []Volume{{Namespace: "homelab-a", Name: "data", Size: "1Gi", StorageClass: "local-path"}, {Namespace: "homelab-a", Name: "old", State: "orphaned"}},
		Restores: []RestoreView{{Name: "rst-1", Phase: PhaseActive, Active: "data-r1", Orphan: "data", CanSwitch: true,
			CanDeleteOrphan: true, SwitchLabel: "Roll back to data", ArgoPaused: true, ArgoApps: "a"}},
	}
	var b strings.Builder
	if err := newTemplate().Execute(&b, st); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{`name="backup" value="cloud/manual-1"`, `value="homelab-a/data"`, `/restores/rst-1/delete-orphan`, `Roll back to data`, `value="data"`} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered page lacks %q", want)
		}
	}
	if strings.Contains(out, `value="homelab-a/old"`) {
		t.Error("orphaned volumes must not be offered for restore")
	}
}

// fakeAPI serves canned JSON by path and records every request.
type fakeAPI struct {
	t      *testing.T
	resp   map[string]any
	calls  []string
	bodies map[string]string
}

func newFake(t *testing.T, resp map[string]any) (*fakeAPI, *kubeClient) {
	f := &fakeAPI{t: t, resp: resp, bodies: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		f.calls = append(f.calls, key)
		b, _ := io.ReadAll(r.Body)
		f.bodies[key] = string(b)
		if v, ok := f.resp[key]; ok {
			if fn, isFn := v.(func() any); isFn {
				v = fn()
			}
			json.NewEncoder(w).Encode(v)
			return
		}
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte("{}"))
	}))
	t.Cleanup(srv.Close)
	return f, &kubeClient{base: srv.URL, http: srv.Client()}
}

func vol(name, claim string) map[string]any {
	return map[string]any{"name": name, "persistentVolumeClaim": map[string]any{"claimName": claim}}
}

func workloads(items ...map[string]any) map[string]any { return map[string]any{"items": items} }

func deploy(name string, vols ...map[string]any) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name},
		"spec":     map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": vols}}},
	}
}

func TestFindWorkloads(t *testing.T) {
	ctx := context.Background()
	empty := workloads()
	_, k := newFake(t, map[string]any{
		"GET /apis/apps/v1/namespaces/ns/deployments":  workloads(deploy("web", vol("other", "x"), vol("config", "cfg"))),
		"GET /apis/apps/v1/namespaces/ns/statefulsets": empty,
		"GET /api/v1/namespaces/ns/pods":               empty,
	})
	refs, volName, err := findWorkloads(ctx, k, "ns", "cfg")
	if err != nil || volName != "config" || !reflect.DeepEqual(refs, []workloadRef{{"Deployment", "web"}}) {
		t.Fatalf("refs=%v vol=%q err=%v", refs, volName, err)
	}

	sts := map[string]any{"metadata": map[string]any{"name": "db"}, "spec": map[string]any{
		"volumeClaimTemplates": []any{map[string]any{"metadata": map[string]any{"name": "data"}}},
	}}
	_, k = newFake(t, map[string]any{
		"GET /apis/apps/v1/namespaces/ns/deployments":  empty,
		"GET /apis/apps/v1/namespaces/ns/statefulsets": workloads(sts),
	})
	if _, _, err := findWorkloads(ctx, k, "ns", "data-db-0"); err == nil || !strings.Contains(err.Error(), "StatefulSet") {
		t.Errorf("statefulset template claim must be refused, got %v", err)
	}

	bare := map[string]any{"metadata": map[string]any{"name": "adhoc"}, "spec": map[string]any{"volumes": []any{vol("v", "cfg")}}}
	_, k = newFake(t, map[string]any{
		"GET /apis/apps/v1/namespaces/ns/deployments":  empty,
		"GET /apis/apps/v1/namespaces/ns/statefulsets": empty,
		"GET /api/v1/namespaces/ns/pods":               workloads(bare),
	})
	if _, _, err := findWorkloads(ctx, k, "ns", "cfg"); err == nil || !strings.Contains(err.Error(), "adhoc") {
		t.Errorf("unmanaged pod mounting the claim must be refused, got %v", err)
	}
}

func TestRepointGuardsAndReverts(t *testing.T) {
	ctx := context.Background()
	// The Deployment reads back as patched once a PATCH has gone through.
	var f *fakeAPI
	var k *kubeClient
	f, k = newFake(t, map[string]any{
		"GET /apis/apps/v1/namespaces/ns/deployments/a": func() any {
			patches := 0
			for _, c := range f.calls {
				if c == "PATCH /apis/apps/v1/namespaces/ns/deployments/a" {
					patches++
				}
			}
			claim := "old" // odd patch count = repointed, even = reverted
			if patches%2 == 1 {
				claim = "new"
			}
			return deploy("a", vol("tmp", "t"), vol("config", claim))
		},
	})
	err := repoint(ctx, k, "ns", []workloadRef{{"Deployment", "a"}, {"Deployment", "missing"}}, "old", "new")
	if err == nil {
		t.Fatal("expected failure on the second workload")
	}
	first := f.bodies["PATCH /apis/apps/v1/namespaces/ns/deployments/a"]
	for _, want := range []string{`"op":"test"`, `"path":"/spec/template/spec/volumes/1/persistentVolumeClaim/claimName"`, `"value":"old"`, `"value":"new"`} {
		if !strings.Contains(first, want) {
			t.Errorf("patch lacks %s: %s", want, first)
		}
	}
	patches := 0
	for _, c := range f.calls {
		if c == "PATCH /apis/apps/v1/namespaces/ns/deployments/a" {
			patches++
		}
	}
	if patches != 2 {
		t.Errorf("first workload should be patched then reverted, got %d patches", patches)
	}
}

func TestPauseAndResumeArgo(t *testing.T) {
	ctx := context.Background()
	app := func(name, ns string, auto any) map[string]any {
		sp := map[string]any{}
		if auto != nil {
			sp["automated"] = auto
		}
		return map[string]any{"metadata": map[string]any{"name": name},
			"spec": map[string]any{"destination": map[string]any{"namespace": ns}, "syncPolicy": sp}}
	}
	f, k := newFake(t, map[string]any{
		"GET /apis/argoproj.io/v1alpha1/namespaces/argocd/applications": workloads(
			app("atuin", "homelab-atuin", map[string]any{"prune": true, "selfHeal": true}),
			app("manual", "homelab-atuin", nil),
			app("other", "homelab-other", map[string]any{"selfHeal": true})),
	})
	dc := DispatchConfig{ArgoNS: "argocd"}
	paused, err := pauseArgo(ctx, k, dc, "homelab-atuin")
	if err != nil || len(paused) != 1 {
		t.Fatalf("paused=%v err=%v", paused, err)
	}
	if got := f.bodies["PATCH /apis/argoproj.io/v1alpha1/namespaces/argocd/applications/atuin"]; !strings.Contains(got, `"automated":null`) {
		t.Errorf("pause patch = %s", got)
	}
	if _, touched := f.bodies["PATCH /apis/argoproj.io/v1alpha1/namespaces/argocd/applications/other"]; touched {
		t.Error("an Application in another namespace must not be paused")
	}
	resumeArgo(ctx, k, dc, paused)
	if got := f.bodies["PATCH /apis/argoproj.io/v1alpha1/namespaces/argocd/applications/atuin"]; !strings.Contains(got, `"selfHeal":true`) {
		t.Errorf("resume must restore the previous policy, got %s", got)
	}
}

func TestResticRestoreJobClonesTemplate(t *testing.T) {
	cj := map[string]any{"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{
		"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "restic", "env": []any{map[string]any{"name": "RESTIC_REPOSITORY", "value": "/drive/r"}}, "volumeMounts": []any{}}},
			"volumes":    []any{},
		}}}}}}
	_, k := newFake(t, map[string]any{"GET /apis/batch/v1/namespaces/homelab-backup-local/cronjobs/backup-local": cj})
	dc := DispatchConfig{Config: Config{LocalNS: "homelab-backup-local", LocalCronJob: "backup-local"}}
	rec := &restoreRec{name: "rst-1", data: map[string]string{"source": "disk", "backup": "manual-9", "namespace": "ns", "claim": "c", "disk": "os"},
		st: restoreState{RestoreJob: "restore-1", Dest: "/var/lib/rancher/k3s/storage/pvc-x_ns_c-r1"}}
	ns, job, err := buildRestoreJob(context.Background(), k, dc, rec)
	if err != nil || ns != "homelab-backup-local" {
		t.Fatalf("ns=%s err=%v", ns, err)
	}
	b, _ := json.Marshal(job)
	for _, want := range []string{`"RESTORE_BACKUP"`, `"manual-9"`, `"mountPath":"/dest"`, `"path":"/var/lib/rancher/k3s/storage/pvc-x_ns_c-r1"`, `"backoffLimit":0`, `RESTIC_REPOSITORY`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("job lacks %s", want)
		}
	}
	if strings.Contains(string(b), "ownerReferences") {
		t.Error("a restore job must not be owned by the CronJob, or it would show up as a disk backup run")
	}
}

func TestFindSnapshotPrefersPVCUID(t *testing.T) {
	pvb := func(snap, uid, volume string, when string) map[string]any {
		return map[string]any{
			"metadata": map[string]any{"labels": map[string]any{"velero.io/pvc-uid": uid}, "creationTimestamp": when},
			"spec":     map[string]any{"pod": map[string]any{"namespace": "ns"}, "volume": volume},
			"status":   map[string]any{"phase": "Completed", "snapshotID": snap},
		}
	}
	_, k := newFake(t, map[string]any{"GET /apis/velero.io/v1/namespaces/velero/podvolumebackups": workloads(
		pvb("by-volume-newer", "other", "config", "2026-10-01T10:00:00Z"),
		pvb("by-uid", "uid-1", "renamed", "2026-10-01T09:00:00Z"),
	)})
	dc := DispatchConfig{Config: Config{VeleroNS: "velero"}}
	got, err := findSnapshot(context.Background(), k, dc, "b1", "ns", "uid-1", "config")
	if err != nil || got != "by-uid" {
		t.Fatalf("got %q err=%v, want the UID match", got, err)
	}
	if _, err := findSnapshot(context.Background(), k, dc, "b1", "ns", "nope", "nope"); err == nil {
		t.Error("expected an error when no volume backup matches")
	}
}

func TestKopiaRestoreJob(t *testing.T) {
	_, k := newFake(t, map[string]any{
		"GET /apis/velero.io/v1/namespaces/velero/backups/b1": map[string]any{"spec": map[string]any{"storageLocation": "cloud-manual"}},
		"GET /apis/velero.io/v1/namespaces/velero/backupstoragelocations/cloud-manual": map[string]any{"spec": map[string]any{
			"objectStorage": map[string]any{"bucket": "bkt", "prefix": "automatic/k3s_backups/manual"},
			"config":        map[string]any{"region": "ap-south-1"},
		}},
	})
	dc := DispatchConfig{Config: Config{VeleroNS: "velero"}, KopiaImage: "kopia/kopia:x", CredSecret: "velero-cloud-credentials", RepoSecret: "velero-repo-credentials"}
	rec := &restoreRec{name: "rst-1", data: map[string]string{"source": "cloud", "backup": "b1", "namespace": "homelab-atuin", "claim": "c"},
		st: restoreState{RestoreJob: "restore-1", Dest: "/x/pvc", Snapshot: "snap123"}}
	ns, job, err := buildRestoreJob(context.Background(), k, dc, rec)
	if err != nil || ns != "velero" {
		t.Fatalf("ns=%s err=%v", ns, err)
	}
	b, _ := json.Marshal(job)
	for _, want := range []string{
		`"PREFIX","value":"automatic/k3s_backups/manual/kopia/homelab-atuin/"`, `"BUCKET","value":"bkt"`, `"REGION","value":"ap-south-1"`,
		`"SNAPSHOT_ID","value":"snap123"`, `"secretName":"velero-cloud-credentials"`, `"optional":true`, `kopia/kopia:x`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("kopia job lacks %s\n%s", want, b)
		}
	}
}
