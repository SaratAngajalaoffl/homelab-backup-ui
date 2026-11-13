package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestUsageGrouping(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	acc := newUsageAccumulator("automatic/", parseBSLPrefixes("cloud-scheduled=automatic/k3s_backups/scheduled;cloud-manual=/automatic/k3s_backups/manual/"))
	for _, o := range []s3Object{
		{"automatic/k3s_backups/scheduled/backups/weekly-1/weekly-1.tar.gz", 100, t0},
		{"automatic/k3s_backups/scheduled/backups/weekly-1/weekly-1-logs.gz", 10, t0.Add(time.Hour)},
		{"automatic/k3s_backups/scheduled/kopia/homelab-immich/p0001", 5000, t0},
		{"automatic/k3s_backups/scheduled/kopia/homelab-immich/p0002", 5000, t0},
		{"automatic/k3s_backups/manual/kopia/homelab-atuin/p1", 7, t0},
		{"automatic/k3s_backups/stray.txt", 1, t0},
		{"automatic/backups/old/x.tar.gz", 300, t0},
		{"automatic/home.tar.gz.gpg", 900, t0},
	} {
		acc.add(o)
	}
	r := acc.report("b", t0)
	if r.Bytes != 11318 || r.Objects != 8 {
		t.Fatalf("totals: %d bytes %d objects", r.Bytes, r.Objects)
	}
	titles := []string{}
	for _, s := range r.Sections {
		titles = append(titles, s.Title)
	}
	if strings.Join(titles, "|") != "Velero location cloud-manual|Velero location cloud-scheduled|Not managed by Velero" {
		t.Fatalf("sections: %v", titles)
	}
	sched := r.Sections[1]
	if sched.Bytes != 10110 || sched.Groups[0].Kind != "kopia" || sched.Groups[0].Name != "homelab-immich" || sched.Groups[0].Objects != 2 {
		t.Errorf("scheduled section: %+v", sched)
	}
	if b := sched.Groups[1]; b.Kind != "backup" || b.Name != "weekly-1" || b.Bytes != 110 || !b.Newest.Equal(t0.Add(time.Hour)) {
		t.Errorf("backup group: %+v", b)
	}
	other := map[string]int64{}
	for _, g := range r.Sections[2].Groups {
		other[g.Name] = g.Bytes
	}
	if other["backups/"] != 300 || other["(loose files)"] != 900 || other["k3s_backups/"] != 1 {
		t.Errorf("unmanaged groups: %v", other)
	}
}

func TestParseUsageLog(t *testing.T) {
	r, err := parseUsageLog("2026/01/01 noise\n" + usageMarker + `{"Bucket":"b","Bytes":42}` + "\n2026/01/01 scanned\n")
	if err != nil || r.Bucket != "b" || r.Bytes != 42 {
		t.Errorf("got %+v, %v", r, err)
	}
	if _, err := parseUsageLog("FATAL: AccessDenied"); err == nil {
		t.Error("expected an error without a report line")
	}
}

func usageDC() DispatchConfig {
	return DispatchConfig{
		Config: Config{VeleroNS: "velero", StateNS: "homelab-backup-ui"}, RequestMaxAge: time.Hour,
		SelfImage: "ghcr.io/x/backup-ui:1", UsageInterval: 6 * time.Hour, CredSecret: "velero-cloud-credentials",
	}
}

func seedBSLs(c *fakeCluster) {
	for _, b := range []struct {
		name, prefix string
		def          bool
	}{{"cloud-scheduled", "automatic/k3s_backups/scheduled", true}, {"cloud-manual", "automatic/k3s_backups/manual", false}} {
		c.put("/apis/velero.io/v1/namespaces/velero/backupstoragelocations/"+b.name, map[string]any{
			"metadata": map[string]any{"name": b.name},
			"spec": map[string]any{
				"default": b.def, "objectStorage": map[string]any{"bucket": "loki", "prefix": b.prefix},
				"config":     map[string]any{"region": "ap-south-1"},
				"credential": map[string]any{"name": "velero-cloud-credentials", "key": "cloud"},
			},
		})
	}
}

func TestUsageScanLifecycle(t *testing.T) {
	c, k := newFakeCluster(t)
	seedBSLs(c)
	dc, ctx := usageDC(), context.Background()

	// No state yet: a scan starts.
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	jobs := c.children("/apis/batch/v1/namespaces/velero/jobs")
	if len(jobs) != 1 {
		t.Fatalf("want one scan job, got %v", jobs)
	}
	job := c.get(jobs[0])
	ctr := dig2(job, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)
	envs := map[string]string{}
	for _, e := range ctr["env"].([]any) {
		m := e.(map[string]any)
		envs[m["name"].(string)], _ = m["value"].(string)
	}
	if ctr["image"] != dc.SelfImage || envs["BUCKET"] != "loki" || envs["SCAN_PREFIX"] != "automatic/" ||
		!strings.Contains(envs["BSL_PREFIXES"], "cloud-manual=automatic/k3s_backups/manual") {
		t.Errorf("scan job: image %v env %v", ctr["image"], envs)
	}
	if dig2(job, "spec", "template", "spec", "automountServiceAccountToken") != false {
		t.Error("scan job must not get an API token")
	}

	// While it runs, nothing new starts.
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	if n := len(c.children("/apis/batch/v1/namespaces/velero/jobs")); n != 1 {
		t.Fatalf("a second scan started while one was running (%d jobs)", n)
	}

	// Job completes: the report is harvested into the ConfigMap.
	name := dig2(job, "metadata", "name").(string)
	job["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Complete", "status": "True"}}}
	c.put("/api/v1/namespaces/velero/pods/"+name+"-abc", map[string]any{"metadata": map[string]any{"name": name + "-abc", "labels": map[string]any{"job-name": name}}})
	c.logs["/api/v1/namespaces/velero/pods/"+name+"-abc/log"] = usageMarker + `{"Bucket":"loki","Prefix":"automatic/","Bytes":2048,"Sections":[{"Title":"Velero location cloud-scheduled","Managed":true,"Bytes":2000,"Groups":[{"Kind":"backup","Name":"gone-1","Bytes":10}]},{"Title":"Not managed by Velero","Bytes":48}]}` + "\n"
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	v, err := loadUsage(ctx, k, dc.StateNS, []Run{{Name: "weekly-2"}})
	if err != nil || v.Report == nil || v.Running || v.Report.Bytes != 2048 || v.Managed != 2000 || v.Unmanaged != 48 {
		t.Fatalf("usage view: %+v %v", v, err)
	}
	if !v.Report.Sections[0].Groups[0].Orphan {
		t.Error("a backup folder with no Backup object should be flagged")
	}

	// Fresh result: no rescan until asked.
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	if n := len(c.children("/apis/batch/v1/namespaces/velero/jobs")); n != 1 {
		t.Fatalf("rescanned without being asked (%d jobs)", n)
	}
	if _, err := createUsageRequest(ctx, k, dc.StateNS); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // job names are per-second
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	if n := len(c.children("/apis/batch/v1/namespaces/velero/jobs")); n != 2 {
		t.Fatalf("rescan request did not start a scan (%d jobs)", n)
	}
}

func TestUsageScanFailureKeepsLastReport(t *testing.T) {
	c, k := newFakeCluster(t)
	dc, ctx := usageDC(), context.Background()
	prev, _ := json.Marshal(UsageReport{Bucket: "loki", Bytes: 1})
	c.put(stateNS+"/"+usageConfigMap, map[string]any{"metadata": map[string]any{"name": usageConfigMap},
		"data": map[string]any{"report": string(prev), "job": "s3-usage-1", "started": time.Now().UTC().Format(time.RFC3339)}})
	c.put("/apis/batch/v1/namespaces/velero/jobs/s3-usage-1", map[string]any{"metadata": map[string]any{"name": "s3-usage-1"},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True", "message": "BackoffLimitExceeded"}}}})
	c.put("/api/v1/namespaces/velero/pods/s3-usage-1-x", map[string]any{"metadata": map[string]any{"name": "s3-usage-1-x", "labels": map[string]any{"job-name": "s3-usage-1"}}})
	c.logs["/api/v1/namespaces/velero/pods/s3-usage-1-x/log"] = "listing s3://loki/automatic/: AccessDenied: Access Denied\n"
	if err := reconcileUsage(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	v, _ := loadUsage(ctx, k, dc.StateNS, nil)
	if v.Report == nil || v.Report.Bytes != 1 || !strings.Contains(v.Error, "AccessDenied") || v.Running {
		t.Errorf("after a failed scan: %+v", v)
	}
}

func TestHumanBytes(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1536: "1.5 KiB", 138 << 30: "138.0 GiB"} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %s, want %s", n, got, want)
		}
	}
}
