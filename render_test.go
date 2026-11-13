package main

import (
	"strings"
	"testing"
	"time"
)

func TestIndexRenders(t *testing.T) {
	now := time.Now()
	st := Status{
		Generated: now,
		Cloud: []Run{
			{Source: "cloud", Name: "weekly-1", Phase: "Completed", Started: now, Bytes: 5 << 30},
			{Source: "cloud", Name: "manual-2", Phase: "InProgress", Started: now},
		},
		Requests: []RequestView{{Name: "del-1", Action: "Delete weekly-0", Targets: "S3", Phase: "Started"}},
		Usage: UsageView{Report: &UsageReport{Bucket: "loki", Prefix: "automatic/", Bytes: 9 << 30, Scanned: now, Sections: []UsageSection{
			{Title: "Velero location cloud-scheduled", Prefix: "automatic/k3s_backups/scheduled/", Managed: true,
				Groups: []UsageGroup{{Kind: "backup", Name: "gone", Orphan: true}}},
			{Title: "Not managed by Velero", Prefix: "automatic/", Groups: []UsageGroup{{Kind: "other", Name: "backups/", Bytes: 1 << 30}}},
		}}},
	}
	var b strings.Builder
	if err := newTemplate().Execute(&b, st); err != nil {
		t.Fatal(err)
	}
	html := b.String()
	for _, want := range []string{`action="/cloud-backups/weekly-1/delete"`, "5.0 GiB", "Not managed by Velero", "not in Velero", "Delete weekly-0", `action="/usage/rescan"`} {
		if !strings.Contains(html, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(html, "/cloud-backups/manual-2/delete") {
		t.Error("a running backup must not offer Delete")
	}
}
