package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func seedBackup(c *fakeCluster, name, phase string) {
	labels := map[string]any{}
	if strings.HasPrefix(name, "weekly-") {
		labels["velero.io/schedule-name"] = "weekly"
	}
	c.put("/apis/velero.io/v1/namespaces/velero/backups/"+name, map[string]any{
		"metadata": map[string]any{"name": name, "uid": "uid-" + name, "labels": labels},
		"status":   map[string]any{"phase": phase},
	})
}

func runDelete(t *testing.T, k *kubeClient, backup string) (string, string) {
	t.Helper()
	ctx := context.Background()
	dc := DispatchConfig{Config: Config{VeleroNS: "velero", StateNS: "homelab-backup-ui"}, RequestMaxAge: time.Hour}
	name, err := createDeleteRequest(ctx, k, dc.StateNS, backup)
	if err != nil {
		t.Fatal(err)
	}
	if err := processDeletes(ctx, k, dc); err != nil {
		t.Fatal(err)
	}
	var cm struct {
		Metadata meta `json:"metadata"`
	}
	if err := k.get(ctx, stateNS+"/"+name, &cm); err != nil {
		t.Fatal(err)
	}
	return cm.Metadata.Annotations[annPhase], cm.Metadata.Annotations[annMessage]
}

func TestDeleteBackup(t *testing.T) {
	c, k := newFakeCluster(t)
	seedBackup(c, "weekly-old", "PartiallyFailed")
	seedBackup(c, "weekly-new", "Completed")
	seedBackup(c, "manual-running", "InProgress")
	seedBackup(c, "manual-small", "Completed")
	dbrs := "/apis/velero.io/v1/namespaces/velero/deletebackuprequests"

	cases := []struct{ backup, wantPhase, wantMsg string }{
		{"nope", "Rejected", "no backup named"},
		{"manual-running", "Rejected", "only finished backups"},
		// manual-small is Completed too, but it does not stand in for a weekly.
		{"weekly-new", "Rejected", "only Completed scheduled backup"},
		{"Bad_Name", "Rejected", "invalid backup name"},
	}
	for _, tc := range cases {
		if phase, msg := runDelete(t, k, tc.backup); phase != tc.wantPhase || !strings.Contains(msg, tc.wantMsg) {
			t.Errorf("%s: %s %q", tc.backup, phase, msg)
		}
	}
	if n := len(c.children(dbrs)); n != 0 {
		t.Fatalf("rejected requests created %d DeleteBackupRequests", n)
	}

	phase, msg := runDelete(t, k, "weekly-old")
	if phase != "Started" {
		t.Fatalf("delete weekly-old: %s %q", phase, msg)
	}
	got := c.children(dbrs)
	if len(got) != 1 {
		t.Fatalf("want one DeleteBackupRequest, got %v", got)
	}
	d := c.get(got[0])
	if dig2(d, "spec", "backupName") != "weekly-old" || dig2(d, "metadata", "labels", "velero.io/backup-name") != "weekly-old" ||
		dig2(d, "metadata", "labels", "velero.io/backup-uid") != "uid-weekly-old" {
		t.Errorf("DeleteBackupRequest: %v", d)
	}

	// A second click while Velero is still deleting is refused.
	if phase, msg := runDelete(t, k, "weekly-old"); phase != "Rejected" || !strings.Contains(msg, "already being deleted") {
		t.Errorf("duplicate delete: %s %q", phase, msg)
	}

	// A manual backup may go while a good weekly remains.
	if phase, msg := runDelete(t, k, "manual-small"); phase != "Started" {
		t.Errorf("delete manual-small: %s %q", phase, msg)
	}
	// With another Completed weekly around, a Completed weekly may go.
	seedBackup(c, "weekly-newer", "Completed")
	if phase, msg := runDelete(t, k, "weekly-new"); phase != "Started" {
		t.Errorf("delete weekly-new with a newer good backup: %s %q", phase, msg)
	}
}

func TestDeleteRefusedDuringRestore(t *testing.T) {
	c, k := newFakeCluster(t)
	seedBackup(c, "a", "Completed")
	seedBackup(c, "b", "Completed")
	c.put(stateNS+"/rst-1", map[string]any{
		"metadata": map[string]any{"name": "rst-1", "labels": map[string]any{restoreLabel: "true"},
			"annotations": map[string]any{annPhase: PhaseRestoring}},
		"data": map[string]any{"source": "cloud", "backup": "a"},
	})
	if phase, msg := runDelete(t, k, "a"); phase != "Rejected" || !strings.Contains(msg, "rst-1") {
		t.Errorf("delete during restore: %s %q", phase, msg)
	}
}
