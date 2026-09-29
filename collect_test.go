package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJobRunsFiltersByOwnerAndPhase(t *testing.T) {
	raw := `{"items":[
	 {"metadata":{"name":"backup-local-1","ownerReferences":[{"kind":"CronJob","name":"backup-local"}]},
	  "status":{"startTime":"2026-09-20T04:00:00Z","completionTime":"2026-09-20T05:00:00Z","conditions":[{"type":"Complete","status":"True"}]}},
	 {"metadata":{"name":"manual-1","annotations":{"cronjob.kubernetes.io/instantiate":"manual"},"ownerReferences":[{"kind":"CronJob","name":"backup-local"}]},
	  "status":{"startTime":"2026-09-28T04:00:00Z","active":1}},
	 {"metadata":{"name":"backup-watchdog-1","ownerReferences":[{"kind":"CronJob","name":"backup-watchdog"}]},
	  "status":{"startTime":"2026-09-29T08:00:00Z","conditions":[{"type":"Failed","status":"True","lastTransitionTime":"2026-09-29T09:00:00Z"}]}}
	]}`
	var jl jobList
	if err := json.Unmarshal([]byte(raw), &jl); err != nil {
		t.Fatal(err)
	}
	runs := jobRuns(jl, "disk", "backup-local")
	if len(runs) != 2 {
		t.Fatalf("want 2 backup-local runs, got %d", len(runs))
	}
	if runs[0].Location != "manual" || runs[1].Location != "scheduled" {
		t.Errorf("locations wrong: %q %q", runs[0].Location, runs[1].Location)
	}
	if runs[0].Name != "manual-1" || runs[0].Phase != "InProgress" || !runs[0].Manual {
		t.Errorf("newest should be the manual in-progress run, got %+v", runs[0])
	}
	if runs[1].Phase != "Completed" {
		t.Errorf("want Completed, got %s", runs[1].Phase)
	}
	wd := jobRuns(jl, "watchdog", "backup-watchdog")
	if len(wd) != 1 || wd[0].Phase != "Failed" {
		t.Errorf("watchdog run wrong: %+v", wd)
	}
}

func TestCloudCardHealth(t *testing.T) {
	now := time.Now()
	ok := Run{Name: "a", Phase: "Completed", Started: now.Add(-49 * time.Hour), Finished: now.Add(-48 * time.Hour)}
	partial := Run{Name: "b", Phase: "PartiallyFailed", Started: now.Add(-2 * time.Hour), Finished: now.Add(-time.Hour)}
	max := 192 * time.Hour
	avail := []string{"cloud: Available (s3://b/p)"}

	if c := cloudCard([]Run{ok}, avail, max); c.Health != HealthOK {
		t.Errorf("fresh completed backup should be ok, got %s", c.Health)
	}
	if c := cloudCard([]Run{partial, ok}, avail, max); c.Health != HealthWarn {
		t.Errorf("newest PartiallyFailed with older success should warn, got %s", c.Health)
	}
	if c := cloudCard([]Run{partial}, avail, max); c.Health != HealthBad {
		t.Errorf("no successful backup at all should be bad, got %s", c.Health)
	}
	old := Run{Name: "c", Phase: "Completed", Started: now.Add(-300 * time.Hour), Finished: now.Add(-299 * time.Hour)}
	if c := cloudCard([]Run{old}, avail, max); c.Health != HealthBad {
		t.Errorf("stale backup should be bad, got %s", c.Health)
	}
	if c := cloudCard([]Run{ok}, []string{"cloud: Unavailable (s3://b/p)"}, max); c.Health != HealthBad {
		t.Errorf("unavailable BSL should be bad, got %s", c.Health)
	}
}

func TestWatchdogCardCountsConsecutiveFailures(t *testing.T) {
	runs := []Run{{Phase: "Failed"}, {Phase: "Failed"}, {Phase: "Completed"}, {Phase: "Failed"}}
	c := watchdogCard(runs)
	if c.Health != HealthBad || c.Big != "2 failed in a row" {
		t.Errorf("got %+v", c)
	}
}
