package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

var testCats = []Category{
	{ID: "apps", Namespaces: []string{"homelab-b", "homelab-a"}},
	{ID: "immich", Namespaces: []string{"homelab-immich", "homelab-a"}},
}

func TestResolveRequest(t *testing.T) {
	ns, err := resolveRequest(testCats, []string{"apps", "immich"}, []string{"cloud", "disk"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"homelab-a", "homelab-b", "homelab-immich"}; !reflect.DeepEqual(ns, want) {
		t.Errorf("namespaces not merged/deduped/sorted: %v", ns)
	}
	bad := []struct {
		name    string
		cats    []string
		targets []string
	}{
		{"no categories", nil, []string{"cloud"}},
		{"no targets", []string{"apps"}, nil},
		{"unknown category", []string{"kube-system"}, []string{"cloud"}},
		{"unknown target", []string{"apps"}, []string{"ftp"}},
	}
	for _, b := range bad {
		if _, err := resolveRequest(testCats, b.cats, b.targets); err == nil {
			t.Errorf("%s: expected rejection", b.name)
		}
	}
}

func TestSameOrigin(t *testing.T) {
	mk := func(host string, h map[string]string) *http.Request {
		r := httptest.NewRequest("POST", "/backups", nil)
		r.Host = host
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return r
	}
	cases := []struct {
		name string
		r    *http.Request
		want bool
	}{
		{"fetch-site same-origin", mk("b.example", map[string]string{"Sec-Fetch-Site": "same-origin"}), true},
		{"fetch-site cross-site", mk("b.example", map[string]string{"Sec-Fetch-Site": "cross-site"}), false},
		{"origin matches", mk("b.example", map[string]string{"Origin": "https://b.example"}), true},
		{"origin differs", mk("b.example", map[string]string{"Origin": "https://evil.example"}), false},
		{"no headers", mk("b.example", nil), false},
	}
	for _, c := range cases {
		if got := sameOrigin(c.r); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestBuildBackup(t *testing.T) {
	dc := DispatchConfig{Config: Config{VeleroNS: "velero"}, ManualLocation: "cloud-manual", ManualTTL: "720h0m0s"}
	b := buildBackup(dc, "manual-1", []string{"apps"}, []string{"homelab-a"})
	spec := b["spec"].(map[string]any)
	if spec["storageLocation"] != "cloud-manual" {
		t.Errorf("wrong location: %v", spec["storageLocation"])
	}
	if !reflect.DeepEqual(spec["excludedResources"], []string{"secrets"}) {
		t.Errorf("secrets must be excluded: %v", spec["excludedResources"])
	}
	if _, ok := spec["hooks"]; ok {
		t.Error("manual backups must never carry hooks")
	}
}

func TestBuildJobInjectsOnlyEnv(t *testing.T) {
	cj := map[string]any{
		"metadata": map[string]any{"name": "backup-local", "uid": "u1"},
		"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{
			"template": map[string]any{"spec": map[string]any{"containers": []any{
				map[string]any{"name": "restic", "env": []any{map[string]any{"name": "RESTIC_REPOSITORY", "value": "/drive/x"}}},
			}}},
		}}},
	}
	job, err := buildJob(cj, "ns", "manual-1", []string{"apps", "immich"}, []string{"homelab-a", "homelab-immich"})
	if err != nil {
		t.Fatal(err)
	}
	env := dig(job, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)["env"].([]any)
	got := map[string]string{}
	for _, e := range env {
		m := e.(map[string]any)
		got[m["name"].(string)] = m["value"].(string)
	}
	if got["BACKUP_NAMESPACES"] != "homelab-a,homelab-immich" || got["BACKUP_TAGS"] != "apps,immich" || got["RESTIC_REPOSITORY"] != "/drive/x" {
		t.Errorf("env wrong: %v", got)
	}
	owner := dig(job, "metadata", "ownerReferences").([]any)[0].(map[string]any)
	if owner["name"] != "backup-local" || owner["uid"] != "u1" {
		t.Errorf("job must be owned by the cronjob: %v", owner)
	}
}
