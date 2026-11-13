package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	requestLabel = "homelab.smugiwara.com/backup-request"
	annPhase     = "homelab.smugiwara.com/phase"
	annMessage   = "homelab.smugiwara.com/message"
	annCreated   = "homelab.smugiwara.com/created"
)

// Category is a named group of namespaces the operator can tick in the UI.
// Granularity is per-namespace on purpose: a stack's database dumps travel
// with its namespace, so "databases" cannot be split from the app around them.
type Category struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Namespaces  []string `json:"namespaces"`
	Default     bool     `json:"default"`
}

func loadJSONFile(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func loadCategories(path string) ([]Category, error) {
	var cats []Category
	if err := loadJSONFile(path, &cats); err != nil {
		return nil, err
	}
	return cats, nil
}

var validTargets = map[string]bool{"cloud": true, "disk": true}

// resolveRequest is the single validation point, used by both the web handler
// (to fail fast) and the dispatcher (which is authoritative). It turns
// user-supplied ids into a sorted, de-duplicated namespace list, and rejects
// anything not in the configured allow-list.
func resolveRequest(cats []Category, catIDs, targets []string) ([]string, error) {
	if len(catIDs) == 0 {
		return nil, fmt.Errorf("select at least one category")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("select at least one target")
	}
	for _, t := range targets {
		if !validTargets[t] {
			return nil, fmt.Errorf("unknown target %q", t)
		}
	}
	byID := map[string]Category{}
	for _, c := range cats {
		byID[c.ID] = c
	}
	seen := map[string]bool{}
	var ns []string
	for _, id := range catIDs {
		c, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("unknown category %q", id)
		}
		for _, n := range c.Namespaces {
			if !seen[n] {
				seen[n] = true
				ns = append(ns, n)
			}
		}
	}
	sort.Strings(ns)
	return ns, nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newRequestName(now time.Time) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "req-" + now.UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
}

// sameOrigin defends the trigger endpoint against cross-site form posts. Basic
// auth credentials are attached automatically by the browser, so without this
// any web page could make the operator's browser start a backup.
func sameOrigin(r *http.Request) bool {
	if s := r.Header.Get("Sec-Fetch-Site"); s != "" {
		return s == "same-origin"
	}
	o := r.Header.Get("Origin")
	if o == "" {
		return false
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

// RequestView is one row of the "requests" table.
type RequestView struct {
	Name       string
	Action     string // "Backup", "Delete <backup>", "Rescan S3 usage"
	Categories string
	Targets    string
	Phase      string
	Message    string
	Created    time.Time
}

type configMapList struct {
	Items []struct {
		Metadata meta              `json:"metadata"`
		Data     map[string]string `json:"data"`
	} `json:"items"`
}

// listRequests shows backup, delete and rescan requests in one table.
func listRequests(ctx context.Context, k *kubeClient, ns string) ([]RequestView, error) {
	var out []RequestView
	for _, label := range []string{requestLabel, deleteRequestLabel, usageRequestLabel} {
		var l configMapList
		sel := url.QueryEscape(label + "=true")
		if err := k.get(ctx, "/api/v1/namespaces/"+ns+"/configmaps?labelSelector="+sel, &l); err != nil {
			return nil, err
		}
		for _, it := range l.Items {
			phase := it.Metadata.Annotations[annPhase]
			if phase == "" {
				phase = "Queued"
			}
			v := RequestView{
				Name: it.Metadata.Name, Action: "Backup", Categories: it.Data["categories"], Targets: it.Data["targets"],
				Phase: phase, Message: it.Metadata.Annotations[annMessage], Created: it.Metadata.CreationTimestamp,
			}
			switch label {
			case deleteRequestLabel:
				v.Action, v.Targets = "Delete "+it.Data["backup"], "S3"
			case usageRequestLabel:
				v.Action, v.Targets = "Rescan S3 usage", "S3"
			}
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })
	if len(out) > 15 {
		out = out[:15]
	}
	return out, nil
}

// createRequest is the ONLY write the web process can make: a ConfigMap. What
// actually runs is decided by the dispatcher from the allow-list.
func createRequest(ctx context.Context, k *kubeClient, ns string, catIDs, targets []string) (string, error) {
	name := newRequestName(time.Now())
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{requestLabel: "true"}},
		"data": map[string]string{
			"categories": strings.Join(catIDs, ","),
			"targets":    strings.Join(targets, ","),
		},
	}
	return name, k.do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", "", cm, nil)
}
