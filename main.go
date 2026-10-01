package main

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed templates/index.html
var tplFS embed.FS

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envHours(k string, def int) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v + "h"); err == nil {
			return d
		}
	}
	return time.Duration(def) * time.Hour
}

func newTemplate() *template.Template {
	return template.Must(template.New("index.html").Funcs(template.FuncMap{
		"ago": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return humanAgo(time.Since(t))
		},
		"ts": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.Local().Format("2006-01-02 15:04")
		},
		"dur": humanDur,
		"dict": func(kv ...any) map[string]any {
			m := map[string]any{}
			for i := 0; i+1 < len(kv); i += 2 {
				m[kv[i].(string)] = kv[i+1]
			}
			return m
		},
	}).ParseFS(tplFS, "templates/index.html"))
}

func main() {
	cfg := Config{
		VeleroNS:       env("VELERO_NS", "velero"),
		LocalNS:        env("LOCAL_NS", "homelab-backup-local"),
		LocalCronJob:   env("LOCAL_CRONJOB", "backup-local"),
		WatchdogCron:   env("WATCHDOG_CRONJOB", "backup-watchdog"),
		SealedCertURL:  env("SEALED_CERT_URL", "http://sealed-secrets-controller.kube-system.svc:8080/v1/cert.pem"),
		StateNS:        env("STATE_NS", "homelab-backup-ui"),
		StateConfigMap: env("STATE_CONFIGMAP", "secrets-export"),
		// Weekly schedules: a run older than 8 days means one was missed.
		CloudMaxAge: envHours("CLOUD_MAX_AGE_HOURS", 192),
		// 0 = disk backups are manual-only, so age is never a fault.
		DiskMaxAge: envHours("DISK_MAX_AGE_HOURS", 0),
	}
	cats, err := loadCategories(env("CATEGORIES_FILE", "/etc/backup-ui/categories.json"))
	if err != nil {
		log.Fatal(err)
	}
	// Optional: without a disk list the restore feature is simply off.
	if f := env("DISKS_FILE", "/etc/backup-ui/disks.json"); f != "" {
		if disks, err := loadDisks(f); err == nil {
			cfg.Disks = disks
		} else if !os.IsNotExist(err) {
			log.Fatal(err)
		}
	}
	kc, err := newKubeClient()
	if err != nil {
		log.Fatal(err)
	}
	if len(os.Args) > 1 && os.Args[1] == "dispatch" {
		runDispatcher(context.Background(), kc, DispatchConfig{
			Config:         cfg,
			Categories:     cats,
			ManualLocation: env("MANUAL_LOCATION", "cloud-manual"),
			ManualTTL:      env("MANUAL_TTL", "720h0m0s"),
			RequestMaxAge:  7 * 24 * time.Hour,
			ArgoNS:         env("ARGOCD_NS", "argocd"),
			HelperImage:    env("RESTORE_HELPER_IMAGE", "busybox:1.37"),
			KopiaImage:     env("RESTORE_KOPIA_IMAGE", "kopia/kopia:latest"),
			CredSecret:     env("RESTORE_CRED_SECRET", "velero-cloud-credentials"),
			RepoSecret:     env("RESTORE_REPO_SECRET", "velero-repo-credentials"),
		})
		return
	}
	tpl := newTemplate()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(collect(ctx, kc, cfg, cats))
	})
	mux.HandleFunc("POST /backups", func(w http.ResponseWriter, r *http.Request) {
		if !sameOrigin(r) {
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		catIDs, targets := r.Form["category"], r.Form["target"]
		// Fail fast for a good error message; the dispatcher re-validates.
		if _, err := resolveRequest(cats, catIDs, targets); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		name, err := createRequest(ctx, kc, cfg.StateNS, catIDs, targets)
		if err != nil {
			log.Printf("create request: %v", err)
			http.Error(w, "could not queue the request", http.StatusBadGateway)
			return
		}
		log.Printf("queued %s categories=%v targets=%v", name, catIDs, targets)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})
	mux.HandleFunc("POST /restores", func(w http.ResponseWriter, r *http.Request) {
		if !sameOrigin(r) {
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		// The volume field is "<namespace>/<claim>".
		ns, claim, _ := strings.Cut(r.FormValue("volume"), "/")
		// The backup field is "<source>/<name>".
		src, backup, _ := strings.Cut(r.FormValue("backup"), "/")
		req := RestoreRequest{Source: src, Backup: backup, Namespace: ns, Claim: claim, Disk: r.FormValue("disk")}
		if _, err := validateRestoreRequest(cats, cfg.Disks, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		name, err := createRestore(ctx, kc, cfg.StateNS, req)
		if err != nil {
			log.Printf("create restore: %v", err)
			http.Error(w, "could not queue the restore", http.StatusBadGateway)
			return
		}
		log.Printf("queued restore %s %+v", name, req)
		http.Redirect(w, r, "/#restores", http.StatusSeeOther)
	})
	mux.HandleFunc("POST /restores/{name}/{action}", func(w http.ResponseWriter, r *http.Request) {
		if !sameOrigin(r) {
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		name, action := r.PathValue("name"), r.PathValue("action")
		if !restoreActions[action] || !strings.HasPrefix(name, "rst-") || !dnsName.MatchString(name) {
			http.Error(w, "unknown restore action", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if _, err := createRestoreAction(ctx, kc, cfg.StateNS, name, action); err != nil {
			log.Printf("create restore action: %v", err)
			http.Error(w, "could not queue the action", http.StatusBadGateway)
			return
		}
		log.Printf("queued %s on %s", action, name)
		http.Redirect(w, r, "/#restores", http.StatusSeeOther)
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := tpl.Execute(w, collect(ctx, kc, cfg, cats)); err != nil {
			log.Printf("render: %v", err)
		}
	})
	addr := env("LISTEN", ":8080")
	log.Printf("backup-ui listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
