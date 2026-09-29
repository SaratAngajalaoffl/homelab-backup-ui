package main

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
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
		DiskMaxAge:  envHours("DISK_MAX_AGE_HOURS", 192),
	}
	kc, err := newKubeClient()
	if err != nil {
		log.Fatal(err)
	}
	tpl := template.Must(template.New("index.html").Funcs(template.FuncMap{
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(collect(ctx, kc, cfg))
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := tpl.Execute(w, collect(ctx, kc, cfg)); err != nil {
			log.Printf("render: %v", err)
		}
	})
	addr := env("LISTEN", ":8080")
	log.Printf("backup-ui listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
