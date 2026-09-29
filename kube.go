package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// kubeClient is a minimal read-only Kubernetes API client. It exists so the
// service needs no client-go (and no vendored dependency tree) for what is
// only ever a handful of GETs. It has no write methods on purpose: v1 of this
// app is a viewer, and its RBAC only grants get/list.
type kubeClient struct {
	base string
	http *http.Client
	// token is re-read on every request: projected service account tokens are
	// rotated by the kubelet, so caching one at startup would break after an hour.
	tokenFile string
}

func newKubeClient() (*kubeClient, error) {
	// KUBE_API points at `kubectl proxy` for local development, where the proxy
	// supplies credentials and no token/CA is needed.
	if api := os.Getenv("KUBE_API"); api != "" {
		return &kubeClient{base: strings.TrimRight(api, "/"), http: &http.Client{Timeout: 15 * time.Second}}, nil
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not in a cluster: set KUBE_API (e.g. http://127.0.0.1:8001 via kubectl proxy)")
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	return &kubeClient{
		base:      fmt.Sprintf("https://%s:%s", host, port),
		tokenFile: saDir + "/token",
		http: &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

func (k *kubeClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if k.tokenFile != "" {
		tok, err := os.ReadFile(k.tokenFile)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	resp, err := k.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}
