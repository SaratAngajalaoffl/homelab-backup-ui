package main

import (
	"bytes"
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
	return k.do(ctx, http.MethodGet, path, "", nil, out)
}

// do performs one API call. Only the dispatcher's ServiceAccount is granted
// write verbs; the web process's RBAC would reject a write regardless of what
// this client attempts.
func (k *kubeClient) do(ctx context.Context, method, path, contentType string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
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
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
