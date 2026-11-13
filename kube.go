package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// kubeClient is a minimal Kubernetes API client. It exists so the service needs
// no client-go (and no vendored dependency tree) for what is only ever a
// handful of calls. What it may actually do is decided by RBAC: the web pod's
// ServiceAccount can read and create request ConfigMaps, and only the
// dispatcher's can write anything else.
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

// apiError is a non-2xx answer from the API server.
type apiError struct {
	Code int
	msg  string
}

func (e *apiError) Error() string { return e.msg }

func isNotFound(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Code == http.StatusNotFound
}

func (k *kubeClient) get(ctx context.Context, path string, out any) error {
	return k.do(ctx, http.MethodGet, path, "", nil, out)
}

// text GETs a non-JSON resource, such as a pod log.
func (k *kubeClient) text(ctx context.Context, path string) (string, error) {
	var raw rawBody
	err := k.do(ctx, http.MethodGet, path, "", nil, &raw)
	return string(raw), err
}

// rawBody makes do() hand back the response untouched instead of decoding JSON.
type rawBody []byte

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
	accept := "application/json"
	if _, ok := out.(*rawBody); ok {
		accept = "*/*"
	}
	req.Header.Set("Accept", accept)
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
		return &apiError{Code: resp.StatusCode, msg: fmt.Sprintf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))}
	}
	if out == nil {
		return nil
	}
	if raw, ok := out.(*rawBody); ok {
		*raw = data
		return nil
	}
	return json.Unmarshal(data, out)
}
