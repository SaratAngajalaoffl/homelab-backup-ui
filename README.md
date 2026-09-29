# homelab-backup-ui

Read-only dashboard over the homelab's backups: Velero (cloud) backups, restic
(external disk) runs, the backup watchdog, and Sealed Secrets key age.

Deployed by the `helm/backup-ui` chart in
[SaratAngajalaoffl/homelab](https://github.com/SaratAngajalaoffl/homelab). It
talks to the Kubernetes API with a read-only service account; it has no write
path and no dependencies beyond the Go standard library.

## Develop

    go test ./...
    kubectl proxy --port=8011 &      # use the homelab kubeconfig
    KUBE_API=http://127.0.0.1:8011 \
    SEALED_CERT_URL=http://127.0.0.1:8011/api/v1/namespaces/kube-system/services/sealed-secrets-controller:8080/proxy/v1/cert.pem \
    LISTEN=127.0.0.1:8090 go run .

## Release

CI (`.github/workflows/ci.yml`) tests every push and PR. Pushes to `main` publish
`ghcr.io/saratangajalaoffl/homelab-backup-ui:latest` and `:sha-<short>`; pushing a
`vX.Y.Z` tag publishes `:X.Y.Z`, which is what the homelab chart pins.

Configuration is via environment variables — see `main.go`.
