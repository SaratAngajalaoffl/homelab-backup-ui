# homelab-backup-ui

Dashboard over the homelab's backups: Velero (cloud) backups, restic
(external disk) runs, the backup watchdog, and Sealed Secrets key age — plus a
form to start a manual backup of selected categories to S3, the Munin disk, or both.

Deployed by the `helm/backup-ui` chart in
[SaratAngajalaoffl/homelab](https://github.com/SaratAngajalaoffl/homelab). It
has no dependencies beyond the Go standard library.

## Two processes, deliberately

- `backup-ui` (web, internet-facing): read-only RBAC, plus the right to create
  **request ConfigMaps**. That is its only write.
- `backup-ui dispatch` (no ingress): polls those ConfigMaps, re-validates them
  against the category allow-list (`CATEGORIES_FILE`), and creates the Velero
  `Backup` and/or the restic `Job`. Only this process can create either — and it is
  the only one that can touch PVCs, workloads and ArgoCD for a restore.

So a compromised web pod cannot craft a `Backup` with exec hooks or a `Job` that
mounts host paths — it can only ask for allow-listed things. POSTs are also
checked for same-origin (Basic Auth credentials are sent automatically by
browsers, so this blocks cross-site form posts).

## Restoring a volume

With a disk list configured (`DISKS_FILE`, see below), the dashboard can restore **one
PVC** from an active backup (Velero/S3 or restic/Munin) onto a chosen disk:

1. pick a backup, a volume and a disk;
2. the dispatcher creates a **new** PVC on that disk's StorageClass and restores into it
   (restic: a clone of the `backup-local` Job; Velero: a Kopia Job reading the BSL bucket);
3. Deployments mounting the old PVC are repointed, and ArgoCD auto-sync for the owning
   Application is paused so `selfHeal` doesn't revert it;
4. the old PVC is labelled `homelab.smugiwara.com/restore-state=orphaned`. From the UI you
   can **switch back** (rollback, and forward again) or **delete** the orphan once you've
   verified the new one.

Restores are ConfigMaps (`rst-*`) advanced by the dispatcher as a state machine, so they
survive a dispatcher restart. Volumes owned by a StatefulSet (database data dirs) can't be
repointed and are refused. The restore Jobs mount the new PV's host path as root, so the
dispatcher only accepts paths under the chosen disk's `hostRoot`.

Extra configuration (all optional; no `DISKS_FILE` = restore off): `DISKS_FILE`
(JSON `[{id,name,storageClass,hostRoot}]`), `ARGOCD_NS`, `RESTORE_HELPER_IMAGE`,
`RESTORE_KOPIA_IMAGE`, `RESTORE_CRED_SECRET`, `RESTORE_REPO_SECRET`.

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
