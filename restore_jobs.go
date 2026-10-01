package main

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// resticRestoreScript runs in a clone of the backup-local CronJob's pod, so it
// has the drive, RESTIC_PASSWORD and the repo settings for free. The backup Job
// tags each snapshot job-<name>, which is how a run in the dashboard maps to a
// snapshot. restic stores the volume as /pvcs/pvc-<uid>_<ns>_<claim>; the uid is
// of the PV at backup time, so the directory is matched on its _<ns>_<claim> tail.
const resticRestoreScript = `set -eu
if ! mountpoint -q /drive; then
  echo "FATAL: /drive is not a mount point - munin drive not attached." >&2
  exit 1
fi
restic cat config >/dev/null
if [ -n "$(ls -A /dest 2>/dev/null)" ]; then
  echo "FATAL: /dest is not empty" >&2
  exit 1
fi
echo "Restoring ${RESTORE_NS}/${RESTORE_CLAIM} from the snapshot tagged job-${RESTORE_BACKUP}"
restic restore latest --tag "job-${RESTORE_BACKUP}" \
  --target /dest/.restore-stage \
  --include "/pvcs/*_${RESTORE_NS}_${RESTORE_CLAIM}"
src=$(ls -d /dest/.restore-stage/pvcs/*_"${RESTORE_NS}"_"${RESTORE_CLAIM}" 2>/dev/null | head -n1)
if [ -z "$src" ]; then
  echo "FATAL: the snapshot has no volume ${RESTORE_NS}/${RESTORE_CLAIM}" >&2
  exit 1
fi
cp -a "$src"/. /dest/
chown "$(stat -c %u:%g "$src")" /dest || true
chmod "$(stat -c %a "$src")" /dest || true
rm -rf /dest/.restore-stage
du -sh /dest
`

// kopiaRestoreScript restores a Velero fs-backup (Kopia) snapshot straight
// into the new volume. Velero's Kopia repository lives in the BSL bucket under
// <prefix>/kopia/<namespace>/ and is encrypted with Velero's repo password
// (the static default unless velero-repo-credentials overrides it).
const kopiaRestoreScript = `set -eu
mkdir -p /tmp/kopia
export KOPIA_CONFIG_PATH=/tmp/kopia/repo.config KOPIA_CACHE_DIRECTORY=/tmp/kopia/cache \
  KOPIA_LOG_DIR=/tmp/kopia/logs KOPIA_CHECK_FOR_UPDATES=false
if [ -n "$(ls -A /dest 2>/dev/null)" ]; then
  echo "FATAL: /dest is not empty" >&2
  exit 1
fi
AKID=$(sed -n 's/^aws_access_key_id[[:space:]]*=[[:space:]]*//p' /creds/cloud | head -n1)
SECRET=$(sed -n 's/^aws_secret_access_key[[:space:]]*=[[:space:]]*//p' /creds/cloud | head -n1)
if [ -z "$AKID" ] || [ -z "$SECRET" ]; then
  echo "FATAL: could not read AWS credentials from the BSL credential secret" >&2
  exit 1
fi
export KOPIA_PASSWORD="${REPO_PASSWORD:-static-passw0rd}"
kopia repository connect s3 --readonly \
  --bucket="$BUCKET" --prefix="$PREFIX" --region="$REGION" \
  --access-key="$AKID" --secret-access-key="$SECRET"
echo "Restoring snapshot ${SNAPSHOT_ID} into /dest"
kopia snapshot restore "$SNAPSHOT_ID" /dest
du -sh /dest
`

// buildRestoreJob returns the namespace and manifest of the Job that fills the
// new volume. Both flavours write to the volume's host directory, since the
// data sources (the Munin drive, restic's password, Velero's bucket credential)
// live in other namespaces than the app's, and a pod can only mount PVCs from
// its own namespace.
func buildRestoreJob(ctx context.Context, k *kubeClient, dc DispatchConfig, r *restoreRec) (string, map[string]any, error) {
	req := r.request()
	name := r.st.RestoreJob
	labels := map[string]string{restoreLabel: "job", annRecord: r.name}
	destVolume := map[string]any{"name": "dest", "hostPath": map[string]any{"path": r.st.Dest, "type": "Directory"}}
	destMount := map[string]any{"name": "dest", "mountPath": "/dest"}

	if req.Source == "disk" {
		var cj map[string]any
		if err := k.get(ctx, "/apis/batch/v1/namespaces/"+dc.LocalNS+"/cronjobs/"+dc.LocalCronJob, &cj); err != nil {
			return "", nil, err
		}
		spec, _ := dig(cj, "spec", "jobTemplate", "spec").(map[string]any)
		podSpec, _ := dig(spec, "template", "spec").(map[string]any)
		containers, _ := podSpec["containers"].([]any)
		if spec == nil || podSpec == nil || len(containers) == 0 {
			return "", nil, fmt.Errorf("cronjob %s has no job template", dc.LocalCronJob)
		}
		c := containers[0].(map[string]any)
		c["command"] = []any{"/bin/sh", "-c", resticRestoreScript}
		delete(c, "args")
		env, _ := c["env"].([]any)
		c["env"] = append(env,
			map[string]any{"name": "RESTORE_NS", "value": req.Namespace},
			map[string]any{"name": "RESTORE_CLAIM", "value": req.Claim},
			map[string]any{"name": "RESTORE_BACKUP", "value": req.Backup},
		)
		mounts, _ := c["volumeMounts"].([]any)
		c["volumeMounts"] = append(mounts, destMount)
		vols, _ := podSpec["volumes"].([]any)
		podSpec["volumes"] = append(vols, destVolume)
		spec["backoffLimit"] = 0
		spec["ttlSecondsAfterFinished"] = 86400
		return dc.LocalNS, map[string]any{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": name, "namespace": dc.LocalNS, "labels": labels},
			"spec":     spec,
		}, nil
	}

	// Cloud: find the BSL the backup went to, for bucket, prefix and credential.
	var b struct {
		Spec struct {
			StorageLocation string `json:"storageLocation"`
		} `json:"spec"`
	}
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backups/"+req.Backup, &b); err != nil {
		return "", nil, err
	}
	var bsl struct {
		Spec struct {
			ObjectStorage struct {
				Bucket string `json:"bucket"`
				Prefix string `json:"prefix"`
			} `json:"objectStorage"`
			Config     map[string]string `json:"config"`
			Credential struct {
				Name string `json:"name"`
				Key  string `json:"key"`
			} `json:"credential"`
		} `json:"spec"`
	}
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/backupstoragelocations/"+b.Spec.StorageLocation, &bsl); err != nil {
		return "", nil, err
	}
	credName, credKey := bsl.Spec.Credential.Name, bsl.Spec.Credential.Key
	if credName == "" {
		credName, credKey = dc.CredSecret, "cloud"
	}
	prefix := strings.Trim(bsl.Spec.ObjectStorage.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	env := []any{
		map[string]any{"name": "BUCKET", "value": bsl.Spec.ObjectStorage.Bucket},
		map[string]any{"name": "PREFIX", "value": prefix + "kopia/" + req.Namespace + "/"},
		map[string]any{"name": "REGION", "value": bsl.Spec.Config["region"]},
		map[string]any{"name": "SNAPSHOT_ID", "value": r.st.Snapshot},
		map[string]any{"name": "REPO_PASSWORD", "valueFrom": map[string]any{"secretKeyRef": map[string]any{
			"name": dc.RepoSecret, "key": "repository-password", "optional": true,
		}}},
	}
	return dc.VeleroNS, map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": name, "namespace": dc.VeleroNS, "labels": labels},
		"spec": map[string]any{
			"backoffLimit": 0, "ttlSecondsAfterFinished": 86400,
			"template": map[string]any{"spec": map[string]any{
				"restartPolicy": "Never",
				// Root so the restored files keep their original owners.
				"securityContext": map[string]any{"runAsUser": 0},
				"containers": []any{map[string]any{
					"name": "kopia", "image": dc.KopiaImage,
					"command": []any{"/bin/sh", "-c", kopiaRestoreScript},
					"env":     env,
					"volumeMounts": []any{destMount,
						map[string]any{"name": "creds", "mountPath": "/creds", "readOnly": true}},
				}},
				"volumes": []any{destVolume, map[string]any{"name": "creds", "secret": map[string]any{
					"secretName": credName, "items": []any{map[string]any{"key": credKey, "path": "cloud"}},
				}}},
			}},
		},
	}, nil
}

type podVolumeBackupList struct {
	Items []struct {
		Metadata meta `json:"metadata"`
		Spec     struct {
			Pod struct {
				Namespace string `json:"namespace"`
			} `json:"pod"`
			Volume string `json:"volume"`
		} `json:"spec"`
		Status struct {
			Phase      string `json:"phase"`
			SnapshotID string `json:"snapshotID"`
		} `json:"status"`
	} `json:"items"`
}

// findSnapshot maps (backup, PVC) to the Kopia snapshot Velero took of it. A
// PodVolumeBackup is per pod volume, so the PVC is matched on its UID (stamped
// on the record as a label) and, failing that, on the pod-volume name the
// workload mounts it as.
func findSnapshot(ctx context.Context, k *kubeClient, dc DispatchConfig, backup, ns, pvcUID, volName string) (string, error) {
	if len(backup) > 63 {
		return "", fmt.Errorf("backup name %q is too long to look up its volume backups", backup)
	}
	var l podVolumeBackupList
	sel := url.QueryEscape("velero.io/backup-name=" + backup)
	if err := k.get(ctx, "/apis/velero.io/v1/namespaces/"+dc.VeleroNS+"/podvolumebackups?labelSelector="+sel, &l); err != nil {
		return "", fmt.Errorf("could not list volume backups: %w", err)
	}
	type cand struct {
		snap  string
		byUID bool
		when  string
	}
	var cands []cand
	for _, p := range l.Items {
		if p.Status.Phase != "Completed" || p.Status.SnapshotID == "" || p.Spec.Pod.Namespace != ns {
			continue
		}
		byUID := pvcUID != "" && p.Metadata.Labels["velero.io/pvc-uid"] == pvcUID
		if !byUID && (volName == "" || p.Spec.Volume != volName) {
			continue
		}
		cands = append(cands, cand{p.Status.SnapshotID, byUID, p.Metadata.CreationTimestamp.String()})
	}
	if len(cands) == 0 {
		return "", fmt.Errorf("backup %s holds no completed volume data for this volume (it may be excluded, empty, or not mounted by a running pod when the backup ran)", backup)
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].byUID != cands[j].byUID {
			return cands[i].byUID
		}
		return cands[i].when > cands[j].when
	})
	return cands[0].snap, nil
}
