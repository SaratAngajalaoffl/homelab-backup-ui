package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

// Deleting an S3 (Velero) backup. Same split as everything else: the web pod
// writes a request ConfigMap, and the dispatcher re-checks it and creates a
// Velero DeleteBackupRequest — what `velero backup delete` does. Velero then
// removes the backup's files from the bucket and its Kopia snapshots; data no
// other backup shares is freed at the next Kopia repository maintenance.

const deleteRequestLabel = "homelab.smugiwara.com/delete-request"

// deletablePhases are the finished states. Anything else is still being
// written (or already being deleted) and is refused.
var deletablePhases = map[string]bool{"Completed": true, "PartiallyFailed": true, "Failed": true, "FailedValidation": true}

func validBackupName(name string) bool {
	// Velero stamps the name into labels, so the label length limit applies.
	return name != "" && len(name) <= 63 && dnsName.MatchString(name)
}

func createDeleteRequest(ctx context.Context, k *kubeClient, ns, backup string) (string, error) {
	name := "del-" + newRequestName(time.Now())[4:]
	cm := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{deleteRequestLabel: "true"}},
		"data":     map[string]string{"backup": backup},
	}
	return name, k.do(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", "", cm, nil)
}

func processDeletes(ctx context.Context, k *kubeClient, dc DispatchConfig) error {
	var l configMapList
	base := cmBase(dc)
	if err := k.get(ctx, base+"?labelSelector="+url.QueryEscape(deleteRequestLabel+"=true"), &l); err != nil {
		return err
	}
	for _, it := range l.Items {
		name := it.Metadata.Name
		if time.Since(it.Metadata.CreationTimestamp) > dc.RequestMaxAge {
			_ = k.do(ctx, http.MethodDelete, base+"/"+name, "", nil, nil)
			continue
		}
		if it.Metadata.Annotations[annPhase] != "" {
			continue
		}
		if err := annotate(ctx, k, base, name, "Processing", ""); err != nil {
			return err
		}
		phase, msg := deleteBackup(ctx, k, dc, it.Data["backup"])
		if err := annotate(ctx, k, base, name, phase, msg); err != nil {
			return err
		}
		log.Printf("delete request %s (%s): %s: %s", name, it.Data["backup"], phase, msg)
	}
	return nil
}

func deleteBackup(ctx context.Context, k *kubeClient, dc DispatchConfig, backup string) (string, string) {
	if !validBackupName(backup) {
		return "Rejected", fmt.Sprintf("invalid backup name %q", backup)
	}
	vns := "/apis/velero.io/v1/namespaces/" + dc.VeleroNS
	var bl veleroBackupList
	if err := k.get(ctx, vns+"/backups", &bl); err != nil {
		return "Rejected", "could not list backups: " + err.Error()
	}
	// A small manual backup of one stack is no substitute for the weekly full
	// one, so a scheduled backup must leave another good SCHEDULED one behind.
	var uid, phase string
	found, scheduled, otherGood, otherGoodScheduled := false, false, 0, 0
	for _, b := range bl.Items {
		isSched := b.Metadata.Labels["velero.io/schedule-name"] != ""
		if b.Metadata.Name == backup {
			found, uid, phase, scheduled = true, b.Metadata.UID, b.Status.Phase, isSched
		} else if b.Status.Phase == "Completed" {
			otherGood++
			if isSched {
				otherGoodScheduled++
			}
		}
	}
	if !found {
		return "Rejected", "no backup named " + backup
	}
	if !deletablePhases[phase] {
		return "Rejected", fmt.Sprintf("%s is %s; only finished backups can be deleted", backup, orDefault(phase, "New"))
	}
	if phase == "Completed" && scheduled && otherGoodScheduled == 0 {
		return "Rejected", backup + " is the only Completed scheduled backup; wait for the next weekly run before deleting it"
	}
	if phase == "Completed" && otherGood == 0 {
		return "Rejected", backup + " is the only Completed backup in S3; take another backup before deleting it"
	}
	recs, err := loadRestoreRecs(ctx, k, dc)
	if err != nil {
		return "Rejected", "could not check restores: " + err.Error()
	}
	for _, r := range recs {
		// Once a restore is Active its data is already copied out of the backup.
		if r.data["source"] == "cloud" && r.data["backup"] == backup && !isTerminalPhase(r.phase) && r.phase != PhaseActive {
			return "Rejected", fmt.Sprintf("restore %s is reading from %s", r.name, backup)
		}
	}
	var dl struct {
		Items []struct {
			Metadata meta `json:"metadata"`
			Status   struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	sel := url.QueryEscape("velero.io/backup-name=" + backup)
	if err := k.get(ctx, vns+"/deletebackuprequests?labelSelector="+sel, &dl); err != nil {
		return "Rejected", "could not check existing deletions: " + err.Error()
	}
	for _, d := range dl.Items {
		if d.Status.Phase != "Processed" {
			return "Rejected", fmt.Sprintf("%s is already being deleted (%s)", backup, d.Metadata.Name)
		}
	}
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	dbr := buildDeleteRequest(dc.VeleroNS, backup, uid, backup+"-"+hex.EncodeToString(b))
	if err := k.do(ctx, http.MethodPost, vns+"/deletebackuprequests", "", dbr, nil); err != nil {
		return "Rejected", "could not create the DeleteBackupRequest: " + err.Error()
	}
	return "Started", fmt.Sprintf("Velero is deleting %s (DeleteBackupRequest %s). Its files leave S3 now; volume data no other backup shares is freed by Kopia maintenance within about a day.",
		backup, dig(dbr, "metadata", "name"))
}

// buildDeleteRequest mirrors what the velero CLI creates.
func buildDeleteRequest(ns, backup, uid, name string) map[string]any {
	labels := map[string]string{"velero.io/backup-name": backup, triggerLabel: "backup-ui"}
	if uid != "" {
		labels["velero.io/backup-uid"] = uid
	}
	return map[string]any{
		"apiVersion": "velero.io/v1", "kind": "DeleteBackupRequest",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": labels},
		"spec":     map[string]any{"backupName": backup},
	}
}
