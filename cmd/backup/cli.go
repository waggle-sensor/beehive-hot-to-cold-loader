// Package backup implements the `state-backup` subcommand.
// It never starts the coordinator or performs lakehouse publication/deletion.
package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/statebackup"
	"beehive-hot-to-cold-loader/internal/storage"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Run executes one state-backup action from args after the `state-backup` verb.
func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("state-backup requires request, list, inspect, download, restore, or approve")
	}
	action := args[0]
	flags := flag.NewFlagSet("state-backup "+action, flag.ContinueOnError)
	bundle := flags.String("bundle", "", "local state.tar.gz file")
	endpoint := flags.String("endpoint", env("HOT_TO_COLD_BACKUP_ENDPOINT", "http://beehive-hot-to-cold-loader.shared.svc.sage-cluster.local:10001"), "internal coordinator endpoint")
	bucket := flags.String("bucket", env("INFLUX_BUCKET", "waggle"), "source bucket for backup discovery/approval")
	bucketID := flags.String("bucket-id", env("INFLUX_BUCKET_ID", ""), "original Influx bucket ID for restore approval")
	org := flags.String("org", env("INFLUX_ORG", "waggle"), "original Influx organization for restore approval")
	state := flags.String("state", env("HOT_TO_COLD_STATE", ""), "original state path; empty precreated directory for restore")
	snapshot := flags.String("snapshot", "", "unpacked snapshot directory for inspection")
	identity := flags.String("identity", "", "remote backup identity key")
	id := flags.String("id", "", "remote backup id")
	lakehouse := flags.String("lakehouse", env("COLD_LAKEHOUSE", ""), "managed lakehouse URL")
	namespace := flags.String("namespace", env("COLD_NAMESPACE", "sage"), "Iceberg namespace")
	table := flags.String("table-name", env("COLD_TABLE_NAME", "observations"), "Iceberg table")
	dest := flags.String("backup-url", env("HOT_TO_COLD_BACKUP_URL", ""), "private backup prefix")
	key := flags.String("pelican-key", env("PELICAN_KEY_PATH", ""), "read-only mounted signing key")
	accept := flags.Bool("accept-post-backup-data-loss", false, "accept the manifest cutoff and possible loss of acknowledged later arrivals")
	stopped := flags.Bool("confirm-clients-stopped", false, "confirm coordinator, writers and readers stopped before disaster recovery")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Hour)
	defer cancel()
	switch action {
	case "request":
		token := os.Getenv("HOT_TO_COLD_ADMIN_TOKEN")
		if token == "" {
			return errors.New("HOT_TO_COLD_ADMIN_TOKEN required")
		}
		client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		call := func(route string) (map[string]any, error) {
			req, e := http.NewRequestWithContext(ctx, "POST", strings.TrimSuffix(*endpoint, "/")+route, bytes.NewBufferString("{}"))
			if e != nil {
				return nil, e
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			resp, e := client.Do(req)
			if e != nil {
				return nil, e
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("backup request HTTP %d", resp.StatusCode)
			}
			var status map[string]any
			e = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&status)
			return status, e
		}
		started, e := call("/v1/backups/create")
		if e != nil {
			return e
		}
		id, ok := started["id"].(string)
		if !ok || id == "" {
			return errors.New("missing backup operation id")
		}
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			status, e := call("/v1/backups/status")
			if e != nil {
				return e
			}
			if status["id"] != id {
				return errors.New("backup status operation differs")
			}
			switch status["phase"] {
			case "complete":
				return json.NewEncoder(os.Stdout).Encode(status)
			case "failed":
				return fmt.Errorf("backup failed: %v", status["error"])
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
			}
		}

	case "inspect":
		if *snapshot == "" {
			return errors.New("--snapshot required")
		}
		m, e := statebackup.Verify(ctx, *snapshot)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	case "restore":
		if !*stopped || *state == "" || *bundle == "" {
			return errors.New("restore requires --state, --bundle and --confirm-clients-stopped")
		}
		return statebackup.Restore(ctx, *bundle, *state)
	case "approve", "download", "list":
		if action == "approve" && (!*stopped || !*accept) {
			return errors.New("approval requires --confirm-clients-stopped and --accept-post-backup-data-loss")
		}
		t, e := storage.NewTableIdentity(*namespace, *table)
		if e != nil {
			return e
		}
		remote, e := storage.NewBackupRemote(*lakehouse, *key, filepath.Dir(*state), t, *dest, 30)
		if e != nil {
			return e
		}
		if action == "list" {
			entries, err := remote.List(ctx, *bucket)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(entries)
		}
		if action == "download" {
			if *bundle == "" {
				return errors.New("--bundle required")
			}
			return remote.Download(ctx, *identity, *id, *bundle)
		}
		abs, e := filepath.Abs(*state)
		if e != nil {
			return e
		}
		saved, e := statebackup.Load(abs)
		if e != nil {
			return e
		}
		if saved.Bucket != *bucket || (saved.SourceBucketID != "" && saved.SourceBucketID != *bucketID) || (saved.SourceOrg != "" && saved.SourceOrg != *org) {
			return errors.New("configured source bucket/org differs from restored backup")
		}
		if e = statebackup.Approve(ctx, abs, remote, *accept); e != nil {
			return e
		}
		fmt.Println("Verified restored state and current remote references; recovery approved. Start with deletion disabled.")
		return nil
	default:
		return errors.New("unknown state-backup operation")
	}
}
