// retention.go deletes known profile files of jobs that have been complete
// longer than the configured duration. Job directories and staging stay.

package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/observability"
)

// ProfileRetention only expires known diagnostic files of completed jobs. It
// never traverses saved research/fixture roots or removes job directories.
type ProfileRetention struct {
	Runner   *Runner
	Duration time.Duration
	cursor   string
}

// profileName accepts the fixed build profiles and one JSON file per stage or
// operation. The suffix must be longer than ".json", so a bare prefix is kept.
func profileName(name string) bool {
	if name == "expected-profile.json" || name == "packing-profile.json" {
		return true
	}
	for _, stage := range observability.States {
		prefix := "step-" + stage + "-"
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".json") && len(name) > len(prefix)+5 {
			return true
		}
	}
	for _, operation := range []string{"reconcile", "verify", "delete"} {
		prefix := "operation-" + operation + "-"
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".json") && len(name) > len(prefix)+5 {
			return true
		}
	}
	return false
}

// Prune removes up to one page of completed jobs' profile files. The cursor
// resumes on the next call. A scan that exceeds 4096 entries stops and keeps
// the rest. The cursor is stored even when a later remove fails.
func (retention *ProfileRetention) Prune(ctx context.Context, now time.Time) (removed int, result error) {
	if retention.Duration < 0 {
		return 0, errors.New("negative profile retention")
	}
	if retention.Duration == 0 || retention.Runner.KeepStaging {
		return 0, nil
	}
	runner := retention.Runner
	if runner.Metrics != nil {
		defer func() {
			runner.Metrics.CleanupFiles.Add(float64(removed))
			if result != nil {
				runner.Metrics.CleanupErrors.Inc()
			} else {
				runner.Metrics.CleanupSuccess.Set(float64(now.UnixNano()) / 1e9)
			}
		}()
	}
	// OpenRoot confines operations even if a directory is concurrently renamed.
	info, err := os.Lstat(runner.Root)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, errors.New("profile job root is not a private directory")
	}
	root, err := os.OpenRoot(runner.Root)
	if err != nil {
		return 0, err
	}
	defer root.Close()
	openedRoot, err := root.Stat(".")
	if err != nil || !os.SameFile(info, openedRoot) {
		return 0, errors.New("profile root changed while opening")
	}
	if err = runner.Store.DB.QueryRowContext(ctx, "SELECT value FROM options WHERE key='observability.profile_cleanup_cursor'").Scan(&retention.cursor); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	defer func() {
		_, err := runner.Store.DB.ExecContext(ctx, `INSERT INTO options(key,value) VALUES('observability.profile_cleanup_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, retention.cursor)
		result = errors.Join(result, err)
	}()
	rows, err := runner.Store.DB.QueryContext(ctx, `SELECT j.id,json_extract(j.payload,'$.work') FROM jobs j JOIN job_milestones m ON m.job_id=j.id AND m.milestone='complete' WHERE j.state='COMPLETE' AND m.at_ns>0 AND m.at_ns<=? AND j.id>? ORDER BY j.id LIMIT 100`, now.Add(-retention.Duration).UnixNano(), retention.cursor)
	if err != nil {
		return 0, err
	}
	type job struct{ id, work string }
	var jobs []job
	for rows.Next() {
		var completed job
		if err = rows.Scan(&completed.id, &completed.work); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, completed)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		retention.cursor = ""
		return 0, nil
	}
	examined := 0
	for _, completed := range jobs {
		if err = ctx.Err(); err != nil {
			return removed, err
		}
		if completed.id == "." || completed.id == ".." || filepath.Base(completed.id) != completed.id || strings.ContainsAny(completed.id, "/\\") || filepath.Clean(completed.work) != filepath.Join(filepath.Clean(runner.Root), completed.id) {
			return removed, fmt.Errorf("job work is outside private root")
		}
		info, err := root.Lstat(completed.id)
		if os.IsNotExist(err) {
			retention.cursor = completed.id
			continue
		}
		if err != nil {
			return removed, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return removed, errors.New("job profile directory is not private")
		}
		dir, err := root.OpenRoot(completed.id)
		if err != nil {
			return removed, err
		}
		listing, err := dir.Open(".")
		if err != nil {
			dir.Close()
			return removed, err
		}
		for {
			entries, readErr := listing.ReadDir(64)
			opened, statErr := listing.Stat()
			if statErr != nil || !os.SameFile(info, opened) {
				listing.Close()
				dir.Close()
				return removed, errors.New("job directory changed while opening")
			}
			for _, entry := range entries {
				examined++
				if examined > 4096 {
					listing.Close()
					dir.Close()
					return removed, errors.New("profile cleanup scan budget exhausted; remaining files retained")
				}
				if err = ctx.Err(); err != nil {
					listing.Close()
					dir.Close()
					return removed, err
				}
				if !profileName(entry.Name()) {
					continue
				}
				info, err := dir.Lstat(entry.Name())
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					listing.Close()
					dir.Close()
					return removed, err
				}
				if !info.Mode().IsRegular() {
					continue
				} // Never follow a symlink or remove a directory.
				if err = dir.Remove(entry.Name()); err != nil && !os.IsNotExist(err) {
					listing.Close()
					dir.Close()
					return removed, err
				}
				if err == nil {
					removed++
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				listing.Close()
				dir.Close()
				return removed, readErr
			}
		}
		listing.Close()
		dir.Close()
		retention.cursor = completed.id
	}
	return removed, nil
}
