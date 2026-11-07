// metrics.go reads one consistent journal snapshot for a scrape.
// Legacy milestones are counted once, without invented timestamps.

package control

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"beehive-hot-to-cold-loader/internal/observability"
)

// migrateMilestones runs once. Legacy counts have no fabricated timestamps.
func (s *Store) migrateMilestones() error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,applied_ns INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS job_milestones(job_id TEXT NOT NULL,mode TEXT NOT NULL,milestone TEXT NOT NULL,at_ns INTEGER NOT NULL,PRIMARY KEY(job_id,milestone))`,
		`CREATE TABLE IF NOT EXISTS milestone_totals(mode TEXT NOT NULL,milestone TEXT NOT NULL,total INTEGER NOT NULL,last_ns INTEGER NOT NULL,baseline INTEGER NOT NULL,PRIMARY KEY(mode,milestone))`,
	} {
		if _, err = tx.Exec(statement); err != nil {
			return err
		}
	}
	var exists int
	if err = tx.QueryRow("SELECT count(*) FROM schema_migrations WHERE version=1").Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		if _, err = tx.Exec(`INSERT OR IGNORE INTO job_milestones SELECT id,mode,'routed',0 FROM jobs WHERE state IN ('ROUTED','DELETING','DELETE_REQUESTED','COMPLETE') AND mode IN ('archive','late')`); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT OR IGNORE INTO job_milestones SELECT id,mode,'complete',0 FROM jobs WHERE state='COMPLETE' AND mode IN ('archive','late')`); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO milestone_totals SELECT mode,milestone,count(*),0,count(*) FROM job_milestones GROUP BY mode,milestone`); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(1,?)`, s.now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) recordMilestone(tx *sql.Tx, job *Job, milestone string) error {
	if job.Mode != "archive" && job.Mode != "late" {
		return fmt.Errorf("invalid job mode %q", job.Mode)
	}
	now := s.now().UnixNano()
	result, err := tx.Exec(`INSERT OR IGNORE INTO job_milestones VALUES(?,?,?,?)`, job.ID, job.Mode, milestone, now)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 0 {
		return err
	}
	_, err = tx.Exec(`INSERT INTO milestone_totals VALUES(?,?,1,?,0) ON CONFLICT(mode,milestone) DO UPDATE SET total=total+1,last_ns=max(last_ns,excluded.last_ns)`, job.Mode, milestone, now)
	return err
}

type SnapshotOptions struct {
	Retention                       time.Duration
	DeletionEnabled, CoordinateOnly bool
	RetryID                         string
	RetryAt                         time.Time
}

// MetricsSnapshot reads a consistent journal view without retaining a write lock
// or doing any filesystem/remote work. All SQL honors the scrape deadline.
func (s *Store) MetricsSnapshot(ctx context.Context, options SnapshotOptions) (observability.Snapshot, error) {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var samples observability.Snapshot
	add := func(name string, value float64, labels ...string) {
		samples = append(samples, observability.Gauge(name, value, labels...))
	}
	now := s.now()
	var frontier int64
	if err = tx.QueryRowContext(ctx, "SELECT frontier FROM config WHERE id=1").Scan(&frontier); err != nil {
		return nil, err
	}
	add("routing_frontier_timestamp_seconds", float64(frontier)/1e9)
	add("lag_seconds", max(0, now.Sub(time.Unix(0, frontier)).Seconds()-options.Retention.Seconds()))
	queries := map[string]string{

		"late_pending":      "SELECT count(*) FROM inbox WHERE done=0",
		"quarantined":       "SELECT count(*) FROM quarantine",
		"jobs_pending":      "SELECT count(*) FROM jobs WHERE state!='COMPLETE'",
		"deletions_pending": "SELECT count(*) FROM jobs WHERE mode='archive' AND state IN ('ROUTED','DELETING','DELETE_REQUESTED')",
		"jobs_failed":       "SELECT count(*) FROM jobs WHERE state!='COMPLETE' AND coalesce(json_extract(payload,'$.error'),'')!=''",
	}
	for name, query := range queries {
		var count int64
		if err = tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, err
		}
		add(name, float64(count))
	}
	for _, name := range []string{"writers", "writers_expired"} {
		comparison := ">"
		if name == "writers_expired" {
			comparison = "<="
		}
		var count int64
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM permits WHERE state='active' AND expires_at "+comparison+" ?", now.Add(-WriteExpiryGrace).UnixNano()).Scan(&count); err != nil {
			return nil, err
		}
		add(name, float64(count))
	}
	for _, name := range []string{"readers", "readers_expired"} {
		comparison := ">"
		if name == "readers_expired" {
			comparison = "<="
		}
		var count int64
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM readers WHERE expires_at "+comparison+" ?", now.Add(-ReadExpiryGrace).UnixNano()).Scan(&count); err != nil {
			return nil, err
		}
		add(name, float64(count))
	}
	var oldest int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(min(accepted_at),0) FROM inbox WHERE done=0").Scan(&oldest); err != nil {
		return nil, err
	}
	age := float64(0)
	if oldest > 0 {
		age = max(0, float64(now.Unix()-oldest))
	}
	add("late_oldest_age_seconds", age)
	for _, mode := range observability.Modes {
		for _, state := range observability.States {
			var count, failed int64
			if err = tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(CASE WHEN state!='COMPLETE' AND coalesce(json_extract(payload,'$.error'),'')!='' THEN 1 ELSE 0 END),0) FROM jobs WHERE mode=? AND state=?`, mode, state).Scan(&count, &failed); err != nil {
				return nil, err
			}
			add("jobs", float64(count), "mode", mode, "state", state)
			add("jobs_with_errors", float64(failed), "mode", mode, "state", state)
		}
		for _, milestone := range []string{"routed", "complete"} {
			var total, last, baseline int64
			err = tx.QueryRowContext(ctx, "SELECT total,last_ns,baseline FROM milestone_totals WHERE mode=? AND milestone=?", mode, milestone).Scan(&total, &last, &baseline)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			sample := observability.Gauge("job_milestones_total", float64(total), "mode", mode, "milestone", milestone)
			sample.Counter = true
			samples = append(samples, sample)
			sample = observability.Gauge("job_milestone_baseline_total", float64(baseline), "mode", mode, "milestone", milestone)
			sample.Counter = true
			samples = append(samples, sample)
			add("last_job_milestone_timestamp_seconds", float64(last)/1e9, "mode", mode, "milestone", milestone)
		}
		var created sql.NullString
		if err = tx.QueryRowContext(ctx, "SELECT min(nullif(json_extract(payload,'$.created_at'),'')) FROM jobs WHERE mode=? AND state!='COMPLETE'", mode).Scan(&created); err != nil {
			return nil, err
		}
		stamp := float64(0)
		if created.Valid {
			createdAt, err := time.Parse(time.RFC3339Nano, created.String)
			if err != nil {
				return nil, err
			}
			stamp = float64(createdAt.UnixNano()) / 1e9
		}
		add("oldest_pending_job_created_timestamp_seconds", stamp, "mode", mode)
	}
	// Unknown state/mode is a journal problem, never an unbounded label.
	var invalid int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE mode NOT IN ('archive','late') OR state NOT IN ('FENCING','EXPORTING','EXPORTED','RECONCILED','BUILT','COMMITTED','REMOTE_VERIFIED','ROUTED','DELETING','DELETE_REQUESTED','COMPLETE')`).Scan(&invalid); err != nil {
		return nil, err
	}
	if invalid > 0 {
		return nil, fmt.Errorf("unknown job mode/state in journal")
	}
	waits := map[string]map[string]int{}
	for _, mode := range observability.Modes {
		waits[mode] = map[string]int{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,mode,state,day FROM jobs WHERE state!='COMPLETE'`)
	if err != nil {
		return nil, err
	}
	type pending struct{ id, mode, state, day string }
	var pendingJobs []pending
	for rows.Next() {
		var pendingJob pending
		if err = rows.Scan(&pendingJob.id, &pendingJob.mode, &pendingJob.state, &pendingJob.day); err != nil {
			rows.Close()
			return nil, err
		}
		pendingJobs = append(pendingJobs, pendingJob)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, pendingJob := range pendingJobs {
		reason := ""
		switch {
		case options.CoordinateOnly:
			reason = "coordinate_only"
		case pendingJob.id == options.RetryID && now.Before(options.RetryAt):
			reason = "retry_backoff"
		case pendingJob.mode == "archive" && (pendingJob.state == "ROUTED" || pendingJob.state == "DELETING") && !options.DeletionEnabled:
			reason = "deletion_disabled"
		case pendingJob.state == "FENCING":
			var count int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM permits p,json_each(p.days) d WHERE p.state='active' AND p.expires_at > ? AND d.value=?`, now.Add(-WriteExpiryGrace).UnixNano(), pendingJob.day).Scan(&count); err != nil {
				return nil, err
			}
			if count > 0 {
				reason = "writers"
			}
		case pendingJob.state == "ROUTED" && pendingJob.mode == "archive":
			_, end, err := Bounds(pendingJob.day)
			if err != nil {
				return nil, err
			}
			var count int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM readers WHERE frontier<? AND expires_at>?`, end.UnixNano(), now.Add(-ReadExpiryGrace).UnixNano()).Scan(&count); err != nil {
				return nil, err
			}
			if count > 0 {
				reason = "readers"
			}
		}
		if reason != "" {
			waits[pendingJob.mode][reason]++
		}
	}
	for _, mode := range observability.Modes {
		for _, reason := range observability.Reasons {
			add("jobs_waiting", float64(waits[mode][reason]), "mode", mode, "reason", reason)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return samples, nil
}
