// worker.go is one archival day: export, reconcile, build, commit, verify,
// delete, and confirm the hot interval is empty.

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// Worker holds the stores used by every stage of one job. Build and comparison
// thread counts are copied onto short-lived DuckDB sessions so they do not
// change normalization.
type Worker struct {
	Store                          *control.Store // the job store
	Influx                         Influx         // the InfluxDB client
	Duck                           DuckDB         // the DuckDB session
	BuildThreads, BuildSortThreads int            // the number of threads to use for the build
	Catalog                        *Catalog       // the catalog client
	Bootstrap                      *Pelican       // the Pelican client
	BootstrapHead, BootstrapBefore string         // the bootstrap head and before
}

// Export writes the engine's line protocol for the job day to local Parquet.
func (w *Worker) Export(ctx context.Context, job *control.Job) error {
	if e := w.Influx.ValidateSource(ctx); e != nil {
		return e
	}
	return w.Influx.Export(ctx, job.Day, filepath.Join(job.Work, "export.parquet"))
}

// Reconcile keeps the highest sequence for each exported identity, then proves
// that result against a fresh Influx query of the same day.
func (w *Worker) Reconcile(ctx context.Context, job *control.Job) (err error) {
	profile := newOperationProfile(job, "reconcile", w.Duck)
	defer func() { profile.finish(job.Work, err) }()
	comparison := w.comparisonDuck()
	normalized := filepath.Join(job.Work, normalizedExportName)
	err = profile.measure("normalization", func() error {
		stats, e := w.Duck.DeduplicateExport(ctx, filepath.Join(job.Work, "export.parquet"), normalized)
		if e != nil {
			return e
		}
		return writeJSON(filepath.Join(job.Work, "export-dedup.json"), stats)
	})
	if err != nil {
		return err
	}
	query := filepath.Join(job.Work, "influx.parquet")
	if err = profile.measure("influx_extraction", func() error { return w.Influx.Query(ctx, job.Day, query) }); err != nil {
		return err
	}
	err = profile.compare(comparison, "exact_comparison", func(duck DuckDB) error {
		return duck.EqualReconciliation(ctx, normalized, query)
	})
	return err
}

// setup installs read-only HTTP secrets for the destination and, when present,
// the bootstrap warehouse. Tokens are not written to profiles or journals.
func (w *Worker) setup() (string, error) {
	tok, e := w.Catalog.IO.token(false, 6*time.Hour+time.Minute)
	if e != nil {
		return "", e
	}
	setup := "LOAD httpfs; LOAD iceberg; CREATE SECRET archival_read (TYPE http, SCOPE " + Q(w.Catalog.Warehouse) + ", BEARER_TOKEN " + Q(tok) + w.Duck.testHTTP + ");"
	if w.Bootstrap != nil {
		token, e := w.Bootstrap.token(false, 6*time.Hour+time.Minute)
		if e != nil {
			return "", e
		}
		setup += "CREATE SECRET bootstrap_read (TYPE http,SCOPE " + Q(w.Bootstrap.Base.String()) + ",BEARER_TOKEN " + Q(token) + w.Duck.testHTTP + ");"
	}
	return setup, nil
}

// Scan reads one UTC day from a pinned Iceberg metadata file.
func (w *Worker) Scan(metadata, day string) string {
	a, b, _ := control.Bounds(day)
	return "SELECT * FROM iceberg_scan(" + Q(metadata) + ") WHERE time >= TIMESTAMPTZ_NS " + Q(a.Format("2006-01-02T15:04:05Z")) + " AND time < TIMESTAMPTZ_NS " + Q(b.Format("2006-01-02T15:04:05Z"))
}

// comparisonDuck changes only fresh exact proof commands. Normalization and
// non-comparison work retain the base session; build has its own override.
func (w *Worker) comparisonDuck() DuckDB {
	duck := w.Duck
	duck.Threads = 16
	return duck
}

// buildDuck clones the base session independently of fresh-proof settings.
func (w *Worker) buildDuck() (DuckDB, error) {
	duck := w.Duck
	duck.Threads = w.BuildThreads
	if duck.Threads == 0 {
		duck.Threads = 16
	}
	duck.SortThreads = w.BuildSortThreads
	if duck.SortThreads == 0 {
		duck.SortThreads = 2
	}
	if duck.Threads < 1 || duck.Threads > 256 || duck.SortThreads < 1 || duck.SortThreads > 256 {
		return DuckDB{}, fmt.Errorf("build threads and sort threads must be 1..256")
	}
	return duck, nil
}

// Build materializes the day's expected rows, packs them, and records the
// remote object names. A persisted files.json is reused after a crash. Bootstrap
// and late modes let newer rows replace rows already in the remote snapshot.
func (w *Worker) Build(ctx context.Context, job *control.Job) error {
	duck, e := w.buildDuck()
	if e != nil {
		return e
	}
	manifest := filepath.Join(job.Work, "files.json")
	// A prior build is reusable only after its complete manifest was persisted.
	if b, e := os.ReadFile(manifest); e == nil {
		var manifest struct {
			Files  []string `json:"files"`
			Source string   `json:"source"`
		}
		if e = json.Unmarshal(b, &manifest); e != nil {
			return e
		}
		job.Files = manifest.Files
		job.SourceMetadata = manifest.Source
		return nil
	}
	if job.Mode == "archive" {
		if _, e := os.Stat(filepath.Join(job.Work, normalizedExportName)); errors.Is(e, os.ErrNotExist) {
			return control.ErrExportNeedsReconcile
		} else if e != nil {
			return e
		}
	}
	expected := filepath.Join(job.Work, "expected.parquet")
	setup := ""
	source := "SELECT * EXCLUDE(seq) FROM (" + Typed(filepath.Join(job.Work, normalizedExportName)) + ")"
	if job.Mode == "archive" && w.Bootstrap != nil && job.Day < w.BootstrapBefore {
		job.SourceMetadata = w.BootstrapHead
		setup, e = w.setup()
		if e != nil {
			return e
		}
		metadata, err := w.Bootstrap.ReadFile(job.SourceMetadata)
		if err != nil {
			return err
		}
		var header struct {
			Properties map[string]string `json:"properties"`
		}
		if err = json.Unmarshal(metadata, &header); err != nil {
			return err
		}
		encoding := header.Properties[ValueEncodingProperty]
		if encoding != "" && encoding != ValueEncoding {
			return fmt.Errorf("unsupported bootstrap value encoding %q", encoding)
		}
		bootstrap := w.Scan(job.SourceMetadata, job.Day)
		if encoding == "" {
			// Legacy native float/int/string values remain readable. A decimal
			// without the convention marker must never acquire unsigned meaning.
			bootstrap = "SELECT * REPLACE (CASE WHEN starts_with(variant_typeof(value),'DECIMAL') THEN error('unmarked bootstrap decimal has no source-kind convention') ELSE value END AS value) FROM (" + bootstrap + ")"
		}
		base := "SELECT *,0::BIGINT AS seq FROM (" + bootstrap + ")"
		hot := "SELECT * REPLACE (1::BIGINT AS seq) FROM (" + Typed(filepath.Join(job.Work, normalizedExportName)) + ")"
		source = "SELECT * EXCLUDE(seq,rn) FROM (SELECT *,row_number() OVER (PARTITION BY time,name," + IdentityTags() + " ORDER BY seq DESC) AS rn FROM ((" + base + ") UNION ALL BY NAME (" + hot + "))) WHERE rn=1"
	}
	if job.Mode == "late" {
		records, e := w.Store.LateRecords(job.LateIDs)
		if e != nil {
			return e
		}
		f, e := os.Create(filepath.Join(job.Work, "late.ndjson"))
		if e != nil {
			return e
		}
		enc := json.NewEncoder(f)
		for i, r := range records {
			if e = enc.Encode(struct {
				control.Record
				Seq int `json:"seq"`
			}{r, i + 1}); e != nil {
				f.Close()
				return e
			}
		}
		if e = f.Close(); e != nil {
			return e
		}
		head, e := w.Store.Metadata()
		if e != nil {
			return e
		}
		if head == "" {
			return errors.New("late job has no active remote snapshot")
		}
		setup, e = w.setup()
		if e != nil {
			return e
		}
		source = "SELECT * EXCLUDE(seq,rn) FROM (SELECT *,row_number() OVER (PARTITION BY time,name," + IdentityTags() + " ORDER BY seq DESC) AS rn FROM ((" + w.Scan(head, job.Day) + ") UNION ALL BY NAME (" + Typed(filepath.Join(job.Work, "late.ndjson")) + "))) WHERE rn=1"
		// UNION BY NAME makes the base's seq NULL. Explicitly set it to zero so
		// queued records always win even if null ordering defaults change.
		source = strings.Replace(source, "("+w.Scan(head, job.Day)+")", "(SELECT *,0::BIGINT AS seq FROM ("+w.Scan(head, job.Day)+"))", 1)
	}
	chronological := job.Mode == "archive" && !(w.Bootstrap != nil && job.Day < w.BootstrapBefore)
	if _, e = duck.MaterializeExpected(ctx, source, expected, setup, chronological); e != nil {
		return e
	}
	dayPrefix, e := w.Catalog.DayPrefix(ctx, job.Day)
	if e != nil {
		return e
	}
	locals, _, e := duck.PackExpected(ctx, expected, job.Work, job.ID)
	if e != nil {
		return e
	}
	paths := make([]string, len(locals))
	for i, p := range locals {
		paths[i] = dayPrefix + "/" + filepath.Base(p)
	}
	job.Files = paths
	return writeJSON(manifest, map[string]any{"files": paths, "source": job.SourceMetadata})
}

// Commit uploads the packed files and appends an Iceberg snapshot. A retry
// adopts the snapshot already marked with this job instead of uploading again.
func (w *Worker) Commit(ctx context.Context, job *control.Job) (string, error) {
	// On uncertain commit, inspect the authoritative catalog before re-uploading.
	t, e := w.Catalog.Catalog.LoadTable(ctx, ident)
	if e != nil {
		return "", e
	}
	if t.Metadata().Properties()["sage.last.job"] == job.ID {
		return t.MetadataLocation(), nil
	}
	for _, remote := range job.Files {
		if e = w.Catalog.IO.Upload(ctx, filepath.Join(job.Work, filepath.Base(remote)), remote); e != nil {
			return "", e
		}
	}
	return w.Catalog.Commit(ctx, job)
}

// Verify re-reads the remote objects and proves them against the local expected
// file. The routing hint is published only from COMMITTED, before deletion.
func (w *Worker) Verify(ctx context.Context, job *control.Job) (err error) {
	profile := newOperationProfile(job, "verify", w.Duck)
	defer func() { profile.finish(job.Work, err) }()
	comparison := w.comparisonDuck()
	if err = profile.measure("object_readback", func() error {
		for _, p := range job.Files {
			if e := w.Catalog.IO.VerifyObject(ctx, filepath.Join(job.Work, filepath.Base(p)), p); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		return err
	}
	setup := ""
	if err = profile.measure("read_setup", func() error { var e error; setup, e = w.setup(); return e }); err != nil {
		return err
	}
	if err = profile.compare(comparison, "exact_comparison", func(duck DuckDB) error {
		return duck.EqualRemote(ctx, filepath.Join(job.Work, "expected.parquet"), w.Scan(job.Metadata, job.Day), setup)
	}); err != nil {
		return err
	}
	if job.State == "COMMITTED" {
		if err = profile.measure("publish_hint", func() error { return w.Catalog.PublishHint(ctx, job.Metadata) }); err != nil {
			return err
		}
	}
	err = profile.measure("verification_journal", func() error {
		hashes := map[string]string{}
		for _, remote := range job.Files {
			hash, e := digest(filepath.Join(job.Work, filepath.Base(remote)))
			if e != nil {
				return e
			}
			hashes[remote] = hash
		}
		expectedHash, e := digest(filepath.Join(job.Work, "expected.parquet"))
		if e != nil {
			return e
		}
		return writeJSON(filepath.Join(job.Work, "verification-"+job.State+".json"), map[string]any{
			"job": job.ID, "day": job.Day, "metadata": job.Metadata, "source_metadata": job.SourceMetadata,
			"source_bucket_id": w.Influx.BucketID, "file_sha256": hashes, "expected_parquet_sha256": expectedHash,
			"full_multiset_equal": true, "hint_published": job.State == "COMMITTED",
		})
	})
	return err
}

// Delete asks Influx to drop the verified day. The caller has already reverified.
func (w *Worker) Delete(ctx context.Context, job *control.Job) (err error) {
	profile := newOperationProfile(job, "delete", w.Duck)
	defer func() { profile.finish(job.Work, err) }()
	if err = profile.measure("source_validation", func() error { return w.Influx.ValidateSource(ctx) }); err != nil {
		return err
	}
	err = profile.measure("delete_request", func() error { return w.Influx.Delete(ctx, job.Day) })
	return err
}

// ConfirmEmpty queries the day again and accepts only a zero-row result.
func (w *Worker) ConfirmEmpty(ctx context.Context, job *control.Job) error {
	path := filepath.Join(job.Work, "after-delete.parquet")
	if e := w.Influx.Query(ctx, job.Day, path); e != nil {
		return e
	}
	reader, e := file.OpenParquetFile(path, false)
	if e != nil {
		return e
	}
	defer reader.Close()
	// An empty result is still a Parquet file with a footer, so size is not a row count.
	if reader.NumRows() != 0 {
		return errors.New("Influx day is not empty after deletion")
	}
	return nil
}
