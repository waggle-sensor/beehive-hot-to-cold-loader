// Command beehive-hot-to-cold-loader copies one Influx bucket into an isolated
// Iceberg v3 lakehouse. Deletion stays off until writers and readers are
// coordinated and the operator enables it explicitly.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"beehive-hot-to-cold-loader/cmd/backup"
	"beehive-hot-to-cold-loader/internal/archive"
	"beehive-hot-to-cold-loader/internal/control"
	"beehive-hot-to-cold-loader/internal/observability"
	"beehive-hot-to-cold-loader/internal/statebackup"
	"beehive-hot-to-cold-loader/internal/storage"
)

// env returns the process variable, or d when that variable is unset or empty.
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// envBool returns the process variable as a bool, or d when unset or empty.
func envBool(k string, d bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	switch strings.ToLower(v) {
	case "1", "t", "true", "yes":
		return true
	case "0", "f", "false", "no":
		return false
	default:
		log.Fatalf("%s must be a boolean, got %q", k, v)
		return d
	}
}

// envDuration returns the process variable as a duration, or d when unset or empty.
func envDuration(k string, d time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("%s must be a duration, got %q: %v", k, v, err)
	}
	return parsed
}

//main function
func main() {

	// handle state-backup command
	if len(os.Args) > 1 && os.Args[1] == "state-backup" {
		if err := backup.Run(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		return
	}

	// handle main command
	if e := run(); e != nil {
		log.Fatal(e)
	}
}

// run checks the deployment gates, holds the singleton lock, and advances one
// recoverable job step per tick. Late corrections precede new days. A routed
// archive day is deleted only after its old readers have drained.
func run() error {
	addr := flag.String("addr", env("HOT_TO_COLD_ADDR", ":10001"), "internal coordination listen address")
	state := flag.String("state", env("HOT_TO_COLD_STATE", "./state"), "durable private state and staging directory")
	lakehouse := flag.String("lakehouse", env("COLD_LAKEHOUSE", ""), "isolated https://host/sage/cold/lakehouse")
	namespace := flag.String("namespace", env("COLD_NAMESPACE", storage.DefaultNamespace), "Iceberg namespace")
	tableName := flag.String("table-name", env("COLD_TABLE_NAME", storage.DefaultTableName), "Iceberg table name")
	start := flag.String("archive-start", env("HOT_TO_COLD_START", ""), "explicit first UTC day covered; history before it is not assumed")
	archiveBefore := flag.String("archive-before", "", "optional exclusive UTC day to bound validation/backfill scheduling")
	readLeaseDuration := flag.Duration("read-lease-duration", envDuration("HOT_TO_COLD_READ_LEASE_DURATION", control.DefaultReadLeaseDuration), "renewable read lease lifetime; 1m deletion grace follows unrenewed expiry (positive, at most 24h)")
	writeLeaseDuration := flag.Duration("write-lease-duration", envDuration("HOT_TO_COLD_WRITE_LEASE_DURATION", control.DefaultWriteLeaseDuration), "renewable writer lease lifetime; 1m drain grace follows expiry (positive, at most 24h)")
	retention := flag.Duration("hot-retention", envDuration("HOT_TO_COLD_HOT_RETENTION", 720*time.Hour), "minimum hot age; complete UTC days, default 30 days")
	interval := flag.Duration("interval", envDuration("HOT_TO_COLD_INTERVAL", time.Hour), "archival scheduler interval")
	lateInterval := flag.Duration("late-interval", envDuration("HOT_TO_COLD_LATE_INTERVAL", 5*time.Minute), "late correction scheduler interval")
	profileRetentionText := flag.String("profile-retention", env("HOT_TO_COLD_PROFILE_RETENTION", "720h"), "performance profile retention after completion; 0 disables expiry")
	profileSteps := flag.Bool("profile-steps", false, "record per-attempt wall/CPU timings and process-lifetime RSS; disabled by default")
	keepStaging := flag.Bool("keep-staging", false, "retain transient job data after completion for debugging")
	coordinateOnly := flag.Bool("coordinate-only", envBool("HOT_TO_COLD_COORDINATE_ONLY", false), "serve loader/read coordination without fencing, export, publication or deletion")
	deleteEnabled := flag.Bool("delete-enabled", envBool("HOT_TO_COLD_DELETE_ENABLED", false), "delete verified/routed Influx days; requires fenced deployment")
	coordinated := flag.Bool("writers-coordinated", envBool("HOT_TO_COLD_WRITERS_COORDINATED", false), "all bucket writers enforce permits and bypass access is restricted")
	readerCoordinated := flag.Bool("readers-coordinated", envBool("HOT_TO_COLD_READERS_COORDINATED", false), "all API readers use managed views; no static hot reader remains")
	influxURL := flag.String("influx-url", env("INFLUX_URL", "http://localhost:8086"), "Influx source URL")
	bucket := flag.String("bucket", env("INFLUX_BUCKET", "waggle"), "Influx bucket")
	org := flag.String("org", env("INFLUX_ORG", "waggle"), "Influx organization")
	bucketID := flag.String("bucket-id", env("INFLUX_BUCKET_ID", ""), "source bucket ID for engine export")
	engine := flag.String("engine-path", env("INFLUX_ENGINE_PATH", ""), "read-only mount of Influx engine")
	influxd := flag.String("influxd", env("INFLUXD_BINARY", "influxd"), "matching pinned Influx engine export executable")
	duckdb := flag.String("duckdb", env("DUCKDB_BINARY", "duckdb"), "DuckDB 2.0.x CLI executable")
	duckdbMemory := flag.String("duckdb-memory", env("DUCKDB_MEMORY", "3GB"), "DuckDB buffer memory limit; leave headroom for process/container overhead")
	buildThreads := flag.Int("duckdb-build-threads", 16, "DuckDB threads for build materialization and exact comparison (1..256)")
	buildSortThreads := flag.Int("duckdb-build-sort-threads", 2, "DuckDB threads for build batch sort/copy (1..256)")
	key := flag.String("pelican-key", env("PELICAN_KEY_PATH", ""), "ES256 signing key path")
	resolveOwner := flag.String("resolve-stopped-owner", "", "offline recovery: clear grants only after owner is confirmed terminated and requests drained")
	confirmStopped := flag.Bool("confirm-owner-stopped", false, "confirm owner termination before offline grant resolution")
	bootstrapLakehouse := flag.String("bootstrap-lakehouse", "", "optional previously audited read-only lakehouse")
	bootstrapBefore := flag.String("bootstrap-before", "", "exclusive UTC date of audited source coverage; merge hot corrections before this date")
	flag.Parse()
	tableIdentity, tableErr := storage.NewTableIdentity(*namespace, *tableName)
	if tableErr != nil {
		return tableErr
	}
	profileRetention, err := time.ParseDuration(*profileRetentionText)
	if err != nil || profileRetention < 0 {
		return errors.New("profile-retention must be a nonnegative duration")
	}
	for _, threads := range []int{*buildThreads, *buildSortThreads} {
		if threads < 1 || threads > 256 {
			return errors.New("DuckDB thread counts must be 1..256")
		}
	}
	var before time.Time
	if *archiveBefore != "" {
		var err error
		before, _, err = control.Bounds(*archiveBefore)
		if err != nil {
			return err
		}
	}
	if *retention <= 0 || *interval <= 0 || *lateInterval <= 0 {
		return errors.New("positive retention/scheduling intervals required")
	}
	if *start == "" || *lakehouse == "" || (!*coordinateOnly && (*bucketID == "" || *engine == "")) {
		return errors.New("archive-start, lakehouse, bucket-id and engine-path are required")
	}
	if !*coordinated && !*coordinateOnly {
		return errors.New("fencing requires --writers-coordinated after all bucket writers are migrated")
	}
	if *coordinateOnly && *deleteEnabled {
		return errors.New("coordinate-only cannot enable deletion")
	}
	if *deleteEnabled && !*readerCoordinated {
		return errors.New("deletion requires --readers-coordinated")
	}
	tokens := []string{os.Getenv("HOT_TO_COLD_LOADER_TOKEN"), os.Getenv("HOT_TO_COLD_READER_TOKEN"), os.Getenv("HOT_TO_COLD_ADMIN_TOKEN")}
	for i, t := range tokens {
		if len(t) < 32 {
			return errors.New("three distinct coordination tokens of at least 32 bytes required")
		}
		for k := 0; k < i; k++ {
			if t == tokens[k] {
				return errors.New("loader, reader and admin tokens must differ")
			}
		}
	}

	if os.Getenv("INFLUX_TOKEN") == "" && !*coordinateOnly {
		return errors.New("INFLUX_TOKEN required")
	}
	root, e := filepath.Abs(*state)
	if e != nil {
		return e
	}
	for _, p := range strings.Split(root, string(filepath.Separator)) {
		if p == "node-data" {
			return errors.New("state cannot be in /node-data")
		}
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return e
	}
	if e = statebackup.StartupCheck(root); e != nil {
		return e
	}
	lock, e := os.OpenFile(filepath.Join(root, "writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return fmt.Errorf("archiver already active: %w", e)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if env("HOT_TO_COLD_BACKUP_SCRATCH", "") != "" {
		if _, err := os.Stat(filepath.Join(root, "control.db")); os.IsNotExist(err) {
			remote, err := storage.NewBackupRemote(*lakehouse, *key, env("HOT_TO_COLD_BACKUP_SCRATCH", ""), tableIdentity, env("HOT_TO_COLD_BACKUP_URL", ""), 30)
			if err != nil {
				return err
			}
			check, stop := context.WithTimeout(ctx, 30*time.Second)
			err = remote.RequireNewState(check)
			stop()
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	store, e := control.Open(filepath.Join(root, "control.db"), *bucket, *lakehouse, *start, *readLeaseDuration, *writeLeaseDuration)
	if e != nil {
		return e
	}
	defer store.Close()
	if err := tableIdentity.BindState(store); err != nil {
		return err
	}
	if *resolveOwner != "" {
		if !*confirmStopped {
			return errors.New("resolve-stopped-owner requires confirm-owner-stopped")
		}
		return store.ResolveOwner(*resolveOwner)
	}
	metrics := observability.New(*retention, *deleteEnabled, *coordinateOnly)
	var runner *archive.Runner
	if !*coordinateOnly {
		source := storage.Influx{URL: *influxURL, Org: *org, Bucket: *bucket, Token: os.Getenv("INFLUX_TOKEN"), Engine: *engine, BucketID: *bucketID, Binary: *influxd, Lock: lock}
		if e = source.ValidateSource(ctx); e != nil {
			return e
		}
		for key, value := range map[string]string{"source.bucket_id": *bucketID, "source.org": *org, "bootstrap.lakehouse": *bootstrapLakehouse, "bootstrap.before": *bootstrapBefore} {
			if e = store.Bind(key, value); e != nil {
				return e
			}
		}
		pelican, e := storage.NewPelican(*lakehouse, *key, root, tableIdentity)
		if e != nil {
			return e
		}
		cat, e := storage.OpenCatalog(ctx, filepath.Join(root, "catalog.db"), *lakehouse, pelican)
		if e != nil {
			return e
		}
		defer cat.DB.Close()
		duck := storage.DuckDB{Binary: *duckdb, Work: root, Memory: *duckdbMemory, Lock: lock}
		if e = duck.Check(ctx); e != nil {
			return e
		}
		if e = cat.Init(ctx); e != nil {
			return e
		}
		worker := &storage.Worker{Store: store, Catalog: cat, Duck: duck, Influx: source, BuildThreads: *buildThreads, BuildSortThreads: *buildSortThreads}
		if *bootstrapLakehouse != "" || *bootstrapBefore != "" {
			if *bootstrapLakehouse == "" || *bootstrapBefore == "" {
				return errors.New("bootstrap-lakehouse and bootstrap-before are required together")
			}
			if _, _, e = control.Bounds(*bootstrapBefore); e != nil {
				return e
			}
			if *bootstrapBefore <= *start {
				return errors.New("bootstrap-before must follow archive-start")
			}
			if *bootstrapLakehouse == *lakehouse {
				return errors.New("bootstrap lakehouse must differ from destination")
			}
			worker.Bootstrap, e = storage.NewPelicanReader(*bootstrapLakehouse, *key, root, tableIdentity)
			if e != nil {
				return e
			}
			worker.BootstrapHead, e = store.Option("bootstrap.metadata")
			if e != nil {
				return e
			}
			if worker.BootstrapHead == "" {
				worker.BootstrapHead, e = worker.Bootstrap.Head()
				if e != nil {
					return e
				}
				if e = store.Bind("bootstrap.metadata", worker.BootstrapHead); e != nil {
					return e
				}
			}
			worker.BootstrapBefore = *bootstrapBefore
		}
		runner = &archive.Runner{Store: store, Worker: worker, Root: filepath.Join(root, "jobs"), Retention: *retention, DeleteEnabled: *deleteEnabled, KeepStaging: *keepStaging, ProfileSteps: *profileSteps, Metrics: metrics}
		if e = runner.CleanupCompleted(); e != nil {
			return e
		}
	}
	backupService := &control.BackupService{Store: store, Scratch: env("HOT_TO_COLD_BACKUP_SCRATCH", ""), StateRoot: root, Image: env("HOT_TO_COLD_BACKUP_IMAGE", ""), Parent: ctx, Boundary: make(chan func())}
	if backupService.Scratch != "" {
		scratch, err := statebackup.ResolvePath(backupService.Scratch)
		if err != nil {
			return err
		}
		actual, err := filepath.EvalSymlinks(root)
		if err != nil {
			return err
		}
		if err = statebackup.Disjoint(actual, scratch); err != nil {
			return err
		}
		keyPath, err := filepath.EvalSymlinks(*key)
		if err != nil {
			return err
		}
		if err = statebackup.Disjoint(actual, keyPath); err != nil {
			return errors.New("backup signing key must be mounted outside the state directory")
		}
	}
	if e = backupService.LoadStatus(); e != nil {
		log.Printf("backup status unavailable; coordinator remains available: %v", e)
	}
	if backupService.Scratch != "" {
		keep := 30
		if text := os.Getenv("HOT_TO_COLD_BACKUP_KEEP"); text != "" {
			if keep, e = strconv.Atoi(text); e != nil {
				return e
			}
		}
		remote, err := storage.NewBackupRemote(*lakehouse, *key, backupService.Scratch, tableIdentity, env("HOT_TO_COLD_BACKUP_URL", ""), keep)
		if err != nil {
			return err
		}
		backupService.Remote = remote
	}
	metricsHandler := metrics.Handler(func(ctx context.Context) (observability.Snapshot, error) {
		id, at := metrics.Retrying()
		samples, err := store.MetricsSnapshot(ctx, control.SnapshotOptions{Retention: *retention, DeletionEnabled: *deleteEnabled, CoordinateOnly: *coordinateOnly, RetryID: id, RetryAt: at})
		if err != nil {
			return nil, err
		}
		return append(samples, backupService.Samples()...), nil
	})
	server := &http.Server{Addr: *addr, Handler: &control.Server{Store: store, LoaderToken: tokens[0], ReaderToken: tokens[1], AdminToken: tokens[2], MetricsHandler: metricsHandler, Retention: *retention, Backups: backupService}, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}
	errch := make(chan error, 1)
	go func() { errch <- server.ListenAndServe() }()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(c)
	}()
	log.Printf("archiver serving %s; retention=%s deletion=%t lakehouse=%s table=%s.%s", *addr, *retention, *deleteEnabled, *lakehouse, tableIdentity.Namespace(), tableIdentity.Name())
	archiveAt, lateAt, retryAt := time.Time{}, time.Time{}, time.Time{}
	maintenance := &archive.ProfileRetention{Runner: runner, Duration: profileRetention}
	var maintenanceAt time.Time
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		// Prefer an already waiting backup over another potentially long step.
		select {
		case runBackupCapture := <-backupService.Boundary:
			metrics.SchedulerIteration.Set(float64(time.Now().UnixNano()) / 1e9)
			runBackupCapture()
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return nil
		case e := <-errch:
			if errors.Is(e, http.ErrServerClosed) {
				return nil
			}
			return e
		case runBackupCapture := <-backupService.Boundary:
			metrics.SchedulerIteration.Set(float64(time.Now().UnixNano()) / 1e9)
			runBackupCapture()
		case now := <-tick.C:
			metrics.SchedulerIteration.Set(float64(now.UnixNano()) / 1e9)
			if !now.Before(retryAt) {
				metrics.Retry("", time.Time{})
			}
			if *coordinateOnly {
				continue
			}
			if !now.Before(maintenanceAt) {
				maintenanceCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				if _, err := maintenance.Prune(maintenanceCtx, now); err != nil {
					log.Printf("profile maintenance: %v", err)
				}
				stop()
				maintenanceAt = now.Add(24 * time.Hour)
			}
			operation := "pending"
			// Resume work already in the journal before admitting anything new.
			j, err := store.Pending(false)
			e = err
			if e == nil && j == nil && !now.Before(lateAt) {
				operation = "late"
				j, e = store.NextLate()
				lateAt = now.Add(*lateInterval)
			}
			if e == nil && j == nil && *deleteEnabled {
				operation = "deletions"
				pending, err := store.Pending(true)
				e = err
				if e == nil && pending != nil {
					operation = "readers"
					n, err := store.OldReaders(pending.Day)
					e = err
					if e == nil && n == 0 {
						j = pending
					}
				}
			}
			if e == nil && j == nil && !now.Before(archiveAt) {
				eligibleNow := now
				if !before.IsZero() && before.Add(*retention).Before(now) {
					eligibleNow = before.Add(*retention)
				}
				operation = "archive"
				j, e = store.Next(eligibleNow, *retention)
				archiveAt = now.Add(*interval)
			}
			if e != nil {
				metrics.SchedulerErrors.WithLabelValues(operation).Inc()
				log.Printf("scheduler: %v", e)
				continue
			}

			if j != nil {
				if now.Before(retryAt) {
					continue
				}
				stepCtx, end := context.WithTimeout(ctx, 6*time.Hour)
				e = runner.Step(stepCtx, j)
				end()
				if e == nil && j.Mode == "archive" && (j.State == "ROUTED" || j.State == "COMPLETE") {
					archiveAt = time.Time{}
				}
				if e != nil && !errors.Is(e, archive.ErrWaiting) {
					log.Printf("job %s day %s state %s: %v", j.ID, j.Day, j.State, e)
					retryAt = time.Now().Add(time.Minute)
					if errors.Is(e, control.ErrStagingCapacity) {
						retryAt = time.Now().Add(15 * time.Minute)
					}
					metrics.Retry(j.ID, retryAt)
				}
			}
		}
	}
}
