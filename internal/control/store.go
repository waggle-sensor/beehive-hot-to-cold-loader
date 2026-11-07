// Package control owns durable admission, read views, and archival journals.
// Writer and reader grants have persisted, renewable lifetimes.
package control

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// ErrExportNeedsReconcile requests a safe rewind while the day stays fenced.
var ErrExportNeedsReconcile = errors.New("export must be normalized and reconciled before building")

// ErrStagingCapacity means this attempt stops and the export can be reused
// after space is freed. The day stays fenced.
var ErrStagingCapacity = errors.New("archival staging capacity exhausted")

// Record is one late or quarantined point. Value text matches the five kinds.
type Record struct {
	Time  int64             `json:"time"`
	Name  string            `json:"name"`
	Tags  map[string]string `json:"tags"`
	Kind  string            `json:"kind"`
	Value string            `json:"value"`
}

// Validate checks the kind, a finite value, and non-empty tag keys and values.
func (r Record) Validate() error {
	if r.Name == "" || r.Tags == nil {
		return errors.New("measurement and tags required")
	}
	var e error
	switch r.Kind {
	case "float":
		var v float64
		v, e = strconv.ParseFloat(r.Value, 64)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("nonfinite value")
		}
	case "int":
		_, e = strconv.ParseInt(r.Value, 10, 64)
	case "uint":
		_, e = strconv.ParseUint(r.Value, 10, 64)
	case "bool":
		if r.Value != "true" && r.Value != "false" {
			return errors.New("invalid bool")
		}
	case "string":
	default:
		return errors.New("invalid value kind")
	}
	if e != nil {
		return e
	}
	for k, v := range r.Tags {
		if k == "" || v == "" {
			return errors.New("empty tag key/value is not an Influx tag")
		}
	}

	return nil
}

// Day is the UTC calendar day containing ns.
func Day(ns int64) string { return time.Unix(0, ns).UTC().Format("2006-01-02") }

// Bounds returns the half-open UTC interval [day, next day).
func Bounds(day string) (time.Time, time.Time, error) {
	dayStart, e := time.Parse("2006-01-02", day)
	if e != nil {
		return dayStart, dayStart, e
	}
	nextDay := dayStart.AddDate(0, 0, 1)
	if !time.Unix(0, dayStart.UnixNano()).Equal(dayStart) || !time.Unix(0, nextDay.UnixNano()).Equal(nextDay) {
		return dayStart, nextDay, errors.New("day outside nanosecond timestamp range")
	}
	return dayStart, nextDay, nil
}

// View is the snapshot a reader acquired. Generation changes when the frontier moves.
const DefaultReadLeaseDuration = 30 * time.Minute

// ReadExpiryGrace lets canceled reads drain before hot deletion proceeds.
const ReadExpiryGrace = time.Minute

type View struct {
	ExpiresAt     string `json:"expires_at"`
	ID            string `json:"id"`             // the ID of the view
	Warehouse     string `json:"warehouse"`      // the warehouse of the view
	Metadata      string `json:"metadata"`       // the metadata of the view
	Frontier      string `json:"frontier"`       // the frontier of the view
	CoverageStart string `json:"coverage_start"` // the coverage start of the view
	Generation    int64  `json:"generation"`     // the generation of the view
}

// Job is the durable archival journal. State names the step that will run next.
type Job struct {
	CreatedAt      string   `json:"created_at"`                // the created at of the job
	Cutoff         string   `json:"cutoff,omitempty"`          // the cutoff of the job
	Retention      string   `json:"retention,omitempty"`       // the retention of the job
	SourceMetadata string   `json:"source_metadata,omitempty"` // the source metadata of the job
	ID             string   `json:"id"`                        // the ID of the job
	Day            string   `json:"day"`                       // the day of the job
	Mode           string   `json:"mode"`                      // the mode of the job
	State          string   `json:"state"`                     // the state of the job
	Metadata       string   `json:"metadata"`                  // the metadata of the job
	Work           string   `json:"work"`                      // the work of the job
	Files          []string `json:"files"`                     // the files of the job
	OldFiles       []string `json:"old_files"`                 // the old files of the job
	LateIDs        []int64  `json:"late_ids"`                  // the late IDs of the job
	Error          string   `json:"error"`                     // the error of the job
}

// Store is the SQLite journal. Read deadlines persist across restarts.
type Store struct {
	DB                 *sql.DB
	Bucket, Warehouse  string
	readLeaseDuration  time.Duration
	writeLeaseDuration time.Duration
	now                func() time.Time
}

// Open binds persistent identity. Optional durations configure read then write leases.
func Open(path, bucket, warehouse, start string, leaseDurations ...time.Duration) (*Store, error) {
	ttl := DefaultReadLeaseDuration
	if len(leaseDurations) > 0 {
		ttl = leaseDurations[0]
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		return nil, errors.New("read lease duration must be positive and at most 24h")
	}
	writeTTL := DefaultWriteLeaseDuration
	if len(leaseDurations) > 1 {
		writeTTL = leaseDurations[1]
	}
	if writeTTL <= 0 || writeTTL > 24*time.Hour {
		return nil, errors.New("write lease duration must be positive and at most 24h")
	}
	if bucket == "" {
		return nil, errors.New("bucket required")
	}
	dayStart, _, e := Bounds(start)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	fail := func(e error) (*Store, error) { db.Close(); return nil, e }
	for _, q := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `PRAGMA busy_timeout=10000`,
		`CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK(id=1),bucket TEXT,warehouse TEXT,start INTEGER,frontier INTEGER,metadata TEXT,generation INTEGER)`,
		`CREATE TABLE IF NOT EXISTS options (key TEXT PRIMARY KEY,value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS permits (id TEXT PRIMARY KEY,owner TEXT NOT NULL,days TEXT NOT NULL,state TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,payload TEXT NOT NULL,day TEXT NOT NULL,done INTEGER NOT NULL DEFAULT 0,accepted_at INTEGER NOT NULL DEFAULT (unixepoch()))`,
		`CREATE TABLE IF NOT EXISTS inbox_requests (id TEXT PRIMARY KEY,payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS quarantine (id TEXT PRIMARY KEY,payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS readers (id TEXT PRIMARY KEY,owner TEXT NOT NULL,frontier INTEGER NOT NULL,generation INTEGER NOT NULL,payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY,day TEXT NOT NULL,mode TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS permits_by_state ON permits(state)`,
		`CREATE INDEX IF NOT EXISTS inbox_pending_seq ON inbox(done,seq)`,
		`CREATE INDEX IF NOT EXISTS inbox_by_day ON inbox(day,done,seq)`,
		`CREATE INDEX IF NOT EXISTS jobs_by_day ON jobs(day,mode,state)`,
		`DROP INDEX IF EXISTS active_day`,
		`CREATE UNIQUE INDEX active_day ON jobs(day) WHERE state NOT IN ('COMPLETE','ROUTED','DELETING','DELETE_REQUESTED')`,
		`CREATE TABLE IF NOT EXISTS days (day TEXT PRIMARY KEY,files TEXT NOT NULL,metadata TEXT NOT NULL)`,
	} {
		if _, e = db.Exec(q); e != nil {
			return fail(e)
		}
	}
	if _, e = db.Exec(`INSERT OR IGNORE INTO config VALUES(1,?,?,?,?,'',0)`, bucket, warehouse, dayStart.UnixNano(), dayStart.UnixNano()); e != nil {
		return fail(e)
	}
	var storedBucket, storedWarehouse string
	var storedStart int64
	if e = db.QueryRow(`SELECT bucket,warehouse,start FROM config WHERE id=1`).Scan(&storedBucket, &storedWarehouse, &storedStart); e != nil {
		return fail(e)
	}
	if storedBucket != bucket || storedWarehouse != warehouse || storedStart != dayStart.UnixNano() {
		return fail(errors.New("durable configuration differs; do not reuse state for another bucket/warehouse/start"))
	}
	// Add a persisted reader deadline. Legacy readers
	// receive one bounded migration window; reopening never extends it.
	columns, e := db.Query("PRAGMA table_info(readers)")
	if e != nil {
		return fail(e)
	}
	hasExpiry := false
	for columns.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if e = columns.Scan(&cid, &name, &typ, &notnull, &def, &pk); e != nil {
			columns.Close()
			return fail(e)
		}
		if name == "expires_at" {
			hasExpiry = true
		}
	}
	e = columns.Err()
	columns.Close()
	if e != nil {
		return fail(e)
	}
	if !hasExpiry {
		if _, e = db.Exec("ALTER TABLE readers ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0"); e != nil {
			return fail(e)
		}
	}
	if _, e = db.Exec("UPDATE readers SET expires_at=? WHERE expires_at=0", time.Now().Add(ttl).UnixNano()); e != nil {
		return fail(e)
	}
	store := &Store{DB: db, Bucket: bucket, Warehouse: warehouse, readLeaseDuration: ttl, writeLeaseDuration: writeTTL, now: time.Now}
	if e = store.migrateWriteLeases(); e != nil {
		return fail(e)
	}
	if e = store.migrateMilestones(); e != nil {
		return fail(e)
	}
	return store, nil
}

// Close releases the SQLite journal. Call it after the scheduler has stopped.
func (s *Store) Close() error { return s.DB.Close() }

// View pins the current metadata for a reader. Repeating the same id returns that pin.
func (s *Store) View(owner, id string) (View, error) {
	v := View{ID: id, Warehouse: s.Warehouse}
	if id == "" || owner == "" {
		return v, errors.New("read id and owner required")
	}
	tx, e := s.DB.Begin()
	if e != nil {
		return v, e
	}
	defer tx.Rollback()
	var start, frontier int64
	e = tx.QueryRow(`SELECT start,frontier,metadata,generation FROM config WHERE id=1`).Scan(&start, &frontier, &v.Metadata, &v.Generation)
	if e != nil {
		return v, e
	}
	// Retrying an acquire returns its original pinned snapshot.
	var previous, previousOwner string
	var expiry int64
	e = tx.QueryRow("SELECT owner,payload,expires_at FROM readers WHERE id=?", id).Scan(&previousOwner, &previous, &expiry)
	if e == nil {
		if previousOwner != owner {
			return v, errors.New("read id belongs to another owner")
		}
		if !s.now().Before(time.Unix(0, expiry)) {
			return v, errors.New("read grant expired; acquire a new view")
		}
		e = json.Unmarshal([]byte(previous), &v)
		v.ExpiresAt = time.Unix(0, expiry).UTC().Format(time.RFC3339Nano)
		return v, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return v, e
	}

	v.Frontier = time.Unix(0, frontier).UTC().Format(time.RFC3339Nano)
	v.CoverageStart = time.Unix(0, start).UTC().Format(time.RFC3339Nano)
	expiry = s.now().Add(s.readLeaseDuration).UnixNano()
	v.ExpiresAt = time.Unix(0, expiry).UTC().Format(time.RFC3339Nano)
	raw, _ := json.Marshal(v)
	_, e = tx.Exec(`INSERT INTO readers(id,owner,frontier,generation,payload,expires_at) VALUES(?,?,?,?,?,?)`, id, owner, frontier, v.Generation, string(raw), expiry)
	if e == nil {
		e = tx.Commit()
	}
	return v, e
}

// RenewRead extends a live reader's lease without changing its pinned view.
// An expired or released lease can never be resurrected, even during drain grace.
func (s *Store) RenewRead(owner, id string) (View, error) {
	var v View
	if owner == "" || id == "" {
		return v, errors.New("read id and owner required")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	var storedOwner, payload string
	var expiry int64
	if err = tx.QueryRow("SELECT owner,payload,expires_at FROM readers WHERE id=?", id).Scan(&storedOwner, &payload, &expiry); err != nil {
		return v, err
	}
	now := s.now()
	if storedOwner != owner || !now.Before(time.Unix(0, expiry)) {
		return v, errors.New("read lease expired or owner mismatch")
	}
	if err = json.Unmarshal([]byte(payload), &v); err != nil {
		return v, err
	}
	next := now.Add(s.readLeaseDuration).UnixNano()
	if next < expiry {
		next = expiry
	}
	v.ExpiresAt = time.Unix(0, next).UTC().Format(time.RFC3339Nano)
	raw, err := json.Marshal(v)
	if err != nil {
		return v, err
	}
	if _, err = tx.Exec("UPDATE readers SET expires_at=?,payload=? WHERE id=?", next, string(raw), id); err != nil {
		return v, err
	}
	return v, tx.Commit()
}

// ReleaseRead drops a reader's pin. The caller must have finished using that snapshot.
func (s *Store) ReleaseRead(id string) error {
	_, e := s.DB.Exec(`DELETE FROM readers WHERE id=?`, id)
	return e
}

// Permit grants owner the right to write days. Repeating an active id returns the same grant.
func (s *Store) Permit(id, owner string, days []string) ([]string, error) {
	if id == "" || owner == "" || len(days) == 0 {
		return nil, errors.New("permit id, owner and days required")
	}
	for _, day := range days {
		if _, _, e := Bounds(day); e != nil {
			return nil, e
		}
	}
	tx, e := s.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var old, oldOwner, state string
	var expiry int64
	e = tx.QueryRow(`SELECT days,owner,state,expires_at FROM permits WHERE id=?`, id).Scan(&old, &oldOwner, &state, &expiry)
	if e == nil {
		if oldOwner != owner || state != "active" || !s.now().Before(time.Unix(0, expiry)) {
			return nil, errors.New("permit unavailable")
		}
		var allowed []string
		e = json.Unmarshal([]byte(old), &allowed)
		return allowed, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var frontier int64
	if e = tx.QueryRow(`SELECT frontier FROM config WHERE id=1`).Scan(&frontier); e != nil {
		return nil, e
	}
	allowed := []string{}
	seen := map[string]bool{}
	for _, day := range days {
		if seen[day] {
			continue
		}
		seen[day] = true
		dayStart, _, _ := Bounds(day)
		if dayStart.UnixNano() < frontier {
			continue
		}
		var count int
		e = tx.QueryRow(`SELECT count(*) FROM jobs WHERE day=? AND mode='archive' AND state!='COMPLETE'`, day).Scan(&count)
		if e != nil {
			return nil, e
		}
		if count == 0 {
			allowed = append(allowed, day)
		}
	}
	data, _ := json.Marshal(allowed)
	_, e = tx.Exec(`INSERT INTO permits(id,owner,days,state,expires_at) VALUES(?,?,?,'active',?)`, id, owner, string(data), s.now().Add(s.writeLeaseDuration).UnixNano())
	if e == nil {
		e = tx.Commit()
	}
	return allowed, e
}

// Writers counts active grants that include day.
func (s *Store) Writers(day string) (int, error) {
	rows, e := s.DB.Query(`SELECT days FROM permits WHERE state='active' AND expires_at > ?`, s.now().Add(-WriteExpiryGrace).UnixNano())
	if e != nil {
		return 0, e
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var raw string
		if e = rows.Scan(&raw); e != nil {
			return 0, e
		}
		var days []string
		if e = json.Unmarshal([]byte(raw), &days); e != nil {
			return 0, e
		}
		for _, d := range days {
			if d == day {
				count++
			}
		}
	}
	return count, rows.Err()
}

// OldReaders counts readers still pinned to a snapshot from before day was archived.
func (s *Store) OldReaders(day string) (int, error) {
	_, end, e := Bounds(day)
	if e != nil {
		return 0, e
	}
	var count int
	e = s.DB.QueryRow(`SELECT count(*) FROM readers WHERE frontier < ? AND expires_at > ?`, end.UnixNano(), s.now().Add(-ReadExpiryGrace).UnixNano()).Scan(&count)
	return count, e
}

// Enqueue accepts a batch of late points for days already behind the frontier.
func (s *Store) Enqueue(id string, records []Record) error {
	if id == "" || len(records) == 0 {
		return errors.New("inbox id and records required")
	}
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	whole, err := json.Marshal(records)
	if err != nil {
		return err
	}
	var previous string
	err = tx.QueryRow("SELECT payload FROM inbox_requests WHERE id=?", id).Scan(&previous)
	if err == nil {
		if previous != string(whole) {
			return errors.New("inbox request retry differs")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, e = tx.Exec("INSERT INTO inbox_requests VALUES(?,?)", id, string(whole)); e != nil {
		return e
	}
	for i, r := range records {
		if e = r.Validate(); e != nil {
			return e
		}
		if r.Kind == "float" && strings.Contains(r.Value, "NaN") {
			return errors.New("nonfinite observation")
		}
		raw, _ := json.Marshal(r)
		key := fmt.Sprintf("%s/%d", id, i)
		var old string
		e = tx.QueryRow(`SELECT payload FROM inbox WHERE id=?`, key).Scan(&old)
		if e == nil {
			if old != string(raw) {
				return errors.New("inbox retry payload differs")
			}
			continue
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		// An unpermitted hot day cannot silently be redirected by a buggy loader.
		dayStart, _, _ := Bounds(Day(r.Time))
		var frontier int64
		tx.QueryRow(`SELECT frontier FROM config WHERE id=1`).Scan(&frontier)
		var count int
		if e = tx.QueryRow(`SELECT count(*) FROM jobs WHERE day=? AND mode='archive' AND state!='COMPLETE'`, Day(r.Time)).Scan(&count); e != nil {
			return e
		}
		if dayStart.UnixNano() >= frontier && count == 0 {
			return errors.New("day is neither fenced nor cold")
		}
		if _, e = tx.Exec(`INSERT INTO inbox(id,payload,day) VALUES(?,?,?)`, key, string(raw), Day(r.Time)); e != nil {
			return e
		}
	}
	return tx.Commit()
}

// Quarantine stores one payload that could not be applied, keyed for idempotent retry.
func (s *Store) Quarantine(id, payload string) error {
	if id == "" {
		return errors.New("quarantine id required")
	}
	var old string
	e := s.DB.QueryRow(`SELECT payload FROM quarantine WHERE id=?`, id).Scan(&old)
	if e == nil && old != payload {
		return errors.New("quarantine retry differs")
	}
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = s.DB.Exec(`INSERT OR IGNORE INTO quarantine VALUES(?,?)`, id, payload)
	return e
}

// Save persists the job payload and its state column together.
func (s *Store) Save(job *Job) error {
	raw, e := json.Marshal(job)
	if e != nil {
		return e
	}
	_, e = s.DB.Exec(`UPDATE jobs SET state=?,payload=? WHERE id=?`, job.State, string(raw), job.ID)
	return e
}

// Next returns an unfinished job, or opens the next UTC day that has aged past retention.
func (s *Store) Next(now time.Time, retention time.Duration) (*Job, error) {
	if retention <= 0 {
		return nil, errors.New("retention must be positive")
	}
	var raw string
	e := s.DB.QueryRow(`SELECT payload FROM jobs WHERE state NOT IN ('COMPLETE','ROUTED','DELETING','DELETE_REQUESTED') ORDER BY rowid LIMIT 1`).Scan(&raw)
	if e == nil {
		var job Job
		e = json.Unmarshal([]byte(raw), &job)
		return &job, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var frontier int64
	if e = s.DB.QueryRow(`SELECT frontier FROM config WHERE id=1`).Scan(&frontier); e != nil {
		return nil, e
	}
	day := Day(frontier)
	dayStart, end, _ := Bounds(day)
	if frontier != dayStart.UnixNano() {
		return nil, errors.New("frontier must be UTC midnight")
	}
	if end.After(now.Add(-retention)) {
		return nil, nil
	}
	job := &Job{ID: uuid.NewString(), Day: day, Mode: "archive", State: "FENCING",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Cutoff: now.Add(-retention).UTC().Format(time.RFC3339Nano), Retention: retention.String()}
	rawBytes, _ := json.Marshal(job)
	_, e = s.DB.Exec(`INSERT INTO jobs VALUES(?,?,?,?,?)`, job.ID, job.Day, job.Mode, job.State, string(rawBytes))
	return job, e
}

// NextLate groups pending inbox rows for one day into a late job.
func (s *Store) NextLate() (*Job, error) {
	var raw string
	e := s.DB.QueryRow(`SELECT payload FROM jobs WHERE state NOT IN ('COMPLETE','ROUTED','DELETING','DELETE_REQUESTED') LIMIT 1`).Scan(&raw)
	if e == nil {
		return nil, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var day string
	e = s.DB.QueryRow(`SELECT i.day FROM inbox i JOIN days d ON i.day=d.day WHERE i.done=0 ORDER BY i.seq LIMIT 1`).Scan(&day)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	job := &Job{ID: uuid.NewString(), Day: day, Mode: "late", State: "RECONCILED", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if e = s.DB.QueryRow(`SELECT files FROM days WHERE day=?`, day).Scan(&raw); e != nil {
		return nil, e
	}
	if e = json.Unmarshal([]byte(raw), &job.OldFiles); e != nil {
		return nil, e
	}
	rows, e := s.DB.Query(`SELECT seq,length(payload) FROM inbox WHERE day=? AND done=0 ORDER BY seq LIMIT 50000`, day)
	if e != nil {
		return nil, e
	}
	total := 0
	for rows.Next() {
		var id int64
		var size int
		if e = rows.Scan(&id, &size); e != nil {
			rows.Close()
			return nil, e
		}
		if total+size > 64<<20 && len(job.LateIDs) > 0 {
			break
		}
		total += size
		job.LateIDs = append(job.LateIDs, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	b, _ := json.Marshal(job)
	_, e = s.DB.Exec(`INSERT INTO jobs VALUES(?,?,?,?,?)`, job.ID, job.Day, job.Mode, job.State, string(b))
	return job, e
}

// LateRecords loads the inbox rows selected for a late job.
func (s *Store) LateRecords(ids []int64) ([]Record, error) {
	records := make([]Record, 0, len(ids))
	for _, id := range ids {
		var raw string
		if e := s.DB.QueryRow(`SELECT payload FROM inbox WHERE seq=?`, id).Scan(&raw); e != nil {
			return nil, e
		}
		var r Record
		if e := json.Unmarshal([]byte(raw), &r); e != nil {
			return nil, e
		}
		records = append(records, r)
	}
	return records, nil
}

// Activate publishes the verified snapshot and, for an archive day, advances the frontier.
func (s *Store) Activate(job *Job) error {
	if job.Metadata == "" {
		return errors.New("verified metadata required")
	}
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var frontier int64
	if e = tx.QueryRow(`SELECT frontier FROM config WHERE id=1`).Scan(&frontier); e != nil {
		return e
	}
	dayStart, end, _ := Bounds(job.Day)
	if job.Mode == "archive" && frontier != dayStart.UnixNano() {
		return errors.New("noncontiguous archive activation")
	}
	if job.Mode == "archive" {
		frontier = end.UnixNano()
	}
	data, _ := json.Marshal(job.Files)
	if _, e = tx.Exec(`INSERT INTO days VALUES(?,?,?) ON CONFLICT(day) DO UPDATE SET files=excluded.files,metadata=excluded.metadata`, job.Day, string(data), job.Metadata); e != nil {
		return e
	}
	if _, e = tx.Exec(`UPDATE config SET frontier=?,metadata=?,generation=generation+1 WHERE id=1`, frontier, job.Metadata); e != nil {
		return e
	}
	job.State = "ROUTED"
	raw, _ := json.Marshal(job)
	_, e = tx.Exec(`UPDATE jobs SET state=?,payload=? WHERE id=?`, job.State, string(raw), job.ID)
	if e != nil {
		return e
	}
	if e = s.recordMilestone(tx, job, "routed"); e != nil {
		return e
	}
	return tx.Commit()
}

// Finish marks the job COMPLETE and the late inbox rows consumed.
func (s *Store) Finish(job *Job) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, id := range job.LateIDs {
		if _, e = tx.Exec(`UPDATE inbox SET done=1 WHERE seq=?`, id); e != nil {
			return e
		}
	}
	job.State = "COMPLETE"
	raw, _ := json.Marshal(job)
	if _, e = tx.Exec(`UPDATE jobs SET state=?,payload=? WHERE id=?`, job.State, string(raw), job.ID); e != nil {
		return e
	}
	if e = s.recordMilestone(tx, job, "complete"); e != nil {
		return e
	}
	return tx.Commit()
}

// Metadata is the snapshot currently published to readers.
func (s *Store) Metadata() (string, error) {
	var m string
	e := s.DB.QueryRow(`SELECT metadata FROM config WHERE id=1`).Scan(&m)
	return m, e
}

// Status is deliberately aggregate; payloads and tokens are not exposed.
func (s *Store) Status() (map[string]any, error) {
	result := map[string]any{}
	var frontier int64
	var m string
	var gen int64
	if e := s.DB.QueryRow(`SELECT frontier,metadata,generation FROM config WHERE id=1`).Scan(&frontier, &m, &gen); e != nil {
		return nil, e
	}
	result["frontier"] = time.Unix(0, frontier).UTC().Format(time.RFC3339Nano)
	result["metadata"] = m
	result["generation"] = gen
	var activeReaders, expiredReaders int64
	if e := s.DB.QueryRow("SELECT count(*) FROM readers WHERE expires_at > ?", s.now().Add(-ReadExpiryGrace).UnixNano()).Scan(&activeReaders); e != nil {
		return nil, e
	}
	if e := s.DB.QueryRow("SELECT count(*) FROM readers WHERE expires_at <= ?", s.now().Add(-ReadExpiryGrace).UnixNano()).Scan(&expiredReaders); e != nil {
		return nil, e
	}
	result["readers"] = activeReaders
	result["readers_expired"] = expiredReaders
	for _, item := range []struct{ name, comparison string }{{"writers", ">"}, {"writers_expired", "<="}} {
		var count int64
		if e := s.DB.QueryRow("SELECT count(*) FROM permits WHERE state='active' AND expires_at "+item.comparison+" ?", s.now().Add(-WriteExpiryGrace).UnixNano()).Scan(&count); e != nil {
			return nil, e
		}
		result[item.name] = count
	}
	for k, q := range map[string]string{"late_pending": "SELECT count(*) FROM inbox WHERE done=0", "quarantined": "SELECT count(*) FROM quarantine", "jobs_pending": "SELECT count(*) FROM jobs WHERE state!='COMPLETE'",
		"deletions_pending": "SELECT count(*) FROM jobs WHERE mode='archive' AND state IN ('ROUTED','DELETING','DELETE_REQUESTED')",
		"jobs_failed":       "SELECT count(*) FROM jobs WHERE state!='COMPLETE' AND coalesce(json_extract(payload,'$.error'),'')!=''"} {
		var count int64
		if e := s.DB.QueryRow(q).Scan(&count); e != nil {
			return nil, e
		}
		result[k] = count
	}
	var oldest int64
	if e := s.DB.QueryRow("SELECT coalesce(min(accepted_at),0) FROM inbox WHERE done=0").Scan(&oldest); e != nil {
		return nil, e
	}
	age := int64(0)
	if oldest > 0 {
		age = time.Now().Unix() - oldest
	}
	result["late_oldest_age_seconds"] = age
	return result, nil
}

// Pending returns a journal without creating a new archival day.
func (s *Store) Pending(deletions bool) (*Job, error) {
	states := "(state NOT IN ('COMPLETE','ROUTED','DELETING','DELETE_REQUESTED') OR (state='ROUTED' AND mode='late'))"
	if deletions {
		states = "state IN ('ROUTED','DELETING','DELETE_REQUESTED')"
	}
	var raw string
	e := s.DB.QueryRow("SELECT payload FROM jobs WHERE " + states + " ORDER BY rowid LIMIT 1").Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var job Job
	e = json.Unmarshal([]byte(raw), &job)
	return &job, e
}

// ResolveOwner is an explicit offline recovery operation, never a timer.
func (s *Store) ResolveOwner(owner string) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec("UPDATE permits SET state='resolved-stopped' WHERE owner=? AND state='active'", owner); e != nil {
		return e
	}
	if _, e = tx.Exec("DELETE FROM readers WHERE owner=?", owner); e != nil {
		return e
	}
	return tx.Commit()
}

// Bind fixes the logical source/bootstrap configuration while permitting safe
// changes to scheduling, credentials and resource limits across restarts.
func (s *Store) Bind(key, value string) error {
	var old string
	e := s.DB.QueryRow("SELECT value FROM options WHERE key=?", key).Scan(&old)
	if e == nil {
		if old != value {
			return fmt.Errorf("durable option %s differs", key)
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = s.DB.Exec("INSERT INTO options VALUES(?,?)", key, value)
	return e
}

// Option reads one bound configuration value. An absent key is empty.
func (s *Store) Option(key string) (string, error) {
	var value string
	e := s.DB.QueryRow("SELECT value FROM options WHERE key=?", key).Scan(&value)
	if errors.Is(e, sql.ErrNoRows) {
		return "", nil
	}
	return value, e
}
