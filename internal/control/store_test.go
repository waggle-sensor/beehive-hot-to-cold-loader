// store_test.go checks the journal: writer grants do not expire, coverage stays contiguous,
// late points require a fenced day, and a reopen restores the same state.

package control

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "control.db"), "waggle", "https://test/sage/cold-storage-archive-test/warehouse", "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func now() time.Time { return time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC) }

func TestFenceAtomicAndNoExpiry(t *testing.T) {
	s := testStore(t)
	hot, e := s.Permit("permit", "pod-a", []string{"2025-03-01", "2025-05-01"})
	if e != nil || len(hot) != 2 {
		t.Fatal(hot, e)
	}
	job, e := s.Next(now(), 720*time.Hour)
	if e != nil || job.Day != "2025-03-01" {
		t.Fatal(job, e)
	}
	hot, e = s.Permit("second", "pod-b", []string{job.Day, "2025-05-01"})
	if e != nil || len(hot) != 1 || hot[0] != "2025-05-01" {
		t.Fatal(hot, e)
	}
	n, e := s.Writers(job.Day)
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	// A fence preserves an already granted write until release or lease expiry.
	hot, e = s.Permit("permit", "pod-a", []string{job.Day})
	if e != nil || len(hot) != 2 {
		t.Fatal(hot, e)
	}
	if e = s.ReleaseWrite("pod-a", "permit"); e != nil {
		t.Fatal(e)
	}
	n, _ = s.Writers(job.Day)
	if n != 0 {
		t.Fatal(n)
	}
}

func TestLateReceiptAndActivation(t *testing.T) {
	s := testStore(t)
	job, _ := s.Next(now(), 720*time.Hour)
	r := Record{Time: 1740787200000000194, Name: "temp", Tags: map[string]string{"node": "a"}, Kind: "int", Value: "9007199254740993"}
	if e := s.Enqueue("event", []Record{r}); e != nil {
		t.Fatal(e)
	}
	if e := s.Enqueue("event", []Record{r}); e != nil {
		t.Fatal(e)
	}
	r.Value = "8"
	if e := s.Enqueue("event", []Record{r}); e == nil {
		t.Fatal("changed replay accepted")
	}
	job.Metadata = "https://test/head.metadata.json"
	job.Files = []string{"file"}
	if e := s.Activate(job); e != nil {
		t.Fatal(e)
	}
	if late, e := s.NextLate(); e != nil || late == nil || len(late.LateIDs) != 1 {
		t.Fatal(late, e)
	}
	// Queued input was durable, not just an in-memory acknowledgement.
	var n int
	s.DB.QueryRow("SELECT count(*) FROM inbox").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}

func TestReadersAndContiguousCoverage(t *testing.T) {
	s := testStore(t)
	old, e := s.View("api-a", "old")
	if e != nil {
		t.Fatal(e)
	}
	job, _ := s.Next(now(), 720*time.Hour)
	job.Metadata = "snapshot-1"
	if e = s.Activate(job); e != nil {
		t.Fatal(e)
	}
	_, e = s.View("api-b", "new")
	if e != nil {
		t.Fatal(e)
	}
	n, e := s.OldReaders(job.Day)
	if e != nil || n != 1 {
		t.Fatal(n, e, old)
	}
	if e = s.ReleaseRead("old"); e != nil {
		t.Fatal(e)
	}
	n, _ = s.OldReaders(job.Day)
	if n != 0 {
		t.Fatal(n)
	}
	job.Day = "2025-03-04"
	if e = s.Activate(job); e == nil {
		t.Fatal("coverage gap accepted")
	}
}

func TestFullDayEligibilityAndRetentionChanges(t *testing.T) {
	s := testStore(t)
	deadline := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	if job, e := s.Next(deadline.Add(-time.Nanosecond), 720*time.Hour); e != nil || job != nil {
		t.Fatal(job, e)
	}
	if job, e := s.Next(deadline, 720*time.Hour); e != nil || job == nil {
		t.Fatal(job, e)
	}
}

func TestInboxRequiresFencedDayAndTypedValues(t *testing.T) {
	s := testStore(t)
	r := Record{Time: 1740787200000000194, Name: "t", Tags: map[string]string{}, Kind: "float", Value: "11.7"}
	if e := s.Enqueue("unfenced", []Record{r}); e == nil {
		t.Fatal("unfenced inbox accepted")
	}
	s.Next(now(), 720*time.Hour)
	for _, v := range []string{"NaN", "Inf", "not-a-number"} {
		r.Value = v
		if e := s.Enqueue(v, []Record{r}); e == nil {
			t.Fatal(v)
		}
	}
}

func TestDurabilityAcrossOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	s, e := Open(path, "waggle", "warehouse", "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	s.Permit("a", "pod", []string{"2025-03-01"})
	s.Next(now(), 720*time.Hour)
	s.Close()
	s, e = Open(path, "waggle", "warehouse", "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if n, e := s.Writers("2025-03-01"); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if job, e := s.Pending(false); e != nil || job == nil || job.State != "FENCING" {
		t.Fatal(job, e)
	}
}

func TestInboxBatchRollbackAndReplayLength(t *testing.T) {
	s := testStore(t)
	s.Next(now(), 720*time.Hour)
	good := Record{Time: 1740787200000000194, Name: "t", Tags: map[string]string{}, Kind: "float", Value: "11.7"}
	bad := good
	bad.Kind = "unsupported"
	if e := s.Enqueue("batch", []Record{good, bad}); e == nil {
		t.Fatal("invalid batch committed")
	}
	var n int
	s.DB.QueryRow("SELECT count(*) FROM inbox").Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
	if e := s.Enqueue("batch", []Record{good, good}); e != nil {
		t.Fatal(e)
	}
	if e := s.Enqueue("batch", []Record{good}); e == nil {
		t.Fatal("changed replay length accepted")
	}
}

func TestLateRoutedJobRemainsRunnableWithDeletionDisabled(t *testing.T) {
	s := testStore(t)
	job, _ := s.Next(now(), 720*time.Hour)
	job.Metadata = "first"
	s.Activate(job)
	r := Record{Time: 1740787200000000194, Name: "t", Tags: map[string]string{}, Kind: "int", Value: "1"}
	s.Enqueue("late", []Record{r})
	late, e := s.NextLate()
	if e != nil || late == nil {
		t.Fatal(late, e)
	}
	late.Metadata = "second"
	if e = s.Activate(late); e != nil {
		t.Fatal(e)
	}
	active, e := s.Pending(false)
	if e != nil || active == nil || active.ID != late.ID {
		t.Fatal(active, e)
	}
}

func TestConcurrentPermitsAndFence(t *testing.T) {
	s := testStore(t)
	type result struct {
		allowed bool
		err     error
	}
	results := make(chan result, 16)
	start := make(chan struct{})
	for n := 0; n < 16; n++ {
		go func(n int) {
			<-start
			days, e := s.Permit(fmt.Sprint(n), fmt.Sprint("loader-", n), []string{"2025-03-01"})
			results <- result{len(days) > 0, e}
		}(n)
	}
	close(start)
	if _, e := s.Next(now(), 720*time.Hour); e != nil {
		t.Fatal(e)
	}
	allowed := 0
	for n := 0; n < 16; n++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.allowed {
			allowed++
		}
	}
	writers, e := s.Writers("2025-03-01")
	if e != nil || writers != allowed {
		t.Fatal(writers, allowed, e)
	}
	if days, e := s.Permit("after", "loader", []string{"2025-03-01"}); e != nil || len(days) > 0 {
		t.Fatal(days, e)
	}
}
