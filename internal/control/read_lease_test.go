// read_lease_test.go checks that renewal keeps the pinned snapshot and that
// an expired grant cannot be revived.

package control

import (
	"path/filepath"
	"testing"
	"time"
)

func TestReadLeaseDurationAndGrace(t *testing.T) {
	s := testStore(t)
	clock := time.Now().UTC()
	s.now = func() time.Time { return clock }
	view, err := s.View("crashed-api", "read")
	if err != nil {
		t.Fatal(err)
	}
	expiry, err := time.Parse(time.RFC3339Nano, view.ExpiresAt)
	if err != nil || !expiry.Equal(clock.Add(DefaultReadLeaseDuration)) {
		t.Fatal(view, err)
	}
	clock = clock.Add(time.Minute)
	again, err := s.View("crashed-api", "read")
	if err != nil || again != view {
		t.Fatal("acquire extended/replaced view", again, err)
	}
	if _, err = s.View("other", "read"); err == nil {
		t.Fatal("owner mismatch accepted")
	}
	clock = expiry
	if _, err = s.View("crashed-api", "read"); err == nil {
		t.Fatal("expired read reacquired")
	}
	if n, err := s.OldReaders("2025-03-01"); err != nil || n != 1 {
		t.Fatal("grace skipped", n, err)
	}
	clock = expiry.Add(ReadExpiryGrace)
	if n, err := s.OldReaders("2025-03-01"); err != nil || n != 0 {
		t.Fatal("expired reader blocks deletion", n, err)
	}
	status, err := s.Status()
	if err != nil || status["readers"] != int64(0) || status["readers_expired"] != int64(1) {
		t.Fatal(status, err)
	}
	// Writer grants have their own renewable deadline.
	if _, err = s.Permit("writer", "loader", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(DefaultWriteLeaseDuration + WriteExpiryGrace)
	if n, err := s.Writers("2025-03-01"); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if err := s.ReleaseRead("read"); err != nil {
		t.Fatal(err)
	}
}

func TestReadLeaseDurationMigrationAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	open := func(ttl time.Duration) *Store {
		s, err := Open(path, "waggle", "warehouse", "2025-03-01", ttl)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open(time.Minute)
	// Simulate a pre-timeout journal, preserving its pinned payload.
	v, err := s.View("legacy", "legacy-read")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec("ALTER TABLE readers DROP COLUMN expires_at"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(2 * time.Minute)
	migrated, err := s.View("legacy", "legacy-read")
	if err != nil || migrated.Frontier != v.Frontier || migrated.ExpiresAt == "" {
		t.Fatal(migrated, err)
	}
	s.Close()
	s = open(time.Hour)
	defer s.Close()
	resumed, err := s.View("legacy", "legacy-read")
	if err != nil || resumed != migrated {
		t.Fatal("restart renewed lease", resumed, err)
	}
	expiry, _ := time.Parse(time.RFC3339Nano, resumed.ExpiresAt)
	s.now = func() time.Time { return expiry.Add(ReadExpiryGrace) }
	if n, err := s.OldReaders("2025-03-01"); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestInvalidReadLeaseDuration(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second, 25 * time.Hour} {
		if s, err := Open(filepath.Join(t.TempDir(), "control.db"), "waggle", "warehouse", "2025-03-01", ttl); err == nil {
			s.Close()
			t.Fatalf("invalid lease duration accepted: %s", ttl)
		}
	}
}

func TestRenewReadPreservesSnapshotAndCannotResurrect(t *testing.T) {
	s := testStore(t)
	clock := time.Now().UTC()
	s.now = func() time.Time { return clock }
	original, err := s.View("api", "read")
	if err != nil {
		t.Fatal(err)
	}
	// Publication may advance routing while this reader still needs its old view.
	job, err := s.Next(now(), 720*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	job.Metadata = "new-snapshot"
	if err := s.Activate(job); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(DefaultReadLeaseDuration / 2)
	if _, err := s.RenewRead("wrong-owner", "read"); err == nil {
		t.Fatal("wrong owner renewed lease")
	}
	renewed, err := s.RenewRead("api", "read")
	if err != nil {
		t.Fatal(err)
	}
	expiry, _ := time.Parse(time.RFC3339Nano, renewed.ExpiresAt)
	if !expiry.Equal(clock.Add(DefaultReadLeaseDuration)) {
		t.Fatal("lease not extended", expiry)
	}
	original.ExpiresAt = renewed.ExpiresAt
	if original != renewed {
		t.Fatal("renew changed pinned view")
	}
	retry, err := s.View("api", "read")
	if err != nil || retry != renewed {
		t.Fatal(retry, err)
	}
	clock = expiry
	if _, err = s.RenewRead("api", "read"); err == nil {
		t.Fatal("expired lease resurrected")
	}
	if err = s.ReleaseRead("read"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RenewRead("api", "read"); err == nil {
		t.Fatal("released lease resurrected")
	}
}
