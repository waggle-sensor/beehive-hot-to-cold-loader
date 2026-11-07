// write_lease.go gives each writer permit a persisted expiry. Renewal extends
// a live permit. It never reopens a fenced day or a released batch.

package control

import (
	"encoding/json"
	"errors"
	"time"
)

// DefaultWriteLeaseDuration is the lifetime added by a new permit or a renewal.
// WriteExpiryGrace is how long an expired permit still blocks a fence.
const DefaultWriteLeaseDuration = 30 * time.Minute
const WriteExpiryGrace = time.Minute

// WriteGrant pins admission for a batch. Renewing never re-admits a fenced day.
type WriteGrant struct {
	ID        string   `json:"id"`
	HotDays   []string `json:"hot_days"`
	ExpiresAt string   `json:"expires_at"`
}

// migrateWriteLeases gives legacy active permits one persisted migration window.
// Restarting must not extend that window. Deploy renewal-aware writers together.
func (s *Store) migrateWriteLeases() error {
	rows, err := s.DB.Query("PRAGMA table_info(permits)")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "expires_at" {
			found = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		if _, err = s.DB.Exec("ALTER TABLE permits ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	_, err = s.DB.Exec("UPDATE permits SET expires_at=? WHERE expires_at=0", s.now().Add(s.writeLeaseDuration).UnixNano())
	if err != nil {
		return err
	}
	_, err = s.DB.Exec("CREATE INDEX IF NOT EXISTS permits_by_expiry ON permits(state,expires_at)")
	return err
}

// AcquireWrite admits the batch, then returns the expiry already stored for it.
// A repeated id returns that same grant.
func (s *Store) AcquireWrite(id, owner string, days []string) (WriteGrant, error) {
	allowed, err := s.Permit(id, owner, days)
	if err != nil {
		return WriteGrant{}, err
	}
	var expiry int64
	if err = s.DB.QueryRow("SELECT expires_at FROM permits WHERE id=?", id).Scan(&expiry); err != nil {
		return WriteGrant{}, err
	}
	return WriteGrant{ID: id, HotDays: allowed, ExpiresAt: time.Unix(0, expiry).UTC().Format(time.RFC3339Nano)}, nil
}

// RenewWrite extends a live permit owned by this writer. The new expiry is the
// later of the current expiry and now plus the configured duration.
func (s *Store) RenewWrite(owner, id string) (WriteGrant, error) {
	var grant WriteGrant
	if owner == "" || id == "" {
		return grant, errors.New("write id and owner required")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return grant, err
	}
	defer tx.Rollback()
	var storedOwner, state, days string
	var expiry int64
	if err = tx.QueryRow("SELECT owner,state,days,expires_at FROM permits WHERE id=?", id).Scan(&storedOwner, &state, &days, &expiry); err != nil {
		return grant, err
	}
	now := s.now()
	if storedOwner != owner || state != "active" || !now.Before(time.Unix(0, expiry)) {
		return grant, errors.New("write lease expired, released or owner mismatch")
	}
	if err = json.Unmarshal([]byte(days), &grant.HotDays); err != nil {
		return grant, err
	}
	next := max(expiry, now.Add(s.writeLeaseDuration).UnixNano())
	if _, err = tx.Exec("UPDATE permits SET expires_at=? WHERE id=?", next, id); err != nil {
		return grant, err
	}
	grant.ID, grant.ExpiresAt = id, time.Unix(0, next).UTC().Format(time.RFC3339Nano)
	return grant, tx.Commit()
}

// ReleaseWrite acknowledges only a live batch with a confirmed write outcome.
// Terminal successful releases are idempotent, but expired/resolved permits fail.
func (s *Store) ReleaseWrite(owner, id string) error {
	if owner == "" || id == "" {
		return errors.New("write id and owner required")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedOwner, state string
	var expiry int64
	if err = tx.QueryRow("SELECT owner,state,expires_at FROM permits WHERE id=?", id).Scan(&storedOwner, &state, &expiry); err != nil {
		return err
	}
	if storedOwner != owner {
		return errors.New("write owner mismatch")
	}
	if state == "complete" {
		return nil
	}
	if state != "active" || !s.now().Before(time.Unix(0, expiry)) {
		return errors.New("write lease expired or resolved; replay batch")
	}
	if _, err = tx.Exec("UPDATE permits SET state='complete' WHERE id=?", id); err != nil {
		return err
	}
	return tx.Commit()
}
