// hash.go holds small helpers shared by snapshot and remote retention.
package statebackup

import (
	"crypto/sha256"
	"encoding/hex"
)

func hashBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// RetentionError means the new backup already passed remote verification;
// only deletion of eligible older backup objects failed. Callers treat the
// backup as complete and record retention_error separately.
type RetentionError struct{ Err error }

func (e *RetentionError) Error() string { return "verified backup; retention failed: " + e.Err.Error() }
func (e *RetentionError) Unwrap() error { return e.Err }
