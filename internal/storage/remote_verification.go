// remote_verification.go is the production proof for a committed or pre-delete
// snapshot. The local expected file uses the typed dictionary; the Iceberg scan
// stays on the original canonical SQL.

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// EqualRemote freshly compares local expected Parquet with an actual remote
// scan. Only the local side uses the typed dictionary; remote canonicalization
// stays original. Independent Equal remains the original SQL on both inputs.
func (d DuckDB) EqualRemote(ctx context.Context, expected, remote, setup string) error {
	path, err := localPath(expected)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || filepath.Ext(path) != ".parquet" {
		return fmt.Errorf("remote verification requires regular local expected Parquet")
	}
	err = d.EqualCanonical(ctx, typedDictionaryCanonical(Parquet([]string{path})), Canonical(remote), setup)
	if d.comparisonProfile != nil {
		d.comparisonProfile.Canonicalization = "local_dictionary"
	}
	return err
}
