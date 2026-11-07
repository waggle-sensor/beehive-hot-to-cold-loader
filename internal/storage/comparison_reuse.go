// comparison_reuse.go remembers the canonical shards from per-file proofs so
// the whole-build proof does not stage those rows again. The ledger lives only
// for the current PackExpected call.

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

type fileBinding struct {
	Path   string
	Bytes  int64
	SHA256 string
}

// bindFile records size and SHA-256 of a regular local file.
func bindFile(path string) (fileBinding, error) {
	info, e := os.Lstat(path)
	if e != nil {
		return fileBinding{}, e
	}
	if !info.Mode().IsRegular() {
		return fileBinding{}, fmt.Errorf("comparison artifact must be a regular file: %s", path)
	}
	hash, e := digest(path)
	return fileBinding{path, info.Size(), hash}, e
}

// validateBindings fails when any recorded file's size or digest has changed.
func validateBindings(bindings []fileBinding) error {
	for _, before := range bindings {
		after, e := bindFile(before.Path)
		if e != nil {
			return e
		}
		if before != after {
			return fmt.Errorf("comparison artifact changed: %s", before.Path)
		}
	}
	return nil
}

// bindCanonical hashes every shard file on both sides of a staged comparison.
func bindCanonical(left, right map[int][]string) ([]fileBinding, error) {
	var out []fileBinding
	for _, side := range []map[int][]string{left, right} {
		for shard := 0; shard < comparisonFanout; shard++ {
			for _, p := range side[shard] {
				b, e := bindFile(p)
				if e != nil {
					return nil, e
				}
				out = append(out, b)
			}
		}
	}
	return out, nil
}

type canonicalStage struct {
	root        string
	left, right map[int][]string
	rows        int64
	bindings    []fileBinding
}

// validate checks both the file bytes and the shard inventory.
func (s *canonicalStage) validate() error {
	if e := validateBindings(s.bindings); e != nil {
		return e
	}
	for _, side := range []struct {
		name string
		want map[int][]string
	}{{"a", s.left}, {"b", s.right}} {
		actual, rows, e := partitionInventory(filepath.Join(s.root, side.name), comparisonFanout)
		if e != nil {
			return e
		}
		if rows != s.rows || !reflect.DeepEqual(actual, side.want) {
			return fmt.Errorf("canonical staging inventory changed")
		}
	}
	return nil
}

type packingReuseEntry struct {
	offset, rows int64
	output       fileBinding
	stage        *canonicalStage
}

// A ledger is private to one PackExpected invocation. Nothing is persisted for
// recovery or trusted by publication/remote/pre-delete verification.
type packingReuse struct {
	root     string
	expected fileBinding
	entries  []packingReuseEntry
}

// newPackingReuse binds the expected file and creates a private shard directory.
func newPackingReuse(expected, work string) (*packingReuse, error) {
	binding, e := bindFile(expected)
	if e != nil {
		return nil, e
	}
	root, e := os.MkdirTemp(work, "packing-canonical-")
	if e != nil {
		return nil, e
	}
	return &packingReuse{root: root, expected: binding}, nil
}

// validate requires the retained windows to cover rows in order without gaps.
func (r *packingReuse) validate(rows int64, paths []string) error {
	actual, e := partitionRows([]string{r.expected.Path})
	if e != nil {
		return e
	}
	if actual != rows {
		return fmt.Errorf("canonical reuse expected row count changed")
	}
	if len(paths) != len(r.entries) {
		return fmt.Errorf("canonical reuse file coverage changed")
	}
	bindings := []fileBinding{r.expected}
	next := int64(0)
	for i, entry := range r.entries {
		if entry.offset != next || entry.rows <= 0 || entry.rows > fileRows || entry.stage == nil || entry.stage.rows != entry.rows || entry.output.Path != paths[i] {
			return fmt.Errorf("canonical reuse source-window coverage changed")
		}
		next += entry.rows
		bindings = append(bindings, entry.output)
		if e := entry.stage.validate(); e != nil {
			return e
		}
	}
	if next != rows {
		return fmt.Errorf("canonical reuse total coverage changed")
	}
	return validateBindings(bindings)
}

// compareReused proves the union of the retained shards against the packed files.
func (d DuckDB) compareReused(ctx context.Context, r *packingReuse, rows int64, paths []string) (profile ComparisonProfile, err error) {
	began := time.Now()
	_, writers := d.comparisonWriterSettings()
	profile = ComparisonProfile{Threads: d.threadCount(), Mode: "fixed", InitialFanout: comparisonFanout, WriterOpenFiles: writers, ReusedInitial: true}
	defer func() {
		profile.TotalSeconds = time.Since(began).Seconds()
		profile.RoutingSeconds = profile.RefinementSeconds
		profile.Passed = err == nil
	}()
	verify := func() error {
		began := time.Now()
		e := r.validate(rows, paths)
		profile.BindingSeconds += time.Since(began).Seconds()
		return e
	}
	if err = verify(); err != nil {
		return profile, err
	}
	left, right := map[int][]string{}, map[int][]string{}
	for _, entry := range r.entries {
		for shard := 0; shard < comparisonFanout; shard++ {
			left[shard] = append(left[shard], entry.stage.left[shard]...)
			right[shard] = append(right[shard], entry.stage.right[shard]...)
		}
	}
	// This is still a new, complete exact multiset comparison over ALL staged rows.
	if err = d.compareCanonicalPartitions(ctx, left, right, rows, r.root, &profile); err != nil {
		return profile, err
	}
	err = verify()
	return profile, err
}
