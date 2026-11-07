// packing_profile.go times expected-file materialization and the per-file
// pack. Per-file canonical shards are reused for the whole-build proof.

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

type PackingStep struct {
	Name       string             `json:"name"`
	FileIndex  int                `json:"file_index"`
	Seconds    float64            `json:"seconds"`
	Failed     bool               `json:"failed"`
	Comparison *ComparisonProfile `json:"comparison,omitempty"`
}
type PackingProfile struct {
	Canonicalization       string        `json:"canonicalization,omitempty"`
	RetainedCanonicalBytes int64         `json:"retained_canonical_bytes"`
	ReuseCanonicalStaging  bool          `json:"reuse_canonical_staging,omitempty"`
	BindingSeconds         float64       `json:"binding_seconds"`
	Passed                 bool          `json:"passed"`
	Threads                int           `json:"threads"`
	SortThreads            int           `json:"sort_threads"`
	Memory                 string        `json:"memory"`
	Rows                   int64         `json:"rows"`
	DataBytes              int64         `json:"data_bytes"`
	TotalSeconds           float64       `json:"total_seconds"`
	Steps                  []PackingStep `json:"steps"`
}

// measure appends one packing step. FileIndex is -1 for a step that covers the whole input.
func (p *PackingProfile) measure(name string, index int, fn func() error) error {
	began := time.Now()
	e := fn()
	p.Steps = append(p.Steps, PackingStep{Name: name, FileIndex: index, Seconds: time.Since(began).Seconds(), Failed: e != nil})
	return e
}

// MaterializeExpected is shared by production and retained-artifact benchmarks.
// No SQL or credentials are written into the timing report.
func (d DuckDB) MaterializeExpected(ctx context.Context, source, output, setup string, chronological bool) (p PackingProfile, err error) {
	began := time.Now()
	p.Threads = d.threadCount()
	p.Memory = d.Memory
	defer func() {
		p.TotalSeconds = time.Since(began).Seconds()
		p.Passed = err == nil
		if e := writeJSON(filepath.Join(filepath.Dir(output), "expected-profile.json"), p); e != nil && err == nil {
			err = e
		}
	}()
	if e := os.Remove(output); e != nil && !os.IsNotExist(e) {
		return p, e
	}
	ordered := source + " ORDER BY time"
	if chronological {
		ordered = source
	}
	if err = p.measure("expected_copy", -1, func() error {
		_, e := d.Run(ctx, setup+"SET preserve_insertion_order=true;"+CopySQL(ordered, output))
		return e
	}); err != nil {
		return p, err
	}
	err = p.measure("expected_sync", -1, func() error { return syncFile(output) })
	return p, err
}

// PackExpected retains the current sequential execution and EVERY verification
// gate. Per-file canonical shards feed a fresh whole-build exact proof.
func (d DuckDB) PackExpected(ctx context.Context, expected, work, id string) (paths []string, p PackingProfile, err error) {
	began := time.Now()
	p.Canonicalization = "tag_dictionary"
	p.Threads = d.threadCount()
	p.SortThreads = d.sortThreadCount()
	p.ReuseCanonicalStaging = true
	p.Memory = d.Memory
	defer func() {
		p.TotalSeconds = time.Since(began).Seconds()
		p.Passed = err == nil
		if e := writeJSON(filepath.Join(work, "packing-profile.json"), p); e != nil && err == nil {
			err = e
		}
	}()
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(id) {
		return nil, p, fmt.Errorf("invalid packing artifact id")
	}
	bindingBegan := time.Now()
	reuse, err := newPackingReuse(expected, work)
	p.BindingSeconds += time.Since(bindingBegan).Seconds()
	if err != nil {
		return nil, p, err
	}
	defer os.RemoveAll(reuse.root)
	if err = p.measure("row_count", -1, func() error { var e error; p.Rows, e = d.Count(ctx, Parquet([]string{expected})); return e }); err != nil {
		return nil, p, err
	}
	for offset, index := int64(0), 0; offset < p.Rows; offset, index = offset+fileRows, index+1 {
		intermediate := filepath.Join(work, "batch.parquet")
		output := filepath.Join(work, fmt.Sprintf("%s-%06d.parquet", id, index))
		if filepath.Clean(expected) == filepath.Clean(intermediate) || filepath.Clean(expected) == filepath.Clean(output) {
			return nil, p, fmt.Errorf("packing output aliases retained input")
		}
		window := fmt.Sprintf("SELECT * EXCLUDE(file_row_number) FROM read_parquet(%s,file_row_number=true,hive_partitioning=false) WHERE file_row_number >= %d AND file_row_number < %d", Q(expected), offset, offset+fileRows)
		for _, path := range []string{intermediate, output} {
			if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
				return nil, p, e
			}
		}
		if err = p.measure("batch_sort_copy", index, func() error {
			sorter := d
			sorter.Threads = d.sortThreadCount()
			_, e := sorter.Run(ctx, CopySQL(window+" ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST", intermediate))
			return e
		}); err != nil {
			return nil, p, err
		}
		if err = p.measure("pack", index, func() error { return pack(ctx, intermediate, output, 262144) }); err != nil {
			return nil, p, err
		}
		if err = p.measure("file_sync", index, func() error { return syncFile(output) }); err != nil {
			return nil, p, err
		}
		if err = p.measure("layout_audit", index, func() error {
			groups, e := auditPacked(ctx, output)
			if e != nil {
				return e
			}
			return writeJSON(output+".layout.json", map[string]any{"groups": groups, "plugin_pure": true, "row_group_cap": 262144, "file_row_cap": fileRows})
		}); err != nil {
			return nil, p, err
		}
		var comparison ComparisonProfile
		checker := d
		checker.Work = reuse.root
		checker.comparisonProfile = &comparison
		err = p.measure("file_exact_comparison", index, func() error {
			began := time.Now()
			bound, e := bindFile(output)
			p.BindingSeconds += time.Since(began).Seconds()
			if e != nil {
				return e
			}
			var stage *canonicalStage
			if e = checker.equalCanonical(ctx, typedDictionaryCanonical(window), typedDictionaryCanonical(Parquet([]string{output})), "", &stage); e != nil {
				return e
			}
			rows := min(fileRows, p.Rows-offset)
			if stage.rows != rows {
				return fmt.Errorf("canonical source window has unexpected row count")
			}
			began = time.Now()
			e = validateBindings([]fileBinding{bound})
			p.BindingSeconds += time.Since(began).Seconds()
			if e != nil {
				return e
			}
			reuse.entries = append(reuse.entries, packingReuseEntry{offset: offset, rows: rows, output: bound, stage: stage})
			for _, binding := range stage.bindings {
				p.RetainedCanonicalBytes += binding.Bytes
			}
			return nil
		})
		p.Steps[len(p.Steps)-1].Comparison = &comparison
		if err != nil {
			return nil, p, err
		}
		info, e := os.Stat(output)
		if e != nil {
			return nil, p, e
		}
		p.DataBytes += info.Size()
		paths = append(paths, output)
	}
	var comparison ComparisonProfile
	err = p.measure("whole_build_exact_comparison", -1, func() error { var e error; comparison, e = d.compareReused(ctx, reuse, p.Rows, paths); return e })
	p.Steps[len(p.Steps)-1].Comparison = &comparison

	return paths, p, err
}
