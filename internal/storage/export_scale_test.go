// export_scale_test.go is an opt-in local scale measurement. It uses only its own
// directory and never contacts Influx or Pelican. Run it alone for a useful RSS reading.

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run this test alone in a precompiled test binary for useful RSS measurements.
// It touches only its own fresh local directory, never Influx or Pelican.
func TestNativeExportDedupScale(t *testing.T) {
	text := os.Getenv("SAGE_ARCHIVE_SCALE_ROWS")
	if text == "" {
		t.Skip("set SAGE_ARCHIVE_SCALE_ROWS and SAGE_ARCHIVE_DUCKDB2 for local export/dedup scale measurements")
	}
	rows, e := strconv.ParseInt(text, 10, 64)
	if e != nil || rows < 1 || rows > 1_000_000_000 {
		t.Fatal("scale rows must be 1..1000000000")
	}
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Fatal("SAGE_ARCHIVE_DUCKDB2 required")
	}
	root := os.Getenv("SAGE_ARCHIVE_SCALE_DIR")
	if root == "" {
		root = t.TempDir()
	} else {
		root, e = localPath(root)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.Mkdir(root, 0700); e != nil {
			t.Fatal("scale directory must be fresh, with an existing parent", e)
		}
	}
	memory := os.Getenv("SAGE_ARCHIVE_SCALE_MEMORY")
	if memory == "" {
		memory = "512MB"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	duck := DuckDB{Binary: binary, Work: root, Memory: memory}
	if e = duck.Check(ctx); e != nil {
		t.Fatal(e)
	}
	profile := filepath.Join(root, "sort-profile.json")
	// DuckDB 2.0 prints ANSI deprecation warnings on stdout for the PRAGMA form,
	// which breaks JSON result parsing. SET writes the profile file only.
	duck.testSetup = "SET enable_profiling='json'; SET profiling_output=" + Q(profile) + ";"
	peakDone, stop := sampleStageBytes(root)
	defer stop()
	began := time.Now()
	raw := filepath.Join(root, "raw.parquet")
	reader, writer := io.Pipe()
	defer reader.Close()
	producer := make(chan error, 1)
	go func() {
		buffer := bufio.NewWriterSize(writer, 1<<20)
		var err error
		for i := int64(0); i < rows; i++ {
			if i%4096 == 0 {
				if err = ctx.Err(); err != nil {
					break
				}
			}
			// Reverse timestamps force an actual global sort. Every tenth identity is
			// overwritten immediately in replay order; all identities remain distinct.
			stamp := int64(1740787200000000194) + (rows - 1 - i)
			if _, err = fmt.Fprintf(buffer, "scale,node=n%d,plugin=p%d value=%di %d\n", i%10000, i%16, i, stamp); err != nil {
				break
			}
			if i%10 == 0 {
				if _, err = fmt.Fprintf(buffer, "scale,node=n%d,plugin=p%d value=%di %d\n", i%10000, i%16, -i-1, stamp); err != nil {
					break
				}
			}
		}
		if err == nil {
			err = buffer.Flush()
		}
		writer.CloseWithError(err)
		producer <- err
	}()
	f, e := os.Create(raw)
	if e != nil {
		t.Fatal(e)
	}
	enc, finish, release, e := newRecordEncoder(f, raw)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	e = streamExport(reader, enc, 1740787200000000000, 1740873600000000000)
	if e != nil {
		reader.CloseWithError(e)
		f.Close()
		t.Fatal(e)
	}
	if e = <-producer; e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = finish(); e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = f.Sync(); e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	exportSeconds := time.Since(began).Seconds()
	normalized := filepath.Join(root, "normalized.parquet")
	began = time.Now()
	stats, e := duck.DeduplicateExport(ctx, raw, normalized)
	if e != nil {
		t.Fatal(e)
	}
	dedupSeconds := time.Since(began).Seconds()
	superseded := (rows + 9) / 10
	if stats.InputRows != rows+superseded || stats.UniqueRows != rows || stats.SupersededRows != superseded {
		t.Fatal(stats)
	}
	// Validate all retained rows in a streaming pass, without an expected-row map.
	var n int64
	e = visitStagingFixture(normalized, func(record exportRecord) error {
		original := rows - 1 - n
		value := original
		sequence := 1 + original + (original+9)/10
		if original%10 == 0 {
			value = -original - 1
			sequence++
		}
		if record.Time != 1740787200000000194+n || record.Name != "scale" || record.Kind != "int" || record.Value != strconv.FormatInt(value, 10) || record.Seq != sequence || record.Tags["node"] != fmt.Sprintf("n%d", original%10000) || record.Tags["plugin"] != fmt.Sprintf("p%d", original%16) {
			t.Fatal("retained point differs", n, record)
		}
		n++
		return nil
	})
	if e != nil || n != rows {
		t.Fatal(n, e)
	}
	stop()
	peak := <-peakDone
	var metrics struct {
		System struct {
			Buffer uint64 `json:"peak_buffer_memory"`
			Spill  uint64 `json:"peak_temp_dir_size"`
		} `json:"system"`
	}
	data, e := os.ReadFile(profile)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &metrics); e != nil {
		t.Fatal(e)
	}
	report := map[string]any{"unique_rows": rows, "stats": stats, "duckdb_memory_limit": memory, "export_seconds": exportSeconds, "dedup_seconds": dedupSeconds, "go_peak_rss_bytes": peakRSS(syscall.RUSAGE_SELF), "largest_child_peak_rss_bytes": peakRSS(syscall.RUSAGE_CHILDREN), "last_native_sort_peak_buffer_bytes": metrics.System.Buffer, "last_native_sort_peak_spill_bytes": metrics.System.Spill, "sampled_peak_staging_file_bytes": peak, "staging_sample_interval_ms": 100, "profile_scope": "last bounded native sort only; not a maximum over all partitions; sampled staging/RSS cover the complete normalization", "rss_scope": "separate parent and largest reaped child high-water marks; not a simultaneous combined peak"}
	if b, e := os.ReadFile("/sys/fs/cgroup/memory.peak"); e == nil {
		if n, e := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); e == nil {
			report["cgroup_lifetime_memory_peak_bytes"] = n
		}
	}
	if e = writeJSON(filepath.Join(root, "export-scale.json"), report); e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(report)
	t.Log(string(b))
}

// peakRSS returns the peak RSS.
func peakRSS(who int) int64 {
	var usage syscall.Rusage
	if e := syscall.Getrusage(who, &usage); e != nil {
		return -1
	}
	n := usage.Maxrss
	if runtime.GOOS != "darwin" {
		n *= 1024
	}
	return n
}

// sampleStageBytes samples the stage bytes.
func sampleStageBytes(root string) (<-chan int64, func()) {
	done := make(chan int64, 1)
	stop := make(chan struct{})
	closed := false
	go func() {
		var peak int64
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		sample := func() {
			var size int64
			filepath.WalkDir(root, func(path string, d os.DirEntry, e error) error {
				if e == nil && !d.IsDir() {
					if info, e := d.Info(); e == nil {
						size += info.Size()
					}
				}
				return nil
			})
			if size > peak {
				peak = size
			}
		}
		for {
			select {
			case <-ticker.C:
				sample()
			case <-stop:
				sample()
				done <- peak
				return
			}
		}
	}()
	return done, func() {
		if !closed {
			closed = true
			close(stop)
		}
	}
}
