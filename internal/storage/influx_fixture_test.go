// influx_fixture_test.go builds the isolated engine fixture: expected files, a TSM
// view that shares nothing with the live bucket, and the day the fixture must delete.

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

// fixtureStart is the start of the fixture.
const fixtureStart int64 = 1740787200000000000

// fixtureEnd is the end of the fixture.
const fixtureEnd int64 = fixtureStart + 86400000000000

// fixtureStrings is the strings of the fixture.
var fixtureStrings = []string{"first\nsecond", "\nleading", "trailing\n", "a\n\nb", `literal\ntext`, "quote \" slash \\\nnext", "a\nforged,node=n1 value=99i 1740787200000000000"}

// fixtureFiles is the files of the fixture.
type fixtureFiles struct {
	TSM        []string `json:"tsm"` // the TSM files of the fixture
	Tombstones []string `json:"tombstones"` // the tombstone files of the fixture
	WAL        []string `json:"wal"` // the WAL files of the fixture
}

// fixtureInventory returns the inventory of the fixture.
func fixtureInventory(root, bucket string) (out fixtureFiles, err error) {
	for _, tree := range []string{"data", "wal"} {
		info, e := os.Lstat(filepath.Join(root, tree))
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return out, e
		}
		if e == nil && !info.IsDir() {
			return out, fmt.Errorf("fixture tree must be a directory without symlinks: %s", tree)
		}
		base := filepath.Join(root, tree, bucket)
		err = filepath.WalkDir(base, func(path string, d os.DirEntry, e error) error {
			if errors.Is(e, os.ErrNotExist) {
				return nil
			}
			if e != nil {
				return e
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("fixture must not contain symlinks: %s", path)
			}
			if d.IsDir() {
				return nil
			}
			info, e := d.Info()
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("not a regular fixture file: %s", path)
			}
			switch filepath.Ext(path) {
			case ".tsm":
				out.TSM = append(out.TSM, path)
			case ".tombstone":
				out.Tombstones = append(out.Tombstones, path)
			case ".wal":
				out.WAL = append(out.WAL, path)
			}
			return nil
		})
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// File replacement proves physical cleanup, independently of the compaction
// level chosen by the engine. Full-compaction evidence is checked separately.
// fixtureReplacement returns the fixture replacement.
func fixtureReplacement(root, bucket string, before fixtureFiles) (fixtureFiles, bool, error) {
	after, e := fixtureInventory(root, bucket)
	if e != nil {
		return after, false, e
	}
	for _, path := range append(append([]string{}, before.TSM...), before.Tombstones...) {
		if _, e = os.Stat(path); e == nil {
			return after, false, nil
		} else if !os.IsNotExist(e) {
			return after, false, e
		}
	}
	return after, len(after.TSM) > 0 && len(after.Tombstones) == 0, nil
}

// A private, WAL-free view proves that the target value has reached immutable
// TSM. It copies files; it never renames, links or mutates the running engine.
// A concurrent compaction can remove a source file: callers retry the view.
// fixtureTSMView returns the fixture TSM view.
func fixtureTSMView(root, bucket, destination string) (fixtureFiles, error) {
	files, e := fixtureInventory(root, bucket)
	if e != nil {
		return files, e
	}
	if e = os.MkdirAll(filepath.Join(destination, "wal", bucket), 0700); e != nil {
		return files, e
	}
	if e = os.MkdirAll(filepath.Join(destination, "data", bucket), 0700); e != nil {
		return files, e
	}
	for _, source := range append(append([]string{}, files.TSM...), files.Tombstones...) {
		relative, e := filepath.Rel(root, source)
		if e != nil {
			return files, e
		}
		target := filepath.Join(destination, relative)
		if e = os.MkdirAll(filepath.Dir(target), 0700); e != nil {
			return files, e
		}
		input, e := os.Open(source)
		if e != nil {
			return files, e
		}
		output, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			input.Close()
			return files, e
		}
		_, e = io.Copy(output, input)
		input.Close()
		closeErr := output.Close()
		if e != nil {
			return files, e
		}
		if closeErr != nil {
			return files, closeErr
		}
	}
	return files, nil
}

// fixtureRecords visits the fixture records.
func fixtureRecords(path string, visit func(control.Record) error) error {
	if strings.HasSuffix(path, ".parquet") {
		return visitStagingFixture(path, func(r exportRecord) error { return visit(r.Record) })
	}
	input, e := os.Open(path)
	if e != nil {
		return e
	}
	defer input.Close()
	decoder := json.NewDecoder(bufio.NewReaderSize(input, 1<<20))
	for {
		var record control.Record
		if e = decoder.Decode(&record); errors.Is(e, io.EOF) {
			return nil
		}
		if e != nil {
			return e
		}
		if e = visit(record); e != nil {
			return e
		}
	}
}

// fixtureHas checks if the fixture has a record.
func fixtureHas(path, name, value string) (bool, error) {
	found := false
	e := fixtureRecords(path, func(r control.Record) error {
		if r.Name == name && r.Value == value {
			found = true
		}
		return nil
	})
	return found, e
}

// Independent expected logical state, rather than export/query agreement alone.
// fixtureExpected returns the fixture expected.
func fixtureExpected(path, day, temperature string, victim bool, fillers int) error {
	file, e := os.Create(path)
	if e != nil {
		return e
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encode := func(stamp int64, name, kind, value string, tags map[string]string) error {
		return encoder.Encode(control.Record{Time: stamp, Name: name, Kind: kind, Value: value, Tags: tags})
	}
	switch day {
	case "2025-02-28":
		return encode(fixtureStart-1, "boundary", "int", "0", map[string]string{"node": "n1"})
	case "2025-03-02":
		for i, stamp := range []int64{fixtureEnd, fixtureEnd + 1} {
			if e = encode(stamp, "boundary", "int", strconv.Itoa(i+3), map[string]string{"node": "n1"}); e != nil {
				return e
			}
		}
		return nil
	case "2025-03-01":
	default:
		return fmt.Errorf("unknown fixture day %s", day)
	}
	if fillers < 0 {
		return nil
	} // Empty target day after the verified fixture deletion.
	rows := []control.Record{
		{Time: fixtureStart + 194, Name: "temp", Kind: "float", Value: temperature, Tags: map[string]string{"plugin": "p", "node": "n1"}},
		{Time: fixtureStart + 195, Name: "count", Kind: "int", Value: "9007199254740993", Tags: map[string]string{"node": "n1"}},
		{Time: fixtureStart + 196, Name: "status", Kind: "string", Value: "123", Tags: map[string]string{"node": "n1"}},
		{Time: fixtureStart + 197, Name: "flag", Kind: "bool", Value: "true", Tags: map[string]string{"node": "n1"}},
		{Time: fixtureStart + 198, Name: "waltemp", Kind: "float", Value: "11.9", Tags: map[string]string{"node": "n1"}},
		{Time: fixtureStart, Name: "boundary", Kind: "int", Value: "1", Tags: map[string]string{"node": "n1"}},
		{Time: fixtureEnd - 1, Name: "boundary", Kind: "int", Value: "2", Tags: map[string]string{"node": "n1"}},
	}
	if victim {
		rows = append(rows, control.Record{Time: fixtureStart + 300, Name: "victim", Kind: "int", Value: "23", Tags: map[string]string{"node": "n1"}})
	}
	for n, value := range fixtureStrings {
		rows = append(rows, control.Record{Time: fixtureStart + 500 + int64(n), Name: "multiline", Kind: "string", Value: value, Tags: map[string]string{"node": "n1"}})
	}
	for _, row := range rows {
		if e = encoder.Encode(row); e != nil {
			return e
		}
	}
	for n := 0; n < fillers; n++ {
		if e = encode(fixtureStart+10000+int64(n), "filler", "int", strconv.Itoa(n), map[string]string{"node": "n1"}); e != nil {
			return e
		}
	}
	return nil
}

// fixtureCompactions is the compactions of the fixture.
type fixtureCompactions struct {
	Completed float64 `json:"completed_full"` // the completed full compactions
	Failed    float64 `json:"failed_full"` // the failed full compactions
	Active    float64 `json:"active_full"` // the active full compactions
}

// fixtureMetric is the metric of the fixture.
var fixtureMetric = regexp.MustCompile(`^(storage_compactions_duration_seconds_count|storage_compactions_failed(?:_total)?|storage_compactions_active)\{(.*)\}\s+(\S+)(?:\s+\S+)?$`)

// fixtureLabel is the label of the fixture.
var fixtureLabel = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)=("(?:\\.|[^"\\])*")`)

// Scope evidence to the test-created bucket AND target shard. A cache snapshot,
// an optimization or a compaction in another bucket cannot satisfy this gate.
// fixtureFullMetrics returns the full metrics of the fixture.
func fixtureFullMetrics(text, bucket, shard string) (out fixtureCompactions, err error) {
	if !strings.Contains(text, "# HELP storage_compactions_duration_seconds") {
		return out, errors.New("fixture /metrics lacks compaction duration family")
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		match := fixtureMetric.FindStringSubmatch(scanner.Text())
		if match == nil {
			continue
		}
		labels := map[string]string{}
		for _, pair := range fixtureLabel.FindAllStringSubmatch(match[2], -1) {
			value, e := strconv.Unquote(pair[2])
			if e != nil {
				return out, e
			}
			labels[pair[1]] = value
		}
		if labels["bucket"] != bucket || labels["id"] != shard || labels["level"] != "full" {
			continue
		}
		value, e := strconv.ParseFloat(match[3], 64)
		if e != nil || value < 0 || value != value || value > 1e15 {
			return out, errors.New("invalid fixture compaction metric")
		}
		switch match[1] {
		case "storage_compactions_duration_seconds_count":
			out.Completed += value
		case "storage_compactions_active":
			out.Active += value
		default:
			out.Failed += value
		}
	}
	return out, scanner.Err()
}

// fixtureWait waits for the fixture.
func fixtureWait(ctx context.Context, label string, probe func(context.Context) (bool, error)) error {
	deadline, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		if e := deadline.Err(); e != nil {
			return fmt.Errorf("waiting for %s: %w; last observation: %v", label, e, last)
		}
		ready, e := probe(deadline)
		if e != nil {
			last = e
		}
		if ready && e == nil {
			return nil
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("waiting for %s: %w; last observation: %v", label, deadline.Err(), last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
