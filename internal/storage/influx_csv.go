// influx_csv.go decodes annotated Flux CSV into primitive staging rows.
// Repeated tag tuples reuse one JSON encoding.

package storage

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

type csvTagColumn struct {
	index      int
	name       string
	stringType bool
}
type csvColumnPlan struct {
	names, types             []string
	time, name, field, value int
	valueType                string
	tags                     []csvTagColumn
}

// compileCSVColumns locates time, measurement, field, value, and tag columns.
func compileCSVColumns(names, types []string) (*csvColumnPlan, error) {
	if len(names) == 0 || len(names) != len(types) {
		return nil, errors.New("malformed annotated CSV")
	}
	plan := &csvColumnPlan{names: names, types: types, time: -1, name: -1, field: -1, value: -1}
	seen := map[string]bool{}
	for index, columnName := range names {
		if seen[columnName] {
			return nil, errors.New("duplicate annotated CSV column")
		}
		seen[columnName] = true
		switch columnName {
		case "_time":
			plan.time = index
		case "_measurement":
			plan.name = index
		case "_field":
			plan.field = index
		case "_value":
			plan.value = index
			plan.valueType = types[index]
		default:
			if columnName != "" && columnName != "result" && columnName != "table" && !strings.HasPrefix(columnName, "_") {
				plan.tags = append(plan.tags, csvTagColumn{index, columnName, types[index] == "string"})
			}
		}
	}
	if plan.time < 0 || plan.name < 0 || plan.field < 0 || plan.value < 0 {
		return nil, errors.New("bucket violates observation schema")
	}
	return plan, nil
}

// Only internal synchronous sinks may borrow canonical tags. The generic
// Encode(control.Record) contract retains independent, sink-owned tag maps.
type csvStagingEncoder interface{ encodeCSV(stagingView) error }

func (records *parquetRecords) encodeCSV(view stagingView) error { return records.appendView(view) }

type csvTagCache struct {
	values []string
	tags   map[string]string
	json   string
}

// update rebuilds the tag JSON when this row's tag values differ from the cache.
func (cache *csvTagCache) update(plan *csvColumnPlan, row []string) error {
	same := cache.tags != nil
	if same {
		for index, col := range plan.tags {
			if cache.values[index] != row[col.index] {
				same = false
				break
			}
		}
	}
	if same {
		return nil
	}
	values := make([]string, len(plan.tags))
	tags := make(map[string]string, len(plan.tags))
	for index, col := range plan.tags {
		value := row[col.index]
		if value != "" {
			if !col.stringType {
				return errors.New("non-string tag")
			}
			value = strings.Clone(value)
			tags[col.name] = value
		}
		values[index] = value
	}
	encoded, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	*cache = csvTagCache{values, tags, string(encoded)}
	return nil
}

// csvScalar parses one annotated value into the canonical text and kind.
func csvScalar(value, kind string) (string, string, error) {
	var parsed any
	var err error
	switch kind {
	case "double":
		parsed, err = strconv.ParseFloat(value, 64)
	case "long":
		parsed, err = strconv.ParseInt(value, 10, 64)
	case "unsignedLong":
		parsed, err = strconv.ParseUint(value, 10, 64)
	case "boolean":
		parsed, err = strconv.ParseBool(value)
	case "string":
		parsed = value
	default:
		return "", "", fmt.Errorf("unsupported Flux value type %s", kind)
	}
	if err != nil {
		return "", "", err
	}
	return Value(parsed)
}

// DecodeCSV reads one annotated result table and emits rows inside [low, high).
func DecodeCSV(reader io.Reader, enc recordEncoder, low, high int64) error {
	csvr := csv.NewReader(reader)
	csvr.FieldsPerRecord = -1
	csvr.ReuseRecord = true
	var names, types, defaults []string
	var plan *csvColumnPlan
	var cache csvTagCache
	borrowed, acceptsView := enc.(csvStagingEncoder)
	for {
		row, err := csvr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(row) == 0 {
			continue
		}
		if strings.HasPrefix(row[0], "#") {
			switch row[0] {
			case "#datatype":
				types = append([]string(nil), row...)
				names = nil
				defaults = nil
			case "#default":
				defaults = append([]string(nil), row...)
			}
			continue
		}
		if len(row) > 1 && row[1] == "result" {
			names = append([]string(nil), row...)
			if plan != nil && (!slices.Equal(plan.names, names) || !slices.Equal(plan.types, types)) {
				plan = nil
				cache = csvTagCache{}
			}
			continue
		}
		if len(row) > 1 && row[1] == "error" {
			return errors.New("Influx query error table")
		}
		if len(names) == 0 || len(row) != len(names) || len(types) != len(names) {
			return errors.New("malformed annotated CSV")
		}
		if plan == nil {
			plan, err = compileCSVColumns(names, types)
			if err != nil {
				return err
			}
		}
		for index := range row {
			if row[index] == "" && index < len(defaults) {
				row[index] = defaults[index]
			}
		}
		if row[plan.field] != "value" {
			return errors.New("bucket violates observation schema")
		}
		stamp, err := time.Parse(time.RFC3339Nano, row[plan.time])
		if err != nil {
			return err
		}
		record := control.Record{Time: stamp.UnixNano(), Name: row[plan.name]}
		if record.Time < low || record.Time >= high {
			return errors.New("query returned an out-of-window record")
		}
		record.Kind, record.Value, err = csvScalar(row[plan.value], plan.valueType)
		if err != nil {
			return err
		}
		if acceptsView {
			if err = cache.update(plan, row); err != nil {
				return err
			}
			record.Tags = cache.tags
		} else {
			record.Tags = make(map[string]string, len(plan.tags))
			for _, col := range plan.tags {
				if row[col.index] != "" {
					if !col.stringType {
						return errors.New("non-string tag")
					}
					record.Tags[col.name] = row[col.index]
				}
			}
		}
		if err = record.Validate(); err != nil {
			return err
		}
		if acceptsView {
			err = borrowed.encodeCSV(stagingView{Time: record.Time, Name: record.Name, Tags: cache.json, Kind: record.Kind, Value: record.Value})
		} else {
			err = enc.Encode(record)
		}
		if err != nil {
			return err
		}
	}
}
