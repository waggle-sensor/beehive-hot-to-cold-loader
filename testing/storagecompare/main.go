// storagecompare is an experiment-only LP decoder. It imports no loader/API
// storage code: stdin is native inspect export-lp, stdout is selected NDJSON.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	lp "github.com/influxdata/line-protocol"
)

type observation struct {
	Time  int64  `json:"time"`
	Name  string `json:"name"`
	Tags  string `json:"tags"`
	Kind  string `json:"kind"`
	Value string `json:"value"`
	Seq   int64  `json:"seq"`
}

func value(v any) (string, string, error) {
	switch x := v.(type) {
	case int64:
		return "int", strconv.FormatInt(x, 10), nil
	case uint64:
		return "uint", strconv.FormatUint(x, 10), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", "", errors.New("nonfinite value")
		}
		return "float", strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return "string", x, nil
	case bool:
		return "bool", strconv.FormatBool(x), nil
	}
	return "", "", fmt.Errorf("unsupported value %T", v)
}

func decode(input io.Reader, output io.Writer, low, high int64, name, vsn, plugin string) error {
	parser := lp.NewStreamParser(bufio.NewReaderSize(input, 1<<20))
	enc := json.NewEncoder(output)
	var seq int64
	for {
		point, err := parser.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, lp.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid export LP: %w", err)
		}
		t := point.Time().UnixNano()
		// Native export may include the upper boundary. Enforce [start,end).
		if t < low || t > high {
			return errors.New("export point outside requested bounds")
		}
		if t == high {
			continue
		}
		tags := map[string]string{}
		for _, tag := range point.TagList() {
			tags[tag.Key] = tag.Value
		}
		if name != "" && point.Name() != name || vsn != "" && tags["vsn"] != vsn || plugin != "" && tags["plugin"] != plugin {
			continue
		}
		fields := point.FieldList()
		if len(fields) != 1 || fields[0].Key != "value" {
			return errors.New("selected point violates single value-field contract")
		}
		kind, text, err := value(fields[0].Value)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(tags)
		if err != nil {
			return err
		}
		if seq == math.MaxInt64 {
			return errors.New("sequence overflow")
		}
		seq++
		if err = enc.Encode(observation{t, point.Name(), string(encoded), kind, text, seq}); err != nil {
			return err
		}
	}
}

func main() {
	start := flag.String("start", "", "inclusive UTC bound")
	end := flag.String("end", "", "exclusive UTC bound")
	name := flag.String("name", "", "literal measurement")
	vsn := flag.String("vsn", "", "literal vsn")
	plugin := flag.String("plugin", "", "literal plugin")
	flag.Parse()
	low, err := time.Parse(time.RFC3339Nano, *start)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	high, err := time.Parse(time.RFC3339Nano, *end)
	if err != nil || !low.Before(high) {
		fmt.Fprintln(os.Stderr, "increasing UTC bounds required")
		os.Exit(1)
	}
	writer := bufio.NewWriterSize(os.Stdout, 1<<20)
	if err = decode(os.Stdin, writer, low.UnixNano(), high.UnixNano(), *name, *vsn, *plugin); err == nil {
		err = writer.Flush()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
