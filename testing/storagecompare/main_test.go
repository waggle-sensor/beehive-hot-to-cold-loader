package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodePreservesValuesTagsAndNanoseconds(t *testing.T) {
	text := "sys.rssh_up,vsn=W023,node=n1 value=18446744073709551615u 1647388800000000194\n" +
		"sys.rssh_up,vsn=W023,node=n1 value=\"first\nsecond\" 1647388800000000195\n" +
		"sys.rssh_up,vsn=W024 value=1i 1647388800000000196\n" +
		"sys.rssh_up,vsn=W023 value=2i 1647475200000000000\n"
	var out bytes.Buffer
	if err := decode(strings.NewReader(text), &out, 1647388800000000000, 1647475200000000000, "sys.rssh_up", "W023", ""); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&out)
	var first, second observation
	if err := dec.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&second); err != nil {
		t.Fatal(err)
	}
	if first.Time != 1647388800000000194 || first.Kind != "uint" || first.Value != "18446744073709551615" || first.Tags != `{"node":"n1","vsn":"W023"}` || second.Value != "first\nsecond" || second.Seq != 2 {
		t.Fatalf("changed records: %+v %+v", first, second)
	}
	if dec.More() {
		t.Fatal("filter or exclusive upper bound failed")
	}
}

func TestDecodeRejectsInvalidContracts(t *testing.T) {
	for _, text := range []string{
		"x value=1i,other=2i 100\n", "x value=1i 99\n", "x value=1i 201\n", "invalid LP\n",
	} {
		if err := decode(strings.NewReader(text), &bytes.Buffer{}, 100, 200, "", "", ""); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
}

func TestReplayOrderIsRetained(t *testing.T) {
	var out bytes.Buffer
	if err := decode(strings.NewReader("x value=1i 100\nx value=2i 100\n"), &out, 100, 200, "", "", ""); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&out)
	for seq := int64(1); seq <= 2; seq++ {
		var row observation
		if err := dec.Decode(&row); err != nil || row.Seq != seq {
			t.Fatal(row, err)
		}
	}
}
