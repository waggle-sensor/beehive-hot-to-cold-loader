// reconciliation.go compares the normalized export with a fresh Influx extract.
// Both sides deduplicate the original tag JSON before parsing it, so a shared
// string is canonicalized once and is never trusted as already canonical.

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Deduplicate original JSON before parsing promoted tags on each fresh input.
// Never trust its text as canonical: preserve Typed/IdentityTags semantics,
// including omitted null promoted keys and sorted non-promoted metadata.
func reconciliationDictionarySQL(source string) string {
	columns, keys := []string{}, []string{}
	for _, tag := range Tags {
		columns = append(columns, "tag_map["+Q(tag)+"] AS "+tag)
		keys = append(keys, Q(tag))
	}
	meta := "map_from_entries(list_filter(map_entries(tag_map),lambda x: x.key NOT IN (" + strings.Join(keys, ",") + "))) AS meta"
	value := PrimitiveValue()
	return "WITH src AS NOT MATERIALIZED (" + source + "),tag_dictionary AS MATERIALIZED (SELECT raw_tags," + IdentityTags() + "::VARCHAR AS canonical_tags FROM (SELECT raw_tags," + strings.Join(columns, ",") + "," + meta + " FROM (SELECT tags AS raw_tags,tags::JSON::MAP(VARCHAR,VARCHAR) AS tag_map FROM (SELECT DISTINCT tags FROM src)))) SELECT epoch_ns(make_timestamp_ns(s.time)::TIMESTAMPTZ_NS) AS time,s.name,d.canonical_tags AS tags," + logicalValueKind("s.value") + " AS kind,to_json(s.value)::VARCHAR AS value FROM (SELECT time,name,tags," + value + " AS value FROM src) s JOIN tag_dictionary d ON s.tags IS NOT DISTINCT FROM d.raw_tags"
}

// reconciliationPrimitiveSource reads one local primitive Parquet input.
func reconciliationPrimitiveSource(path string) string { return Parquet([]string{path}) }

// EqualReconciliation compares independently extracted local primitive Parquet.
// Every call freshly reads both inputs and rebuilds its own dictionaries/staging.
// Original Equal remains the independent typed/remote/pre-delete proof path.
func (d DuckDB) EqualReconciliation(ctx context.Context, export, query string) error {
	paths := []string{export, query}
	infos := make([]os.FileInfo, 2)
	for i, path := range paths {
		local, err := localPath(path)
		if err != nil {
			return err
		}
		info, err := os.Lstat(local)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || filepath.Ext(local) != ".parquet" {
			return fmt.Errorf("reconciliation requires regular local primitive Parquet")
		}
		paths[i], infos[i] = local, info
	}
	if os.SameFile(infos[0], infos[1]) {
		return fmt.Errorf("reconciliation requires independent input files")
	}
	err := d.EqualCanonical(ctx, reconciliationDictionarySQL(reconciliationPrimitiveSource(paths[0])), reconciliationDictionarySQL(reconciliationPrimitiveSource(paths[1])), "")
	if d.comparisonProfile != nil {
		d.comparisonProfile.Canonicalization = "raw_tag_dictionary"
	}
	return err
}
