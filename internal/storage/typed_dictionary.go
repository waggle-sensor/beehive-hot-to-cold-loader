// typed_dictionary.go builds the canonical tag string once per distinct
// promoted-tag tuple, then joins it back to every source row.

package storage

import "strings"

// typedTagColumns lists the promoted tags plus meta, in dictionary order.
func typedTagColumns() string { return strings.Join(Tags, ",") + ",meta" }

// typedDictionaryCanonical reconstructs tags from each typed input's actual
// promoted columns and metadata. Repeated tuples serialize once per query; every
// row retains the same canonical time/name/tags/kind/value used by Canonical.
// Used for both build inputs and the local expected side of remote proofs.
// The Iceberg side and independent Equal proofs deliberately keep Canonical.
func typedDictionaryCanonical(source string) string {
	keys := append(append([]string{}, Tags...), "meta")
	predicates := make([]string, len(keys))
	for i, key := range keys {
		predicates[i] = "s." + key + " IS NOT DISTINCT FROM d." + key
	}
	return "WITH src AS NOT MATERIALIZED (" + source + "),tag_dictionary AS MATERIALIZED (SELECT " + typedTagColumns() + "," + IdentityTags() + "::VARCHAR AS tags FROM (SELECT DISTINCT " + typedTagColumns() + " FROM src)) SELECT epoch_ns(s.time) AS time,s.name,d.tags," + logicalValueKind("s.value") + " AS kind,to_json(s.value)::VARCHAR AS value FROM src s JOIN tag_dictionary d ON " + strings.Join(predicates, " AND ")
}
