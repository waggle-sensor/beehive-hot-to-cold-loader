// value.go defines the on-disk uint64 convention inside Iceberg VARIANT field 11.

package storage

// ValueEncodingProperty is the Iceberg table property that names this convention.
// ValueEncoding is the only value that property may have on a managed table.
const ValueEncodingProperty = "sage.value.encoding"
const ValueEncoding = "variant-uint64-decimal-v1"

// PrimitiveValue preserves the five source kinds. Decimal is reserved for uint64:
// Parquet cannot retain a native unsigned VARIANT subtype over its full range.
func PrimitiveValue() string {
	return `CASE kind WHEN 'float' THEN value::DOUBLE::VARIANT WHEN 'int' THEN value::BIGINT::VARIANT WHEN 'uint' THEN value::UBIGINT::DECIMAL(20,0)::VARIANT WHEN 'bool' THEN value::BOOLEAN::VARIANT WHEN 'string' THEN value::VARCHAR::VARIANT ELSE error('invalid value kind') END`
}

// logicalValueKind compares the source kind independently of Parquet's decimal
// precision normalization (20 becomes 38 on read). Invalid encodings fail proofs.
func logicalValueKind(value string) string {
	return "CASE WHEN variant_typeof(" + value + ") IN ('DECIMAL(20, 0)','DECIMAL(38, 0)') THEN CASE WHEN " + value + "::DECIMAL(38,0) BETWEEN 0 AND 18446744073709551615 THEN 'UINT64' ELSE error('invalid encoded uint64') END ELSE variant_typeof(" + value + ")::VARCHAR END"
}
