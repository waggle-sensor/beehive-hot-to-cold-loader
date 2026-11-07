// remote_comparison_test.go checks that the remote benchmark refuses a client that
// can write, and that an object hash is verified through the read-only client.

package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestRemoteBenchmarkObjectIntegrityAndReadOnly(t *testing.T) {
	writer, _ := mockPelican(t)
	object := writer.Base.String() + "/sage/obs/data/test.parquet"
	bytes := []byte("fixture object bytes")
	if e := writer.WriteFile(object, bytes); e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(bytes)
	target := remoteBenchmarkTarget{FileSHA256: map[string]string{object: hex.EncodeToString(hash[:])}}
	if e := verifyRemoteObjectHashes(context.Background(), writer, target); e == nil {
		t.Fatal("benchmark accepted write-capable reader")
	}
	reader := *writer
	reader.ReadOnly = true
	if e := verifyRemoteObjectHashes(context.Background(), &reader, target); e != nil {
		t.Fatal(e)
	}
	if e := reader.WriteFile(object, []byte("not allowed")); e == nil {
		t.Fatal("benchmark reader wrote an object")
	}
	if e := writer.WriteFile(object, []byte("changed")); e != nil {
		t.Fatal(e)
	}
	if e := verifyRemoteObjectHashes(context.Background(), &reader, target); e == nil {
		t.Fatal("changed bytes accepted")
	}
	target.FileSHA256 = map[string]string{"https://outside.test/sage/cold-storage-other/warehouse/sage/obs/data/test.parquet": hex.EncodeToString(hash[:])}
	if e := verifyRemoteObjectHashes(context.Background(), &reader, target); e == nil {
		t.Fatal("cross-origin object accepted")
	}
}
