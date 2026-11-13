package control

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLakehouseIdentityMigrationPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	location := "https://origin/sage/cold/lakehouse"
	store, err := Open(path, "waggle", location, "2025-03-01")
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.View("reader", "request")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	oldPayload := strings.Replace(string(payload), `"lakehouse":`, `"warehouse":`, 1)
	if _, err := store.DB.Exec("UPDATE readers SET payload=? WHERE id=?", oldPayload, view.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("INSERT INTO options VALUES('bootstrap.warehouse',?)", location+"-source"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("ALTER TABLE config RENAME COLUMN lakehouse TO warehouse"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	for attempt := 0; attempt < 2; attempt++ {
		store, err = Open(path, "waggle", location, "2025-03-01")
		if err != nil {
			t.Fatal(err)
		}
		restored, err := store.View("reader", "request")
		if err != nil || restored != view {
			t.Fatal(restored, view, err)
		}
		option, err := store.Option("bootstrap.lakehouse")
		if err != nil || option != location+"-source" {
			t.Fatal(option, err)
		}
		var count int
		if err := store.DB.QueryRow("SELECT count(*) FROM options WHERE key='bootstrap.warehouse'").Scan(&count); err != nil || count != 0 {
			t.Fatal(count, err)
		}
		store.Close()
	}
}

func TestLakehouseIdentityMigrationRollsBackConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	store, err := Open(path, "waggle", "https://origin/sage/cold/lakehouse", "2025-03-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("INSERT INTO options VALUES('bootstrap.warehouse','old'),('bootstrap.lakehouse','different')"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("ALTER TABLE config RENAME COLUMN lakehouse TO warehouse"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	if failed, err := Open(path, "waggle", "https://origin/sage/cold/lakehouse", "2025-03-01", time.Minute); err == nil {
		failed.Close()
		t.Fatal("accepted conflicting bootstrap identities")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var location string
	if err := db.QueryRow("SELECT warehouse FROM config WHERE id=1").Scan(&location); err != nil {
		t.Fatal("schema rename was not rolled back", err)
	}
}
