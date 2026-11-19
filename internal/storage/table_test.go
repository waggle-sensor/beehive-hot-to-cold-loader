package storage

import (
	"beehive-hot-to-cold-loader/internal/control"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestTableIdentity(t *testing.T) {
	var defaults TableIdentity
	if defaults.Namespace() != "sage" || defaults.Name() != "observations" || defaults.Path() != "sage/observations" {
		t.Fatal(defaults)
	}
	custom, err := NewTableIdentity("telemetry", "readings_2026")
	if err != nil {
		t.Fatal(err)
	}
	if custom.Location("https://origin/sage/cold/lakehouse") != "https://origin/sage/cold/lakehouse/telemetry/readings_2026" {
		t.Fatal(custom)
	}
	for _, value := range []string{"", ".", "..", "a/b", "a.b", "a%2fb", "a b", "a-b", "1table", "name?x", "name#x"} {
		if _, err := NewTableIdentity(value, "observations"); err == nil {
			t.Fatal("invalid namespace", value)
		}
		if _, err := NewTableIdentity("sage", value); err == nil {
			t.Fatal("invalid table", value)
		}
	}
}

func TestCustomTableCatalogAndState(t *testing.T) {
	base, root := mockPelican(t)
	identity, err := NewTableIdentity("telemetry", "readings")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewPelican(base.Base.String(), filepath.Join(root, "key.pem"), root, identity)
	if err != nil {
		t.Fatal(err)
	}
	client.Client = base.Client
	cat, err := OpenCatalog(context.Background(), filepath.Join(root, "catalog.db"), base.Base.String(), client)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.DB.Close()
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	table, err := cat.Catalog.LoadTable(context.Background(), cat.Identifier())
	if err != nil {
		t.Fatal(err)
	}
	if table.Location() != identity.Location(base.Base.String()) {
		t.Fatal(table.Location())
	}
	if err := cat.PublishHint(context.Background(), table.MetadataLocation()); err != nil {
		t.Fatal(err)
	}
	head, err := client.Head()
	if err != nil || head != table.MetadataLocation() {
		t.Fatal(head, err)
	}
	dayPrefix, err := cat.DayPrefix(context.Background(), "2025-03-01")
	if err != nil || !strings.HasPrefix(dayPrefix, identity.Location(base.Base.String())+"/data/") {
		t.Fatal(dayPrefix, err)
	}
	if _, err := client.validate((TableIdentity{}).Location(base.Base.String()) + "/metadata/head.metadata.json"); err == nil {
		t.Fatal("accepted a different table")
	}

	store, err := control.Open(filepath.Join(root, "control.db"), "waggle", base.Base.String(), "2025-03-01")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := identity.BindState(store); err != nil {
		t.Fatal(err)
	}
	if err := identity.BindState(store); err != nil {
		t.Fatal("same-table restart", err)
	}
	for _, pair := range [][2]string{{"other", "readings"}, {"telemetry", "other"}} {
		changed, _ := NewTableIdentity(pair[0], pair[1])
		if err := changed.BindState(store); err == nil {
			t.Fatal("reused state for another table", pair)
		}
	}
	if _, err := store.DB.Exec("UPDATE config SET metadata=? WHERE id=1", (TableIdentity{}).Location(base.Base.String())+"/metadata/head.metadata.json"); err != nil {
		t.Fatal(err)
	}
	if err := identity.BindState(store); err == nil {
		t.Fatal("accepted a published pointer to another table")
	}
}
