package storage

import (
	"fmt"
	"strings"

	"beehive-hot-to-cold-loader/internal/control"
)

// BindState prevents a restart from routing an existing journal to another table.
func (t TableIdentity) BindState(store *control.Store) error {
	metadata, err := store.Metadata()
	if err != nil {
		return err
	}
	if metadata != "" && !strings.HasPrefix(metadata, t.Location(store.Lakehouse)+"/metadata/") {
		return fmt.Errorf("published metadata differs from configured Iceberg table; do not reuse state for another table")
	}
	if err := store.Bind("table.namespace", t.Namespace()); err != nil {
		return err
	}
	return store.Bind("table.name", t.Name())
}
