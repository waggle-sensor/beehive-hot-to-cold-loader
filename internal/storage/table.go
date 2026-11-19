// TableIdentity keeps catalog names and their storage paths in sync.
package storage

import (
	"fmt"
	"regexp"
)

const DefaultNamespace = "sage"
const DefaultTableName = "observations"

// TableIdentity's zero value selects the default table.
type TableIdentity struct{ namespace, name string }

// NewTableIdentity creates a new TableIdentity.
func NewTableIdentity(namespace, name string) (TableIdentity, error) {
	identifier := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	if !identifier.MatchString(namespace) || !identifier.MatchString(name) {
		return TableIdentity{}, fmt.Errorf("Iceberg namespace and table name must start with a letter or underscore and contain only letters, digits, or underscores")
	}
	return TableIdentity{namespace: namespace, name: name}, nil
}

// Namespace returns the namespace of the TableIdentity.
func (t TableIdentity) Namespace() string {
	if t.namespace == "" {
		return DefaultNamespace
	}
	return t.namespace
}

// Name returns the name of the TableIdentity.
func (t TableIdentity) Name() string {
	if t.name == "" {
		return DefaultTableName
	}
	return t.name
}

// Path returns the path of the TableIdentity.
func (t TableIdentity) Path() string { return t.Namespace() + "/" + t.Name() }

// Location returns the location of the TableIdentity in the lakehouse.
func (t TableIdentity) Location(lakehouse string) string { return lakehouse + "/" + t.Path() }
