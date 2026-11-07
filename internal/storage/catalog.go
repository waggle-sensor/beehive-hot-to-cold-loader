// catalog.go owns the managed Iceberg table sage.obs. The schema, day(time)
// partition, and layout properties are fixed; a mismatch refuses startup.

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Catalog is the local SQL catalog plus the remote warehouse it publishes to.
type Catalog struct {
	DB        *sql.DB         // the database for the catalog
	Catalog   *sqlcat.Catalog // the catalog
	IO        *Pelican        // the Pelican client
	Warehouse string          // the warehouse for the catalog
}

// Schema is the published v3 schema: promoted tags, a meta map, and a VARIANT value.
func Schema() *iceberg.Schema {
	fields := []iceberg.NestedField{{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}}, {ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String}}
	for i, k := range Tags {
		fields = append(fields, iceberg.NestedField{ID: i + 3, Name: k, Type: iceberg.PrimitiveTypes.String})
	}
	fields = append(fields, iceberg.NestedField{ID: 10, Name: "meta", Type: &iceberg.MapType{KeyID: 12, KeyType: iceberg.PrimitiveTypes.String, ValueID: 13, ValueType: iceberg.PrimitiveTypes.String}}, iceberg.NestedField{ID: 11, Name: "value", Type: iceberg.VariantType{}})
	return iceberg.NewSchema(0, fields...)
}

var registerHTTPS sync.Once            // registerHTTPS is used to register the HTTPS transport for the Pelican instance
var transportsMu sync.RWMutex          // transportsMu is used to lock the transports map
var transports = map[string]*Pelican{} // transports is a map of warehouses to Pelican instances

// OpenCatalog opens or creates the local catalog database for this warehouse.
func OpenCatalog(ctx context.Context, path, warehouse string, p *Pelican) (*Catalog, error) {
	// Only this daemon owns the HTTPS implementation. Scope validation occurs on
	// every operation, including manifest references supplied by Iceberg itself.
	transportsMu.Lock()
	transports[warehouse] = p
	transportsMu.Unlock()
	registerHTTPS.Do(func() {
		iceio.Register("https", func(_ context.Context, u *url.URL, _ map[string]string) (iceio.IO, error) {
			transportsMu.RLock()
			defer transportsMu.RUnlock()
			for _, io := range transports {
				if u.Scheme == io.Base.Scheme && u.Host == io.Base.Host && (u.Path == io.Base.Path || strings.HasPrefix(u.Path, io.Base.Path+"/")) {
					return io, nil
				}
			}
			return nil, errors.New("no transport registered for this managed warehouse")
		})
	})
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	if _, e = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=10000;`); e != nil {
		db.Close()
		return nil, e
	}
	cat, e := sqlcat.NewCatalog("sage", db, sqlcat.SQLite, iceberg.Properties{"warehouse": warehouse})
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Catalog{db, cat, p, warehouse}, nil
}

var ident = table.Identifier{"sage", "obs"} // ident is the identifier for the sage.obs table

// Init accepts an existing matching table or creates one in an empty prefix.
func (c *Catalog) Init(ctx context.Context) error {
	t, e := c.Catalog.LoadTable(ctx, ident)
	if e == nil {
		if t.Metadata().Version() != 3 || !t.Schema().Equals(Schema()) || t.Location() != c.Warehouse+"/sage/obs" {
			return errors.New("existing managed table schema/location differs")
		}
		if _, e = partitionField(t); e != nil {
			return e
		}
		for key, value := range map[string]string{ValueEncodingProperty: ValueEncoding, "sage.layout": "plugin-vsn-time", "sage.layout.version": "bounded-plugin-v1", "sage.batch.order": "time", "sage.row-group.rows": "262144", "write.metadata.delete-after-commit.enabled": "false"} {
			if t.Metadata().Properties()[key] != value {
				return fmt.Errorf("managed layout property %s differs", key)
			}
		}
		return nil
	}
	if !errors.Is(e, catalog.ErrNoSuchTable) {
		return e
	}
	// A missing local catalog must not silently adopt an occupied remote table.
	tok, err := c.IO.Token(false)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", c.Warehouse+"/sage/obs", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Depth", "0")
	resp, err := c.IO.Client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		return fmt.Errorf("new managed table root must be absent; PROPFIND HTTP %d", resp.StatusCode)
	}
	if e = c.Catalog.CreateNamespace(ctx, table.Identifier{"sage"}, nil); e != nil && !errors.Is(e, catalog.ErrNamespaceAlreadyExists) {
		return e
	}
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "day_time", Transform: iceberg.DayTransform{}})
	// CreateTable reassigns IDs depth-first (making value ID 13). Use the
	// catalog's atomic create transaction with explicit schema updates instead.
	unsorted := table.UnsortedSortOrder
	_, _, e = c.Catalog.CommitTable(ctx, ident, []table.Requirement{table.AssertCreate()}, []table.Update{
		table.NewUpgradeFormatVersionUpdate(3), table.NewAssignUUIDUpdate(uuid.New()),
		table.NewAddSchemaUpdate(Schema()), table.NewSetCurrentSchemaUpdate(-1),
		table.NewAddPartitionSpecUpdate(&spec, true), table.NewSetDefaultSpecUpdate(-1),
		table.NewAddSortOrderUpdate(&unsorted), table.NewSetDefaultSortOrderUpdate(-1),
		table.NewSetLocationUpdate(c.Warehouse + "/sage/obs"),
		table.NewSetPropertiesUpdate(iceberg.Properties{
			ValueEncodingProperty: ValueEncoding, "sage.layout": "plugin-vsn-time", "sage.layout.version": "bounded-plugin-v1",
			"sage.batch.order": "time", "sage.row-group.rows": "262144",
			"write.metadata.delete-after-commit.enabled": "false",
		}),
	})
	return e
}

// Commit appends data files, or replaces them for a late correction. A retry
// of the same job returns the snapshot already recorded for that job.
func (c *Catalog) Commit(ctx context.Context, job *control.Job) (string, error) {
	t, e := c.Catalog.LoadTable(ctx, ident)
	if e != nil {
		return "", e
	}
	if t.Metadata().Properties()[ValueEncodingProperty] != ValueEncoding {
		return "", errors.New("managed table value encoding differs; refusing to publish")
	}
	if t.Metadata().Properties()["sage.last.job"] == job.ID {
		return t.MetadataLocation(), nil
	}
	// One writer, one pending job; recovery runs before any new commit.
	tx := t.NewTransaction()
	if e = tx.AssertRefSnapshotID(""); e != nil {
		return "", e
	}
	props := iceberg.Properties{"sage.job": job.ID, "sage.day": job.Day, "sage.mode": job.Mode}
	if job.Mode == "late" {
		e = tx.ReplaceDataFiles(ctx, job.OldFiles, job.Files, props)
	} else if len(job.Files) > 0 {
		e = tx.AddFiles(ctx, job.Files, props, false, table.WithAddFilesConcurrency(1))
	}
	if e != nil {
		return "", e
	}
	files, _ := json.Marshal(job.Files)
	if e = tx.SetProperties(iceberg.Properties{"sage.last.job": job.ID, "sage.last.files": string(files)}); e != nil {
		return "", e
	}
	t, e = tx.Commit(ctx)
	if e != nil {
		return "", e
	}
	return t.MetadataLocation(), nil
}

// PublishHint points version-hint.text at metadata and reads it back.
func (c *Catalog) PublishHint(ctx context.Context, metadata string) error {
	if _, e := c.IO.validate(metadata); e != nil {
		return e
	}
	base := metadata[strings.LastIndex(metadata, "/")+1:]
	stem := strings.TrimSuffix(base, ".metadata.json")
	if stem == base || stem == "" || strings.ContainsAny(stem, "/\\\n\r") {
		return fmt.Errorf("invalid metadata stem")
	}
	hint := c.Warehouse + "/sage/obs/metadata/version-hint.text"
	if e := c.IO.WriteFile(hint, []byte(stem)); e != nil {
		return e
	}
	got, e := c.IO.ReadFile(hint)
	if e != nil {
		return e
	}
	if string(got) != stem {
		return errors.New("published hint readback differs")
	}
	return nil
}

// partitionField requires exactly one day(time) transform on field 1.
func partitionField(t *table.Table) (iceberg.PartitionField, error) {
	spec := t.Spec()
	if spec.NumFields() != 1 {
		return iceberg.PartitionField{}, errors.New("managed table must have one day(time) partition field")
	}
	field := spec.Field(0)
	if len(field.SourceIDs) != 1 || field.SourceID() != 1 || field.Transform.String() != "day" || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(field.Name) {
		return field, errors.New("managed table must be partitioned by day(time)")
	}
	return field, nil
}

// DayPrefix is the remote directory for one UTC day's data files.
func (c *Catalog) DayPrefix(ctx context.Context, day string) (string, error) {
	t, e := c.Catalog.LoadTable(ctx, ident)
	if e != nil {
		return "", e
	}
	field, e := partitionField(t)
	if e != nil {
		return "", e
	}
	dayStart, _, e := control.Bounds(day)
	if e != nil {
		return "", e
	}
	return c.Warehouse + "/sage/obs/data/" + field.Name + "=" + strconv.FormatInt(dayStart.Unix()/86400, 10), nil
}
