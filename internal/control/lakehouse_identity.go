package control

import (
	"database/sql"
	"errors"
)

// migrateLakehouseIdentity renames persisted identity once; it does not retain
// configuration aliases or change any URL, grant deadline, or archival progress.
func migrateLakehouseIdentity(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query("PRAGMA table_info(config)")
	if err != nil {
		return err
	}
	var oldColumn, newColumn bool
	for rows.Next() {
		var cid, notNull, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			rows.Close()
			return err
		}
		oldColumn = oldColumn || name == "warehouse"
		newColumn = newColumn || name == "lakehouse"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if oldColumn && newColumn {
		return errors.New("ambiguous persisted lakehouse identity columns")
	}
	if oldColumn {
		if _, err = tx.Exec("ALTER TABLE config RENAME COLUMN warehouse TO lakehouse"); err != nil {
			return err
		}
	}
	var conflicts int
	if err = tx.QueryRow(`SELECT count(*) FROM options old JOIN options current
        ON old.key='bootstrap.warehouse' AND current.key='bootstrap.lakehouse'
        WHERE old.value<>current.value`).Scan(&conflicts); err != nil {
		return err
	}
	if conflicts != 0 {
		return errors.New("conflicting persisted bootstrap lakehouse identity")
	}
	if _, err = tx.Exec(`INSERT OR IGNORE INTO options(key,value)
        SELECT 'bootstrap.lakehouse',value FROM options WHERE key='bootstrap.warehouse'`); err != nil {
		return err
	}
	if _, err = tx.Exec("DELETE FROM options WHERE key='bootstrap.warehouse'"); err != nil {
		return err
	}
	if err = tx.QueryRow(`SELECT count(*) FROM readers WHERE json_valid(payload)
        AND json_type(payload,'$.warehouse') IS NOT NULL AND json_type(payload,'$.lakehouse') IS NOT NULL
        AND json_extract(payload,'$.warehouse')<>json_extract(payload,'$.lakehouse')`).Scan(&conflicts); err != nil {
		return err
	}
	if conflicts != 0 {
		return errors.New("conflicting persisted reader lakehouse identity")
	}
	if _, err = tx.Exec(`UPDATE readers SET payload=json_remove(
        json_set(payload,'$.lakehouse',json_extract(payload,'$.warehouse')),'$.warehouse')
        WHERE json_valid(payload) AND json_type(payload,'$.warehouse')='text'`); err != nil {
		return err
	}
	return tx.Commit()
}
