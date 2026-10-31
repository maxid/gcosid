package database

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/maxid/gcosid"
)

// OpenMySQL opens a MySQL pool configured the way CosID's JDBC runtime behaves.
//
// The important flag is clientFoundRows: MySQL Connector/J sets CLIENT_FOUND_ROWS
// by default, so `executeUpdate` reports *matched* rows. go-sql-driver reports
// *changed* rows unless asked otherwise. CosID treats `affected == 0` as "the
// machine slot is gone" (guard) and "the slot was claimed by someone else"
// (distributeByRevert); with changed-row counts a heartbeat that writes the same
// timestamp back would look like a lost lease. Sharing cosid_machine with a Java
// process therefore requires this flag.
func OpenMySQL(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", gcosid.ErrInvalidConfig, err)
	}
	// Timestamps are read into time.Time and found-rows semantics are required;
	// both are driver-level, so they are forced here rather than left to callers.
	cfg.ParseTime = true
	cfg.ClientFoundRows = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("gcosid/database: open mysql: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gcosid/database: ping mysql: %w", err)
	}
	return db, nil
}

// NormaliseDSN renders the DSN gcosid expects, useful for logs and tests.
func NormaliseDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("%w: parse dsn: %w", gcosid.ErrInvalidConfig, err)
	}
	cfg.ParseTime = true
	cfg.ClientFoundRows = true
	return cfg.FormatDSN(), nil
}
