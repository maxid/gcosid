// Package database implements gcosid's segment allocator on top of database/sql.
//
// The protocol is CosID 2.15.1's JdbcIdSegmentDistributor: bump the counter row
// in one statement, then read the new upper bound. There is no version column
// and no compare-and-set loop — the UPDATE is the atomic reservation.
package database

import "fmt"

// Dialect supplies the two statements the allocator runs. Both must target the
// CosID counter table:
//
//	cosid(name varchar primary key, last_max_id bigint, last_fetch_time bigint)
//
// and `name` is `{namespace}.{name}`.
type Dialect interface {
	// Increment bumps last_max_id by the given step and refreshes last_fetch_time.
	Increment() string
	// Fetch reads the current last_max_id back.
	Fetch() string
	// Placeholder renders the n-th (1-based) bind parameter.
	Placeholder(n int) string
}

// MySQL speaks the upstream SQL verbatim (MySQL, MariaDB, and MySQL-compatible
// engines).
type MySQL struct{}

// Increment implements Dialect.
func (MySQL) Increment() string {
	return "update cosid set last_max_id=(last_max_id + ?),last_fetch_time=unix_timestamp() where name = ?"
}

// Fetch implements Dialect.
func (MySQL) Fetch() string { return "select last_max_id from cosid where name = ?" }

// Placeholder implements Dialect.
func (MySQL) Placeholder(int) string { return "?" }

// Postgres uses $n placeholders and a different epoch extractor.
type Postgres struct{}

// Increment implements Dialect.
func (Postgres) Increment() string {
	return "UPDATE cosid SET last_max_id = last_max_id + $1, last_fetch_time = extract(epoch from now())::bigint WHERE name = $2"
}

// Fetch implements Dialect.
func (Postgres) Fetch() string { return "SELECT last_max_id FROM cosid WHERE name = $1" }

// Placeholder implements Dialect.
func (Postgres) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }
