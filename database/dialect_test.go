package database_test

import (
	"strings"
	"testing"

	"github.com/maxid/gcosid/database"
)

// Dialect SQL is a pure value: no database is needed to check that the
// placeholders and the counter table match CosID 2.15.1.
func TestMySQLDialectMatchesUpstream(t *testing.T) {
	d := database.MySQL{}

	increment := d.Increment()
	// CosID 2.15.1: update cosid set last_max_id=(last_max_id + ?),last_fetch_time=unix_timestamp() where name = ?;
	if !strings.Contains(increment, "last_max_id=(last_max_id + ?)") ||
		!strings.Contains(increment, "last_fetch_time=unix_timestamp()") {
		t.Fatalf("increment = %q", increment)
	}
	if fetch := d.Fetch(); fetch != "select last_max_id from cosid where name = ?" {
		t.Fatalf("fetch = %q", fetch)
	}
	if ph := d.Placeholder(2); ph != "?" {
		t.Fatalf("placeholder = %q", ph)
	}
}

func TestPostgresDialectUsesNumberedPlaceholders(t *testing.T) {
	d := database.Postgres{}

	increment := d.Increment()
	if !strings.Contains(increment, "$1") || !strings.Contains(increment, "$2") {
		t.Fatalf("increment = %q", increment)
	}
	if !strings.Contains(increment, "extract(epoch from now())") {
		t.Fatalf("increment must refresh the fetch time: %q", increment)
	}
	if fetch := d.Fetch(); !strings.Contains(fetch, "$1") {
		t.Fatalf("fetch = %q", fetch)
	}
	if ph := d.Placeholder(3); ph != "$3" {
		t.Fatalf("placeholder = %q", ph)
	}
}
