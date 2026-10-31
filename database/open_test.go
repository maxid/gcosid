package database_test

import (
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/maxid/gcosid/database"
)

// CosID's JDBC runtime reports matched rows, because Connector/J sets
// CLIENT_FOUND_ROWS by default. go-sql-driver does not, so the DSN is normalised
// before opening; without it a heartbeat that rewrites the same timestamp would
// look like a lost lease.
func TestNormaliseDSNEnablesFoundRows(t *testing.T) {
	dsn, err := database.NormaliseDSN("user:pass@tcp(127.0.0.1:3306)/xdmp")
	if err != nil {
		t.Fatalf("normalise: %v", err)
	}

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cfg.ClientFoundRows {
		t.Fatalf("clientFoundRows not set in %q", dsn)
	}
	if !cfg.ParseTime {
		t.Fatalf("parseTime not set in %q", dsn)
	}
	if !strings.Contains(dsn, "user:pass@") {
		t.Fatalf("credentials lost: %q", dsn)
	}
}

func TestNormaliseDSNRejectsBadDSN(t *testing.T) {
	if _, err := database.NormaliseDSN("not a dsn"); err == nil {
		t.Fatal("invalid dsn must fail")
	}
}
