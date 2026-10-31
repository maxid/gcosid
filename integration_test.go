//go:build integration

package gcosid_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	rdb "github.com/redis/go-redis/v9"
	mysqlcontainer "github.com/testcontainers/testcontainers-go/modules/mysql"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/maxid/gcosid"
	"github.com/maxid/gcosid/database"
	gredis "github.com/maxid/gcosid/redis"
)

// These tests run against real MySQL and Redis containers:
//
//	go test -tags=integration ./...
//
// They exist because sqlmock and fake clients only prove that a statement looks
// right; uniqueness under concurrency needs the real constraint.

func startMySQL(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()

	// A provided environment wins over containers: CI without Docker can point
	// these at a real MySQL.
	if dsn := os.Getenv("GCOSID_TEST_MYSQL_DSN"); dsn != "" {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open provided mysql: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		seedMySQL(t, db)
		return db
	}

	container, err := mysqlcontainer.Run(ctx, mysqlImage(),
		mysqlcontainer.WithDatabase("cosid"),
		mysqlcontainer.WithUsername("test"),
		mysqlcontainer.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("mysql container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("mysql host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("mysql port: %v", err)
	}

	dsn := fmt.Sprintf("test:test@tcp(%s:%s)/cosid?multiStatements=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The CosID counter table, verbatim.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cosid
		(
			name            VARCHAR(100) NOT NULL,
			last_max_id     BIGINT UNSIGNED NOT NULL DEFAULT 0,
			last_fetch_time BIGINT UNSIGNED NOT NULL DEFAULT 0,
			CONSTRAINT cosid_pk PRIMARY KEY (name)
		) ENGINE = InnoDB`); err != nil {
		t.Fatalf("create cosid table: %v", err)
	}
	seedMySQL(t, db)
	return db
}

func seedMySQL(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO cosid (name, last_max_id, last_fetch_time) VALUES ('xdmp.itest', 0, unix_timestamp())
		 ON DUPLICATE KEY UPDATE last_max_id = 0`); err != nil {
		t.Fatalf("seed cosid row: %v", err)
	}
}

// TestMySQLSegmentReservationsNeverOverlap is the test sqlmock cannot write:
// concurrent generators must never receive the same id.
func TestMySQLSegmentReservationsNeverOverlap(t *testing.T) {
	db := startMySQL(t)
	ctx := context.Background()

	alloc, err := database.NewAllocator(db, database.MySQL{}, database.WithNamespace("xdmp"))
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}

	const workers, perWorker = 8, 200
	ids := make(chan int64, workers*perWorker)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen, err := gcosid.NewSegmentGenerator("itest", 50, alloc)
			if err != nil {
				t.Errorf("generator: %v", err)
				return
			}
			defer gen.Close(context.Background())
			for j := 0; j < perWorker; j++ {
				id, err := gen.Next(ctx)
				if err != nil {
					t.Errorf("next: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]bool, workers*perWorker)
	for id := range ids {
		if seen[id] {
			t.Fatalf("id %d handed out twice by the real database", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("unique ids = %d, want %d", len(seen), workers*perWorker)
	}
}

func TestMySQLMissingSegmentRowIsDistinctError(t *testing.T) {
	db := startMySQL(t)

	alloc, _ := database.NewAllocator(db, database.MySQL{}, database.WithNamespace("xdmp"))
	if _, err := alloc.NextMaxId(context.Background(), "unknown", 10); !errors.Is(err, database.ErrSegmentMissing) {
		t.Fatalf("err = %v, want ErrSegmentMissing", err)
	}
}

func startRedis(t *testing.T) *rdb.Client {
	t.Helper()
	ctx := context.Background()

	if addr := os.Getenv("GCOSID_TEST_REDIS_ADDR"); addr != "" {
		client := rdb.NewClient(&rdb.Options{Addr: addr})
		t.Cleanup(func() { _ = client.Close() })
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatalf("flush provided redis: %v", err)
		}
		return client
	}

	container, err := rediscontainer.Run(ctx, redisImage())
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	addr, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	client := rdb.NewClient(&rdb.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// redisAdapter keeps the driver out of the library while the test uses the real
// client.
type redisAdapter struct{ client *rdb.Client }

func (a redisAdapter) IncrBy(ctx context.Context, key string, step int64) (int64, error) {
	return a.client.IncrBy(ctx, key, step).Result()
}

func (a redisAdapter) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return a.client.Eval(ctx, script, keys, args...).Result()
}

func TestRedisSegmentAndMachineSlots(t *testing.T) {
	client := startRedis(t)
	adapter := redisAdapter{client: client}
	ctx := context.Background()

	alloc, err := gredis.NewSegmentAllocator(adapter, "xdmp")
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}
	first, err := alloc.NextMaxId(ctx, "order", 10)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	second, err := alloc.NextMaxId(ctx, "order", 10)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != 10 || second != 20 {
		t.Fatalf("max ids = %d, %d, want 10, 20", first, second)
	}
	// The key must be the CosID one, hash tag included.
	if got := client.Get(ctx, "cosid:{xdmp.order}:adder").Val(); got != "20" {
		t.Fatalf("cosid:{xdmp.order}:adder = %q, want 20", got)
	}

	storageA, _ := gredis.NewMachineStorage(adapter, "xdmp")
	storageB, _ := gredis.NewMachineStorage(adapter, "xdmp")

	a, err := storageA.Allocate(ctx, "xdmp", "node-a", 10)
	if err != nil {
		t.Fatalf("allocate a: %v", err)
	}
	b, err := storageB.Allocate(ctx, "xdmp", "node-b", 10)
	if err != nil {
		t.Fatalf("allocate b: %v", err)
	}
	if a.MachineID == b.MachineID {
		t.Fatalf("two instances share machine id %d", a.MachineID)
	}

	// Re-allocating for a known instance returns its slot.
	again, err := storageA.Allocate(ctx, "xdmp", "node-a", 10)
	if err != nil || again.MachineID != a.MachineID {
		t.Fatalf("re-allocate = %+v, %v", again, err)
	}

	// Revert puts the slot back for reuse.
	if err := storageB.Revert(ctx, "xdmp", "node-b"); err != nil {
		t.Fatalf("revert: %v", err)
	}
	storageC, _ := gredis.NewMachineStorage(adapter, "xdmp")
	c, err := storageC.Allocate(ctx, "xdmp", "node-c", 10)
	if err != nil {
		t.Fatalf("allocate c: %v", err)
	}
	if c.MachineID != b.MachineID {
		t.Fatalf("reverted slot %d not reused, got %d", b.MachineID, c.MachineID)
	}
}

// A broken connection must be retryable: it is an outage, not a configuration
// error. sqlmock used to assert this; a closed port does it for real.
func TestMySQLConnectionFailureIsRetryable(t *testing.T) {
	db, err := sql.Open("mysql", "test:test@tcp(127.0.0.1:1)/cosid?timeout=1s")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	alloc, _ := database.NewAllocator(db, database.MySQL{}, database.WithNamespace("xdmp"))
	if _, err := alloc.NextMaxId(context.Background(), "itest", 10); !errors.Is(err, gcosid.ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}
}

// Exhausting the machine space must fail instead of reusing a live slot.
func TestRedisMachineExhaustion(t *testing.T) {
	adapter := redisAdapter{client: startRedis(t)}
	ctx := context.Background()

	// The CosID adder starts at -1, so maxMachineID=1 allows ids 0 and 1; the
	// third instance must be refused.
	for _, instance := range []string{"node-a", "node-b"} {
		storage, _ := gredis.NewMachineStorage(adapter, "exhaust")
		if _, err := storage.Allocate(ctx, "exhaust", instance, 1); err != nil {
			t.Fatalf("allocate %s: %v", instance, err)
		}
	}

	third, _ := gredis.NewMachineStorage(adapter, "exhaust")
	if _, err := third.Allocate(ctx, "exhaust", "node-c", 1); !errors.Is(err, gcosid.ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
}

// Guarding a slot that was never allocated is a configuration error, not a
// transient failure.
func TestRedisMachineGuardRequiresASlot(t *testing.T) {
	adapter := redisAdapter{client: startRedis(t)}

	storage, _ := gredis.NewMachineStorage(adapter, "guard")
	err := storage.Update(context.Background(), "guard", gcosid.MachineState{MachineID: 1, InstanceID: "node-x"})
	if !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
}

// mysqlImage and redisImage honour TESTCONTAINERS_* style overrides so a
// mirrored registry can be used without editing the test.
func mysqlImage() string {
	if v := os.Getenv("GCOSID_MYSQL_IMAGE"); v != "" {
		return v
	}
	return "mysql:8.4.7"
}

func redisImage() string {
	if v := os.Getenv("GCOSID_REDIS_IMAGE"); v != "" {
		return v
	}
	return "redis:7"
}
