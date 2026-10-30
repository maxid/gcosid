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
	if _, err := db.ExecContext(ctx,
		`INSERT INTO cosid (name, last_max_id, last_fetch_time) VALUES ('xdmp.itest', 0, unix_timestamp())`); err != nil {
		t.Fatalf("seed cosid row: %v", err)
	}
	return db
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

// mysqlImage and redisImage honour TESTCONTAINERS_* style overrides so a
// mirrored registry can be used without editing the test.
func mysqlImage() string {
	if v := os.Getenv("GCOSID_MYSQL_IMAGE"); v != "" {
		return v
	}
	return "mysql:8.4"
}

func redisImage() string {
	if v := os.Getenv("GCOSID_REDIS_IMAGE"); v != "" {
		return v
	}
	return "redis:7"
}
