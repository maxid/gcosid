// Command repository shows where id injection belongs in a real service.
//
// gcosid has no ORM hook: unlike CosID's Spring/MyBatis/Jackson plugins, nothing
// fills an entity's id behind your back. The repository is the call site — it is
// the only place that knows an INSERT is about to happen.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/maxid/gcosid"
)

// Order is the entity. The tag names the generator that fills ID.
type Order struct {
	ID     int64 `cosid:"order"`
	SKU    string
	Amount int64
}

// ErrDuplicateID is what a primary-key conflict looks like without injection:
// every row would carry id 0.
var ErrDuplicateID = errors.New("order: duplicate id")

// OrderRepo is a repository that owns the injector.
type OrderRepo struct {
	ids *gcosid.Injector

	mu   sync.Mutex
	rows map[int64]Order
}

// NewOrderRepo wires the injection into the repository, not the entity.
func NewOrderRepo(ids *gcosid.Injector) *OrderRepo {
	return &OrderRepo{ids: ids, rows: map[int64]Order{}}
}

// Create fills missing ids and then "inserts". Call Ensure before every write that
// needs a new id — it is idempotent, so calling it again on an entity that already
// has an id does nothing.
func (r *OrderRepo) Create(ctx context.Context, order *Order) error {
	if _, err := r.ids.Ensure(ctx, order); err != nil {
		return fmt.Errorf("assign id: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.rows[order.ID]; exists {
		return ErrDuplicateID
	}
	r.rows[order.ID] = *order
	return nil
}

// CreateBatch is the same for a slice: one call, one generator round trip per id.
func (r *OrderRepo) CreateBatch(ctx context.Context, orders []*Order) (int, error) {
	filled, err := r.ids.EnsureAll(ctx, orders)
	if err != nil {
		return 0, fmt.Errorf("assign ids: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, order := range orders {
		if _, exists := r.rows[order.ID]; exists {
			return 0, ErrDuplicateID
		}
		r.rows[order.ID] = *order
	}
	return filled, nil
}

// Get reads back what was stored.
func (r *OrderRepo) Get(id int64) (Order, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	order, ok := r.rows[id]
	return order, ok
}

func main() {
	ctx := context.Background()

	// 1. One generator per entity kind, registered by the name used in the tag.
	chain, err := gcosid.NewChainGenerator("order", 100, gcosid.NewMemoryAllocator(100))
	if err != nil {
		log.Fatal(err)
	}
	defer chain.Close(ctx)

	injector := gcosid.NewInjector()
	injector.Register("order", chain)

	repo := NewOrderRepo(injector)

	// 2. Create: the id is assigned inside the repository.
	order := &Order{SKU: "SKU-1", Amount: 1999}
	if err := repo.Create(ctx, order); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created id=%d\n", order.ID)

	// 3. Idempotent: an entity that already has an id keeps it.
	existing := &Order{ID: order.ID, SKU: "SKU-1", Amount: 2000}
	filled, err := injector.Ensure(ctx, existing)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("existing id kept=%d filled=%t\n", existing.ID, filled)

	// 4. Batch.
	batch := []*Order{{SKU: "A"}, {SKU: "B"}, {SKU: "C"}}
	count, err := repo.CreateBatch(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("batch filled %d:", count)
	for _, o := range batch {
		fmt.Printf(" %d", o.ID)
	}
	fmt.Println()

	// 5. What happens without injection: the id stays 0 and a second row collides.
	raw := &Order{SKU: "SKU-X"}
	if err := repo.Create(context.WithoutCancel(ctx), raw); err != nil {
		log.Fatal(err)
	}
	_ = raw

	uninjected := &Order{SKU: "SKU-Y"}
	fmt.Printf("not injected: id=%d (row would use the zero id)\n", uninjected.ID)
}
