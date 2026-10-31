// Command inject shows how gcosid fills entity ids.
//
// It is the Go equivalent of CosID's @CosId annotation: the entity only carries a
// struct tag, and the injector resolves the generator by name.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/maxid/gcosid"
	"github.com/maxid/gcosid/database"
)

// Order is the entity. `cosid:"order"` names the generator that fills ID.
type Order struct {
	ID     int64 `cosid:"order"`
	Amount int64
}

// OrderNo is a second entity using the same generator as a string field.
type OrderNo struct {
	No      string `cosid:"orderNo"`
	OrderID int64
}

func main() {
	ctx := context.Background()

	// 1. A generator. Here: an in-memory allocator so the example needs no
	//    database; production wires database.NewAllocator or the Redis one.
	chain, err := gcosid.NewChainGenerator("order", 100, gcosid.NewMemoryAllocator(100))
	if err != nil {
		log.Fatal(err)
	}
	defer chain.Close(ctx)

	// A string generator: decimal rendering of the same ids.
	orderNo := gcosid.StringOf(chain, gcosid.ToString{})

	// 2. The injector registry. Names must match the struct tags.
	injector := gcosid.NewInjector()
	injector.Register("order", chain)
	injector.RegisterString("orderNo", orderNo)

	// 3. Fill one entity. An entity that already has an id is left alone.
	order := &Order{Amount: 1999}
	if _, err := injector.Ensure(ctx, order); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("order id = %d\n", order.ID)

	again := &Order{ID: order.ID, Amount: 2000}
	filled, err := injector.Ensure(ctx, again)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("existing id kept = %t (filled=%t)\n", again.ID == order.ID, filled)

	// 4. Fill a batch in one call.
	batch := []*Order{{}, {}, {}}
	count, err := injector.EnsureAll(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("filled %d of %d:", count, len(batch))
	for _, o := range batch {
		fmt.Printf(" %d", o.ID)
	}
	fmt.Println()

	// 5. String-typed id fields.
	no := &OrderNo{}
	if _, err := injector.Ensure(ctx, no); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("order no = %q\n", no.No)

	// 6. Diagnostics: the same entity-level view an operator dashboard reads.
	fmt.Println("chain stat:", chain.Stat().String())
	fmt.Println("database allocator is swappable:", fmt.Sprintf("%T", (*database.Allocator)(nil)))
}
