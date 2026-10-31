package gcosid

import (
	"context"
	"errors"
	"testing"
)

type order struct {
	ID   int64 `cosid:"order"`
	Name string
}

type user struct {
	ID       int64 `cosid:"user"`
	Nickname string
}

type composite struct {
	order
	Owner string
}

type noID struct {
	Name string
}

type badType struct {
	ID float64 `cosid:"order"`
}

type stringID struct {
	ID string `cosid:"uuid"`
}

func newTestInjector(t *testing.T) *Injector {
	t.Helper()
	injector := NewInjector()
	injector.Register("order", NewAtomicGenerator())
	injector.Register("user", NewAtomicGenerator())
	injector.RegisterString("uuid", UUIDGenerator{})
	return injector
}

// The entity carries a tag; the injector fills it only when empty, which is
// CosID's EnsureId behaviour.
func TestInjectorFillsOnlyEmptyIDs(t *testing.T) {
	injector := newTestInjector(t)
	entity := &order{Name: "first"}

	filled, err := injector.Ensure(context.Background(), entity)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if !filled || entity.ID != 1 {
		t.Fatalf("filled = %v, id = %d", filled, entity.ID)
	}

	// An entity that already has an id is left alone.
	entity.ID = 42
	filled, err = injector.Ensure(context.Background(), entity)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if filled || entity.ID != 42 {
		t.Fatalf("existing id was overwritten: filled = %v, id = %d", filled, entity.ID)
	}
}

func TestInjectorUsesTheRegisteredGenerator(t *testing.T) {
	injector := newTestInjector(t)

	first := &user{}
	if _, err := injector.Ensure(context.Background(), first); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	second := &order{}
	if _, err := injector.Ensure(context.Background(), second); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	// Different generators count independently.
	if first.ID != 1 || second.ID != 1 {
		t.Fatalf("ids = %d / %d, want 1 / 1", first.ID, second.ID)
	}
}

func TestInjectorHandlesEmbeddedStructs(t *testing.T) {
	injector := newTestInjector(t)
	entity := &composite{Owner: "alice"}

	filled, err := injector.Ensure(context.Background(), entity)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if !filled || entity.ID == 0 {
		t.Fatalf("embedded id not filled: %+v", entity)
	}
}

func TestInjectorStringFields(t *testing.T) {
	injector := newTestInjector(t)
	entity := &stringID{}

	if _, err := injector.Ensure(context.Background(), entity); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if len(entity.ID) != 36 {
		t.Fatalf("string id = %q, want a UUID", entity.ID)
	}
}

// A numeric generator can still feed a string field: the decimal rendering is
// what CosID's ToStringIdConverter does.
func TestInjectorStringFieldFromNumericGenerator(t *testing.T) {
	injector := NewInjector()
	injector.Register("order", fixedGenerator(7))

	type row struct {
		ID string `cosid:"order"`
	}
	entity := &row{}
	if _, err := injector.Ensure(context.Background(), entity); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if entity.ID != "7" {
		t.Fatalf("id = %q, want \"7\"", entity.ID)
	}
}

func TestInjectorEnsureAll(t *testing.T) {
	injector := newTestInjector(t)
	entities := []*order{{}, {}, {ID: 99}}

	filled, err := injector.EnsureAll(context.Background(), entities)
	if err != nil {
		t.Fatalf("ensure all: %v", err)
	}
	if filled != 2 {
		t.Fatalf("filled = %d, want 2", filled)
	}
	if entities[0].ID != 1 || entities[1].ID != 2 || entities[2].ID != 99 {
		t.Fatalf("ids = %d/%d/%d", entities[0].ID, entities[1].ID, entities[2].ID)
	}
}

func TestInjectorRejectsBadEntities(t *testing.T) {
	injector := newTestInjector(t)
	ctx := context.Background()

	if _, err := injector.Ensure(ctx, order{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-pointer err = %v", err)
	}
	if _, err := injector.Ensure(ctx, (*order)(nil)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil pointer err = %v", err)
	}
	if _, err := injector.Ensure(ctx, &noID{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("no tagged field err = %v", err)
	}
	if _, err := injector.Ensure(ctx, &badType{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported field type err = %v", err)
	}
	if _, err := injector.EnsureAll(ctx, []order{{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-pointer elements err = %v", err)
	}
	if _, err := injector.EnsureAll(ctx, order{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-slice err = %v", err)
	}
}

func TestInjectorUnknownGenerator(t *testing.T) {
	injector := NewInjector()
	type lonely struct {
		ID int64 `cosid:"missing"`
	}
	if _, err := injector.Ensure(context.Background(), &lonely{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
}

func TestInjectorFromProvider(t *testing.T) {
	provider := NewProvider()
	provider.Set("order", NewAtomicGenerator())

	injector := NewInjectorFromProvider(provider)
	entity := &order{}
	if _, err := injector.Ensure(context.Background(), entity); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if entity.ID != 1 {
		t.Fatalf("id = %d", entity.ID)
	}

	// The provider is the live registry: replacing a generator is picked up.
	provider.Set("order", NewAtomicGenerator())
	next := &order{}
	if _, err := injector.Ensure(context.Background(), next); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if next.ID != 1 {
		t.Fatalf("id = %d, want 1 from the replaced generator", next.ID)
	}
}
