package gcosid

import (
	"context"
	"errors"
	"testing"
)

func TestProviderRegistry(t *testing.T) {
	p := NewProvider()

	share := NewAtomicGenerator()
	p.SetShare(share)
	if p.Share() == nil {
		t.Fatal("share was not installed")
	}
	// CosID registers the shared generator under its name too, so an entity
	// tagged with the share name resolves through the same map.
	if got, ok := p.Get(ShareName); !ok || got != share {
		t.Fatalf("Get(%q) = %v, %v", ShareName, got, ok)
	}

	p.Set("order", NewAtomicGenerator())
	p.Set("user", NewAtomicGenerator())
	// entries() includes the share slot, hence three names.
	if names := p.Names(); len(names) != 3 || names[0] != ShareName || names[1] != "order" || names[2] != "user" {
		t.Fatalf("names = %v", names)
	}
	if all := p.All(); len(all) != 3 {
		t.Fatalf("all = %d generators, want 3", len(all))
	}

	gen, err := p.Required("order")
	if err != nil || gen == nil {
		t.Fatalf("required = %v, %v", gen, err)
	}
	if _, err := p.Required("missing"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing err = %v", err)
	}

	if p.Remove("order") == nil {
		t.Fatal("remove returned nil")
	}
	if _, ok := p.Get("order"); ok {
		t.Fatal("generator still present after remove")
	}

	if p.RemoveShare() == nil || p.Share() != nil {
		t.Fatal("share was not cleared")
	}
	if _, ok := p.Get(ShareName); ok {
		t.Fatal("share is still reachable by name after RemoveShare")
	}

	p.Clear()
	if names := p.Names(); len(names) != 0 {
		t.Fatalf("names after clear = %v", names)
	}
}

// Set/Remove with the share name must route to the share slot instead of creating
// a second, divergent entry — that is CosID's DefaultIdGeneratorProvider.
func TestProviderShareRouting(t *testing.T) {
	p := NewProvider()

	viaName := NewAtomicGenerator()
	p.Set(ShareName, viaName)
	if p.Share() != viaName {
		t.Fatal("Set(SHARE) did not install the shared generator")
	}

	if p.Remove(ShareName) != viaName {
		t.Fatal("Remove(SHARE) did not return the shared generator")
	}
	if p.Share() != nil {
		t.Fatal("share survived Remove(SHARE)")
	}

	if _, err := p.ShareRequired(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ShareRequired err = %v, want ErrInvalidConfig", err)
	}
	p.SetShare(NewAtomicGenerator())
	if _, err := p.ShareRequired(); err != nil {
		t.Fatalf("ShareRequired: %v", err)
	}
}

func TestLazyGeneratorBuildsOnce(t *testing.T) {
	calls := 0
	g := NewLazyGenerator(func(context.Context) (Generator, error) {
		calls++
		return NewAtomicGenerator(), nil
	})

	for want := int64(1); want <= 3; want++ {
		id, err := g.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if id != want {
			t.Fatalf("id = %d, want %d", id, want)
		}
	}
	if calls != 1 {
		t.Fatalf("initialiser called %d times, want 1", calls)
	}
}

func TestLazyGeneratorPropagatesInitError(t *testing.T) {
	g := NewLazyGenerator(func(context.Context) (Generator, error) {
		return nil, ErrAllocatorUnavailable
	})
	if _, err := g.Next(context.Background()); !errors.Is(err, ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}
}

func TestLazyGeneratorWithoutInitialiser(t *testing.T) {
	g := NewLazyGenerator(nil)
	if _, err := g.Next(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
}
