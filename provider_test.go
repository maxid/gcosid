package gcosid

import (
	"context"
	"errors"
	"testing"
)

func TestProviderRegistry(t *testing.T) {
	p := NewProvider()

	p.SetShare(NewAtomicGenerator())
	if p.Share() == nil {
		t.Fatal("share was not installed")
	}

	p.Set("order", NewAtomicGenerator())
	p.Set("user", NewAtomicGenerator())
	if names := p.Names(); len(names) != 2 || names[0] != "order" || names[1] != "user" {
		t.Fatalf("names = %v", names)
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

	p.Clear()
	if names := p.Names(); len(names) != 0 {
		t.Fatalf("names after clear = %v", names)
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
