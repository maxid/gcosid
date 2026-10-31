package gcosid

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ShareName is CosID's IdGeneratorProvider.SHARE: the generator used when a
// caller does not name one. `@CosId` without a value resolves to it, and every
// registry operation treats it like any other name.
const ShareName = "__share__"

// Provider is CosID's IdGeneratorProvider: a registry of named generators.
type Provider struct {
	mu         sync.RWMutex
	share      Generator
	generators map[string]Generator
}

// NewProvider creates an empty registry.
func NewProvider() *Provider {
	return &Provider{generators: map[string]Generator{}}
}

// Share returns the shared generator, or nil.
func (p *Provider) Share() Generator {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.share
}

// SetShare installs the shared generator. As in CosID, the shared generator is
// also reachable by name, so Get(ShareName) works.
func (p *Provider) SetShare(gen Generator) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.share = gen
	p.generators[ShareName] = gen
}

// RemoveShare clears the shared generator and returns it.
func (p *Provider) RemoveShare() Generator {
	p.mu.Lock()
	defer p.mu.Unlock()
	gen := p.share
	p.share = nil
	delete(p.generators, ShareName)
	return gen
}

// Get returns a named generator.
func (p *Provider) Get(name string) (Generator, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	gen, ok := p.generators[name]
	return gen, ok
}

// Required returns a named generator or an error naming it, mirroring
// IdGeneratorProvider.getRequired.
func (p *Provider) Required(name string) (Generator, error) {
	if gen, ok := p.Get(name); ok {
		return gen, nil
	}
	return nil, fmt.Errorf("%w: no id generator named %q", ErrInvalidConfig, name)
}

// Set registers a generator. The share name routes to SetShare so the two views
// never disagree.
func (p *Provider) Set(name string, gen Generator) {
	if name == ShareName {
		p.SetShare(gen)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generators[name] = gen
}

// Remove drops a generator and returns it.
func (p *Provider) Remove(name string) Generator {
	if name == ShareName {
		return p.RemoveShare()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	gen := p.generators[name]
	delete(p.generators, name)
	return gen
}

// Clear drops every generator, including the shared one.
func (p *Provider) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.share = nil
	p.generators = map[string]Generator{}
}

// ShareRequired returns the shared generator or an error naming it. It is what an
// unnamed id field resolves to.
func (p *Provider) ShareRequired() (Generator, error) {
	if gen := p.Share(); gen != nil {
		return gen, nil
	}
	return nil, fmt.Errorf("%w: no shared id generator is registered", ErrInvalidConfig)
}

// All returns every registered generator, including the shared one.
func (p *Provider) All() []Generator {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Generator, 0, len(p.generators))
	for _, gen := range p.generators {
		out = append(out, gen)
	}
	return out
}

// Names lists the registered generator names in a stable order.
func (p *Provider) Names() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.generators))
	for name := range p.generators {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// LazyGenerator defers construction until the first Next call — CosID's
// LazyIdGenerator. It lets a distribution wire a generator whose backing store
// (or machine id) is only available after start-up.
type LazyGenerator struct {
	once sync.Once
	gen  Generator
	err  error
	init func(context.Context) (Generator, error)
}

// NewLazyGenerator creates a generator that builds itself on first use.
func NewLazyGenerator(init func(context.Context) (Generator, error)) *LazyGenerator {
	return &LazyGenerator{init: init}
}

// Next implements Generator.
func (g *LazyGenerator) Next(ctx context.Context) (int64, error) {
	g.once.Do(func() {
		if g.init == nil {
			g.err = fmt.Errorf("%w: lazy generator has no initialiser", ErrInvalidConfig)
			return
		}
		g.gen, g.err = g.init(ctx)
	})
	if g.err != nil {
		return 0, g.err
	}
	return g.gen.Next(ctx)
}
