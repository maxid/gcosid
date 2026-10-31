package gcosid

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InstanceID identifies one process in a namespace — CosID's InstanceId. A stable
// instance (StatefulSet pod, fixed host) is never reclaimed by heartbeat age.
type InstanceID struct {
	ID     string
	Stable bool
}

// NewInstanceID builds an instance id from host and port.
func NewInstanceID(host string, port int, stable bool) InstanceID {
	return InstanceID{ID: fmt.Sprintf("%s:%d", host, port), Stable: stable}
}

// NamespacedInstanceID is the registry key: one instance may hold a slot in
// several namespaces — CosID's NamespacedInstanceId.
type NamespacedInstanceID struct {
	Namespace string
	Instance  InstanceID
}

// GuardianState records the outcome of the last heartbeat — CosID's GuardianState.
type GuardianState struct {
	GuardAt time.Time
	Err     error
}

// Failed reports whether the last heartbeat failed.
func (g GuardianState) Failed() bool { return g.Err != nil }

// Guarder defaults, matching DefaultMachineIdGuarder.
const (
	DefaultGuardInitialDelay = time.Minute
	DefaultGuardDelay        = time.Minute
)

// Guarder periodically renews the lease on every registered machine slot. Losing
// the lease means another instance may take the slot, so the failure is exposed
// rather than swallowed — CosID's MachineIdGuarder.
type Guarder struct {
	distributor  MachineIdDistributor
	safeGuard    time.Duration
	initialDelay time.Duration
	delay        time.Duration
	now          func() time.Time

	mu      sync.RWMutex
	states  map[NamespacedInstanceID]GuardianState
	running bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// GuarderOption configures a Guarder.
type GuarderOption func(*Guarder)

// WithGuardTiming overrides the heartbeat schedule.
func WithGuardTiming(initialDelay, delay time.Duration) GuarderOption {
	return func(g *Guarder) {
		if initialDelay >= 0 {
			g.initialDelay = initialDelay
		}
		if delay > 0 {
			g.delay = delay
		}
	}
}

// WithGuardClock overrides the clock (tests).
func WithGuardClock(now func() time.Time) GuarderOption {
	return func(g *Guarder) {
		if now != nil {
			g.now = now
		}
	}
}

// NewGuarder creates a guarder for a distributor.
func NewGuarder(distributor MachineIdDistributor, safeGuard time.Duration, opts ...GuarderOption) (*Guarder, error) {
	if distributor == nil {
		return nil, fmt.Errorf("%w: distributor is required", ErrInvalidConfig)
	}
	if safeGuard <= 0 {
		safeGuard = 10 * time.Minute
	}
	g := &Guarder{
		distributor:  distributor,
		safeGuard:    safeGuard,
		initialDelay: DefaultGuardInitialDelay,
		delay:        DefaultGuardDelay,
		now:          time.Now,
		states:       map[NamespacedInstanceID]GuardianState{},
	}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// Register starts guarding the slot of (namespace, instance).
func (g *Guarder) Register(namespace string, instance InstanceID) error {
	if namespace == "" {
		return fmt.Errorf("%w: namespace is required", ErrInvalidConfig)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.states[NamespacedInstanceID{Namespace: namespace, Instance: instance}] = GuardianState{}
	return nil
}

// Unregister stops guarding a slot.
func (g *Guarder) Unregister(namespace string, instance InstanceID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.states, NamespacedInstanceID{Namespace: namespace, Instance: instance})
}

// States returns a copy of the current heartbeat outcomes.
func (g *Guarder) States() map[NamespacedInstanceID]GuardianState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[NamespacedInstanceID]GuardianState, len(g.states))
	for k, v := range g.states {
		out[k] = v
	}
	return out
}

// HasFailure reports whether any heartbeat failed. Callers should treat this as
// an alert: a lost slot can be handed to another instance.
func (g *Guarder) HasFailure() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, state := range g.states {
		if state.Failed() {
			return true
		}
	}
	return false
}

// Start begins the heartbeat loop.
func (g *Guarder) Start() {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return
	}
	g.running = true
	g.ctx, g.cancel = context.WithCancel(context.Background())
	ctx := g.ctx
	g.mu.Unlock()

	g.wg.Add(1)
	go g.loop(ctx)
}

// Stop ends the heartbeat loop and waits for it.
func (g *Guarder) Stop() {
	g.mu.Lock()
	if !g.running {
		g.mu.Unlock()
		return
	}
	g.running = false
	cancel := g.cancel
	g.mu.Unlock()

	cancel()
	g.wg.Wait()
}

// Running reports whether the loop is active.
func (g *Guarder) Running() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.running
}

func (g *Guarder) loop(ctx context.Context) {
	defer g.wg.Done()

	timer := time.NewTimer(g.initialDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	g.SafeGuard(ctx)

	ticker := time.NewTicker(g.delay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.SafeGuard(ctx)
		}
	}
}

// SafeGuard renews every registered slot once. It is exported so a caller can
// guard on demand (and so tests do not have to wait for the timer).
func (g *Guarder) SafeGuard(ctx context.Context) {
	for _, key := range g.keys() {
		guardAt := g.now()
		err := g.distributor.Guard(ctx, key.Namespace)
		g.mu.Lock()
		if _, ok := g.states[key]; ok {
			g.states[key] = GuardianState{GuardAt: guardAt, Err: err}
		}
		g.mu.Unlock()
	}
}

func (g *Guarder) keys() []NamespacedInstanceID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	keys := make([]NamespacedInstanceID, 0, len(g.states))
	for k := range g.states {
		keys = append(keys, k)
	}
	return keys
}

// NoopGuarder is MachineIdGuarder.NONE: it guards nothing, for deployments with
// a manual or StatefulSet machine id.
type NoopGuarder struct{}

// Register implements the guarder contract.
func (NoopGuarder) Register(string, InstanceID) error { return nil }

// Unregister implements the guarder contract.
func (NoopGuarder) Unregister(string, InstanceID) {}

// States implements the guarder contract.
func (NoopGuarder) States() map[NamespacedInstanceID]GuardianState {
	return map[NamespacedInstanceID]GuardianState{}
}

// HasFailure implements the guarder contract.
func (NoopGuarder) HasFailure() bool { return false }

// Start implements the guarder contract.
func (NoopGuarder) Start() {}

// Stop implements the guarder contract.
func (NoopGuarder) Stop() {}

// Running implements the guarder contract.
func (NoopGuarder) Running() bool { return false }
