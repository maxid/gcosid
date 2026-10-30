// Package gcosid allocates unique identifiers.
//
// The public surface mirrors CosID's capability set: segment (number-range)
// allocation with double buffering, snowflake IDs with a configurable bit
// layout, machine-id distribution, and the string converters (radix, prefix,
// group, snowflake-friendly). Which strategy a consumer uses is their decision;
// the library provides all of them and never silently mixes their layouts.
//
// Status: experimental (v0.0.0-dev). The API is not stable yet.
package gcosid

import (
	"context"
	"errors"
)

// Configuration errors are separate types from transient failures so callers
// can decide between "fail fast, config is wrong" and "retry later".
var (
	// ErrClosed means the generator was closed.
	ErrClosed = errors.New("gcosid: generator is closed")
	// ErrAllocatorUnavailable means the backing store could not hand out a
	// range right now. Retryable.
	ErrAllocatorUnavailable = errors.New("gcosid: allocator unavailable")
	// ErrInvalidConfig means the configuration can never work.
	ErrInvalidConfig = errors.New("gcosid: invalid configuration")
	// ErrExhausted means the configured ID space ran out.
	ErrExhausted = errors.New("gcosid: id space exhausted")
	// ErrClockMovedBackwards means the clock went back further than the
	// configured tolerance.
	ErrClockMovedBackwards = errors.New("gcosid: clock moved backwards")
	// ErrInvalidMachineID means the machine id does not fit the layout.
	ErrInvalidMachineID = errors.New("gcosid: invalid machine id")
	// ErrNamespaceConflict means two generators share a namespace with
	// incompatible layouts.
	ErrNamespaceConflict = errors.New("gcosid: namespace conflict")
)

// Generator allocates one id per call. Implementations are safe for concurrent
// use and never return a zero value together with an error.
type Generator interface {
	Next(ctx context.Context) (int64, error)
}

// NamedGenerator is a Generator tied to an allocation namespace.
type NamedGenerator interface {
	Generator
	// Namespace returns the allocation namespace (the segment business code or
	// the snowflake layout name).
	Namespace() string
	// Close stops background prefetching and waits for it to exit.
	Close(ctx context.Context) error
}

// StringConverter renders an id for display or transport.
type StringConverter interface {
	Format(id int64) string
}
