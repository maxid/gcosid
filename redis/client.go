// Package redis implements CosID's Redis storage protocol.
//
// The key layout and the machine Lua scripts are copied from CosID 2.15.1
// (cosid-spring-redis), because a deployment that runs Java and Go side by side
// must see the same counters and the same leased slots. Changing a key name here
// silently splits the allocation domain in two.
package redis

import "context"

// Client is the minimal Redis surface the adapters need. *go-redis/redis.Client
// satisfies it through the thin adapter in ClientFrom; keeping the interface
// here means the library does not depend on a Redis driver.
type Client interface {
	// IncrBy adds step to key and returns the new value.
	IncrBy(ctx context.Context, key string, step int64) (int64, error)
	// Eval runs a Lua script with keys and args and returns the raw reply.
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}
