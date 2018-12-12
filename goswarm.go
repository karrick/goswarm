package goswarm

import "time"

// Querier specifies a type that provides memoizing the results of expensive function calls and
// returning the cached result when the same input key occurs again.
type Querier interface {
	Query(string) (interface{}, error)
}

// Config specifies the configuration parameters for a Swarm instance.
type Config struct {
	// GoodStaleDuration specifies how long a value remains fresh in the data map. A zero-value
	// time.Duration value implies the value never stales, and updates will only be fetched
	// after the value expires.
	GoodStaleDuration time.Duration

	// GoodExpiryDuration specifies how long a value is allowed to be served from the data
	// map. A zero-value time.Duration value implies the value never expires and can always be
	// served.
	GoodExpiryDuration time.Duration

	// BadStaleDuration specifies how long an error result remains fresh in the data map. A
	// zero-value time.Duration value implies that the error result is stale as soon as it's
	// inserted into the data map, and Querying the key again will trigger an asynchronous
	// lookup of the key to try to replace the error value with a good value.
	BadStaleDuration time.Duration

	// BadExpiryDuration specifies how long an error result is allowed to be served from the
	// data map. A zero-value time.Duration value implies the value never expires and can always
	// be served.
	BadExpiryDuration time.Duration

	// Lookup specifies the user callback function to invoke when looking up the value to be
	// associated with a stale key, expired key, or a key that has yet to be loaded into the
	// data map.
	Lookup func(string) (interface{}, error)

	// GCPeriodicity specifies how frequently the data map purges expired entries.
	GCPeriodicity time.Duration

	// GCTimeout specifies how long the GC should wait for values to unlock.
	GCTimeout time.Duration
}

const defaultGCTimeout = 10 * time.Second
