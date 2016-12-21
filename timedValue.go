package goswarm

import (
	"sync"
	"time"
)

// TimedValue couples a value or the error with both a stale and expiry time for the value and
// error.
type TimedValue struct {
	// Value stores the datum returned by the lookup function.
	Value interface{}

	// Err stores the error returned by the lookup function.
	Err error

	// Stale stores the time at which the value becomes stale. On Query, a stale value will
	// trigger an asynchronous lookup of a replacement value, and the original value is
	// returned. A zero-value for Stale implies the value never goes stale, and querying the key
	// associated for this value will never trigger an asynchronous lookup of a replacement
	// value.
	Stale time.Time

	// Expiry stores the time at which the value expires. On Query, an expired value will block
	// until a synchronous lookup of a replacement value is attempted. Once the lookup returns,
	// the Query method will return with the new value or the error returned by the lookup
	// function.
	Expiry time.Time
}

// helper function to wrap non TimedValue items as TimedValue items.
func newTimedValue(value interface{}, err error, staleDuration, expiryDuration time.Duration) *TimedValue {
	switch val := value.(type) {
	case TimedValue:
		return &val
	case *TimedValue:
		return val
	default:
		if staleDuration == 0 && expiryDuration == 0 {
			return &TimedValue{Value: value, Err: err}
		}
		var stale, expiry time.Time
		now := time.Now()
		if staleDuration > 0 {
			stale = now.Add(staleDuration)
		}
		if expiryDuration > 0 {
			expiry = now.Add(expiryDuration)
		}
		return &TimedValue{Value: value, Err: err, Stale: stale, Expiry: expiry}
	}
}

// lockingTimedValue is a pointer to a value and the lock that protects it. All access to the
// embedded TimedValue ought to be protected by use of the lock.
type lockingTimedValue struct {
	lock sync.Mutex  // *all* access to this structure requires holding this lock
	tv   *TimedValue // nil when value fetch not yet complete
}
