package goswarm

import (
	"sync/atomic"
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

// IsExpired returns true if and only if value is expired. A value is expired when its non-zero
// expiry time is before the current time, or when the value represents an error and expiry time is
// the time.Time zero-value.
func (tv *TimedValue) IsExpired() bool {
	return tv.isExpired(time.Now())
}

// provided for internal use so we don't need to repeatedly get the current time
func (tv *TimedValue) isExpired(when time.Time) bool {
	if tv.Err == nil {
		return !tv.Expiry.IsZero() && when.After(tv.Expiry)
	}
	// NOTE: When a TimedValue stores an error result, then Expiry and Expiry zero-values imply
	// the value is immediately expired.
	return tv.Expiry.IsZero() || when.After(tv.Expiry)
}

// IsStale returns true if and only if value is stale. A value is stale when its non-zero stale time
// is before the current time, or when the value represents an error and stale time is the time.Time
// zero-value.
func (tv *TimedValue) IsStale() bool {
	return tv.isStale(time.Now())
}

// provided for internal use so we don't need to repeatedly get the current time
func (tv *TimedValue) isStale(when time.Time) bool {
	if tv.Err == nil {
		return !tv.Stale.IsZero() && when.After(tv.Stale)
	}
	// NOTE: When a TimedValue stores an error result, then Stale and Expiry zero-values imply
	// the value is immediately stale.
	return tv.Stale.IsZero() || when.After(tv.Stale)
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

type atomicTimedValue struct {
	// av is accessed with atomic.Value's Load() and Store() methods to
	// atomically access the underlying *TimedValue.
	av atomic.Value

	// pending is accessed with sync/atomic primitives to control whether an
	// asynchronous lookup ought to be spawned to update av. 1 when a go routine
	// is waiting on Lookup return; 0 otherwise
	pending int32
}
