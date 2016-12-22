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

// lockingTimedValue is a pointer to a value and the lock that protects it. All access to the
// embedded TimedValue ought to be protected by use of the lock.
type lockingTimedValue struct {
	lock sync.Mutex  // *all* access to this structure requires holding this lock
	tv   *TimedValue // nil when value fetch not yet complete
}
