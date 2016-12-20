package goswarm

import (
	"sync"
	"time"
)

// lockingTimedValue is a pointer to a value and the lock that protects it. All access to the TimedValue
// ought to be protected by use of the lock.
type lockingTimedValue struct {
	lock sync.Mutex  // *all* access to this structure requires holding this lock
	tv   *TimedValue // nil when value fetch not yet complete
}

// TimedValue couples a value with both a stale and expiry times for the value. The time.Time zero
// value for stale implies the value never stales. The time.Time zero value for expiry implies the
// value never expires. Neither, either, and both of these may be set to the zero value or an actual
// time value.
type TimedValue struct {
	Value  interface{}
	Err    error
	Stale  time.Time
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
