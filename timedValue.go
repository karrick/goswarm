package goswarm

import (
	"sync/atomic"
	"time"
)

// TimedValueStatus is an enumeration of the states of a TimedValue: Fresh,
// Stale, or Expired.
type TimedValueStatus int

const (
	// Fresh items are not yet Stale, and will be returned immediately on Query
	// without scheduling an asynchronous Lookup.
	Fresh TimedValueStatus = iota

	// Stale items have exceeded their Fresh status, and will be returned
	// immediately on Query, but will also schedule an asynchronous Lookup if a
	// Lookup for this key is not yet in flight.
	Stale

	// Expired items have exceeded their Fresh and Stale status, and will be
	// evicted during the next background cache eviction loop, controlled by
	// GCPeriodicity parameter, or upon Query, which will cause the Query to
	// block until a new value can be obtained from the Lookup function.
	Expired
)

// TimedValue couples a value or the error with both a stale and expiry time for
// the value and error.
type TimedValue struct {
	// Value stores the datum returned by the lookup function.
	Value interface{}

	// Err stores the error returned by the lookup function.
	Err error

	// Stale stores the time at which the value becomes stale. On Query, a stale
	// value will trigger an asynchronous lookup of a replacement value, and the
	// original value is returned. A zero-value for Stale implies the value
	// never goes stale, and querying the key associated for this value will
	// never trigger an asynchronous lookup of a replacement value.
	Stale time.Time

	// Expiry stores the time at which the value expires. On Query, an expired
	// value will block until a synchronous lookup of a replacement value is
	// attempted. Once the lookup returns, the Query method will return with the
	// new value or the error returned by the lookup function.
	Expiry time.Time

	// Created stores the time at which the value was created.
	Created time.Time
}

// IsExpired returns true when the value is expired.
//
// A value is expired when its non-zero expiry time is before the current time,
// or when the value represents an error and expiry time is the time.Time
// zero-value.
func (tv *TimedValue) IsExpired() bool {
	return tv.IsExpiredAt(time.Now())
}

// IsExpiredAt returns true when the value is expired at the specified time.
//
// A value is expired when its non-zero expiry time is before the specified
// time, or when the value represents an error and expiry time is the time.Time
// zero-value.
func (tv *TimedValue) IsExpiredAt(when time.Time) bool {
	if tv.Err == nil {
		return !tv.Expiry.IsZero() && when.After(tv.Expiry)
	}
	// NOTE: When a TimedValue stores an error result, then a zero-value for the
	// Expiry imply the value is immediately expired.
	return tv.Expiry.IsZero() || when.After(tv.Expiry)
}

// IsStale returns true when the value is stale.
//
// A value is stale when its non-zero stale time is before the current time, or
// when the value represents an error and stale time is the time.Time
// zero-value.
func (tv *TimedValue) IsStale() bool {
	return tv.IsStaleAt(time.Now())
}

// IsStaleAt returns true when the value is stale at the specified time.
//
// A value is stale when its non-zero stale time is before the specified time,
// or when the value represents an error and stale time is the time.Time
// zero-value.
func (tv *TimedValue) IsStaleAt(when time.Time) bool {
	if tv.Err == nil {
		return !tv.Stale.IsZero() && when.After(tv.Stale)
	}
	// NOTE: When a TimedValue stores an error result, then a zero-value for the
	// Stale or Expiry imply the value is immediately stale.
	return tv.Stale.IsZero() || when.After(tv.Stale)
}

// Status returns Fresh, Stale, or Exired, depending on the status of the
// TimedValue item at the current time.
func (tv *TimedValue) Status() TimedValueStatus {
	return tv.StatusAt(time.Now())
}

// StatusAt returns Fresh, Stale, or Exired, depending on the status of the
// TimedValue item at the specified time.
func (tv *TimedValue) StatusAt(when time.Time) TimedValueStatus {
	if tv.IsExpiredAt(when) {
		return Expired
	}
	if tv.IsStaleAt(when) {
		return Stale
	}
	return Fresh
}

// helper function to wrap non TimedValue items as TimedValue items.
func newTimedValue(value interface{}, err error, staleDuration, expiryDuration time.Duration) *TimedValue {
	switch val := value.(type) {
	case TimedValue:
		if val.Created.IsZero() {
			val.Created = time.Now()
		}
		return &val
	case *TimedValue:
		if val.Created.IsZero() {
			val.Created = time.Now()
		}
		return val
	default:
		if staleDuration == 0 && expiryDuration == 0 {
			return &TimedValue{Value: value, Err: err, Created: time.Now()}
		}
		var stale, expiry time.Time
		now := time.Now()
		if staleDuration > 0 {
			stale = now.Add(staleDuration)
		}
		if expiryDuration > 0 {
			expiry = now.Add(expiryDuration)
		}
		return &TimedValue{Value: value, Err: err, Created: time.Now(), Stale: stale, Expiry: expiry}
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
