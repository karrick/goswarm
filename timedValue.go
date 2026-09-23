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
// the value and error. It is an alias of SwarmTimedValue instantiated with
// interface{} values, and is the type of value stored by Simple.
type TimedValue = SwarmTimedValue[interface{}]

// SwarmTimedValue couples a value or the error with both a stale and expiry
// time for the value and error. The type parameter T specifies the type of the
// value.
type SwarmTimedValue[T any] struct {
	// Value stores the datum returned by the lookup function.
	Value T

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
func (tv *SwarmTimedValue[T]) IsExpired() bool {
	return tv.IsExpiredAt(time.Now())
}

// IsExpiredAt returns true when the value is expired at the specified time.
//
// A value is expired when its non-zero expiry time is before the specified
// time, or when the value represents an error and expiry time is the time.Time
// zero-value.
func (tv *SwarmTimedValue[T]) IsExpiredAt(when time.Time) bool {
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
func (tv *SwarmTimedValue[T]) IsStale() bool {
	return tv.IsStaleAt(time.Now())
}

// IsStaleAt returns true when the value is stale at the specified time.
//
// A value is stale when its non-zero stale time is before the specified time,
// or when the value represents an error and stale time is the time.Time
// zero-value.
func (tv *SwarmTimedValue[T]) IsStaleAt(when time.Time) bool {
	if tv.Err == nil {
		return !tv.Stale.IsZero() && when.After(tv.Stale)
	}
	// NOTE: When a TimedValue stores an error result, then a zero-value for the
	// Stale or Expiry imply the value is immediately stale.
	return tv.Stale.IsZero() || when.After(tv.Stale)
}

// Status returns Fresh, Stale, or Exired, depending on the status of the
// TimedValue item at the current time.
func (tv *SwarmTimedValue[T]) Status() TimedValueStatus {
	return tv.StatusAt(time.Now())
}

// StatusAt returns Fresh, Stale, or Exired, depending on the status of the
// TimedValue item at the specified time.
func (tv *SwarmTimedValue[T]) StatusAt(when time.Time) TimedValueStatus {
	if tv.IsExpiredAt(when) {
		return Expired
	}
	if tv.IsStaleAt(when) {
		return Stale
	}
	return Fresh
}

// helper function to wrap values as SwarmTimedValue items, using the specified
// stale and expiry durations.
func newTimedValue[T any](value T, err error, staleDuration, expiryDuration time.Duration) *SwarmTimedValue[T] {
	now := time.Now()
	tv := &SwarmTimedValue[T]{Value: value, Err: err, Created: now}
	if staleDuration > 0 {
		tv.Stale = now.Add(staleDuration)
	}
	if expiryDuration > 0 {
		tv.Expiry = now.Add(expiryDuration)
	}
	return tv
}

type atomicTimedValue[T any] struct {
	// av is accessed with atomic.Pointer's Load() and Store() methods to
	// atomically access the underlying *SwarmTimedValue.
	av atomic.Pointer[SwarmTimedValue[T]]

	// pending is accessed with sync/atomic primitives to control whether an
	// asynchronous lookup ought to be spawned to update av. 1 when a go routine
	// is waiting on Lookup return; 0 otherwise
	pending int32
}
