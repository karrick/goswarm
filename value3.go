package goswarm

import (
	"fmt"
	"sync"
	"time"
)

// Value3 is a type whose semantic value might be expired (effectively identical
// to unknown), an error, a fresh value, or a stale value.
type Value3 struct {
	times CacheTimes
	value interface{}
	err   error
	lock  sync.RWMutex
}

func (v Value3) Load() (interface{}, error, VST) {
	var value interface{}
	var err error
	var vst VST
	v.WithSharedLock(func(sl *SharedLockedValue) {
		value, err, vst = sl.Load()
	})
	return value, err, vst
}

func (v *Value3) StoreValue(times CacheTimes, value interface{}) {
	v.WithExclusiveLock(func(el *ExclusiveLockedValue) {
		el.StoreValue(times, value)
	})
}

func (v *Value3) StoreError(times CacheTimes, err error) {
	v.WithExclusiveLock(func(el *ExclusiveLockedValue) {
		el.StoreError(times, err)
	})
}

func (v *Value3) StoreErrorf(times CacheTimes, format string, a ...interface{}) {
	v.WithExclusiveLock(func(el *ExclusiveLockedValue) {
		el.StoreErrorf(times, format, a...)
	})
}

// Reset returns the instance to its initialized state.
func (v *Value3) Reset() {
	v.WithExclusiveLock(func(el *ExclusiveLockedValue) {
		el.Reset()
	})
}

func (v Value3) resetWhenExpired() {
	v.lock.Lock()
	if !(v.times.ExpiresAt.IsZero() || time.Now().Before(v.times.ExpiresAt)) { // if expired
		var zero CacheTimes
		v.times = zero
		v.value = nil
		v.err = nil
	}
	v.lock.Unlock()
}

func (v *Value3) WithExclusiveLock(callback func(*ExclusiveLockedValue)) {
	v.lock.Lock()
	defer v.lock.Unlock()
	callback(&ExclusiveLockedValue{v: v})
}

func (v *Value3) WithSharedLock(callback func(*SharedLockedValue)) {
	v.lock.RLock()
	defer v.lock.RUnlock()
	callback(&SharedLockedValue{v: v})
}

type ExclusiveLockedValue struct {
	v *Value3
}

func (el *ExclusiveLockedValue) Load() (interface{}, error, VST) {
	now := time.Now()
	// When expired, it does not matter what is stored in the structure.
	if !(el.v.times.ExpiresAt.IsZero() || now.Before(el.v.times.ExpiresAt)) {
		return nil, nil, ValueNone
	}
	// When not expired, and an error, return it.
	if el.v.err != nil {
		err := el.v.err
		return nil, err, ValueError
	}
	value := el.v.value
	// Now it's just a question of fresh or stale.
	if el.v.times.StaleAt.IsZero() || now.Before(el.v.times.StaleAt) {
		return value, nil, ValueFresh
	}
	return value, nil, ValueStale
}

func (el *ExclusiveLockedValue) StoreValue(times CacheTimes, value interface{}) {
	el.v.times = times
	el.v.value = value
	el.v.err = nil
}

func (el *ExclusiveLockedValue) StoreError(times CacheTimes, err error) {
	el.v.times = times
	el.v.value = nil
	el.v.err = err
}

func (el *ExclusiveLockedValue) StoreErrorf(times CacheTimes, format string, a ...interface{}) {
	el.v.times = times
	el.v.value = nil
	el.v.err = fmt.Errorf(format, a...)
}

// Reset returns the instance to its initialized state.
func (el *ExclusiveLockedValue) Reset() {
	var zero CacheTimes
	el.v.times = zero
	el.v.value = nil
	el.v.err = nil
}

type SharedLockedValue struct {
	v *Value3
}

func (sl *SharedLockedValue) Load() (interface{}, error, VST) {
	now := time.Now()
	// When expired, it does not matter what is stored in the structure.
	if !(sl.v.times.ExpiresAt.IsZero() || now.Before(sl.v.times.ExpiresAt)) {
		return nil, nil, ValueNone
	}
	// When not expired, and an error, return it.
	if sl.v.err != nil {
		err := sl.v.err
		return nil, err, ValueError
	}
	value := sl.v.value
	// Now it's just a question of fresh or stale.
	if sl.v.times.StaleAt.IsZero() || now.Before(sl.v.times.StaleAt) {
		return value, nil, ValueFresh
	}
	return value, nil, ValueStale
}
