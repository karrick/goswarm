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
	v.WithSharedLock(func(v *SharedLockedValue) {
		value, err, vst = v.Load()
	})
	return value, err, vst
}

func (v *Value3) StoreOk(times CacheTimes, value interface{}) {
	v.WithExclusiveLock(func(v *ExclusiveLockedValue) {
		v.StoreOk(times, value)
	})
}

func (v *Value3) StoreError(times CacheTimes, err error) {
	v.WithExclusiveLock(func(v *ExclusiveLockedValue) {
		v.StoreError(times, err)
	})
}

func (v *Value3) StoreErrorf(times CacheTimes, format string, a ...interface{}) {
	v.WithExclusiveLock(func(v *ExclusiveLockedValue) {
		v.StoreErrorf(times, format, a...)
	})
}

// Reset returns the instance to its initialized state.
func (v *Value3) Reset() {
	v.WithExclusiveLock(func(v *ExclusiveLockedValue) {
		v.Reset()
	})
}

func (v Value3) resetWhenExpired() {
	v.lock.Lock()
	if !(v.times.ExpiresAt.IsZero() || time.Now().Before(v.times.ExpiresAt)) {
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

func (v *ExclusiveLockedValue) Load() (interface{}, error, VST) {
	now := time.Now()
	// When expired, it does not matter what is stored in the structure.
	if !(v.v.times.ExpiresAt.IsZero() || now.Before(v.v.times.ExpiresAt)) {
		return nil, nil, ValueNone
	}
	// When not expired, and an error, return it.
	if v.v.err != nil {
		err := v.v.err
		return nil, err, ValueError
	}
	// Now it's just a question of fresh or stale.
	if v.v.times.StaleAt.IsZero() || now.Before(v.v.times.StaleAt) {
		value := v.v.value
		return value, nil, ValueFresh
	}
	value := v.v.value
	return value, nil, ValueStale
}

func (v *ExclusiveLockedValue) StoreOk(times CacheTimes, value interface{}) {
	v.v.times = times
	v.v.value = value
	v.v.err = nil
}

func (v *ExclusiveLockedValue) StoreError(times CacheTimes, err error) {
	v.v.times = times
	v.v.value = nil
	v.v.err = err
}

func (v *ExclusiveLockedValue) StoreErrorf(times CacheTimes, format string, a ...interface{}) {
	v.v.times = times
	v.v.value = nil
	v.v.err = fmt.Errorf(format, a...)
}

// Reset returns the instance to its initialized state.
func (v *ExclusiveLockedValue) Reset() {
	var zero CacheTimes
	v.v.times = zero
	v.v.value = nil
	v.v.err = nil
}

type SharedLockedValue struct {
	v *Value3
}

func (v *SharedLockedValue) Load() (interface{}, error, VST) {
	now := time.Now()
	// When expired, it does not matter what is stored in the structure.
	if !(v.v.times.ExpiresAt.IsZero() || now.Before(v.v.times.ExpiresAt)) {
		return nil, nil, ValueNone
	}
	// When not expired, and an error, return it.
	if v.v.err != nil {
		err := v.v.err
		return nil, err, ValueError
	}
	// Now it's just a question of fresh or stale.
	if v.v.times.StaleAt.IsZero() || now.Before(v.v.times.StaleAt) {
		value := v.v.value
		return value, nil, ValueFresh
	}
	value := v.v.value
	return value, nil, ValueStale
}
