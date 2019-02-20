package goswarm

import (
	"fmt"
	"sync"
	"time"
)

// Value2 is a type whose semantic value might be expired (effectively identical
// to unknown), an error, a fresh value, or a stale value.
type Value2 struct {
	times CacheTimes
	value interface{}
	err   error
	lock  sync.RWMutex
}

func (v Value2) Load() (interface{}, error, VST) {
	v.lock.RLock()
	now := time.Now()
	// When expired, it does not matter what is stored in the structure.
	if !(v.times.ExpiresAt.IsZero() || now.Before(v.times.ExpiresAt)) {
		v.lock.RUnlock()
		return nil, nil, ValueNone
	}
	// When not expired, and an error, return it.
	if v.err != nil {
		err := v.err
		v.lock.RUnlock()
		return nil, err, ValueError
	}
	// Now it's just a question of fresh or stale.
	if v.times.StaleAt.IsZero() || now.Before(v.times.StaleAt) {
		value := v.value
		v.lock.RUnlock()
		return value, nil, ValueFresh
	}
	value := v.value
	v.lock.RUnlock()
	return value, nil, ValueStale
}

func (v *Value2) StoreValue(times CacheTimes, value interface{}) {
	v.lock.Lock()
	v.times = times
	v.value = value
	v.err = nil
	v.lock.Unlock()
}

func (v *Value2) StoreError(times CacheTimes, err error) {
	v.lock.Lock()
	v.times = times
	v.value = nil
	v.err = err
	v.lock.Unlock()
}

func (v *Value2) StoreErrorf(times CacheTimes, format string, a ...interface{}) {
	v.lock.Lock()
	v.times = times
	v.value = nil
	v.err = fmt.Errorf(format, a...)
	v.lock.Unlock()
}

// Reset returns the instance to its initialized state.
func (v *Value2) Reset() {
	v.lock.Lock()
	var zero CacheTimes
	v.times = zero
	v.value = nil
	v.err = nil
	v.lock.Unlock()
}

func (v Value2) resetWhenExpired() {
	v.lock.Lock()
	if !(v.times.ExpiresAt.IsZero() || time.Now().Before(v.times.ExpiresAt)) {
		var zero CacheTimes
		v.times = zero
		v.value = nil
		v.err = nil
	}
	v.lock.Unlock()
}
