package goswarm

import (
	"sync"
	"time"
)

type RequestStatus uint32

const (
	RequestIdle    RequestStatus = iota // cache entry is neither in queue nor being requested
	RequestQueued                       // cache entry is queued for refresh
	RequestPending                      // cache entry request sent; awaiting return from downstream
	// NOTE: Might be able to consolidate queued and pending.
)

// CacheEntry represents a single entry in a cache. This structure is meant to
// be consumed by upstream clients.
//
// TODO: optimize this structure
type CacheEntry struct {
	times   CacheTimes
	value   interface{} // must be able to cache and return regular values
	err     error       // must be able to cache and return error values
	cond    *sync.Cond
	request RequestStatus // idle -> queued -> pending -> idle, etc . . .
}

// NewCacheEntry returns a new cache entry, and is needed to initialize the
// condition variable. Cache entries are initialized both stale and expired, as
// the zero-values for integers represents the epoch. Remember that expired is
// synonymous with unknown. Cache entries are also initialized as idle, and
// cache entries do not themselves transition from idle to queued, to pending,
// and back to idle. Those transitions are controlled by the cache.
func NewCacheEntry() *CacheEntry {
	return &CacheEntry{cond: sync.NewCond(new(sync.RWMutex))}
}

// Load waits until either a value or an error is available for this cache entry
// and returns the exclusive pair. In other words, when the returned error is
// nil, the value is the accepted value of the cache entry, even if that value
// is also nil. This is so upstream clients can store a nil value in the
// cache. Alternatively, when the returned error is non-nil, then the value will
// be nil. WARNING: Load will block until have a value or error. Must queue
// before calling.
func (entry *CacheEntry) Load(now time.Time) (interface{}, error) {
	entry.cond.L.Lock()
	// NOTE: Remember that cache elements are initialize as expired. Also
	// remember that expired is synonymous with unknown. Once we have either a
	// value or an error, then expiry will be updated and this function will
	// return with either a value or an error.
	for entry.times.ExpiresAt.After(now) {
		entry.cond.Wait()
	}
	value, err := entry.value, entry.err
	entry.cond.L.Unlock()
	return value, err
}

// WARNING: must hold entry lock for duration of this call.
func (entry *CacheEntry) transition(now time.Time) {
	if entry.times.ExpiresAt.After(now) {
		// release memory held by these fields
		entry.value = nil
		entry.err = nil
	}
}

// Update obtains lock for cache entry and invokes the provided callback. After
// the callback returns this will broadcast to the entry's condition variable
// and unlock its lock. Any goroutines waiting on the cache entry will have a
// chance to use it.
func (entry *CacheEntry) Update(callback func(*CacheEntry)) {
	entry.cond.L.Lock()
	for entry.request != RequestIdle {
		entry.cond.Wait()
	}
	callback(entry)
	entry.cond.Broadcast()
	entry.cond.L.Unlock()
}

// SetQueued ensures the entry is marked as queued and returns true when this
// entry was idle. This method is meant to be called by the cache.
func (entry *CacheEntry) SetQueued() bool {
	entry.cond.L.Lock()
	wasIdle := entry.request == RequestIdle
	entry.request = RequestQueued
	entry.cond.L.Unlock()
	return wasIdle
}

// SetPending marks the entry for pending. This method is meant to be called by
// the cache.
func (entry *CacheEntry) SetPending() {
	entry.cond.L.Lock()
	entry.request = RequestPending
	entry.cond.L.Unlock()
}

// Store will either store the value, or the error.
func (entry *CacheEntry) Store(times CacheTimes, value interface{}, err error) {
	entry.cond.L.Lock()
	for entry.request != RequestIdle {
		entry.cond.Wait()
	}

	// update the entry
	entry.times = times
	entry.request = RequestIdle
	// Ensure we only store the value or the error, and free up the other
	// field. Ensure this is done even when the upstream client passed both
	// a non-nil value and a non-nil error.
	if err == nil {
		entry.value = value
		entry.err = nil
	} else {
		entry.value = nil
		entry.err = err
	}

	entry.cond.Broadcast()
	entry.cond.L.Unlock()
}