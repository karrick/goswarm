package goswarm

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Swarm memoizes responses from a Lookup function, providing very low-level
// time-based control of how values go stale or expire. The type parameter T
// specifies the type of the values stored in the Swarm, removing the need for
// callers to perform type assertions on values returned by Load and Query.
//
// When a new value is stored using the Store method, the Swarm instance wraps
// the value in a SwarmTimedValue struct, adds the Swarm instance's good stale
// and expiry durations to the current time, and stores the resultant
// SwarmTimedValue instance. When a SwarmTimedValue is stored using the
// StoreTimedValue method, the data map uses its Stale and Expiry values.
type Swarm[T any] struct {
	config     *SwarmConfig[T]
	data       map[string]*atomicTimedValue[T]
	lock       sync.RWMutex
	halt       chan struct{}
	closeError chan error
	gcFlag     int32
	stats      Stats
}

// Stats contains various cache statistics.
type Stats struct {
	Count        int64 // Count represents the number of items in the cache.
	Creates      int64 // Creates represents how many new cache items were created since the previous Stats call.
	Deletes      int64 // Deletes represents how many Delete calls were made since the previous Stats call. Note this counts when Delete is called with a key that is not in the cache.
	Evictions    int64 // Evictions represents how many key-value pairs were evicted since the previous Stats call.
	Hits         int64 // Hits represents how many Load and Query calls found and returned a Fresh item.
	LookupErrors int64 // LookupErrors represents the number of Lookup invocations that returned an error since the previous Stats call.
	Misses       int64 // Misses represents how many Load and Query calls either did not find the item, or found an expired item.
	Queries      int64 // Queries represents how many Load and Query calls were made since the previous Stats call.
	Stales       int64 // Stales represents how many Load and Query calls found and returned a Stale item.
	Stores       int64 // Stores represents how many Store calls were made since the previous Stats call.
	Updates      int64 // Updates represents how many Update calls were made since the previous Stats call.
}

// NewSwarm returns a Swarm that attempts to respond to Query methods by
// consulting its TTL cache, then invoking the configured Lookup function if a
// valid response is not stored. Note this function accepts a pointer so
// creating an instance with defaults can be done by passing a nil value rather
// than a pointer to a SwarmConfig instance, in which case the type parameter
// must be provided explicitly, e.g., goswarm.NewSwarm[int](nil).
//
//	swarm, err := goswarm.NewSwarm(&goswarm.SwarmConfig[uint64]{
//	    GoodStaleDuration:  time.Minute,
//	    GoodExpiryDuration: 24 * time.Hour,
//	    BadStaleDuration:   time.Minute,
//	    BadExpiryDuration:  5 * time.Minute,
//	    Lookup:             func(key string) (uint64, error) {
//	        // TODO: do slow calculation or make a network call
//	        return strconv.ParseUint(key, 10, 64)
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer func() { _ = swarm.Close() }()
func NewSwarm[T any](config *SwarmConfig[T]) (*Swarm[T], error) {
	if config == nil {
		config = &SwarmConfig[T]{}
	}
	if config.GoodStaleDuration < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative good stale duration: %v", config.GoodStaleDuration)
	}
	if config.GoodExpiryDuration < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative good expiry duration: %v", config.GoodExpiryDuration)
	}

	if config.GoodStaleDuration > 0 && config.GoodExpiryDuration > 0 && config.GoodStaleDuration >= config.GoodExpiryDuration {
		return nil, fmt.Errorf("cannot create Swarm with good stale duration not less than good expiry duration: %v; %v", config.GoodStaleDuration, config.GoodExpiryDuration)
	}

	if config.BadStaleDuration < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative bad stale duration: %v", config.BadStaleDuration)
	}
	if config.BadExpiryDuration < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative bad expiry duration: %v", config.BadExpiryDuration)
	}

	if config.BadStaleDuration > 0 && config.BadExpiryDuration > 0 && config.BadStaleDuration >= config.BadExpiryDuration {
		return nil, fmt.Errorf("cannot create Swarm with bad stale duration not less than bad expiry duration: %v; %v", config.BadStaleDuration, config.BadExpiryDuration)
	}
	if config.GCPeriodicity < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative GCPeriodicity duration: %v", config.GCPeriodicity)
	}
	if config.GCTimeout < 0 {
		return nil, fmt.Errorf("cannot create Swarm with negative GCTimeout duration: %v", config.GCTimeout)
	}
	if config.GCTimeout == 0 {
		config.GCTimeout = defaultGCTimeout
	}
	if config.Lookup == nil {
		config.Lookup = func(_ string) (T, error) {
			var zero T
			return zero, errors.New("no lookup defined")
		}
	}
	s := &Swarm[T]{
		config: config,
		data:   make(map[string]*atomicTimedValue[T]),
	}
	if config.GCPeriodicity > 0 {
		s.halt = make(chan struct{})
		s.closeError = make(chan error)
		go s.run()
	}
	return s, nil
}

// Close releases all memory and go-routines used by the Swarm. If
// during instantiation, GCPeriodicity was greater than the zero-value for
// time.Duration, this method may block while completing any in progress GC run.
func (s *Swarm[T]) Close() error {
	if s.config.GCPeriodicity > 0 {
		close(s.halt)
		return <-s.closeError
	}
	return nil
}

// Delete removes the key and associated value from the data map.
func (s *Swarm[T]) Delete(key string) {
	atomic.AddInt64(&s.stats.Deletes, 1)

	s.lock.RLock()
	_, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		// If key is not in the data map then there is nothing to delete.
		return
	}

	// Element is in data map, but need to acquire exclusive map lock before we
	// delete it.
	s.lock.Lock()
	delete(s.data, key)
	s.lock.Unlock()
}

type gcPair struct {
	key    string
	doomed bool
}

// GC examines all key value pairs in the Swarm and deletes those whose
// values have expired.
func (s *Swarm[T]) GC() {
	// Bail if another GC thread is already running. This may happen
	// automatically when GCPeriodicity is shorter than GCTimeout, or when user
	// manually invokes GC method.
	if !atomic.CompareAndSwapInt32(&s.gcFlag, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&s.gcFlag, 0)

	// MARK PHASE
	s.lock.RLock()

	// Create asynchronous goroutines to collect each key-value pair
	// individually, so overall mark phase task does not block waiting for any
	// of the key locks.  We use a channel to collect key-value pair results in
	// order to serialize the parallel collection of pairs.

	// Ultimately, however, we do not desire to spend more than a specified
	// duration of time collecting key-value pairs during the mark phase, so a
	// context is created with a deadline to allow for early termination of the
	// mark phase. This logic does allow some key-value pairs to remain expired
	// after their eviction time, but with a long enough GCTimeout it is likely
	// that those evicted key-value pairs will be eventually collected during a
	// future GC run.

	// Although context.WithTimeout or context.WithDeadline _could_ be used
	// below, we do not desire to require Go 1.7 or above when a simple
	// time.Timer channel will do the job.
	now := time.Now()
	timeoutC := time.After(s.config.GCTimeout)

	// Create a buffered channel large enough to receive all key-value pairs so
	// that in the event of early mark phase termination due to timeout, the
	// goroutines created below will not block on sending to a full channel that
	// is no longer consumed after mark phase has ended.
	totalCount := len(s.data)
	allPairs := make(chan gcPair, totalCount)

	// Loop through all existing key-value pairs in the cache, creating
	// goroutines for each pair to individually wait for the respective key
	// lock, test the eviction logic, and send the result to the results
	// channel.
	for key, atv := range s.data {
		go func(key string, atv *atomicTimedValue[T], allPairs chan<- gcPair) {
			if tv := atv.av.Load(); tv != nil {
				allPairs <- gcPair{
					key:    key,
					doomed: tv.IsExpiredAt(now),
				}
			}
		}(key, atv, allPairs)
	}

	// After looping through all key-value pairs, we no longer need to hold the
	// read lock for the cache while waiting for the results to arrive.
	s.lock.RUnlock()

	// COLLECT PHASE: Spawn goroutine to collect locked key-value pairs.
	var doomed []string
	var receivedCount int
loop:
	for {
		select {
		case <-timeoutC:
			// The above channel is closed when either the timeout has expired
			// or when the number of received pairs equals the number of
			// key-value pairs in the cache, done manually below by calling
			// `cancel()`.
			break loop
		case pair := <-allPairs:
			if pair.doomed {
				doomed = append(doomed, pair.key)
			}
			// Once all key-value pairs have been received we can terminate the
			// collection phase.
			if receivedCount++; receivedCount == totalCount {
				break loop
			}
		}
	}

	// SWEEP PHASE: Grab the write lock and delete all doomed key-value pairs
	// from the cache.
	s.lock.Lock()
	for _, key := range doomed {
		delete(s.data, key)
	}
	atomic.AddInt64(&s.stats.Evictions, int64(len(doomed)))
	s.lock.Unlock()
}

// Load returns the value associated with the specified key, and a boolean value
// indicating whether or not the key was found in the map. When the key is not
// found, or its value has expired, the zero-value of T is returned.
func (s *Swarm[T]) Load(key string) (T, bool) {
	var zero T

	atomic.AddInt64(&s.stats.Queries, 1)

	// Do not want to use getOrCreateLockingTimeValue, because there's no reason
	// to create ATV if key is not present in data map.
	s.lock.RLock()
	atv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, false
	}

	tv := atv.av.Load()
	if tv == nil {
		// Element either recently erased by another routine while this method
		// was waiting for element lock above, or has not been populated by
		// fetch, in which case the value is not really there yet.
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, false
	}

	now := time.Now()

	if tv.IsExpiredAt(now) {
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, false
	}
	if tv.IsStaleAt(now) {
		atomic.AddInt64(&s.stats.Stales, 1)
		return tv.Value, true
	}
	atomic.AddInt64(&s.stats.Hits, 1)
	return tv.Value, true
}

// LoadTimedValue returns the SwarmTimedValue associated with the specified key,
// or nil if the key is not found in the map.
func (s *Swarm[T]) LoadTimedValue(key string) *SwarmTimedValue[T] {
	atomic.AddInt64(&s.stats.Queries, 1)

	// Do not want to use getOrCreateLockingTimeValue, because there's no reason
	// to create ATV if key is not present in data map.
	s.lock.RLock()
	atv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		atomic.AddInt64(&s.stats.Misses, 1)
		return nil
	}

	tv := atv.av.Load()
	if tv == nil {
		// Element either recently erased by another routine while this method
		// was waiting for element lock above, or has not been populated by
		// fetch, in which case the value is not really there yet.
		atomic.AddInt64(&s.stats.Misses, 1)
		return nil
	}

	now := time.Now()

	if tv.IsExpiredAt(now) {
		atomic.AddInt64(&s.stats.Misses, 1)
	} else if tv.IsStaleAt(now) {
		atomic.AddInt64(&s.stats.Stales, 1)
	} else {
		atomic.AddInt64(&s.stats.Hits, 1)
	}
	return tv
}

// Query loads the value associated with the specified key from the data
// map. When a stale value is found on Query, at most one asynchronous lookup of
// a new value is triggered, and the current value is returned from the data
// map. When no value or an expired value is found on Query, a synchronous
// lookup of a new value is triggered, then the new value is stored and
// returned.
func (s *Swarm[T]) Query(key string) (T, error) {
	atomic.AddInt64(&s.stats.Queries, 1)

	atv := s.getOrCreateAtomicTimedValue(key)
	tv := atv.av.Load()
	if tv == nil {
		atomic.AddInt64(&s.stats.Misses, 1)
		tv = s.update(key, atv)
		return tv.Value, tv.Err
	}

	now := time.Now()

	if tv.IsExpiredAt(now) {
		// Expired is considered a blocking miss.
		atomic.AddInt64(&s.stats.Misses, 1)
		tv = s.update(key, atv)
		return tv.Value, tv.Err
	}

	if tv.IsStaleAt(now) {
		// If no other goroutine is looking up this value, spin one off.
		if atomic.CompareAndSwapInt32(&atv.pending, 0, 1) {
			go func() {
				defer atomic.StoreInt32(&atv.pending, 0)
				_ = s.update(key, atv)
			}()
		}
		atomic.AddInt64(&s.stats.Stales, 1)
		return tv.Value, tv.Err
	}

	atomic.AddInt64(&s.stats.Hits, 1)
	return tv.Value, tv.Err
}

// Range invokes specified callback function for each non-expired key in the
// data map. Each key-value pair is independently locked until the callback
// function invoked with the specified key returns. This method does not block
// access to the Swarm instance, allowing keys to be added and removed like
// normal even while the callbacks are running.
func (s *Swarm[T]) Range(callback func(key string, value *SwarmTimedValue[T])) {
	// Need to have read lock while enumerating key-value pairs from map
	s.lock.RLock()
	for key, atv := range s.data {
		// Now that we have a key-value pair from the map, we can release the
		// map's lock to prevent blocking other routines that need it.
		s.lock.RUnlock()

		if tv := atv.av.Load(); tv != nil {
			// We have an element. If it's not yet expired, invoke the user's
			// callback with the key and value.
			if !tv.IsExpired() {
				callback(key, tv)
			}
		}

		// After callback is done with element, re-acquire map-level lock before
		// we grab the next key-value pair from the map.
		s.lock.RLock()
	}
	s.lock.RUnlock()
}

// RangeBreak invokes specified callback function for each non-expired key in
// the data map. Each key-value pair is independently locked until the callback
// function invoked with the specified key returns. This method does not block
// access to the Swarm instance, allowing keys to be added and removed like
// normal even while the callbacks are running. When the callback returns true,
// this function performs an early termination of enumerating the cache,
// returning true it its caller.
func (s *Swarm[T]) RangeBreak(callback func(key string, value *SwarmTimedValue[T]) bool) bool {
	// Need to have read lock while enumerating key-value pairs from map
	s.lock.RLock()
	for key, atv := range s.data {
		// Now that we have a key-value pair from the map, we can release the
		// map's lock to prevent blocking other routines that need it.
		s.lock.RUnlock()

		if tv := atv.av.Load(); tv != nil {
			// We have an element. If it's not yet expired, invoke the user's
			// callback with the key and value.
			if !tv.IsExpired() {
				if callback(key, tv) {
					return true
				}
			}
		}

		// After callback is done with element, re-acquire map-level lock before
		// we grab the next key-value pair from the map.
		s.lock.RLock()
	}
	s.lock.RUnlock()
	return false
}

// Stats returns a snapshot of the cache's statistics. Note all statistics will
// be reset when this method is invoked, allowing the client to determine the
// number of each respective events that have taken place since the previous
// time this method was invoked.
func (s *Swarm[T]) Stats() Stats {
	s.lock.RLock()
	count := int64(len(s.data))
	s.lock.RUnlock()

	return Stats{
		Count:        count,
		Creates:      atomic.SwapInt64(&s.stats.Creates, 0),
		Deletes:      atomic.SwapInt64(&s.stats.Deletes, 0),
		Evictions:    atomic.SwapInt64(&s.stats.Evictions, 0),
		Hits:         atomic.SwapInt64(&s.stats.Hits, 0),
		LookupErrors: atomic.SwapInt64(&s.stats.LookupErrors, 0),
		Misses:       atomic.SwapInt64(&s.stats.Misses, 0),
		Queries:      atomic.SwapInt64(&s.stats.Queries, 0),
		Stales:       atomic.SwapInt64(&s.stats.Stales, 0),
		Stores:       atomic.SwapInt64(&s.stats.Stores, 0),
		Updates:      atomic.SwapInt64(&s.stats.Updates, 0),
	}
}

// Store saves the key-value pair to the cache, overwriting whatever was
// previously stored. The value is wrapped in a SwarmTimedValue whose stale and
// expiry times are derived from the configured GoodStaleDuration and
// GoodExpiryDuration.
func (s *Swarm[T]) Store(key string, value T) {
	atomic.AddInt64(&s.stats.Stores, 1)
	atv := s.getOrCreateAtomicTimedValue(key)
	atv.av.Store(newTimedValue(value, nil, s.config.GoodStaleDuration, s.config.GoodExpiryDuration))
}

// StoreTimedValue saves the key and the provided SwarmTimedValue to the cache,
// overwriting whatever was previously stored. Unlike Store, the configured
// durations are ignored, and the Stale and Expiry times of the provided
// SwarmTimedValue are used. When its Created time is the zero-value, it is set
// to the current time.
func (s *Swarm[T]) StoreTimedValue(key string, tv *SwarmTimedValue[T]) {
	atomic.AddInt64(&s.stats.Stores, 1)
	atv := s.getOrCreateAtomicTimedValue(key)
	if tv.Created.IsZero() {
		tv.Created = time.Now()
	}
	atv.av.Store(tv)
}

// Update forces an update of the value associated with the specified key.
func (s *Swarm[T]) Update(key string) {
	atomic.AddInt64(&s.stats.Updates, 1)
	atv := s.getOrCreateAtomicTimedValue(key)
	s.update(key, atv)
}

////////////////////////////////////////

func (s *Swarm[T]) run() {
	for {
		select {
		case <-time.After(s.config.GCPeriodicity):
			s.GC()
		case <-s.halt:
			s.closeError <- nil
			// there is no cleanup required, so we just return
			return
		}
	}
}

func (s *Swarm[T]) getOrCreateAtomicTimedValue(key string) *atomicTimedValue[T] {
	s.lock.RLock()
	atv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		s.lock.Lock()
		// check whether value filled while waiting for exclusive access to map
		// lock
		atv, ok = s.data[key]
		if !ok {
			atv = new(atomicTimedValue[T])
			s.data[key] = atv
			atomic.AddInt64(&s.stats.Creates, 1)
		}
		s.lock.Unlock()
	}
	return atv
}

// The update method attempts to update a new value for the specified key. If
// the update is successful, it stores the value in the TimedValue associated
// with the key.
func (s *Swarm[T]) update(key string, atv *atomicTimedValue[T]) *SwarmTimedValue[T] {
	value, err := s.config.Lookup(key)
	if err == nil {
		tv := newTimedValue(value, nil, s.config.GoodStaleDuration, s.config.GoodExpiryDuration)
		atv.av.Store(tv)
		return tv
	}

	// lookup gave us an error
	atomic.AddInt64(&s.stats.LookupErrors, 1)
	staleDuration := s.config.BadStaleDuration
	expiryDuration := s.config.BadExpiryDuration

	// new error overwrites previous error, and also used when initial value
	tv := atv.av.Load()
	if tv == nil || tv.Err != nil {
		tv = newTimedValue(value, err, staleDuration, expiryDuration)
		atv.av.Store(tv)
		return tv
	}

	// received error this time, but still have old value, and we only replace a
	// good value with an error if the good value has expired
	if tv.IsExpired() {
		tv = newTimedValue(value, err, staleDuration, expiryDuration)
		atv.av.Store(tv)
	}
	return tv
}
