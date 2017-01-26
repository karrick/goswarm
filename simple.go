package goswarm

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Simple memoizes responses from a Querier, providing very low-level time-based control of how
// values go stale or expire. When a new value is stored in the Simple instance, if it is a
// TimedValue item--or a pointer to a TimedValue item)--the data map will use the provided Stale and
// Expiry values. If the new value is not a TimedValue instance or pointer to a TimedValue instance,
// then the Simple instance wraps the value in a TimedValue struct, and adds the Simple instance's
// stale and expiry durations to the current time and stores the resultant TimedValue instance.
type Simple struct {
	config     *Config
	data       map[string]*lockingTimedValue
	lock       sync.RWMutex
	halt       chan struct{}
	closeError chan error
}

// NewSimple returns Swarm that attempts to respond to Query methods by consulting its TTL cache,
// then directing the call to the underlying Querier if a valid response is not stored. Note this
// function accepts a pointer so creating an instance with defaults can be done by passing a nil
// value rather than a pointer to a Config instance.
//
//    simple, err := goswarm.NewSimple(&goswarm.Simple{
//        GoodStaleDuration:  time.Minute,
//        GoodExpiryDuration: 24 * time.Hour,
//        BadStaleDuration:   time.Minute,
//        BadExpiryDuration:  5 * time.Minute,
//        Lookup:             func(key string) (interface{}, error) {
//            // TODO: do slow calculation or make a network call
//            result := key // example
//            return result, nil
//        },
//    })
//    if err != nil {
//        log.Fatal(err)
//    }
//    defer func() { _ = simple.Close() }()
func NewSimple(config *Config) (*Simple, error) {
	if config == nil {
		config = &Config{}
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
		config.Lookup = func(_ string) (interface{}, error) { return nil, errors.New("no lookup defined") }
	}
	s := &Simple{
		config: config,
		data:   make(map[string]*lockingTimedValue),
	}
	if config.GCPeriodicity > 0 {
		s.halt = make(chan struct{})
		s.closeError = make(chan error)
		go s.run()
	}
	return s, nil
}

// Close releases all memory and go-routines used by the Simple swarm. If during instantiation,
// GCPeriodicity was greater than the zero-value for time.Duration, this method may block while
// completing any in progress GC run.
func (s *Simple) Close() error {
	if s.config.GCPeriodicity > 0 {
		close(s.halt)
		return <-s.closeError
	}
	return nil
}

// Delete removes the key and associated value from the data map.
func (s *Simple) Delete(key string) {
	s.lock.RLock()
	_, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		// If key is not in the data map then there is nothing to delete.
		return
	}

	// Element is in data map, but need to acquire its lock before we do anything with it, even
	// deleting it.
	s.lock.Lock()
	delete(s.data, key)
	s.lock.Unlock()
}

type gcPair struct {
	key    string
	doomed bool
}

var gcFlag int32

// GC examines all key value pairs in the Simple swarm and deletes those whose values have expired.
func (s *Simple) GC() {
	// Bail if another GC thread is already running. This may happen automatically when
	// GCPeriodicity is shorter than GCTimeout, or when user manually invokes GC method.
	if !atomic.CompareAndSwapInt32(&gcFlag, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&gcFlag, 0)

	// MARK PHASE
	s.lock.RLock()

	// Create asynchronous goroutines to collect each key-value pair individually, so overall
	// mark phase task does not block waiting for any of the key locks.  We use a channel to
	// collect key-value pair results in order to serialize the parallel collection of pairs.

	// Ultimately, however, we do not desire to spend more than a specified duration of time
	// collecting key-value pairs during the mark phase, so a context is created with a deadline
	// to allow for early termination of the mark phase. This logic does allow some key-value
	// pairs to remain expired after their eviction time, but with a long enough GCTimeout it is
	// likely that those evicted key-value pairs will be eventually collected during a future GC
	// run.

	// Although context.WithTimeout or context.WithDeadline _could_ be used below, we do not
	// desire to require Go 1.7 or above when a simple time.Timer channel will do the job.
	now := time.Now()
	timeoutC := time.After(s.config.GCTimeout)

	// Create a buffered channel large enough to receive all key-value pairs so that in the
	// event of early mark phase termination due to timeout, the goroutines created below will
	// not block on sending to a full channel that is no longer consumed after mark phase has
	// ended.
	totalCount := len(s.data)
	allPairs := make(chan gcPair, totalCount)

	// Loop through all existing key-value pairs in the cache, creating goroutines for each pair
	// to individually wait for the respective key lock, test the eviction logic, and send the
	// result to the results channel.
	for key, ltv := range s.data {
		go func(key string, ltv *lockingTimedValue, allPairs chan<- gcPair) {
			ltv.lock.Lock()
			defer ltv.lock.Unlock()
			allPairs <- gcPair{
				key:    key,
				doomed: ltv.tv != nil && ltv.tv.isExpired(now),
			}
		}(key, ltv, allPairs)
	}

	// After looping through all key-value pairs, we no longer need to hold the read lock for
	// the cache while waiting for the results to arrive.
	s.lock.RUnlock()

	// COLLECT PHASE: Spawn goroutine to collect locked key-value pairs.
	var doomed []string
	var receivedCount int
loop:
	for {
		select {
		case <-timeoutC:
			// The above channel is closed when either the timeout has expired or when
			// the number of received pairs equals the number of key-value pairs in the
			// cache, done manually below by calling `cancel()`.
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

	// SWEEP PHASE: Grab the write lock and delete all doomed key-value pairs from the cache.
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, key := range doomed {
		delete(s.data, key)
	}
}

// Load returns the value associated with the specified key, and a boolean value indicating whether
// or not the key was found in the map.
func (s *Simple) Load(key string) (interface{}, bool) {
	// Do not want to use getOrCreateLockingTimeValue, because there's no reason to create LTV
	// if key is not present in data map.
	s.lock.RLock()
	ltv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		return nil, false
	}

	ltv.lock.Lock()
	defer ltv.lock.Unlock()

	if ltv.tv == nil {
		// Element either recently erased by another routine while this method was waiting
		// for element lock above, or has not been populated by fetch, in which case the
		// value is not really there yet.
		return nil, false
	}
	return ltv.tv.Value, true
}

// Query loads the value associated with the specified key from the data map. When a stale value is
// found on Query, an asynchronous lookup of a new value is triggered, then the current value is
// returned from the data map. When an expired value is found on Query, a synchronous lookup of a
// new value is triggered, then the new value is stored and returned.
func (s *Simple) Query(key string) (interface{}, error) {
	ltv := s.getOrCreateLockingTimedValue(key)
	// log.Printf("Query: waiting for ltv.lock: %q", key)
	ltv.lock.Lock()
	defer ltv.lock.Unlock()

	// check whether value filled while waiting for lock above
	if ltv.tv == nil {
		s.update(key, ltv)
		return ltv.tv.Value, ltv.tv.Err
	}

	now := time.Now()

	if ltv.tv.isExpired(now) {
		s.update(key, ltv)
	} else if ltv.tv.isStale(now) {
		go s.lockAndUpdate(key, ltv)
	}

	return ltv.tv.Value, ltv.tv.Err
}

// Range invokes specified callback function for each non-expired key in the data map. Each
// key-value pair is independently locked until the callback function invoked with the specified key
// returns. This method does not block access to the Simple instance, allowing keys to be added and
// removed like normal even while the callbacks are running.
func (s *Simple) Range(callback func(key string, value *TimedValue)) {
	// Need to have read lock while enumerating key-value pairs from map
	s.lock.RLock()
	for key, ltv := range s.data {
		// Now that we have a key-value pair from the map, we can release its lock to
		// prevent blocking other routines that need it. But we do need to acquire element
		// level lock to allow use of the key-value pair.
		s.lock.RUnlock()
		ltv.lock.Lock()

		// The element is ours. If it's a valid value, and not yet expired, invoke the
		// user's callback with the key and value.
		if ltv.tv != nil && !ltv.tv.IsExpired() {
			callback(key, ltv.tv)
		}

		// After callback is done with element, release its lock and re-acquire map-level
		// lock before we grab the next key-value pair from the map.
		ltv.lock.Unlock()
		s.lock.RLock()
	}
	s.lock.RUnlock()
}

// Store saves the key-value pair to the cache, overwriting whatever was previously stored.
func (s *Simple) Store(key string, value interface{}) {
	ltv := s.getOrCreateLockingTimedValue(key)
	ltv.lock.Lock()
	ltv.tv = newTimedValue(value, nil, s.config.GoodStaleDuration, s.config.GoodExpiryDuration)
	ltv.lock.Unlock()
}

// Update forces an update of the value associated with the specified key.
func (s *Simple) Update(key string) {
	ltv := s.getOrCreateLockingTimedValue(key)
	ltv.lock.Lock()
	s.update(key, ltv)
	ltv.lock.Unlock()
}

////////////////////////////////////////

func (s *Simple) getOrCreateLockingTimedValue(key string) *lockingTimedValue {
	s.lock.RLock()
	ltv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		s.lock.Lock()
		// check whether value filled while waiting for lock above
		ltv, ok = s.data[key]
		if !ok {
			ltv = &lockingTimedValue{}
			s.data[key] = ltv
		}
		s.lock.Unlock()
	}
	return ltv
}

func (s *Simple) lockAndUpdate(key string, ltv *lockingTimedValue) {
	// log.Printf("lockAndFetch: waiting on lock for: %q", key)
	ltv.lock.Lock()
	// log.Printf("lockAndFetch: have lock for: %q", key)

	// By the time lock acquired, value might be replaced already, or even might be expired. If
	// the value is neither expired nor stale, it must have been updated by different routine,
	// and we can return it without another fetch.
	now := time.Now()

	if ltv.tv == nil {
		s.update(key, ltv)
	} else if ltv.tv.isExpired(now) {
		s.update(key, ltv)
	} else if ltv.tv.isStale(now) {
		s.update(key, ltv)
	}

	ltv.lock.Unlock()
}

func (s *Simple) run() {
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

// The update method attempts to update a new value for the specified key. If the update is
// successful, it stores the value in the lockingTimedValue associated with the key. WARNING: It is
// *imperative* that the element lock is acquired around a call to this method.
func (s *Simple) update(key string, ltv *lockingTimedValue) {
	staleDuration := s.config.GoodStaleDuration
	expiryDuration := s.config.GoodExpiryDuration

	value, err := s.config.Lookup(key)

	if err == nil {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
		return
	}

	// lookup gave us an error
	staleDuration = s.config.BadStaleDuration
	expiryDuration = s.config.BadExpiryDuration

	// new error overwrites previous error, and also used when initial value
	if ltv.tv == nil || ltv.tv.Err != nil {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
		return
	}

	// received error this time, but still have old value, and we only replace a good value with
	// an error if the good value has expired
	if ltv.tv.IsExpired() {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
	}
}
