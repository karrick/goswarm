package goswarm

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Simple memoizes responses from a Querier, providing very low-level time-based control of how
// values go stale or expire. When new values are stored in the Simple instance, if they are
// TimedValue (or a pointer to a TimedValue) items the existing Stale and Expiry values are used for
// the newly inserted value. If the new value is not a TimedValue struct or pointer to a TimedValue
// struct, then the Simple instance wraps the value in a TimedValue struct, and adds the Simple
// instance's stale and expiry durations to the current time and stores the resultant TimedValue
// struct.
type Simple struct {
	config *Config
	data   map[string]*lockingTimedValue
	lock   sync.RWMutex
	halt   chan struct{}
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
	if config.Lookup == nil {
		config.Lookup = func(_ string) (interface{}, error) { return nil, errors.New("no lookup defined") }
	}
	s := &Simple{
		config: config,
		data:   make(map[string]*lockingTimedValue),
	}
	if config.GCPeriodicity > 0 {
		s.halt = make(chan struct{})
		go s.run()
	}
	return s, nil
}

// Query loads the value associated with the specified key from the data map. When a stale value is
// found on Query, an asynchronous lookup of a new value is triggered, then the current value is
// returned from the data map. When an expired value is found on Query, a synchronous lookup of a
// new value is triggered, then the new value is stored and returned.
func (s *Simple) Query(key string) (interface{}, error) {
	return s.ensureTopLevelThenAcquire(key, func(ltv *lockingTimedValue) (interface{}, error) {
		// NOTE: check whether value filled while waiting for lock above
		if ltv.tv == nil {
			s.fetch(key, ltv)
			return ltv.tv.Value, ltv.tv.Err
		}

		now := time.Now()

		// NOTE: When value is an error, we want to reverse handling of zero time checks: a
		// zero expiry time does not imply no expiry, but rather already expired. Similarly,
		// a zero stale time does not imply no stale, but rather already stale.
		if ltv.tv.Err != nil {
			if ltv.tv.Expiry.IsZero() || now.After(ltv.tv.Expiry) {
				s.fetch(key, ltv) // synchronous fetch
			} else if ltv.tv.Stale.IsZero() || now.After(ltv.tv.Stale) {
				// NOTE: following immediately blocks until this method's deferred unlock executes
				go s.asynchronousFetch(key, ltv)
			}
			return ltv.tv.Value, ltv.tv.Err
		}

		// NOTE: When value is not an error, a zero expiry time means it never expires and a zero
		// stale time means the value does not get stale.
		if !ltv.tv.Expiry.IsZero() && now.After(ltv.tv.Expiry) {
			s.fetch(key, ltv) // synchronous fetch
		} else if !ltv.tv.Stale.IsZero() && now.After(ltv.tv.Stale) {
			// NOTE: following immediately blocks until this method's deferred unlock executes
			go s.asynchronousFetch(key, ltv)
		}
		return ltv.tv.Value, ltv.tv.Err
	})
}

func (s *Simple) asynchronousFetch(key string, ltv *lockingTimedValue) {
	ltv.lock.Lock()

	// NOTE: By the time lock acquired, value might be replaced already, or even might be
	// expired. If the value is neither expired nor stale, it must have been updated by
	// different routine, and we can return it without another fetch.
	now := time.Now()

	if !ltv.tv.Expiry.IsZero() && now.After(ltv.tv.Expiry) {
		s.fetch(key, ltv)
	}
	if !ltv.tv.Stale.IsZero() && now.After(ltv.tv.Stale) {
		s.fetch(key, ltv)
	}

	ltv.lock.Unlock()
}

// Fetch method attempts to fetch a new value for the specified key. If the fetch is successful, it
// stores the value in the lockingTimedValue associated with the key.
func (s *Simple) fetch(key string, ltv *lockingTimedValue) {
	staleDuration := s.config.GoodStaleDuration
	expiryDuration := s.config.GoodExpiryDuration

	value, err := s.config.Lookup(key)

	if err == nil {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
		return
	}

	// NOTE: lookup gave us an error
	staleDuration = s.config.BadStaleDuration
	expiryDuration = s.config.BadExpiryDuration

	// NOTE: new error overwrites previous error, and also used when initial value
	if ltv.tv == nil || ltv.tv.Err != nil {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
		return
	}

	// NOTE: received error this time, but still have old value, and we only replace a value
	// that has expired
	if !ltv.tv.Expiry.IsZero() {
		now := time.Now()
		if now.After(ltv.tv.Expiry) {
			ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
			return
		}
	}
}

// Load returns the value associated with the specified key, and a boolean value
// indicating whether or not the key was found in the map.
func (s *Simple) Load(key string) (interface{}, bool) {
	s.lock.RLock()
	ltv, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		return nil, false
	}

	// FIXME race condition before first value lookup complete

	ltv.lock.Lock()
	defer ltv.lock.Unlock()
	return ltv.tv.Value, true
}

// Store saves the key/value pair to the cache, overwriting whatever was previously stored.
func (s *Simple) Store(key string, value interface{}) {
	_, _ = s.ensureTopLevelThenAcquire(key, func(ltv *lockingTimedValue) (interface{}, error) {
		ltv.tv = newTimedValue(value, nil, s.config.GoodStaleDuration, s.config.GoodExpiryDuration)
		return nil, nil
	})
}

func (s *Simple) ensureTopLevelThenAcquire(key string, callback func(*lockingTimedValue) (interface{}, error)) (interface{}, error) {
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

	ltv.lock.Lock()
	defer ltv.lock.Unlock()
	return callback(ltv)
}

// Delete removes the key and associated value from the data map.
func (s *Simple) Delete(key string) {
	s.lock.RLock()
	_, ok := s.data[key]
	s.lock.RUnlock()
	if !ok {
		return
	}

	s.lock.Lock()
	delete(s.data, key)
	s.lock.Unlock()
}

// GC examines all key value pairs in the Simple swarm and deletes those whose values have expired.
func (s *Simple) GC() {
	keys := make(chan string, len(s.data))

	s.lock.Lock()
	now := time.Now()

	var keyKiller sync.WaitGroup
	keyKiller.Add(1)
	go func() {
		for key := range keys {
			delete(s.data, key)
		}
		keyKiller.Done()
	}()

	var keyChecker sync.WaitGroup
	keyChecker.Add(len(s.data))
	for key, ltv := range s.data {
		// NOTE: We do not desire to block on collecting expired keys because one key takes
		// too long to acquire its lock.
		go func(key string, ltv *lockingTimedValue) {
			var addIt bool // NOTE: use flag rather than blocking send to channel
			ltv.lock.Lock()
			if ltv.tv != nil && !ltv.tv.Expiry.IsZero() && now.After(ltv.tv.Expiry) {
				addIt = true
			}
			ltv.lock.Unlock()
			if addIt {
				keys <- key
			}
			keyChecker.Done()
		}(key, ltv)
	}
	keyChecker.Wait()
	close(keys)
	keyKiller.Wait()
	s.lock.Unlock()
}

// Close releases all memory and go-routines used by the Simple swarm.
func (s *Simple) Close() error {
	if s.config.GCPeriodicity > 0 {
		close(s.halt)
	}
	return nil
}

func (s *Simple) run() {
	active := true
	for active {
		select {
		case <-time.After(s.config.GCPeriodicity):
			s.GC()
		case <-s.halt:
			active = false
		}
	}
}
