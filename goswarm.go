package goswarm

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Config specifies the configuration parameters for a Swarm instance.
type Config struct {
	GoodStaleDuration  time.Duration
	GoodExpiryDuration time.Duration
	BadStaleDuration   time.Duration
	BadExpiryDuration  time.Duration
	Lookup             func(string) (interface{}, error)
}

// Swarm memoizes responses from a Querier.
type Swarm struct {
	config *Config
	data   map[string]*lockingTimedValue
	lock   sync.RWMutex
}

// NewSwarm returns Swarm that attempts to respond to Query methods by consulting its TTL cache,
// then directing the call to the underlying Querier if a valid response is not stored. Note this
// function accepts a pointer so creating an instance with defaults can be done by passing a nil
// value rather than a pointer to a Config instance.
func NewSwarm(config *Config) (*Swarm, error) {
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
	if config.Lookup == nil {
		return nil, errors.New("cannot create Swarm without defined lookup")
	}
	return &Swarm{
		config: config,
		data:   make(map[string]*lockingTimedValue),
	}, nil
}

// LoadStore loads the value associated with the specified key from the cache.
func (swarm *Swarm) LoadStore(key string) (interface{}, error) {
	return swarm.ensureTopLevelThenAcquire(key, func(ltv *lockingTimedValue) (interface{}, error) {
		// NOTE: check whether value filled while waiting for lock above
		if ltv.tv == nil {
			swarm.fetch(key, ltv)
			return ltv.tv.Value, ltv.tv.Err
		}

		now := time.Now()

		// NOTE: When value is an error, we want to reverse handling of zero time checks: a
		// zero expiry time does not imply no expiry, but rather already expired. Similarly,
		// a zero stale time does not imply no stale, but rather already stale.
		if ltv.tv.Err != nil {
			if ltv.tv.Expiry.IsZero() || now.After(ltv.tv.Expiry) {
				swarm.fetch(key, ltv) // synchronous fetch
			} else if ltv.tv.Stale.IsZero() || now.After(ltv.tv.Stale) {
				// NOTE: following immediately blocks until this method's deferred unlock executes
				go swarm.asynchronousFetch(key, ltv)
			}
			return ltv.tv.Value, ltv.tv.Err
		}

		// NOTE: When value is not an error, a zero expiry time means it never expires and a zero
		// stale time means the value does not get stale.
		if !ltv.tv.Expiry.IsZero() && now.After(ltv.tv.Expiry) {
			swarm.fetch(key, ltv) // synchronous fetch
		} else if !ltv.tv.Stale.IsZero() && now.After(ltv.tv.Stale) {
			// NOTE: following immediately blocks until this method's deferred unlock executes
			go swarm.asynchronousFetch(key, ltv)
		}
		return ltv.tv.Value, ltv.tv.Err
	})
}

func (swarm *Swarm) asynchronousFetch(key string, ltv *lockingTimedValue) {
	ltv.lock.Lock()

	// NOTE: By the time lock acquired, value might be replaced already, or even might be
	// expired. If the value is neither expired nor stale, it must have been updated by
	// different routine, and we can return it without another fetch.
	now := time.Now()

	if !ltv.tv.Expiry.IsZero() && now.After(ltv.tv.Expiry) {
		swarm.fetch(key, ltv)
	}
	if !ltv.tv.Stale.IsZero() && now.After(ltv.tv.Stale) {
		swarm.fetch(key, ltv)
	}

	ltv.lock.Unlock()
}

// Fetch method attempts to fetch a new value for the specified key. If the fetch is successful, it
// stores the value in the lockingTimedValue associated with the key.
func (swarm *Swarm) fetch(key string, ltv *lockingTimedValue) {
	staleDuration := swarm.config.GoodStaleDuration
	expiryDuration := swarm.config.GoodExpiryDuration

	value, err := swarm.config.Lookup(key)

	if err == nil {
		ltv.tv = newTimedValue(value, err, staleDuration, expiryDuration)
		return
	}

	// NOTE: lookup gave us an error
	staleDuration = swarm.config.BadStaleDuration
	expiryDuration = swarm.config.BadExpiryDuration

	// NOTE: new error always replaces old error
	if ltv.tv.Err != nil {
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

func (swarm *Swarm) Load(key string) (interface{}, bool) {
	swarm.lock.RLock()
	ltv, ok := swarm.data[key]
	swarm.lock.RUnlock()
	if !ok {
		return nil, false
	}

	ltv.lock.Lock()
	defer ltv.lock.Unlock()
	return ltv.tv.Value, true
}

// Store saves the key/value pair to the cache, overwriting whatever was previously stored.
func (swarm *Swarm) Store(key string, value interface{}) {
	_, _ = swarm.ensureTopLevelThenAcquire(key, func(ltv *lockingTimedValue) (interface{}, error) {
		ltv.tv = newTimedValue(value, nil, swarm.config.GoodStaleDuration, swarm.config.GoodExpiryDuration)
		return nil, nil
	})
}

func (swarm *Swarm) ensureTopLevelThenAcquire(key string, callback func(*lockingTimedValue) (interface{}, error)) (interface{}, error) {
	swarm.lock.RLock()
	ltv, ok := swarm.data[key]
	swarm.lock.RUnlock()
	if !ok {
		swarm.lock.Lock()
		// check whether value filled while waiting for lock above
		ltv, ok = swarm.data[key]
		if !ok {
			ltv = &lockingTimedValue{}
			swarm.data[key] = ltv
		}
		swarm.lock.Unlock()
	}

	ltv.lock.Lock()
	defer ltv.lock.Unlock()
	return callback(ltv)
}

func (swarm *Swarm) Delete(key string) {
	swarm.lock.RLock()
	_, ok := swarm.data[key]
	swarm.lock.RUnlock()
	if !ok {
		return
	}

	swarm.lock.Lock()
	delete(swarm.data, key)
	swarm.lock.Unlock()
}

func (swarm *Swarm) GC() {
	keys := make(chan string, len(swarm.data))

	swarm.lock.Lock()
	now := time.Now()

	var keyKiller sync.WaitGroup
	keyKiller.Add(1)
	go func() {
		for key := range keys {
			delete(swarm.data, key)
		}
		keyKiller.Done()
	}()

	var keyChecker sync.WaitGroup
	keyChecker.Add(len(swarm.data))
	for key, ltv := range swarm.data {
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
	swarm.lock.Unlock()
}

func (swarm *Swarm) Close() error {
	return nil
}
