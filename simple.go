package goswarm

// Simple memoizes responses from a Querier, providing very low-level
// time-based control of how values go stale or expire. When a new value is
// stored in the Simple instance, if it is a TimedValue item--or a pointer to a
// TimedValue item)--the data map will use the provided Stale and Expiry
// values. If the new value is not a TimedValue instance or pointer to a
// TimedValue instance, then the Simple instance wraps the value in a
// TimedValue struct, and adds the Simple instance's stale and expiry durations
// to the current time and stores the resultant TimedValue instance.
//
// Simple stores values of any type as interface{}, requiring callers to perform
// type assertions on values returned by Load and Query. Consider using Swarm,
// created by NewSwarm, to store values of a specific type.
type Simple struct {
	swarm *Swarm[string, interface{}]
}

// NewSimple returns Swarm that attempts to respond to Query methods by
// consulting its TTL cache, then directing the call to the underlying Querier
// if a valid response is not stored. Note this function accepts a pointer so
// creating an instance with defaults can be done by passing a nil value rather
// than a pointer to a Config instance.
//
//	simple, err := goswarm.NewSimple(&goswarm.Config{
//	    GoodStaleDuration:  time.Minute,
//	    GoodExpiryDuration: 24 * time.Hour,
//	    BadStaleDuration:   time.Minute,
//	    BadExpiryDuration:  5 * time.Minute,
//	    Lookup:             func(key string) (interface{}, error) {
//	        // TODO: do slow calculation or make a network call
//	        result := key // example
//	        return result, nil
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer func() { _ = simple.Close() }()
func NewSimple(config *Config) (*Simple, error) {
	swarm, err := NewSwarm(config)
	if err != nil {
		return nil, err
	}
	return &Simple{swarm: swarm}, nil
}

// Close releases all memory and go-routines used by the Simple swarm. If
// during instantiation, GCPeriodicity was greater than the zero-value for
// time.Duration, this method may block while completing any in progress GC run.
func (s *Simple) Close() error { return s.swarm.Close() }

// Delete removes the key and associated value from the data map.
func (s *Simple) Delete(key string) { s.swarm.Delete(key) }

// GC examines all key value pairs in the Simple swarm and deletes those whose
// values have expired.
func (s *Simple) GC() { s.swarm.GC() }

// Load returns the value associated with the specified key, and a boolean value
// indicating whether or not the key was found in the map.
func (s *Simple) Load(key string) (interface{}, bool) { return s.swarm.Load(key) }

// LoadTimedValue returns the TimedValue associated with the specified key, or
// nil if the key is not found in the map.
func (s *Simple) LoadTimedValue(key string) *TimedValue { return s.swarm.LoadTimedValue(key) }

// Query loads the value associated with the specified key from the data
// map. When a stale value is found on Query, at most one asynchronous lookup of
// a new value is triggered, and the current value is returned from the data
// map. When no value or an expired value is found on Query, a synchronous
// lookup of a new value is triggered, then the new value is stored and
// returned.
func (s *Simple) Query(key string) (interface{}, error) { return s.swarm.Query(key) }

// Range invokes specified callback function for each non-expired key in the
// data map. Each key-value pair is independently locked until the callback
// function invoked with the specified key returns. This method does not block
// access to the Simple instance, allowing keys to be added and removed like
// normal even while the callbacks are running.
func (s *Simple) Range(callback func(key string, value *TimedValue)) {
	s.swarm.Range(callback)
}

// RangeBreak invokes specified callback function for each non-expired key in
// the data map. Each key-value pair is independently locked until the callback
// function invoked with the specified key returns. This method does not block
// access to the Simple instance, allowing keys to be added and removed like
// normal even while the callbacks are running. When the callback returns true,
// this function performs an early termination of enumerating the cache,
// returning true it its caller.
func (s *Simple) RangeBreak(callback func(key string, value *TimedValue) bool) bool {
	return s.swarm.RangeBreak(callback)
}

// Stats returns a snapshot of the cache's statistics. Note all statistics will
// be reset when this method is invoked, allowing the client to determine the
// number of each respective events that have taken place since the previous
// time this method was invoked.
func (s *Simple) Stats() Stats { return s.swarm.Stats() }

// Store saves the key-value pair to the cache, overwriting whatever was
// previously stored.
func (s *Simple) Store(key string, value interface{}) {
	// NOTE: The configured durations are ignored when value is already a
	// TimedValue.
	switch val := value.(type) {
	case TimedValue:
		s.swarm.StoreTimedValue(key, &val)
	case *TimedValue:
		s.swarm.StoreTimedValue(key, val)
	default:
		s.swarm.Store(key, value)
	}
}

// Update forces an update of the value associated with the specified key.
func (s *Simple) Update(key string) { s.swarm.Update(key) }
