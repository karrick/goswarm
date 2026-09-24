package goswarm

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Ensure Simple continues to satisfy the Querier interface.
var _ Querier = (*Simple)(nil)

type point struct {
	X, Y int
}

func TestSwarmQueryReturnsTypedValue(t *testing.T) {
	var invoked uint64
	swarm, err := NewSwarm(&SwarmConfig[string, uint64]{Lookup: func(key string) (uint64, error) {
		atomic.AddUint64(&invoked, 1)
		return strconv.ParseUint(key, 10, 64)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	// NOTE: No type assertion required on the returned value.
	var value uint64
	value, err = swarm.Query("42")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := value, uint64(42); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Second query served from cache.
	value, err = swarm.Query("42")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := value, uint64(42); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := atomic.LoadUint64(&invoked), uint64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestSwarmQueryLookupErrorReturnsZeroValue(t *testing.T) {
	swarm, err := NewSwarm(&SwarmConfig[string, point]{Lookup: func(_ string) (point, error) {
		return point{}, errors.New("lookup failure")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	value, err := swarm.Query("miss")
	if err == nil || !strings.Contains(err.Error(), "lookup failure") {
		t.Errorf("GOT: %v; WANT: %v", err, "lookup failure")
	}
	if got, want := value, (point{}); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestSwarmNilConfig(t *testing.T) {
	swarm, err := NewSwarm[string, string](nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	value, err := swarm.Query("miss")
	if err == nil || !strings.Contains(err.Error(), "no lookup defined") {
		t.Errorf("GOT: %v; WANT: %v", err, "no lookup defined")
	}
	if got, want := value, ""; got != want {
		t.Errorf("GOT: %q; WANT: %q", got, want)
	}
}

func TestSwarmInvalidConfig(t *testing.T) {
	_, err := NewSwarm(&SwarmConfig[string, int]{GoodStaleDuration: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "negative good stale duration") {
		t.Errorf("GOT: %v; WANT: %v", err, "negative good stale duration")
	}
}

func TestSwarmStoreAndLoad(t *testing.T) {
	swarm, err := NewSwarm[string, point](nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	swarm.Store("origin", point{})
	swarm.Store("p", point{X: 1, Y: 2})

	value, ok := swarm.Load("p")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, (point{X: 1, Y: 2}); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// A stored zero-value is distinguishable from a missing key.
	value, ok = swarm.Load("origin")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, (point{}); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	value, ok = swarm.Load("missing")
	if got, want := ok, false; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, (point{}); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	swarm.Delete("p")
	if _, ok = swarm.Load("p"); ok {
		t.Errorf("GOT: %v; WANT: %v", ok, false)
	}
}

func TestSwarmStoreUsesConfiguredDurations(t *testing.T) {
	swarm, err := NewSwarm(&SwarmConfig[string, int]{
		GoodStaleDuration:  time.Minute,
		GoodExpiryDuration: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	swarm.Store("key", 13)

	tv := swarm.LoadTimedValue("key")
	if tv == nil {
		t.Fatal("GOT: nil; WANT: non-nil")
	}
	if got, want := tv.Value, 13; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if tv.Stale.IsZero() || tv.Expiry.IsZero() || tv.Created.IsZero() {
		t.Errorf("GOT: %#v; WANT: non-zero Created, Stale, and Expiry", tv)
	}
	if got, want := tv.Status(), Fresh; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestSwarmStoreTimedValue(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		swarm, err := NewSwarm[string, int](nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = swarm.Close() }()

		swarm.StoreTimedValue("expired", &SwarmTimedValue[int]{Value: 42, Expiry: time.Now().Add(-time.Minute)})

		value, ok := swarm.Load("expired")
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		tv := swarm.LoadTimedValue("expired")
		if got, want := tv.IsExpired(), true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := tv.Value, 42; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if tv.Created.IsZero() {
			t.Errorf("GOT: %v; WANT: non-zero Created", tv.Created)
		}
	})

	t.Run("stale triggers asynchronous lookup", func(t *testing.T) {
		var wg sync.WaitGroup
		swarm, err := NewSwarm(&SwarmConfig[string, string]{Lookup: func(_ string) (string, error) {
			defer wg.Done()
			return "new", nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = swarm.Close() }()

		swarm.StoreTimedValue("key", &SwarmTimedValue[string]{Value: "old", Stale: time.Now().Add(-time.Minute)})

		wg.Add(1)
		value, err := swarm.Query("key")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := value, "old"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		wg.Wait()

		// Allow update goroutine to store the new value after Lookup returns.
		time.Sleep(5 * time.Millisecond)

		value, err = swarm.Query("key")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := value, "new"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func TestSwarmRange(t *testing.T) {
	swarm, err := NewSwarm[string, int](nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	swarm.Store("alpha", 1)
	swarm.Store("bravo", 2)
	swarm.StoreTimedValue("expired", &SwarmTimedValue[int]{Value: 100, Expiry: time.Now().Add(-time.Minute)})

	var sum int
	swarm.Range(func(_ string, value *SwarmTimedValue[int]) {
		sum += value.Value
	})
	if got, want := sum, 3; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	terminated := swarm.RangeBreak(func(_ string, value *SwarmTimedValue[int]) bool {
		return true
	})
	if got, want := terminated, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	swarm.Store("ensure range released top level lock", 0)
}

func TestSwarmGC(t *testing.T) {
	swarm, err := NewSwarm(&SwarmConfig[string, string]{
		GCPeriodicity: 10 * time.Millisecond,
		GCTimeout:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	swarm.Store("good", "good")
	swarm.StoreTimedValue("expired", &SwarmTimedValue[string]{Value: "expired", Expiry: time.Now().Add(-time.Minute)})

	time.Sleep(25 * time.Millisecond)

	stats := swarm.Stats()
	if got, want := stats.Count, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Evictions, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Stores, int64(2); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	if got, want := swarm.Close(), error(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestSwarmIntegerKeys(t *testing.T) {
	var invoked uint64
	swarm, err := NewSwarm(&SwarmConfig[int64, string]{Lookup: func(key int64) (string, error) {
		atomic.AddUint64(&invoked, 1)
		return strconv.FormatInt(key, 10), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	for i := 0; i < 2; i++ {
		value, err := swarm.Query(42)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := value, "42"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
	if got, want := atomic.LoadUint64(&invoked), uint64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	swarm.Store(-1, "negative one")
	if value, ok := swarm.Load(-1); !ok || value != "negative one" {
		t.Errorf("GOT: %q, %v; WANT: %q, %v", value, ok, "negative one", true)
	}
	swarm.Delete(-1)
	if _, ok := swarm.Load(-1); ok {
		t.Errorf("GOT: %v; WANT: %v", ok, false)
	}
}

func TestSwarmStructKeys(t *testing.T) {
	swarm, err := NewSwarm(&SwarmConfig[point, int]{Lookup: func(key point) (int, error) {
		return key.X * key.Y, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swarm.Close() }()

	value, err := swarm.Query(point{X: 3, Y: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := value, 12; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	swarm.StoreTimedValue(point{X: 1, Y: 1}, &SwarmTimedValue[int]{Value: 100, Expiry: time.Now().Add(-time.Minute)})

	keys := make(map[point]int)
	swarm.Range(func(key point, value *SwarmTimedValue[int]) {
		keys[key] = value.Value
	})
	if got, want := len(keys), 1; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := keys[point{X: 3, Y: 4}], 12; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	swarm.GC()
	if got, want := swarm.Stats().Evictions, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}
