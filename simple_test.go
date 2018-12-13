package goswarm

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ensureErrorL(t *testing.T, querier Querier, key, expectedError string) {
	value, err := querier.Query(key)
	if value != nil {
		t.Errorf("Actual: %v; Expected: %v", value, nil)
	}
	if err == nil || !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Actual: %v; Expected: %s", err, expectedError)
	}
}

func ensureValueL(t *testing.T, querier Querier, key string, expectedValue uint64) {
	value, err := querier.Query(key)
	if value.(uint64) != expectedValue {
		t.Errorf("Actual: %d; Expected: %d", value, expectedValue)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

////////////////////////////////////////

func TestSimpleSynchronousLookupWhenMiss(t *testing.T) {
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		invoked++
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	ensureValueL(t, swr, "miss", 42)

	if actual, expected := invoked, uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleNoStaleNoExpireNoLookupWhenHit(t *testing.T) {
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		t.Fatal("lookup ought not to have been invoked")
		return nil, errors.New("lookup ought not to have been invoked")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	swr.Store("hit", uint64(13))

	ensureValueL(t, swr, "hit", 13)
}

func TestSimpleNoStaleExpireNoLookupWhenBeforeExpire(t *testing.T) {
	swr, err := NewSimple(&Config{
		Lookup: func(_ string) (interface{}, error) {
			t.Fatal("lookup ought not to have been invoked")
			return nil, errors.New("lookup ought not to have been invoked")
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that expires one minute in the future
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Expiry: now.Add(time.Minute)})

	ensureValueL(t, swr, "hit", 13)
}

func TestSimpleNoStaleExpireSynchronousLookupWhenAfterExpire(t *testing.T) {
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		atomic.AddUint64(&invoked, 1)
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that expired one minute ago
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(42), Err: nil, Created: now, Expiry: now.Add(-time.Minute)})

	ensureValueL(t, swr, "hit", 42)

	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleStaleNoExpireNoLookupWhenBeforeStale(t *testing.T) {
	swr, err := NewSimple(&Config{
		Lookup: func(_ string) (interface{}, error) {
			t.Fatal("lookup ought not to have been invoked")
			return nil, errors.New("lookup ought not to have been invoked")
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that goes stale one minute in the future
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Stale: now.Add(time.Minute)})

	ensureValueL(t, swr, "hit", 13)
}

func TestSimpleStaleNoExpireSynchronousLookupOnlyOnceWhenAfterStale(t *testing.T) {
	var wg sync.WaitGroup
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		time.Sleep(5 * time.Millisecond)
		atomic.AddUint64(&invoked, 1)
		wg.Done()
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that went stale one minute ago
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Stale: now.Add(-time.Minute)})

	wg.Add(1)
	ensureValueL(t, swr, "hit", 13)
	ensureValueL(t, swr, "hit", 13)
	ensureValueL(t, swr, "hit", 13)
	wg.Wait()

	time.Sleep(5 * time.Millisecond)

	ensureValueL(t, swr, "hit", 42)
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleStaleExpireNoLookupWhenBeforeStale(t *testing.T) {
	swr, err := NewSimple(&Config{
		Lookup: func(_ string) (interface{}, error) {
			t.Fatal("lookup ought not to have been invoked")
			return nil, errors.New("lookup ought not to have been invoked")
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that goes stale one minute in the future and expires one hour in the future
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Stale: now.Add(time.Minute), Expiry: now.Add(time.Hour)})

	ensureValueL(t, swr, "hit", 13)
}

func TestSimpleStaleExpireSynchronousLookupWhenAfterStaleAndBeforeExpire(t *testing.T) {
	var wg sync.WaitGroup
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		atomic.AddUint64(&invoked, 1)
		wg.Done()
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that went stale one minute ago and expires one minute in the future
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Stale: now.Add(-time.Minute), Expiry: now.Add(time.Minute)})

	// expect to receive the old value back immediately, then expect lookup to be asynchronously invoked
	wg.Add(1)
	ensureValueL(t, swr, "hit", 13)
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	ensureValueL(t, swr, "hit", 42)
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleStaleExpireSynchronousLookupWhenAfterExpire(t *testing.T) {
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		atomic.AddUint64(&invoked, 1)
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that went stale one hour ago and expired one minute ago
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(42), Err: nil, Created: now, Stale: now.Add(-time.Hour), Expiry: now.Add(-time.Minute)})

	ensureValueL(t, swr, "hit", 42)

	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleErrDoesNotReplaceStaleValue(t *testing.T) {
	var wg sync.WaitGroup
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		atomic.AddUint64(&invoked, 1)
		wg.Done()
		return nil, errors.New("fetch error")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that went stale one minute ago
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Created: now, Stale: now.Add(-time.Minute)})

	wg.Add(1)
	ensureValueL(t, swr, "hit", 13)
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	wg.Add(1)
	ensureValueL(t, swr, "hit", 13)
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(2); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleNewErrReplacesOldError(t *testing.T) {
	var wg sync.WaitGroup
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		atomic.AddUint64(&invoked, 1)
		wg.Done()
		return nil, errors.New("new error")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value that went stale one minute ago
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: nil, Err: errors.New("original error"), Created: now, Stale: now.Add(-time.Minute)})

	wg.Add(1)
	ensureErrorL(t, swr, "hit", "new error")
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleErrReplacesExpiredValue(t *testing.T) {
	// make stale value, but fetch duration ought cause it to expire
	var wg sync.WaitGroup
	var invoked uint64
	swr, err := NewSimple(&Config{Lookup: func(_ string) (interface{}, error) {
		time.Sleep(5 * time.Millisecond)
		atomic.AddUint64(&invoked, 1)
		wg.Done()
		return nil, errors.New("new error")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	// NOTE: storing a value is already stale, but will expire during the fetch
	now := time.Now()
	swr.Store("hit", &TimedValue{Value: nil, Err: errors.New("original error"), Created: now, Stale: now.Add(-time.Hour), Expiry: now.Add(5 * time.Millisecond)})

	wg.Add(1)
	ensureErrorL(t, swr, "hit", "original error")
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	wg.Add(1)
	ensureErrorL(t, swr, "hit", "new error")
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(2); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleRange(t *testing.T) {
	swr, err := NewSimple(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	swr.Store("no expiry", "shall not expire")
	swr.Store("stale value", TimedValue{Value: "stale value", Stale: time.Now().Add(-time.Minute)})
	swr.Store("expired value", TimedValue{Value: "expired value", Expiry: time.Now().Add(-time.Minute)})

	called := make(map[string]struct{})
	swr.Range(func(key string, value *TimedValue) {
		called[key] = struct{}{}
		swr.Store(strconv.Itoa(rand.Intn(50)), "make sure we can invoke methods that require locking")
	})

	if _, ok := called["no expiry"]; !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if _, ok := called["stale value"]; !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if _, ok := called["expired value"]; ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, false)
	}

	swr.Store("ensure range released top level lock", struct{}{})
}

func TestSimpleGC(t *testing.T) {
	swr, err := NewSimple(&Config{
		GCPeriodicity: 10 * time.Millisecond,
		GCTimeout:     10 * time.Millisecond,
		Lookup: func(key string) (interface{}, error) {
			time.Sleep(10 * time.Millisecond)
			return key, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	// populate swr with lots of data, some expired, some stale, some good
	const itemCount = 10000

	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("key%d", i)
		if rand.Intn(2) < 1 {
			go func() { _, _ = swr.Query(key) }()
		} else {
			var value interface{}
			switch rand.Intn(4) {
			case 0:
				value = TimedValue{Value: "expired", Expiry: now.Add(-time.Minute)}
			case 1:
				value = TimedValue{Value: "stale", Stale: now.Add(-time.Minute)}
			case 2:
				value = TimedValue{Value: "future stale", Stale: now.Add(time.Minute)}
			case 3:
				value = TimedValue{Value: "future expiry", Expiry: now.Add(time.Minute)}
			case 4:
				value = "good"
			}
			swr.Store(key, value)
		}
	}

	time.Sleep(25 * time.Millisecond)
	if actual, expected := swr.Close(), error(nil); actual != expected {
		t.Errorf("Actual: %s; Expected: %s", actual, expected)
	}
}

func TestStatsQuery(t *testing.T) {
	var haveLookupFail bool

	swr, err := NewSimple(&Config{
		Lookup: func(key string) (interface{}, error) {
			if haveLookupFail {
				return nil, errors.New("lookup failure")
			}
			time.Sleep(10 * time.Millisecond)
			return key, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get stats before cache methods invoked.
	stats := swr.Stats()
	if got, want := stats.Creates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Deletes, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Evictions, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Loads, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.LookupErrors, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Queries, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesFresh, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesMiss, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesStale, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Size, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Stores, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Updates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Invoke Query with key not yet in cache.
	_, err = swr.Query("foo")
	if got, want := err, error(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Get stats after new key-value pair added.
	stats = swr.Stats()
	if got, want := stats.Creates, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Deletes, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Evictions, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Loads, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.LookupErrors, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Queries, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesFresh, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesMiss, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesStale, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Size, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Stores, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Updates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Invoke Query with key already in cache.
	_, err = swr.Query("foo")
	if got, want := err, error(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Get stats after new key-value pair added.
	stats = swr.Stats()
	if got, want := stats.Creates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Deletes, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Evictions, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Loads, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.LookupErrors, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Queries, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesFresh, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesMiss, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesStale, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Size, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Stores, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Updates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	// Invoke Query with key already in cache.
	haveLookupFail = true
	_, err = swr.Query("bar")
	if err == nil || !strings.Contains(err.Error(), "lookup failure") {
		t.Errorf("GOT: %v; WANT: %v", err, "lookup failure")
	}

	// Get stats after new key-value pair added.
	stats = swr.Stats()
	if got, want := stats.Creates, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Deletes, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Evictions, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Loads, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.LookupErrors, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Queries, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesFresh, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesMiss, int64(1); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.QueriesStale, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Size, int64(2); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Stores, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := stats.Updates, int64(0); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	t.Run("stale", func(t *testing.T) {
		swr, err := NewSimple(&Config{
			Lookup: func(key string) (interface{}, error) {
				time.Sleep(10 * time.Millisecond)
				now := time.Now()
				return &TimedValue{
					Value:   key,
					Created: now,
					Stale:   now.Add(-time.Second),
					Expiry:  now.Add(time.Second),
				}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Invoke Query with key not yet in cache, populating it with stale
		// value.
		_, err = swr.Query("foo")
		if got, want := err, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		stats := swr.Stats()

		// Invoke Query with key and its stale value already in cache.
		_, err = swr.Query("foo")
		if got, want := err, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		stats = swr.Stats()

		if got, want := stats.Creates, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Deletes, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Evictions, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Loads, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.LookupErrors, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Queries, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesFresh, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesMiss, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesStale, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Size, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Stores, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Updates, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("expired", func(t *testing.T) {
		swr, err := NewSimple(&Config{
			Lookup: func(key string) (interface{}, error) {
				time.Sleep(10 * time.Millisecond)
				now := time.Now()
				return &TimedValue{
					Value:   key,
					Created: now,
					Stale:   now.Add(-time.Minute),
					Expiry:  now.Add(-time.Second),
				}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Invoke Query with key not yet in cache, populating it with expired
		// value.
		_, err = swr.Query("foo")
		if got, want := err, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		stats := swr.Stats()

		// Invoke Query with key and its expired value already in cache.
		_, err = swr.Query("foo")
		if got, want := err, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		stats = swr.Stats()

		if got, want := stats.Creates, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Deletes, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Evictions, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Loads, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.LookupErrors, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Queries, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesFresh, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesMiss, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.QueriesStale, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Size, int64(1); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Stores, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stats.Updates, int64(0); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}
