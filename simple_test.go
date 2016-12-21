package goswarm

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type errNoLookupDefined struct{}

func (e errNoLookupDefined) Error() string {
	return "no lookup defined"
}

// func makeFailingLookup(wg *sync.WaitGroup) func(string) (interface{}, error) {
// 	return func(_ string) (interface{}, error) {
// 		if wg != nil {
// 			defer wg.Done()
// 		}
// 		return nil, errors.New("lookup failed")
// 	}
// }

func failingLookup(_ string) (interface{}, error) {
	return nil, errors.New("lookup failed")
}

////////////////////////////////////////

func ensureError(t *testing.T, swr *Simple, key, expectedError string) {
	value, err := swr.Query(key)
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if err == nil || err.Error() != expectedError {
		t.Errorf("Actual: %s; Expected: %s", err, expectedError)
	}
}

func ensureValue(t *testing.T, swr *Simple, key string, expectedValue uint64) {
	value, err := swr.Query(key)
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
		atomic.AddUint64(&invoked, 1)
		return uint64(42), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	ensureValue(t, swr, "miss", 42)

	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
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

	ensureValue(t, swr, "hit", 13)
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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Expiry: time.Now().Add(time.Minute)})

	ensureValue(t, swr, "hit", 13)
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
	swr.Store("hit", &TimedValue{Value: uint64(42), Err: nil, Expiry: time.Now().Add(-time.Minute)})

	ensureValue(t, swr, "hit", 42)

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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Stale: time.Now().Add(time.Minute)})

	ensureValue(t, swr, "hit", 13)
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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Stale: time.Now().Add(-time.Minute)})

	wg.Add(1)
	ensureValue(t, swr, "hit", 13)
	ensureValue(t, swr, "hit", 13)
	ensureValue(t, swr, "hit", 13)
	wg.Wait()

	time.Sleep(5 * time.Millisecond)

	ensureValue(t, swr, "hit", 42)
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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Stale: time.Now().Add(time.Minute), Expiry: time.Now().Add(time.Hour)})

	ensureValue(t, swr, "hit", 13)
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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Stale: time.Now().Add(-time.Minute), Expiry: time.Now().Add(time.Minute)})

	// expect to receive the old value back immediately, then expect lookup to be asynchronously invoked
	wg.Add(1)
	ensureValue(t, swr, "hit", 13)
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	ensureValue(t, swr, "hit", 42)
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
	swr.Store("hit", &TimedValue{Value: uint64(42), Err: nil, Stale: time.Now().Add(-time.Hour), Expiry: time.Now().Add(-time.Minute)})

	ensureValue(t, swr, "hit", 42)

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
	swr.Store("hit", &TimedValue{Value: uint64(13), Err: nil, Stale: time.Now().Add(-time.Minute)})

	wg.Add(1)
	ensureValue(t, swr, "hit", 13)
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	wg.Add(1)
	ensureValue(t, swr, "hit", 13)
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
	swr.Store("hit", &TimedValue{Value: nil, Err: errors.New("original error"), Stale: time.Now().Add(-time.Minute)})

	wg.Add(1)
	ensureError(t, swr, "hit", "new error")
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
	swr.Store("hit", &TimedValue{Value: nil, Err: errors.New("original error"), Stale: time.Now().Add(-time.Hour), Expiry: time.Now().Add(5 * time.Millisecond)})

	wg.Add(1)
	ensureError(t, swr, "hit", "original error")
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(1); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}

	wg.Add(1)
	ensureError(t, swr, "hit", "new error")
	wg.Wait()
	if actual, expected := atomic.AddUint64(&invoked, 0), uint64(2); actual != expected {
		t.Errorf("Actual: %d; Expected: %d", actual, expected)
	}
}

func TestSimpleRange(t *testing.T) {
	swr, err := NewSimple(&Config{
		Lookup: func(_ string) (interface{}, error) { return nil, errors.New("don't care") },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = swr.Close() }()

	swr.Store("no expiry", "shall not expire")
	swr.Store("stale value", TimedValue{Value: "stale value", Stale: time.Now().Add(-time.Minute)})
	swr.Store("expired value", TimedValue{Value: "expired value", Expiry: time.Now().Add(-time.Minute)})

	called := make(map[string]struct{})
	var calledLock sync.Mutex
	swr.Range(func(key string, value *TimedValue) {
		calledLock.Lock()
		called[key] = struct{}{}
		calledLock.Unlock()
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
}
