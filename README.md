# goswarm

Go Stale While Asynchronously Revalidate Memoization

## DESCRIPTION

goswarm is a library written in Go for storing the results of
expensive function calls and returning the cached result when the same
input key occurs again.

In addition to the examples provided below, documentation can be found
at
[![GoDoc](https://godoc.org/github.com/karrick/goswarm?status.svg)](https://godoc.org/github.com/karrick/goswarm).

```Go
    simple, err := goswarm.NewSimple(nil)
    if err != nil {
        log.Fatal(err)
    }

    // you can store any Go type in a Swarm
    simple.Store("someKeyString", 42)
    simple.Store("anotherKey", struct{}{})
    simple.Store("yetAnotherKey", make(chan interface{}))

    // but when you retrieve it, you are responsible to perform type assertions
    key := "yetAnotherKey"
    value, ok := simple.Load(key)
    if !ok {
        panic(fmt.Errorf("cannot find %q", key))
    }
    value = value.(chan interface{})

    simple.Delete("anotherKey")
```

As seen above, goswarm's API is similar to that of any associative
array, also known as a map in Go terminology: It allows storing a
value to be retrieved using a specified key, and overwriting any value
that might already be present in the map. It allows loading the value
associated with the specified key already stored in the map. It allows
deleting a specified key and its associated value stored in the map.

goswarm differs from most traditional associative array APIs in that
it provides a method to load the value associated with a specified
key, and if that key is not found in the map, to invoke a specified
lookup function to fetch the value for that key.

```Go
    simple, err := goswarm.NewSimple(&goswarm.Config{
        Lookup: func(key string) (interface{}, error) {
            // TODO: do slow calculation or make a network call
            result := key // example
            return result, nil
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = simple.Close() }()

    value, err := simple.Query("%version")
    if !err {
        panic(fmt.Errorf("cannot retrieve value for key %q: %s", key, err))
    }
    fmt.Printf("The value is: %v\n", value)
```

## Stale-While-Revalidate and Stale-If-Error

In addition, goswarm provides stale-while-revalidate and
stale-if-error compatible features in its simple API.

### Stale-While-Revalidate

When the user requests the value associated with a particular key,
goswarm determines whether that key-value pair is present, and if so,
whether that value has become stale. If the value is stale, goswarm
will return the previously stored value to the client, but spawn an
asynchronous routine to fetch a new value for that key and store that
value in the map to be used for future queries.

When the user requests the value associated with a particular key that
has expired, goswarm will not return that value, but rather
synchronously fetch an updated value to store in the map and return to
the user.

### Stale-If-Error

When fetching a new value to replace a value that has become stale,
the lookup callback funciton might return an error. Perhaps the remote
network resource used to fetch responses is offline. In these cases,
goswarm will not overwrite the stale value with the error, but
continue to serve the stale value until the lookup callback function
returns a new value rather than an error, or the value expires, in
which case, the error is returned to the user.

## Periodic Removal Of Expired Keys

If `GCPeriodicty` configuration value is greater than the zero-value
for time.Duration, goswarm spawns a separate go-routine that invokes
the `GC` method periodically, removing all key-value pairs from the
data map that have an expired time. When this feature is used, the
`Close` method must be invoked to stop and release that go-routine.

```Go
    simple, err := goswarm.NewSimple(&goswarm.Config{
        GoodExpiryDuration: 24 * time.Hour,
        BadExpiryDuration:  5 * time.Minute,
        GCPeriodicity:      time.Hour,
        Lookup:             func(key string) (interface{}, error) {
            // TODO: do slow calculation or make a network call
            result := key // example
            return result, nil
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = simple.Close() }()
```

## Note About Choosing Stale and Expiry Durations

Different applications may require different logic, however if your
application needs to continue processing data and serving requests
even when a downstream dependency is subject to frequent high latency
periods or faults, it is recommended to set the GoodStaleDuration
period to a low enough value to ensure data is reasonably up-to-date,
but extend the GoodExpiryDuration to be long enough that your
application can still operate using possibly stale data. The goswarm
library will repeatedly attempt to fetch a new value from the
downstream service, but until a defined very long period of time
transpires, your service will be relatively insulated from these type
of downstream faults and latencies.

```Go
    simple, err := goswarm.NewSimple(&goswarm.Config{
        GoodStaleDuration:  time.Minute,
        GoodExpiryDuration: 24 * time.Hour,
        BadStaleDuration:   time.Minute,
        BadExpiryDuration:  5 * time.Minute,
        GCPeriodicity:      time.Hour,
        Lookup:             func(key string) (interface{}, error) {
            // TODO: do slow calculation or make a network call
            result := key // example
            return result, nil
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = simple.Close() }()

    value, err := simple.Query("%version")
    if !err {
        panic(fmt.Errorf("cannot retrieve value for key %q: %s", key, err))
    }
    fmt.Printf("The value is: %v\n", value)
```
