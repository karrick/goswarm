# goswarm

Go Stale While Asynchronously Revalidate Memoization

## DESCRIPTION

goswarm is a library written in Go for storing the results of expensive function calls and returning
the cached result when the same input key occurs again.

goswarm's API is similar to that of any associative array, also known as a map in Go terminology: It
allows storing a value to be retrieved using a specified key, and overwriting any value that might
already be present in the map. It allows loading the value associated with the specified key already
stored in the map. It allows deleting a specified key and its associated value stored in the map.

goswarm differs from most traditional associative array APIs in that it provides a method to load
the value associated with a specified key, and if that key is not found in the map, to invoke a
specified lookup function to fetch the value for that key.

NOTE: Should lookup function be specified in LoadStore method call? Maybe have two methods: one
which uses predefined lookup and one which uses lookup argument?

In addition, goswarm provides stale-while-revalidate and stale-if-error compatible features in its
simple API.

### Stale-While-Revalidate

When the user requests the value associated with a particular key, goswarm determines whether that
key-value pair is present, and if so, whether that value has become stale. If the value is stale,
goswarm will return the previously stored value to the client, but spawn an asynchronous routine to
fetch a new value for that key and store that value in the map to be used for future queries.

When the user requests the value associated with a particular key that has expired, goswarm will not
return that value, but rather synchronously fetch an updated value to store in the map and return to
the user.

### Stale-If-Error

When fetching a new value to replace a value that has become stale, the lookup callback funciton
might return an error. Perhaps the remote network resource used to fetch responses is offline. In
these cases, goswarm will not overwrite the stale value with the error, but continue to serve the
stale value until the lookup callback function returns a new value rather than an error, or the
value expires, in which case, the error is returned to the user.
