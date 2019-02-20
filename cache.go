package goswarm

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/karrick/goheap"
)

type cacheRefreshRequest struct {
	priority int64
	entry    *CacheEntry
}

const (
	DefaultFresh               = 10 * time.Minute
	DefaultFreshJitter         = 5 * time.Minute
	DefaultStale               = 5 * time.Minute
	DefaultStaleJitter         = time.Minute
	DefaultEvictionPeriodicity = 15 * time.Minute
	DefaultRefreshPeriodicity  = time.Minute
)

type CacheConfig struct {
	Fresh               time.Duration // fresh denotes the average time a new cache entry will be fresh.
	FreshJitter         time.Duration // freshJitter denotes the random jitter added to or subtracted from the fresh time for each cache entry.
	Stale               time.Duration // stale denotes the average time a new cache entry will be stale.
	StaleJitter         time.Duration // staleJitter denotes the random jitter added to or subtracted from the stale time for each cache entry.
	EvictionPeriodicity time.Duration // EvictionPeriodicity denotes the time to wait between evicting expired entries from the cache.
	RefreshPeriodicity  time.Duration // RefreshPeriodicity denotes the time to wait between refreshing stale entries in the cache.
}

type Cache struct {
	lock                                    sync.RWMutex
	data                                    map[string]*CacheEntry
	refreshHeap                             *goheap.MinHeap
	fresh, freshJitter, stale, staleJitter  time.Duration
	evictionPeriodicity, refreshPeriodicity time.Duration
	refreshRequests                         chan cacheRefreshRequest
	reset, stop                             chan struct{}
	err                                     chan error
}

// expectedStaleHeapSize does not need to be accurate.  If the value is too
// small, then the heap will simply allocate more RAM and cope with the
// additional memory requirements.  If the value is too large, then some
// allocated memory is wasted.
const expectedStaleHeapSize = 1000000 // one million

func NewCache(config *CacheConfig) (*Cache, error) {
	if config == nil {
		config = &CacheConfig{
			Fresh:               DefaultFresh,
			FreshJitter:         DefaultFreshJitter,
			Stale:               DefaultStale,
			StaleJitter:         DefaultStaleJitter,
			EvictionPeriodicity: DefaultEvictionPeriodicity,
			RefreshPeriodicity:  DefaultRefreshPeriodicity,
		}
	} else {
		// TODO: validate config items
	}

	c := &Cache{
		data:                make(map[string]*CacheEntry),
		fresh:               config.Fresh,
		freshJitter:         config.FreshJitter,
		stale:               config.Stale,
		staleJitter:         config.StaleJitter,
		refreshHeap:         goheap.NewMinHeap(expectedStaleHeapSize),
		refreshRequests:     make(chan cacheRefreshRequest),
		evictionPeriodicity: config.EvictionPeriodicity,
		refreshPeriodicity:  config.RefreshPeriodicity,
		reset:               make(chan struct{}),
		stop:                make(chan struct{}),
		err:                 make(chan error),
	}

	go c.run()
	return c, nil
}

func (c *Cache) run() {
	previousEviction := time.Now()
	previousRefresh := previousEviction

	evictionTimer := time.NewTimer(c.evictionPeriodicity)
	refreshTimer := time.NewTimer(c.refreshPeriodicity)

	for {
		select {
		case request := <-c.refreshRequests:
			c.refreshHeap.Put(request.priority, request.entry)

		case previousRefresh = <-refreshTimer.C:
			c.refresh()
			refreshTimer.Reset(c.refreshPeriodicity)

		case previousEviction = <-evictionTimer.C:
			c.evict()
			evictionTimer.Reset(c.evictionPeriodicity)

		case <-c.reset:
			// cache config updated
			now := time.Now()

			if !evictionTimer.Stop() {
				<-evictionTimer.C
			}
			if remaining := now.Sub(previousEviction); remaining > 0 {
				evictionTimer.Reset(remaining)
			} else {
				previousEviction = now
				c.evict()
				evictionTimer.Reset(c.evictionPeriodicity)
			}

			if !refreshTimer.Stop() {
				<-refreshTimer.C
			}
			if remaining := now.Sub(previousRefresh); remaining > 0 {
				refreshTimer.Reset(remaining)
			} else {
				previousRefresh = now
				c.refresh()
				refreshTimer.Reset(c.refreshPeriodicity)
			}

		case <-c.stop:
			if !evictionTimer.Stop() {
				<-evictionTimer.C
			}
			if !refreshTimer.Stop() {
				<-refreshTimer.C
			}
			c.err <- nil
			return
		}
	}
}

func (c *Cache) evict() {
	// Poor design: holds exclusive lock while enumerating keys, and each loop
	// waits for an item to not be locked, when that item might be locked for a
	// very long time by the code using this cache.
	c.lock.Lock()
	now := time.Now()

	for key, entry := range c.data {
		// TODO: how long wait for a lock; alternative is use CAS on some ownership field.
		entry.cond.L.Lock()

		if !now.Before(entry.times.ExpiresAt) {
			delete(c.data, key)
		}

		entry.cond.L.Unlock()
	}

	c.lock.Unlock()
}

func (c *Cache) refresh() {
	// TODO: when running refresh queue, if item was evicted, then skip
}

// NOTE: it looks like it's possible to do Load after Close, but I'm not sure
// why that would ever be useful. It is also possible to do Store after Close,
// which would be useful to close the cache, then commit it to a file stream.
func (c *Cache) Close() error {
	c.stop <- struct{}{} // tell run to wrap it up
	return <-c.err       // wait for error from run and return it
}

func (c *Cache) LoadFromFile(pathname string) error {
	return withOpenFile(pathname, func(fh *os.File) error {
		return c.LoadFromReader(fh)
	})
}

func (c *Cache) LoadFromReader(r io.Reader) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.loadFromReader(r)
}

func (c *Cache) loadFromReader(r io.Reader) error {
	return errors.New("TODO")
}

func (c *Cache) StoreToFile(pathname string) error {
	return withTempFile(pathname, func(tempFile *os.File, tempPathname string) error {
		return c.StoreToWriter(tempFile)
	})
}

func (c *Cache) StoreToWriter(w io.Writer) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.storeToWriter(w)
}

func (c *Cache) storeToWriter(w io.Writer) error {
	return errors.New("TODO")
}

func (c *Cache) UpdateTimes(config *CacheConfig) {
	c.lock.Lock()
	c.fresh = config.Fresh
	c.freshJitter = config.FreshJitter
	c.stale = config.Stale
	c.staleJitter = config.StaleJitter
	// TODO: update existing entries
	c.lock.Unlock()
}
