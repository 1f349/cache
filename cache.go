package cache

import (
	"sync"
	"time"
)

// timeNow is an alias used for easier testing
var timeNow = time.Now

// timeUntil is the same as time.Until but uses the test specific timeNow
func timeUntil(t time.Time) time.Duration { return t.Sub(timeNow()) }

// Cache manages items in a sync.Map, items are allowed to be permanent or have
// an expiry time. The keys which have an expiry time are stored in chain.
// chainAdd and chainDel are used to add/remove items to/from chain. These
// actions are completed in the cleaner goroutine to be concurrency safe.
type Cache[K comparable, V any] struct {
	items    sync.Map
	chain    *keyed[K] // linked list of to-expire keys
	close    chan struct{}
	chainAdd chan keyed[K]
	chainDel chan K
}

// keyed is the same as item but uses the key as the data for the chain based,
// item removal scheduler. It also contains the atomic pointer to the next item
// in the chain.
type keyed[K any] struct {
	item[K]
	next *keyed[K]
}

// item is a piece of data stored as a value in items or as a key in chain. The
// data is stored along with the expiry date for quick access.
type item[T any] struct {
	data    T
	expires time.Time
}

// HasExpired returns true if the expiry time is non-zero and is before the value
// of timeNow.
func (c item[T]) HasExpired() bool {
	return !c.expires.IsZero() && c.expires.Before(timeNow())
}

// New creates a *Cache[K, V] ready to be used by the caller
func New[K comparable, V any]() *Cache[K, V] {
	c := &Cache[K, V]{
		items:    sync.Map{},
		close:    make(chan struct{}, 1),
		chainAdd: make(chan keyed[K], 1),
		chainDel: make(chan K, 1),
	}
	go c.cleaner()
	return c
}

// Close simply sends a signal to stop the cleaner goroutine. The cache is left
// in its current state and can still be read. The only difference being that
// items which have expired will not be garbage collected.
func (c *Cache[K, V]) Close() {
	close(c.close)
}

// cleaner handles removing expired keys. The chainAdd and chainDel channels are
// handled here to prevent race conditions. This ensures the expiry timer can be
// stopped before modifying the chain.
//
// The cleaner is stopped whenever the chain is empty due to there being no chain
// to manage.
func (c *Cache[K, V]) cleaner() {
	// cleaner is always called from Set or Delete methods with a value sent on chainAdd or chainDel
	select {
	case node := <-c.chainAdd:
		c.chainInsert(node)
	case key := <-c.chainDel:
		c.chainSplice(key)
	default:
		// skip if chainAdd or chainDel isn't ready
	}

	// at this point if the chain is empty then exit
	if c.chain == nil {
		return
	}

	// create a timer for the next expiry
	t := time.NewTimer(timeUntil(c.chain.expires))

	for {
		select {
		case <-c.close:
			// exit the cleaner goroutine
			return
		case node := <-c.chainAdd:
			// stop the timer safely
			if !t.Stop() {
				<-t.C
			}
			// the chain will not be empty after this insert so no check is required
			c.chainInsert(node)
		case key := <-c.chainDel:
			// stop the timer safely
			if !t.Stop() {
				<-t.C
			}
			c.chainSplice(key)
		case <-t.C:
			// if there is no chain then kill the expiry scheduler
			if c.chain == nil {
				return
			}

			// remove all expired entries
			for c.chain != nil && c.chain.HasExpired() {
				c.items.CompareAndDelete(c.chain.data, c.chain.item)
				c.chain = c.chain.next
			}
		}

		// if there is no chain then kill the expiry scheduler
		if c.chain == nil {
			return
		}

		t.Reset(timeUntil(c.chain.expires))
	}
}

func (c *Cache[K, V]) chainInsert(node keyed[K]) {
	// quick path for an empty chain
	if c.chain == nil {
		c.chain = &node
		return
	}

	// loop through the chain to add an item
	ring := c.chain
	for {
		if ring.next == nil {
			// add as last item
			ring.next = &node
			break
		}
		if ring.expires.After(node.expires) {
			// add between two nodes
			node.next = ring.next
			ring.next = &node
			break
		}
		// move to the next ring in the chain
		ring = ring.next
	}
}

func (c *Cache[K, V]) chainSplice(key K) {
	// quick path if chain is empty
	if c.chain == nil {
		return
	}

	// quick path if the first node matches
	if c.chain.data == key {
		node := c.chain
		c.chain = node.next
		node.next = nil
		return
	}

	// loop through the chain to find an item
	ring := c.chain
	for {
		// if the node is nil then the end has been reached
		node := ring.next
		if node == nil {
			break
		}
		// if the node is found then snip it out
		if node.data == key {
			ring.next = node.next
			node.next = nil
			break
		}
		// move to the next ring in the chain
		ring = node
	}
}

// GetExpires returns (value, expiry, true) for any existing key. Or (V{},
// time.Time{}, false) for any unknown key.
func (c *Cache[K, V]) GetExpires(key K) (V, time.Time, bool) {
	var v V // empty value

	obj, exists := c.items.Load(key)
	if !exists {
		return v, time.Time{}, false
	}

	i := obj.(*item[V])
	if i.HasExpired() {
		return v, time.Time{}, false
	}

	return i.data, i.expires, true
}

// Get returns (value, true) for any existing key. Or (V{}, false) for any
// unknown key. This is equivalent to calling GetExpires and ignoring the expiry
// time return value.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	value, _, found := c.GetExpires(key)
	return value, found
}

// SetPermanent adds an item to the cache without an expiry date.
//
// If an item is added with the same key then this item with be overwritten.
func (c *Cache[K, V]) SetPermanent(key K, value V) {
	// if the cache is closed then just return
	select {
	case <-c.close:
		return
	default:
	}

	i := &item[V]{data: value} // expires is not set here
	c.items.Store(key, i)
}

// Set adds an item to the cache with an expiry date.
//
// If an item is added with the same key then this item with be overwritten.
func (c *Cache[K, V]) Set(key K, value V, expires time.Time) {
	// if the cache is closed then just return
	select {
	case <-c.close:
		return
	default:
	}

	if expires.Before(timeNow()) {
		return
	}

	i := &item[V]{data: value, expires: expires}
	c.items.Store(key, i)
	c.chainAdd <- keyed[K]{item: item[K]{data: key, expires: expires}}
}

// Delete removes an item from the cache.
func (c *Cache[K, V]) Delete(key K) {
	// if the cache is closed then just return
	select {
	case <-c.close:
		return
	default:
	}

	c.items.Delete(key)
	c.chainDel <- key
}

// Range calls f with every key-value pair, which has not expired, currently
// stored in the map.
//
// If f returns false, range stops the iteration.
//
// See sync/Map.Range for implementation specific details.
func (c *Cache[K, V]) Range(f func(key K, value V) bool) {
	c.items.Range(func(key, value any) bool {
		item := value.(*item[V])
		if item.HasExpired() {
			return true
		}

		return f(key.(K), item.data)
	})
}
