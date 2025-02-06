package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var nextYear = time.Now().Year() + 1

func TestItem_HasExpired(t *testing.T) {
	n := time.Now()

	// date before now is expired
	a := item[string]{expires: n}
	timeNow = func() time.Time { return n.Add(time.Second) }
	assert.True(t, a.HasExpired())

	// date after now is valid
	a = item[string]{expires: n.Add(time.Second * 2)}
	assert.False(t, a.HasExpired())

	// empty date is always valid
	a = item[string]{}
	assert.False(t, a.HasExpired())
}

func TestCache_GetExpires(t *testing.T) {
	c := New[string, string]()
	c.items.Store("a", &item[string]{
		data:    "b",
		expires: time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	v, exp, found := c.GetExpires("a")
	assert.Equal(t, "b", v)
	assert.Equal(t, time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC), exp)
	assert.True(t, found)

	v, exp, found = c.GetExpires("b")
	assert.Equal(t, "", v)
	assert.Equal(t, time.Time{}, exp)
	assert.False(t, found)
}

func TestCache_Get(t *testing.T) {
	c := New[string, string]()
	c.items.Store("a", &item[string]{
		data:    "b",
		expires: time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	v, found := c.Get("a")
	assert.Equal(t, "b", v)
	assert.True(t, found)

	v, found = c.Get("b")
	assert.Equal(t, "", v)
	assert.False(t, found)
}

func TestCache_SetPermanent(t *testing.T) {
	c := New[string, string]()
	c.SetPermanent("a", "b")

	value, ok := c.items.Load("a")
	v := value.(*item[string])
	assert.Equal(t, item[string]{data: "b"}, *v)
	assert.True(t, ok)

	value, ok = c.items.Load("b")
	assert.False(t, ok)
}

func TestCache_Set(t *testing.T) {
	timeNow = func() time.Time { return time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC) }

	c := New[string, string]()
	c.Set("a", "b", 5*time.Minute)

	value, ok := c.items.Load("a")
	v := value.(*item[string])
	assert.Equal(t, item[string]{
		data:    "b",
		expires: timeNow().Add(5 * time.Minute),
	}, *v)
	assert.True(t, ok)

	value, ok = c.items.Load("b")
	assert.False(t, ok)
}

func TestCache_SetAbs(t *testing.T) {
	c := New[string, string]()
	c.SetAbs("a", "b", time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC))

	value, ok := c.items.Load("a")
	v := value.(*item[string])
	assert.Equal(t, item[string]{
		data:    "b",
		expires: time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC),
	}, *v)
	assert.True(t, ok)

	value, ok = c.items.Load("b")
	assert.False(t, ok)
}

func TestCache_Delete(t *testing.T) {
	c := New[string, string]()
	c.items.Store("a", &item[string]{data: "b", expires: time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC)})
	c.chain = &keyed[string]{item: item[string]{data: "a", expires: time.Date(nextYear, time.January, 1, 0, 0, 0, 0, time.UTC)}}
	c.Delete("a")

	// scheduler should finish after deleting the item
	time.Sleep(time.Second)
	assert.Nil(t, c.chain)
}

func TestCache_Range(t *testing.T) {
	c := New[string, string]()
	c.items.Store("a", &item[string]{data: "b"})
	c.items.Store("b", &item[string]{data: "c"})
	c.items.Store("c", &item[string]{data: "d"})
	c.items.Store("d", &item[string]{data: "e"})
	c.Range(func(key string, value string) bool {
		assert.Equal(t, string([]byte{[]byte(key)[0] + 1}), value)
		return true
	})
}

func TestCache_Cleaner(t *testing.T) {
	timeNow = time.Now

	c := New[string, string]()
	c.Set("a", "b", 2*time.Second)

	// check before expiry
	time.Sleep(time.Second)
	get, b := c.Get("a")
	assert.True(t, b)
	assert.Equal(t, "b", get)

	// check after expiry
	time.Sleep(1001 * time.Millisecond)
	get, b = c.Get("a")
	assert.False(t, b)
	assert.Equal(t, "", get)

	// scheduler should finish after the chain is empty
	time.Sleep(time.Second)
	assert.Nil(t, c.chain)
}

func TestCache_UpdateExpiry(t *testing.T) {
	timeNow = time.Now

	c := New[string, string]()
	c.Set("a", "b", 2*time.Second)

	time.Sleep(time.Second)
	get, b := c.Get("a")
	assert.True(t, b)
	assert.Equal(t, "b", get)

	c.Set("a", "b", 5*time.Second)

	// after expiry of the first set call
	time.Sleep(2 * time.Second)
	get, b = c.Get("a")
	assert.True(t, b)
	assert.Equal(t, "b", get)
}

func TestCache_ClearerDeath(t *testing.T) {
	timeNow = time.Now

	c := New[string, string]()

	time.Sleep(10 * time.Millisecond)

	var added bool
	go func() {
		c.chainAdd <- keyed[string]{item: item[string]{data: "a"}}
		c.chainAdd <- keyed[string]{item: item[string]{data: "b"}}
		added = true
	}()

	time.Sleep(10 * time.Millisecond)
	assert.True(t, added)
}
