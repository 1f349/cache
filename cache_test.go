package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestItem_HasExpired(t *testing.T) {
	n := time.Now()
	a := item[string]{expires: n}
	timeNow = func() time.Time { return n.Add(time.Second) }
	assert.True(t, a.HasExpired())
	a = item[string]{expires: n.Add(time.Second * 2)}
	assert.False(t, a.HasExpired())
}
