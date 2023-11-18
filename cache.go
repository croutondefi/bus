package bus

import (
	"runtime"
	"sync"
	"time"
)

// enspired by github.com/patrickmn/go-cache

type ExpProcessor func(any)

type item struct {
	Object   any
	ExpireAt time.Time
}

type ICache interface {
	Set(k string, x interface{}, exp time.Time)
}

type Cache struct {
	*cache
}

type cache struct {
	items     map[string]item
	mu        sync.RWMutex
	processor ExpProcessor
	janitor   *janitor
}

type janitor struct {
	Interval time.Duration
	stop     chan bool
}

func (j *janitor) Run(c *Cache) {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			c.ProcessExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func stopJanitor(c *Cache) {
	c.janitor.stop <- true
}

func runJanitor(c *Cache, ci time.Duration) {
	j := &janitor{
		Interval: ci,
		stop:     make(chan bool),
	}
	c.janitor = j
	go j.Run(c)
}

func NewCache(p ExpProcessor) Cache {
	c := &cache{
		items:     make(map[string]item),
		processor: p,
	}
	C := Cache{c}

	runJanitor(&C, time.Second*1)
	runtime.SetFinalizer(&C, stopJanitor)

	return C
}

func (c *Cache) Set(k string, x interface{}, exp time.Time) {
	c.set(k, x, exp)
}

func (c *cache) set(k string, x interface{}, exp time.Time) {
	c.mu.Lock()
	c.items[k] = item{
		Object:   x,
		ExpireAt: exp,
	}
	c.mu.Unlock()
}

// Process all expired items from the cache.
func (c *Cache) ProcessExpired() {
	now := time.Now()
	c.mu.Lock()
	for k, v := range c.items {
		if v.ExpireAt.Before(now) {
			c.processor(v.Object)
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}
