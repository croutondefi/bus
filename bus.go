// Copyright 2021 Mustafa Turan. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 license that can
// be found in the LICENSE file.

package bus

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"
)

const DefaultQueryCapacity = 10

type (
	eventAction struct {
		event   Event
		handler Handler
		ctx     context.Context
	}

	// Next is a sequential unique id generator func type
	Next func() string

	// IDGenerator is a sequential unique id generator interface
	IDGenerator interface {
		Generate() string
	}

	ctxKey int8
)

type ICache interface {
	Set(k string, x interface{}, exp time.Time)
	Restore(k string, x interface{}, exp time.Time)
}

// Bus is a message bus
type Bus struct {
	storages []Storage
	cache    ICache
	query    chan eventAction
	mutex    sync.RWMutex
	idgen    Next
	topics   map[string][]Handler
	handlers map[string]Handler
}

type cacheProxy struct {
	b     *Bus
	cache *Cache
}

func (c *cacheProxy) Restore(k string, x interface{}, exp time.Time) {
	c.cache.Set(k, x, exp)
}

func (c *cacheProxy) Set(k string, x interface{}, exp time.Time) {
	for i := range c.b.storages {
		event := x.(Event)
		_ = c.b.storages[i].Save(&event)
	}
	c.cache.Set(k, x, exp)
}

// Handler is a receiver for event reference with the given regex pattern
type Handler struct {
	key string
	// handler func to process events
	Handle func(ctx context.Context, e Event)
	// topic matcher as regex pattern
	Matcher string
}

const (
	// CtxKeyTxID tx id context key
	CtxKeyTxID = ctxKey(116)

	// CtxKeySource source context key
	CtxKeySource = ctxKey(117)

	// Version syncs with package version
	Version = "3.2.2"

	empty = ""
)

func (a *eventAction) Handle() {
	a.handler.Handle(a.ctx, a.event)
}

func worker(id int, query <-chan eventAction) {
	for action := range query {
		action.Handle()
	}
}

// NewBus inits a new bus
func NewBus(g IDGenerator, cap int) (*Bus, error) {
	if g == nil {
		return nil, fmt.Errorf("bus: Next() id generator func can't be nil")
	}

	b := &Bus{
		query:    make(chan eventAction),
		idgen:    g.Generate,
		topics:   make(map[string][]Handler),
		handlers: make(map[string]Handler),
	}

	cache := NewCache(func(e any) {
		event, ok := e.(Event)

		if !ok {
			return
		}

		b.handleEvent(context.Background(), event)
	})

	b.cache = &cacheProxy{
		b:     b,
		cache: &cache,
	}

	for i := 0; i < cap; i++ {
		go worker(i, b.query)
	}

	return b, nil
}

func (b *Bus) AddStorage(s Storage) {
	b.storages = append(b.storages, s)
}

func (b *Bus) Preload() error {
	for _, s := range b.storages {
		events, err := s.Load()
		if err != nil {
			return err
		}

		for i := range events {
			b.cache.Restore(events[i].ID, events[i], events[i].ScheduledAt)
		}
	}

	return nil
}

// Emit inits a new event and delivers to the interested in handlers with
// sync safety
func (b *Bus) Emit(ctx context.Context, topic string, data interface{}) error {
	return b.EmitWithOpts(ctx, topic, data)
}

// EmitWithOpts inits a new event and delivers to the interested in handlers
// with sync safety and options
func (b *Bus) EmitWithOpts(ctx context.Context, topic string, data interface{}, opts ...EventOption) error {
	e := Event{Topic: topic, Data: data}
	for _, o := range opts {
		e = o(e)
	}

	if e.TxID == empty {
		txID, _ := ctx.Value(CtxKeyTxID).(string)
		if txID == empty {
			txID = b.idgen()
			ctx = context.WithValue(ctx, CtxKeyTxID, txID)
		}
		e.TxID = txID
	}
	if e.ID == empty {
		e.ID = b.idgen()
	}
	if e.ScheduledAt.IsZero() {
		e.ScheduledAt = time.Now()
	} else {
		b.cache.Set(e.ID, e, e.ScheduledAt)

		return nil
	}

	return b.handleEvent(ctx, e)
}

func (b *Bus) handleEvent(ctx context.Context, e Event) error {
	b.mutex.RLock()
	handlers, ok := b.topics[e.Topic]
	b.mutex.RUnlock()

	if !ok {
		return fmt.Errorf("bus: topic(%s) not found", e.Topic)
	}

	for _, h := range handlers {
		go func(h Handler) {
			b.query <- eventAction{
				event:   e,
				handler: h,
				ctx:     ctx,
			}
		}(h)
	}

	return nil
}

// Topics lists the all registered topics
func (b *Bus) Topics() []string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	topics, index := make([]string, len(b.topics)), 0

	for topic := range b.topics {
		topics[index] = topic
		index++
	}
	return topics
}

// RegisterTopics registers topics and fullfills handlers
func (b *Bus) RegisterTopics(topics ...string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, n := range topics {
		b.registerTopic(n)
	}
}

// DeregisterTopics deletes topic
func (b *Bus) DeregisterTopics(topics ...string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, n := range topics {
		b.deregisterTopic(n)
	}
}

// TopicHandlerKeys returns all handlers for the topic
func (b *Bus) TopicHandlerKeys(topic string) []string {
	b.mutex.RLock()
	handlers := b.topics[topic]
	b.mutex.RUnlock()

	keys := make([]string, len(handlers))

	for i, h := range handlers {
		keys[i] = h.key
	}

	return keys
}

// HandlerKeys returns list of registered handler keys
func (b *Bus) HandlerKeys() []string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	keys, index := make([]string, len(b.handlers)), 0

	for k := range b.handlers {
		keys[index] = k
		index++
	}
	return keys
}

// HandlerTopicSubscriptions returns all topic subscriptions of the handler
func (b *Bus) HandlerTopicSubscriptions(handlerKey string) []string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.handlerTopicSubscriptions(handlerKey)
}

// RegisterHandler re/register the handler to the registry
func (b *Bus) RegisterHandler(key string, h Handler) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	h.key = key
	b.registerHandler(h)
}

// DeregisterHandler deletes handler from the registry
func (b *Bus) DeregisterHandler(key string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.deregisterHandler(key)
}

// Generate is an implementation of IDGenerator for bus.Next fn type
func (n Next) Generate() string {
	return n()
}

func (b *Bus) registerHandler(h Handler) {
	b.deregisterHandler(h.key)
	b.handlers[h.key] = h
	for _, t := range b.handlerTopicSubscriptions(h.key) {
		b.registerTopicHandler(t, h)
	}
}

func (b *Bus) deregisterHandler(handlerKey string) {
	if _, ok := b.handlers[handlerKey]; ok {
		for _, t := range b.handlerTopicSubscriptions(handlerKey) {
			b.deregisterTopicHandler(t, handlerKey)
		}
		delete(b.handlers, handlerKey)
	}
}

func (b *Bus) registerTopicHandler(topic string, h Handler) {
	b.topics[topic] = append(b.topics[topic], h)
}

func (b *Bus) deregisterTopicHandler(topic, handlerKey string) {
	l := len(b.topics[topic])
	for i, h := range b.topics[topic] {
		if h.key == handlerKey {
			b.topics[topic][i] = b.topics[topic][l-1]
			b.topics[topic] = b.topics[topic][:l-1]
			break
		}
	}
}

func (b *Bus) registerTopic(topic string) {
	if _, ok := b.topics[topic]; ok {
		return
	}

	b.topics[topic] = b.buildHandlers(topic)
}

func (b *Bus) deregisterTopic(topic string) {
	delete(b.topics, topic)
}

func (b *Bus) buildHandlers(topic string) []Handler {
	handlers := make([]Handler, 0)
	for _, h := range b.handlers {
		if matched, _ := regexp.MatchString(h.Matcher, topic); matched {
			handlers = append(handlers, h)
		}
	}
	return handlers
}

func (b *Bus) handlerTopicSubscriptions(handlerKey string) []string {
	var subscriptions []string
	h, ok := b.handlers[handlerKey]
	if !ok {
		return subscriptions
	}

	for topic := range b.topics {
		if matched, _ := regexp.MatchString(h.Matcher, topic); matched {
			subscriptions = append(subscriptions, topic)
		}
	}
	return subscriptions
}
