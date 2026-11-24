package topic

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"

	"github.com/google/uuid"
)

// Topic 标识订阅者与发布者使用的同一个事件主题。
type Topic string

// Event 保存事件所属的主题和随事件传递的数据。
type Event[T any] struct {
	Topic Topic
	Data  T
}

// Subscriber 表示一个可以从指定主题接收事件的订阅。
type Subscriber[T any] interface {
	// Receive 等待下一条事件。上下文取消后结束等待。
	Receive(context.Context) (Event[T], error)
	// Unsubscribe 释放当前订阅的通道。消费者退出时需要调用它。
	Unsubscribe()
}

type hub[T any] struct {
	t Topic

	mu sync.RWMutex
	m  map[string]chan Event[T]
}

func newHub[T any](t Topic) *hub[T] {
	return &hub[T]{
		t: t,
		m: make(map[string]chan Event[T]),
	}
}

func (h *hub[T]) subscribe() Subscriber[T] {
	n := uuid.NewString()
	c := make(chan Event[T], runtime.GOMAXPROCS(0)*2)

	h.mu.Lock()
	h.m[n] = c
	h.mu.Unlock()

	return &subscriber[T]{h: h, n: n, c: c}
}

func (h *hub[T]) publish(ctx context.Context, data T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for n, c := range h.m {
		select {
		case <-ctx.Done():
			return
		case c <- Event[T]{Topic: h.t, Data: data}:
		default:
			close(c)
			delete(h.m, n)
		}
	}
}

type subscriber[T any] struct {
	h *hub[T]
	n string
	c chan Event[T]
}

func (s *subscriber[T]) Receive(ctx context.Context) (Event[T], error) {
	select {
	case <-ctx.Done():
		return Event[T]{}, ctx.Err()
	case e, ok := <-s.c:
		if !ok {
			return Event[T]{}, errors.New("topic is closed")
		}
		return e, nil
	}
}

func (s *subscriber[T]) Unsubscribe() {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()

	if c, ok := s.h.m[s.n]; ok {
		close(c)
		delete(s.h.m, s.n)
	}
}

type bus struct {
	mu          sync.RWMutex
	typedTopics map[reflect.Type]map[Topic]any
}

func typeOf[T any]() reflect.Type {
	var zero T
	return reflect.TypeOf(zero)
}

func subscribe[T any](b *bus, t Topic) (Subscriber[T], error) {
	typ := typeOf[T]()

	b.mu.Lock()
	defer b.mu.Unlock()

	tm, ok := b.typedTopics[typ]
	if !ok {
		tm = make(map[Topic]any)
		b.typedTopics[typ] = tm
	}

	h, ok := tm[t]
	if !ok {
		nh := newHub[T](t)
		tm[t] = nh
		return nh.subscribe(), nil
	}

	return h.(*hub[T]).subscribe(), nil
}

func publish[T any](b *bus, ctx context.Context, t Topic, data T) error {
	typ := typeOf[T]()

	b.mu.RLock()
	h, ok := b.typedTopics[typ][t]
	b.mu.RUnlock()
	if !ok {
		return nil
	}

	h.(*hub[T]).publish(ctx, data)
	return nil
}

var globalBus = &bus{
	typedTopics: make(map[reflect.Type]map[Topic]any),
}

// Subscribe 在进程共用的事件总线上订阅指定主题，返回该消费者专用的订阅。
func Subscribe[T any](t Topic) (Subscriber[T], error) {
	return subscribe[T](globalBus, t)
}

// Publish 向指定主题发布数据。发布失败时向调用方返回错误。
func Publish[T any](ctx context.Context, t Topic, data T) error {
	return publish[T](globalBus, ctx, t, data)
}
