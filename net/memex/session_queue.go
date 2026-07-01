package memex

import (
	"sync"
)

// eventQueue is a thread-safe, select-compatible, unbounded FIFO queue
// for sessionEvents. It guarantees that pushes are non-blocking and never
// drop events, preventing deadlocks and lost wants/cancels.
type eventQueue struct {
	mu     sync.Mutex
	ch     chan struct{}
	list   []sessionEvent
	closed bool
}

func newEventQueue() *eventQueue {
	return &eventQueue{
		ch: make(chan struct{}, 1),
	}
}

// Push appends an event to the queue and non-blockingly signals the channel.
func (q *eventQueue) Push(ev sessionEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.list = append(q.list, ev)
	select {
	case q.ch <- struct{}{}:
	default:
	}
}

// PopAll returns all pending events in the queue and clears it.
func (q *eventQueue) PopAll() []sessionEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	res := q.list
	q.list = nil
	return res
}

// Close closes the signaling channel and prevents further pushes.
func (q *eventQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}
