// Message state and the per-queue ready heap.
package queue

import (
	"container/heap"
	"time"
)

// state tracks where a message is in its lifecycle.
type state int

const (
	stateReady   state = iota // deliverable now, sitting in the ready heap
	stateDelayed              // waiting for its not-before time
	stateLeased               // delivered, invisible until Deadline
)

// message is the engine's internal record of one enqueued message.
type message struct {
	ID        string
	Seq       uint64 // global send order; FIFO position, survives redelivery
	Body      []byte
	SentAt    time.Time
	NotBefore time.Time // zero unless the send had a delay
	state     state

	// Lease fields, meaningful only while state == stateLeased.
	Receipt  string
	Deadline time.Time
	Receives int // times this message has been delivered
}

// readyHeap orders deliverable messages by Seq so redelivered messages keep
// their original FIFO position instead of jumping to the back.
type readyHeap []*message

func (h readyHeap) Len() int           { return len(h) }
func (h readyHeap) Less(i, j int) bool { return h[i].Seq < h[j].Seq }
func (h readyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x any)        { *h = append(*h, x.(*message)) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return m
}

// queueState is one queue's live state inside the engine.
type queueState struct {
	name string
	cfg  Config

	byID    map[string]*message // every live message in this queue
	ready   readyHeap           // state == stateReady
	pending map[string]*message // stateDelayed + stateLeased, swept by time

	// wake is closed and replaced whenever a message may have become
	// deliverable; long-poll receivers wait on the current channel.
	wake chan struct{}

	// Monotonic counters for stats; not persisted, reset on restart.
	totalSent  int64
	totalAcked int64
	totalDead  int64
}

func newQueueState(name string, cfg Config) *queueState {
	return &queueState{
		name:    name,
		cfg:     cfg,
		byID:    make(map[string]*message),
		pending: make(map[string]*message),
		wake:    make(chan struct{}),
	}
}

// broadcast wakes every long-poll waiter on this queue.
func (q *queueState) broadcast() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// pushReady moves m into the ready heap.
func (q *queueState) pushReady(m *message) {
	m.state = stateReady
	m.Receipt = ""
	m.Deadline = time.Time{}
	heap.Push(&q.ready, m)
}

// counts returns (ready, delayed, leased) message counts.
func (q *queueState) counts() (ready, delayed, leased int) {
	ready = q.ready.Len()
	for _, m := range q.pending {
		if m.state == stateDelayed {
			delayed++
		} else {
			leased++
		}
	}
	return
}
