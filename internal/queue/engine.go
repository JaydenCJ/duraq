// The queue engine: an in-memory state machine whose every transition is
// committed to the write-ahead log before it becomes observable.
//
// Concurrency model: one mutex around all state, a per-queue broadcast
// channel for long-poll wakeups, and a time-based Sweep that the server
// drives from a single goroutine. The engine itself never starts goroutines
// and never reads the wall clock directly — time is injected — so tests are
// fully deterministic.
package queue

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JaydenCJ/duraq/internal/wal"
)

// Typed errors, mapped to HTTP statuses by the server.
var (
	ErrQueueNotFound   = errors.New("queue not found")
	ErrMessageNotFound = errors.New("message not found")
	ErrNotLeased       = errors.New("message is not currently leased")
	ErrWrongReceipt    = errors.New("receipt does not match the current lease")
	ErrBadName         = errors.New("invalid queue name")
)

// Appender is the slice of *wal.Log the engine needs; nil means in-memory
// only (used by tests and the offline stats command).
type Appender interface {
	Append(wal.Record) error
}

// Delivery is one message handed to a consumer, plus the receipt that
// authorizes ack / nack / extend for this lease.
type Delivery struct {
	ID       string
	Receipt  string
	Body     []byte
	Receives int
	SentAt   time.Time
}

// Stats is a point-in-time summary of one queue.
type Stats struct {
	Name       string `json:"name"`
	Config     Config `json:"config"`
	Ready      int    `json:"ready"`
	Delayed    int    `json:"delayed"`
	InFlight   int    `json:"in_flight"`
	TotalSent  int64  `json:"total_sent"`
	TotalAcked int64  `json:"total_acked"`
	TotalDead  int64  `json:"total_dead_lettered"`
}

// Engine owns all queues. Safe for concurrent use.
type Engine struct {
	mu     sync.Mutex
	log    Appender
	now    func() time.Time
	queues map[string]*queueState
	seq    uint64
}

// New builds an engine. log may be nil (no persistence); now must not be nil.
func New(log Appender, now func() time.Time) *Engine {
	return &Engine{log: log, now: now, queues: make(map[string]*queueState)}
}

// Open loads (or initializes) the write-ahead log in dir and returns a fully
// replayed engine. tornTail reports that a partial final record from a crash
// was discarded.
func Open(dir string, syncEach bool, now func() time.Time) (*Engine, *wal.Log, bool, error) {
	log, recs, torn, err := wal.Open(dir, syncEach)
	if err != nil {
		return nil, nil, false, err
	}
	e := New(log, now)
	if err := e.Load(recs); err != nil {
		log.Close()
		return nil, nil, false, err
	}
	return e, log, torn, nil
}

// append commits a record, or is a no-op without a log.
func (e *Engine) append(r wal.Record) error {
	if e.log == nil {
		return nil
	}
	return e.log.Append(r)
}

func (e *Engine) nextID() string {
	e.seq++
	return fmt.Sprintf("%016x", e.seq)
}

func (e *Engine) nextReceipt() string {
	e.seq++
	return fmt.Sprintf("r%016x", e.seq)
}

// bumpSeq advances the ID counter past any identifier seen during replay so
// new IDs never collide with logged ones.
func (e *Engine) bumpSeq(id string) {
	s := strings.TrimPrefix(id, "r")
	if n, err := strconv.ParseUint(s, 16, 64); err == nil && n > e.seq {
		e.seq = n
	}
}

func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// --- queue lifecycle ---------------------------------------------------

// CreateQueue creates name with cfg, or reconfigures it if it exists.
// Returns true when the queue was newly created.
func (e *Engine) CreateQueue(name string, cfg Config) (bool, error) {
	if !ValidName(name) {
		return false, fmt.Errorf("%w: %q", ErrBadName, name)
	}
	if cfg.DeadLetter == name {
		return false, fmt.Errorf("queue %q cannot be its own dead_letter", name)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}
	rec := wal.Record{Op: wal.OpQueueCreate, Queue: name, Config: cfgJSON, TS: ms(e.now())}
	if err := e.append(rec); err != nil {
		return false, err
	}
	q, ok := e.queues[name]
	if ok {
		q.cfg = cfg
		return false, nil
	}
	e.queues[name] = newQueueState(name, cfg)
	return true, nil
}

// DeleteQueue removes name and every message in it, waking any waiters so
// their long-polls fail fast instead of hanging until timeout.
func (e *Engine) DeleteQueue(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	q, ok := e.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	if err := e.append(wal.Record{Op: wal.OpQueueDelete, Queue: name, TS: ms(e.now())}); err != nil {
		return err
	}
	delete(e.queues, name)
	q.broadcast()
	return nil
}

// Stats returns the current counters for one queue.
func (e *Engine) Stats(name string) (Stats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	q, ok := e.queues[name]
	if !ok {
		return Stats{}, ErrQueueNotFound
	}
	e.sweepQueue(q, e.now())
	return statsOf(q), nil
}

// ListStats returns stats for every queue, sorted by name.
func (e *Engine) ListStats() []Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	out := make([]Stats, 0, len(e.queues))
	for _, q := range e.queues {
		e.sweepQueue(q, now)
		out = append(out, statsOf(q))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func statsOf(q *queueState) Stats {
	ready, delayed, leased := q.counts()
	return Stats{
		Name: q.name, Config: q.cfg,
		Ready: ready, Delayed: delayed, InFlight: leased,
		TotalSent: q.totalSent, TotalAcked: q.totalAcked, TotalDead: q.totalDead,
	}
}

// --- producing ----------------------------------------------------------

// Send enqueues body on queue, optionally delayed. The message ID is
// returned once the record is committed to the log.
func (e *Engine) Send(queue string, body []byte, delay time.Duration) (string, error) {
	if delay < 0 || delay > MaxDelay {
		return "", fmt.Errorf("delay must be between 0s and %s", MaxDelay)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, ok := e.queues[queue]
	if !ok {
		return "", ErrQueueNotFound
	}
	now := e.now()
	id := e.nextID()
	m := &message{ID: id, Seq: e.seq, Body: append([]byte(nil), body...), SentAt: now}
	if delay > 0 {
		m.NotBefore = now.Add(delay)
	}
	rec := wal.Record{Op: wal.OpSend, Queue: queue, ID: id, TS: ms(now), NBF: ms(m.NotBefore)}
	rec.SetBody(m.Body)
	if err := e.append(rec); err != nil {
		e.seq-- // the send never happened; reuse the ID
		return "", err
	}
	q.byID[id] = m
	q.totalSent++
	if delay > 0 {
		m.state = stateDelayed
		q.pending[id] = m
	} else {
		q.pushReady(m)
		q.broadcast()
	}
	return id, nil
}

// --- consuming ----------------------------------------------------------

// Receive leases up to max messages from queue. With wait > 0 it long-polls:
// the call blocks until a message is deliverable, the wait elapses (returns
// an empty slice), or ctx is canceled. visibility 0 means the queue default.
func (e *Engine) Receive(ctx context.Context, queue string, max int, wait, visibility time.Duration) ([]Delivery, error) {
	if max < 1 {
		max = 1
	}
	if visibility < 0 || visibility > MaxVisibility {
		return nil, fmt.Errorf("visibility must be between 0s and %s", MaxVisibility)
	}
	var timerC <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timerC = t.C
	}
	for {
		e.mu.Lock()
		q, ok := e.queues[queue]
		if !ok {
			e.mu.Unlock()
			return nil, ErrQueueNotFound
		}
		now := e.now()
		e.sweepQueue(q, now)
		ds, err := e.lease(q, max, visibility, now)
		wake := q.wake
		e.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if len(ds) > 0 || wait <= 0 {
			return ds, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timerC:
			return nil, nil // long-poll elapsed: empty, not an error
		case <-wake:
			// Something may be deliverable now; loop and retry.
		}
	}
}

// lease pops up to max ready messages, marks them invisible, and logs each
// lease. Caller holds the lock.
func (e *Engine) lease(q *queueState, max int, visibility time.Duration, now time.Time) ([]Delivery, error) {
	if visibility == 0 {
		visibility = q.cfg.Visibility
	}
	var ds []Delivery
	for len(ds) < max && q.ready.Len() > 0 {
		m := heap.Pop(&q.ready).(*message)
		m.state = stateLeased
		m.Receipt = e.nextReceipt()
		m.Deadline = now.Add(visibility)
		m.Receives++
		rec := wal.Record{
			Op: wal.OpReceive, Queue: q.name, ID: m.ID, TS: ms(now),
			Receipt: m.Receipt, Deadline: ms(m.Deadline), Count: m.Receives,
		}
		if err := e.append(rec); err != nil {
			// Roll the lease back so the message is not lost in limbo.
			m.Receives--
			q.pushReady(m)
			return ds, err
		}
		q.pending[m.ID] = m
		ds = append(ds, Delivery{
			ID: m.ID, Receipt: m.Receipt,
			Body: append([]byte(nil), m.Body...), Receives: m.Receives, SentAt: m.SentAt,
		})
	}
	return ds, nil
}

// lookupLease resolves queue/id and checks the receipt. Caller holds the lock.
func (e *Engine) lookupLease(queue, id, receipt string) (*queueState, *message, error) {
	q, ok := e.queues[queue]
	if !ok {
		return nil, nil, ErrQueueNotFound
	}
	e.sweepQueue(q, e.now())
	m, ok := q.byID[id]
	if !ok {
		return nil, nil, ErrMessageNotFound
	}
	if m.state != stateLeased {
		return nil, nil, ErrNotLeased
	}
	if m.Receipt != receipt {
		return nil, nil, ErrWrongReceipt
	}
	return q, m, nil
}

// Ack deletes a leased message: processing succeeded.
func (e *Engine) Ack(queue, id, receipt string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	q, m, err := e.lookupLease(queue, id, receipt)
	if err != nil {
		return err
	}
	rec := wal.Record{Op: wal.OpAck, Queue: queue, ID: id, Receipt: receipt, TS: ms(e.now())}
	if err := e.append(rec); err != nil {
		return err
	}
	delete(q.byID, m.ID)
	delete(q.pending, m.ID)
	q.totalAcked++
	return nil
}

// Nack returns a leased message to the queue immediately (visibility = 0).
// A message that has exhausted max_receives is dead-lettered instead.
func (e *Engine) Nack(queue, id, receipt string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	q, m, err := e.lookupLease(queue, id, receipt)
	if err != nil {
		return err
	}
	if q.cfg.MaxReceives > 0 && m.Receives >= q.cfg.MaxReceives {
		return e.deadLetter(q, m)
	}
	rec := wal.Record{Op: wal.OpNack, Queue: queue, ID: id, Receipt: receipt, TS: ms(e.now())}
	if err := e.append(rec); err != nil {
		return err
	}
	delete(q.pending, m.ID)
	q.pushReady(m)
	q.broadcast()
	return nil
}

// Extend pushes the lease deadline to now + visibility, so a slow consumer
// can keep a message invisible while it finishes.
func (e *Engine) Extend(queue, id, receipt string, visibility time.Duration) error {
	if visibility <= 0 || visibility > MaxVisibility {
		return fmt.Errorf("visibility must be greater than 0s and at most %s", MaxVisibility)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, m, err := e.lookupLease(queue, id, receipt)
	if err != nil {
		return err
	}
	deadline := e.now().Add(visibility)
	rec := wal.Record{
		Op: wal.OpExtend, Queue: queue, ID: id, Receipt: receipt,
		Deadline: ms(deadline), TS: ms(e.now()),
	}
	if err := e.append(rec); err != nil {
		return err
	}
	m.Deadline = deadline
	return nil
}

// Redrive moves up to max ready messages from one queue to another,
// resetting their receive counts — the recovery path after draining a DLQ.
func (e *Engine) Redrive(from, to string, max int) (int, error) {
	if from == to {
		return 0, fmt.Errorf("cannot redrive a queue into itself")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	src, ok := e.queues[from]
	if !ok {
		return 0, ErrQueueNotFound
	}
	dst, ok := e.queues[to]
	if !ok {
		return 0, fmt.Errorf("%w: target %q", ErrQueueNotFound, to)
	}
	e.sweepQueue(src, e.now())
	moved := 0
	for (max <= 0 || moved < max) && src.ready.Len() > 0 {
		m := heap.Pop(&src.ready).(*message)
		rec := wal.Record{Op: wal.OpMove, Queue: from, ID: m.ID, To: to, TS: ms(e.now())}
		if err := e.append(rec); err != nil {
			src.pushReady(m)
			return moved, err
		}
		delete(src.byID, m.ID)
		e.seq++
		m.Seq = e.seq // arrives at the back of the target queue
		m.Receives = 0
		m.NotBefore = time.Time{}
		dst.byID[m.ID] = m
		dst.pushReady(m)
		moved++
	}
	if moved > 0 {
		dst.broadcast()
	}
	return moved, nil
}

// --- time ---------------------------------------------------------------

// Sweep settles every time-based transition that is due at now: delayed
// messages become ready, expired leases are requeued or dead-lettered. It
// returns the next moment any queue needs sweeping again (zero if none).
// The server calls this from its sweeper goroutine; tests call it directly.
func (e *Engine) Sweep(now time.Time) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	var next time.Time
	for _, q := range e.queues {
		n := e.sweepQueue(q, now)
		if !n.IsZero() && (next.IsZero() || n.Before(next)) {
			next = n
		}
	}
	return next
}

// sweepQueue settles one queue and returns its next due time. Caller holds
// the lock.
func (e *Engine) sweepQueue(q *queueState, now time.Time) time.Time {
	var next time.Time
	woke := false
	for id, m := range q.pending {
		var due time.Time
		switch m.state {
		case stateDelayed:
			due = m.NotBefore
		case stateLeased:
			due = m.Deadline
		}
		if due.After(now) {
			if next.IsZero() || due.Before(next) {
				next = due
			}
			continue
		}
		if m.state == stateLeased && q.cfg.MaxReceives > 0 && m.Receives >= q.cfg.MaxReceives {
			// Poisoned: consumed max_receives times without an ack.
			if err := e.deadLetter(q, m); err != nil {
				// Log write failed; leave the lease in place and retry on
				// the next sweep rather than losing the message.
				continue
			}
		} else {
			delete(q.pending, id)
			q.pushReady(m)
			woke = true
		}
	}
	if woke {
		q.broadcast()
	}
	return next
}

// deadLetter moves m out of q — into q.cfg.DeadLetter if set (creating it
// with defaults if needed), otherwise dropping it. Caller holds the lock.
func (e *Engine) deadLetter(q *queueState, m *message) error {
	target := q.cfg.DeadLetter
	if target != "" {
		if _, ok := e.queues[target]; !ok {
			cfgJSON, _ := json.Marshal(DefaultConfig())
			rec := wal.Record{Op: wal.OpQueueCreate, Queue: target, Config: cfgJSON, TS: ms(e.now())}
			if err := e.append(rec); err != nil {
				return err
			}
			e.queues[target] = newQueueState(target, DefaultConfig())
		}
	}
	rec := wal.Record{Op: wal.OpDead, Queue: q.name, ID: m.ID, To: target, TS: ms(e.now())}
	if err := e.append(rec); err != nil {
		return err
	}
	delete(q.byID, m.ID)
	delete(q.pending, m.ID)
	q.totalDead++
	if target == "" {
		return nil // documented: no dead_letter configured means drop
	}
	dst := e.queues[target]
	e.seq++
	m.Seq = e.seq
	m.NotBefore = time.Time{}
	dst.byID[m.ID] = m
	dst.pushReady(m)
	dst.broadcast()
	return nil
}

// --- persistence --------------------------------------------------------

// Load replays records into a fresh engine. It never writes to the log.
func (e *Engine) Load(recs []wal.Record) error {
	for i, r := range recs {
		if err := e.apply(r); err != nil {
			return fmt.Errorf("replay record %d (%s %s/%s): %w", i+1, r.Op, r.Queue, r.ID, err)
		}
	}
	return nil
}

func (e *Engine) apply(r wal.Record) error {
	switch r.Op {
	case wal.OpQueueCreate:
		cfg, err := ParseConfig(r.Config)
		if err != nil {
			return err
		}
		if q, ok := e.queues[r.Queue]; ok {
			q.cfg = cfg
		} else {
			e.queues[r.Queue] = newQueueState(r.Queue, cfg)
		}
		return nil
	case wal.OpQueueDelete:
		if _, ok := e.queues[r.Queue]; !ok {
			return ErrQueueNotFound
		}
		delete(e.queues, r.Queue)
		return nil
	}

	q, ok := e.queues[r.Queue]
	if !ok {
		return ErrQueueNotFound
	}
	switch r.Op {
	case wal.OpSend:
		body, err := r.GetBody()
		if err != nil {
			return err
		}
		e.bumpSeq(r.ID)
		m := &message{
			ID: r.ID, Seq: seqOf(r.ID), Body: body,
			SentAt: time.UnixMilli(r.TS), Receives: r.Count, state: stateDelayed,
		}
		if r.NBF > 0 {
			m.NotBefore = time.UnixMilli(r.NBF)
		}
		q.byID[r.ID] = m
		q.pending[r.ID] = m
		q.totalSent++
		return nil
	}

	m, ok := q.byID[r.ID]
	if !ok {
		return ErrMessageNotFound
	}
	switch r.Op {
	case wal.OpReceive:
		e.bumpSeq(r.Receipt)
		e.removeReady(q, m)
		m.state = stateLeased
		m.Receipt = r.Receipt
		m.Deadline = time.UnixMilli(r.Deadline)
		m.Receives = r.Count
		q.pending[m.ID] = m
	case wal.OpAck:
		delete(q.byID, m.ID)
		delete(q.pending, m.ID)
		e.removeReady(q, m)
		q.totalAcked++
	case wal.OpNack:
		delete(q.pending, m.ID)
		q.pushReady(m)
	case wal.OpExtend:
		m.Deadline = time.UnixMilli(r.Deadline)
	case wal.OpDead, wal.OpMove:
		delete(q.byID, m.ID)
		delete(q.pending, m.ID)
		e.removeReady(q, m)
		if r.Op == wal.OpDead {
			q.totalDead++
		}
		if r.To == "" {
			return nil // dropped poison message
		}
		dst, ok := e.queues[r.To]
		if !ok {
			return fmt.Errorf("%w: target %q", ErrQueueNotFound, r.To)
		}
		e.seq++
		m.Seq = e.seq
		m.NotBefore = time.Time{}
		if r.Op == wal.OpMove {
			m.Receives = 0
		}
		dst.byID[m.ID] = m
		dst.pushReady(m)
	}
	return nil
}

// removeReady drops m from q's ready heap if present (replay can see acks
// for messages that Load left in the ready state).
func (e *Engine) removeReady(q *queueState, m *message) {
	for i, r := range q.ready {
		if r == m {
			heap.Remove(&q.ready, i)
			return
		}
	}
}

func seqOf(id string) uint64 {
	n, _ := strconv.ParseUint(id, 16, 64)
	return n
}

// Snapshot serializes live state as the minimal record sequence that Load
// reconstructs it from — the input to log compaction. Queues are sorted by
// name and messages by sequence so snapshots are deterministic.
func (e *Engine) Snapshot() []wal.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	names := make([]string, 0, len(e.queues))
	for n := range e.queues {
		names = append(names, n)
	}
	sort.Strings(names)
	var recs []wal.Record
	for _, n := range names {
		q := e.queues[n]
		cfgJSON, _ := json.Marshal(q.cfg)
		recs = append(recs, wal.Record{Op: wal.OpQueueCreate, Queue: n, Config: cfgJSON})
		msgs := make([]*message, 0, len(q.byID))
		for _, m := range q.byID {
			msgs = append(msgs, m)
		}
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
		for _, m := range msgs {
			send := wal.Record{
				Op: wal.OpSend, Queue: n, ID: m.ID,
				TS: ms(m.SentAt), NBF: ms(m.NotBefore),
			}
			send.SetBody(m.Body)
			if m.state != stateLeased {
				send.Count = m.Receives
			}
			recs = append(recs, send)
			if m.state == stateLeased {
				recs = append(recs, wal.Record{
					Op: wal.OpReceive, Queue: n, ID: m.ID,
					Receipt: m.Receipt, Deadline: ms(m.Deadline), Count: m.Receives,
				})
			}
		}
	}
	return recs
}
