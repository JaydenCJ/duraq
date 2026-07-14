// Engine tests: lifecycle, FIFO, visibility timeouts, delays, dead-letter
// queues, redrive, long-poll wakeups, and WAL persistence across restarts.
// Time is a fake clock advanced by hand, so nothing here sleeps.
package queue

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JaydenCJ/duraq/internal/wal"
)

// fakeClock is a hand-cranked clock: tests control every tick.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newEngine returns a memory-only engine (no WAL) plus its clock.
func newEngine(t *testing.T) (*Engine, *fakeClock) {
	t.Helper()
	clk := newClock()
	return New(nil, clk.Now), clk
}

// mkQueue creates a queue with the given config, failing the test on error.
func mkQueue(t *testing.T, e *Engine, name string, cfg Config) {
	t.Helper()
	if cfg.Visibility == 0 {
		cfg.Visibility = DefaultVisibility
	}
	if _, err := e.CreateQueue(name, cfg); err != nil {
		t.Fatalf("CreateQueue(%s): %v", name, err)
	}
}

// recvOne receives exactly one message without waiting.
func recvOne(t *testing.T, e *Engine, q string) Delivery {
	t.Helper()
	ds, err := e.Receive(context.Background(), q, 1, 0, 0)
	if err != nil {
		t.Fatalf("Receive(%s): %v", q, err)
	}
	if len(ds) != 1 {
		t.Fatalf("Receive(%s) returned %d messages, want 1", q, len(ds))
	}
	return ds[0]
}

// recvNone asserts an immediate receive comes back empty.
func recvNone(t *testing.T, e *Engine, q string) {
	t.Helper()
	ds, err := e.Receive(context.Background(), q, 1, 0, 0)
	if err != nil {
		t.Fatalf("Receive(%s): %v", q, err)
	}
	if len(ds) != 0 {
		t.Fatalf("queue %s should be empty, got %q", q, ds[0].Body)
	}
}

// --- lifecycle ------------------------------------------------------------

func TestCreateQueueReturnsCreated(t *testing.T) {
	e, _ := newEngine(t)
	created, err := e.CreateQueue("jobs", DefaultConfig())
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	created, err = e.CreateQueue("jobs", DefaultConfig())
	if err != nil || created {
		t.Fatalf("second create should be an update: created=%v err=%v", created, err)
	}
}

func TestCreateQueueReconfigures(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	mkQueue(t, e, "jobs", Config{Visibility: 5 * time.Second, MaxReceives: 3})
	s, err := e.Stats("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if s.Config.Visibility != 5*time.Second || s.Config.MaxReceives != 3 {
		t.Fatalf("config not updated: %+v", s.Config)
	}
}

func TestCreateQueueRejectsBadNames(t *testing.T) {
	e, _ := newEngine(t)
	for _, name := range []string{"", "a b", "sla/sh", ".dotfirst", "-dashfirst", string(make([]byte, 200))} {
		if _, err := e.CreateQueue(name, DefaultConfig()); !errors.Is(err, ErrBadName) {
			t.Fatalf("CreateQueue(%q) = %v, want ErrBadName", name, err)
		}
	}
}

func TestCreateQueueRejectsSelfDeadLetter(t *testing.T) {
	e, _ := newEngine(t)
	if _, err := e.CreateQueue("jobs", Config{Visibility: time.Second, DeadLetter: "jobs"}); err == nil {
		t.Fatal("a queue must not be its own dead_letter")
	}
}

func TestDeleteQueueRemovesMessages(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	e.Send("jobs", []byte("x"), 0)
	if err := e.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Stats("jobs"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("stats after delete = %v, want ErrQueueNotFound", err)
	}
}

// --- send / receive ---------------------------------------------------------

func TestSendToUnknownQueueFails(t *testing.T) {
	e, _ := newEngine(t)
	if _, err := e.Send("ghost", []byte("x"), 0); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestSendReceiveRoundTrip(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	id, err := e.Send("jobs", []byte(`{"task":"resize"}`), 0)
	if err != nil || id == "" {
		t.Fatalf("Send: id=%q err=%v", id, err)
	}
	d := recvOne(t, e, "jobs")
	if d.ID != id || string(d.Body) != `{"task":"resize"}` || d.Receives != 1 || d.Receipt == "" {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestBinaryBodySurvives(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	raw := []byte{0x00, 0xff, 0x80, 0x7f}
	e.Send("jobs", raw, 0)
	d := recvOne(t, e, "jobs")
	if !bytes.Equal(d.Body, raw) {
		t.Fatalf("body = %v, want %v", d.Body, raw)
	}
}

func TestFIFOOrder(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	for _, b := range []string{"a", "b", "c"} {
		e.Send("jobs", []byte(b), 0)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := string(recvOne(t, e, "jobs").Body); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestReceiveBatch(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	for i := 0; i < 5; i++ {
		e.Send("jobs", []byte{byte('a' + i)}, 0)
	}
	ds, err := e.Receive(context.Background(), "jobs", 3, 0, 0)
	if err != nil || len(ds) != 3 {
		t.Fatalf("batch: %d msgs, err=%v", len(ds), err)
	}
	if string(ds[0].Body) != "a" || string(ds[2].Body) != "c" {
		t.Fatalf("batch out of order: %q %q %q", ds[0].Body, ds[1].Body, ds[2].Body)
	}
}

// --- visibility ------------------------------------------------------------

func TestLeasedMessageIsInvisible(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	recvOne(t, e, "jobs")
	recvNone(t, e, "jobs") // still leased: nothing to deliver
}

func TestExpiredLeaseRedelivers(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	first := recvOne(t, e, "jobs")
	clk.Advance(31 * time.Second)
	second := recvOne(t, e, "jobs") // Receive sweeps internally
	if second.ID != first.ID {
		t.Fatalf("redelivered a different message: %s vs %s", second.ID, first.ID)
	}
	if second.Receives != 2 {
		t.Fatalf("receives = %d, want 2", second.Receives)
	}
	if second.Receipt == first.Receipt {
		t.Fatal("a new lease must issue a new receipt")
	}
}

func TestRedeliveryKeepsFIFOPosition(t *testing.T) {
	// A message whose lease expires must go back to the FRONT (its original
	// send order), not behind messages sent after it.
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 10 * time.Second})
	e.Send("jobs", []byte("first"), 0)
	recvOne(t, e, "jobs")
	e.Send("jobs", []byte("second"), 0)
	clk.Advance(11 * time.Second)
	if got := string(recvOne(t, e, "jobs").Body); got != "first" {
		t.Fatalf("expired message lost its position: got %q", got)
	}
}

func TestPerRequestVisibilityOverride(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	if _, err := e.Receive(context.Background(), "jobs", 1, 0, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(3 * time.Second) // past the 2s override, well inside the 30s default
	if d := recvOne(t, e, "jobs"); d.Receives != 2 {
		t.Fatalf("override ignored: receives=%d", d.Receives)
	}
}

func TestSweepReturnsNextDeadline(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	recvOne(t, e, "jobs")
	next := e.Sweep(clk.Now())
	want := clk.Now().Add(30 * time.Second)
	if !next.Equal(want) {
		t.Fatalf("next sweep = %v, want %v", next, want)
	}
	if !e.Sweep(clk.Now().Add(time.Hour)).IsZero() {
		t.Fatal("nothing pending after expiry sweep dead-letters/requeues")
	}
}

// --- ack / nack / extend -----------------------------------------------------

func TestAckDeletesMessage(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 5 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	if err := e.Ack("jobs", d.ID, d.Receipt); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	recvNone(t, e, "jobs") // acked messages never come back
	s, _ := e.Stats("jobs")
	if s.TotalAcked != 1 {
		t.Fatalf("TotalAcked = %d", s.TotalAcked)
	}
}

func TestAckWithWrongReceiptFails(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	if err := e.Ack("jobs", d.ID, "r0000000000009999"); !errors.Is(err, ErrWrongReceipt) {
		t.Fatalf("got %v", err)
	}
}

func TestAckAfterLeaseExpiryFails(t *testing.T) {
	// The classic race: worker finishes late, lease already expired and the
	// message went back to ready. The stale receipt must be rejected.
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 5 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	clk.Advance(6 * time.Second)
	if err := e.Ack("jobs", d.ID, d.Receipt); !errors.Is(err, ErrNotLeased) {
		t.Fatalf("stale ack = %v, want ErrNotLeased", err)
	}
}

func TestNackReturnsMessageImmediately(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 30 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	if err := e.Nack("jobs", d.ID, d.Receipt); err != nil {
		t.Fatal(err)
	}
	if d2 := recvOne(t, e, "jobs"); d2.ID != d.ID || d2.Receives != 2 {
		t.Fatalf("nacked message not redelivered: %+v", d2)
	}
}

func TestExtendKeepsMessageInvisible(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: 10 * time.Second})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	clk.Advance(8 * time.Second)
	if err := e.Extend("jobs", d.ID, d.Receipt, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second) // 13s total: past original deadline, inside extension
	recvNone(t, e, "jobs")
	if err := e.Ack("jobs", d.ID, d.Receipt); err != nil {
		t.Fatalf("ack inside extended lease: %v", err)
	}
}

// --- delay ------------------------------------------------------------------

func TestDelayedMessageIsNotDeliveredEarly(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	e.Send("jobs", []byte("later"), 10*time.Second)
	recvNone(t, e, "jobs")
	clk.Advance(11 * time.Second)
	if got := string(recvOne(t, e, "jobs").Body); got != "later" {
		t.Fatalf("got %q", got)
	}
}

func TestDelayBounds(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	if _, err := e.Send("jobs", []byte("x"), -time.Second); err == nil {
		t.Fatal("negative delay must fail")
	}
	if _, err := e.Send("jobs", []byte("x"), MaxDelay+time.Second); err == nil {
		t.Fatal("over-limit delay must fail")
	}
}

// --- dead-letter --------------------------------------------------------------

func TestPoisonMessageMovesToDeadLetterQueue(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs.dlq", Config{})
	mkQueue(t, e, "jobs", Config{Visibility: 5 * time.Second, MaxReceives: 2, DeadLetter: "jobs.dlq"})
	e.Send("jobs", []byte("poison"), 0)

	recvOne(t, e, "jobs") // receive 1
	clk.Advance(6 * time.Second)
	recvOne(t, e, "jobs") // receive 2 — the last allowed
	clk.Advance(6 * time.Second)

	recvNone(t, e, "jobs") // gone from the main queue
	d := recvOne(t, e, "jobs.dlq")
	if string(d.Body) != "poison" {
		t.Fatalf("DLQ body = %q", d.Body)
	}
	s, _ := e.Stats("jobs")
	if s.TotalDead != 1 {
		t.Fatalf("TotalDead = %d", s.TotalDead)
	}
}

func TestDeadLetterQueueAutoCreated(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: time.Second, MaxReceives: 1, DeadLetter: "graveyard"})
	e.Send("jobs", []byte("x"), 0)
	recvOne(t, e, "jobs")
	clk.Advance(2 * time.Second)
	e.Sweep(clk.Now())
	if _, err := e.Stats("graveyard"); err != nil {
		t.Fatalf("DLQ should exist after dead-lettering: %v", err)
	}
	recvOne(t, e, "graveyard")
}

func TestNackOnExhaustedMessageDeadLetters(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs.dlq", Config{})
	mkQueue(t, e, "jobs", Config{MaxReceives: 1, DeadLetter: "jobs.dlq"})
	e.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e, "jobs")
	if err := e.Nack("jobs", d.ID, d.Receipt); err != nil {
		t.Fatal(err)
	}
	recvNone(t, e, "jobs")
	recvOne(t, e, "jobs.dlq")
}

func TestPoisonDroppedWithoutDeadLetter(t *testing.T) {
	// Documented behavior: max_receives with no dead_letter drops the message.
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs", Config{Visibility: time.Second, MaxReceives: 1})
	e.Send("jobs", []byte("x"), 0)
	recvOne(t, e, "jobs")
	clk.Advance(2 * time.Second)
	recvNone(t, e, "jobs")
	s, _ := e.Stats("jobs")
	if s.TotalDead != 1 || s.Ready != 0 || s.InFlight != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// --- redrive --------------------------------------------------------------

func TestRedriveMovesMessagesAndResetsReceives(t *testing.T) {
	e, clk := newEngine(t)
	mkQueue(t, e, "jobs.dlq", Config{})
	mkQueue(t, e, "jobs", Config{Visibility: time.Second, MaxReceives: 1, DeadLetter: "jobs.dlq"})
	e.Send("jobs", []byte("x"), 0)
	recvOne(t, e, "jobs")
	clk.Advance(2 * time.Second)
	e.Sweep(clk.Now())

	moved, err := e.Redrive("jobs.dlq", "jobs", 0)
	if err != nil || moved != 1 {
		t.Fatalf("Redrive: moved=%d err=%v", moved, err)
	}
	d := recvOne(t, e, "jobs")
	if d.Receives != 1 {
		t.Fatalf("redriven message should restart at receives=1, got %d", d.Receives)
	}
}

func TestRedriveHonorsMax(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "a", Config{})
	mkQueue(t, e, "b", Config{})
	for i := 0; i < 5; i++ {
		e.Send("a", []byte{byte('0' + i)}, 0)
	}
	moved, err := e.Redrive("a", "b", 2)
	if err != nil || moved != 2 {
		t.Fatalf("moved=%d err=%v", moved, err)
	}
	sa, _ := e.Stats("a")
	sb, _ := e.Stats("b")
	if sa.Ready != 3 || sb.Ready != 2 {
		t.Fatalf("a=%d b=%d", sa.Ready, sb.Ready)
	}
}

func TestRedriveRejectsSelfAndMissingTarget(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "a", Config{})
	if _, err := e.Redrive("a", "a", 0); err == nil {
		t.Fatal("self-redrive must fail")
	}
	if _, err := e.Redrive("a", "ghost", 0); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("got %v", err)
	}
}

// --- long-poll --------------------------------------------------------------

func TestLongPollWokenBySend(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	got := make(chan Delivery, 1)
	errs := make(chan error, 1)
	go func() {
		ds, err := e.Receive(context.Background(), "jobs", 1, 30*time.Second, 0)
		if err != nil || len(ds) != 1 {
			errs <- err
			return
		}
		got <- ds[0]
	}()
	// The send below is what unblocks the poller; the 30s wait never elapses.
	if _, err := e.Send("jobs", []byte("wake"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if string(d.Body) != "wake" {
			t.Fatalf("body = %q", d.Body)
		}
	case err := <-errs:
		t.Fatalf("receive failed: %v", err)
	}
}

func TestLongPollCanceledByContext(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		_, err := e.Receive(ctx, "jobs", 1, 30*time.Second, 0)
		errs <- err
	}()
	cancel()
	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestLongPollUnblockedByQueueDeletion(t *testing.T) {
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	started := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		close(started)
		_, err := e.Receive(context.Background(), "jobs", 1, 30*time.Second, 0)
		errs <- err
	}()
	<-started
	// Deleting the queue broadcasts, so the poller fails fast with 404
	// semantics instead of hanging for the full 30s.
	if err := e.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("got %v", err)
	}
}

// --- persistence --------------------------------------------------------------

// reopen closes nothing (engines are GC'd) and replays dir into a new engine.
func reopen(t *testing.T, dir string, clk *fakeClock) (*Engine, *wal.Log) {
	t.Helper()
	e, log, _, err := Open(dir, true, clk.Now)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { log.Close() })
	return e, log
}

func TestMessagesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{})
	e1.Send("jobs", []byte("persist me"), 0)
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	if got := string(recvOne(t, e2, "jobs").Body); got != "persist me" {
		t.Fatalf("got %q", got)
	}
}

func TestLeaseSurvivesRestart(t *testing.T) {
	// A message received before a crash must stay invisible until its
	// deadline after the restart, and the old receipt must still ack it.
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{Visibility: time.Hour})
	e1.Send("jobs", []byte("x"), 0)
	d := recvOne(t, e1, "jobs")
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	recvNone(t, e2, "jobs")
	if err := e2.Ack("jobs", d.ID, d.Receipt); err != nil {
		t.Fatalf("ack with pre-restart receipt: %v", err)
	}
}

func TestAckedMessagesStayGoneAfterRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{})
	e1.Send("jobs", []byte("done"), 0)
	d := recvOne(t, e1, "jobs")
	e1.Ack("jobs", d.ID, d.Receipt)
	e1.Send("jobs", []byte("open"), 0)
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	if got := string(recvOne(t, e2, "jobs").Body); got != "open" {
		t.Fatalf("got %q", got)
	}
	recvNone(t, e2, "jobs")
}

func TestDelaySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{})
	e1.Send("jobs", []byte("later"), 10*time.Minute)
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	recvNone(t, e2, "jobs")
	clk.Advance(11 * time.Minute)
	if got := string(recvOne(t, e2, "jobs").Body); got != "later" {
		t.Fatalf("got %q", got)
	}
}

func TestNewIDsDoNotCollideAfterRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{})
	id1, _ := e1.Send("jobs", []byte("a"), 0)
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	id2, _ := e2.Send("jobs", []byte("b"), 0)
	if id1 == id2 {
		t.Fatalf("ID collision after restart: %s", id1)
	}
}

func TestCompactionPreservesState(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	e1, log1 := reopen(t, dir, clk)
	mkQueue(t, e1, "jobs", Config{Visibility: time.Hour, MaxReceives: 4, DeadLetter: "jobs.dlq"})
	mkQueue(t, e1, "jobs.dlq", Config{})
	e1.Send("jobs", []byte("ready"), 0)
	e1.Send("jobs", []byte("leased"), 0)
	e1.Send("jobs", []byte("delayed"), 10*time.Minute)
	recvOne(t, e1, "jobs") // leases "ready" (first in FIFO)
	before, _ := e1.Stats("jobs")

	if err := log1.Compact(e1.Snapshot()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	log1.Close()

	e2, _ := reopen(t, dir, clk)
	after, _ := e2.Stats("jobs")
	if after != before {
		t.Fatalf("stats diverge after compaction:\nbefore %+v\nafter  %+v", before, after)
	}
	if after.Config.MaxReceives != 4 || after.Config.DeadLetter != "jobs.dlq" {
		t.Fatalf("config lost: %+v", after.Config)
	}
}

func TestSnapshotIsMinimal(t *testing.T) {
	// 1 queue + 3 sends + 1 recv + 1 ack = 6 log records, but the snapshot
	// only needs the queue and the 2 live messages.
	e, _ := newEngine(t)
	mkQueue(t, e, "jobs", Config{})
	e.Send("jobs", []byte("a"), 0)
	e.Send("jobs", []byte("b"), 0)
	e.Send("jobs", []byte("c"), 0)
	d := recvOne(t, e, "jobs")
	e.Ack("jobs", d.ID, d.Receipt)
	snap := e.Snapshot()
	if len(snap) != 3 { // qcreate + 2 sends
		t.Fatalf("snapshot has %d records, want 3: %+v", len(snap), snap)
	}
}
