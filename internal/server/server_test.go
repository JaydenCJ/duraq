// HTTP API tests, run fully in-process against the handler (no sockets):
// status codes, error envelope, the full produce/consume/ack lifecycle,
// delays, redrive, and body encoding on the wire.
package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JaydenCJ/duraq/internal/queue"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
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

func newTestServer(t *testing.T) (*Server, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	return New(queue.New(nil, clk.Now)), clk
}

// do runs one request through the handler and returns the recorder.
func do(s *Server, method, target, body string) *httptest.ResponseRecorder {
	var r *httptest.ResponseRecorder = httptest.NewRecorder()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	s.ServeHTTP(r, req)
	return r
}

// decode unmarshals a JSON response body into a map for assertions.
func decode(t *testing.T, r *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, r.Body.String())
	}
	return m
}

// errCode extracts the error envelope's code field.
func errCode(t *testing.T, r *httptest.ResponseRecorder) string {
	t.Helper()
	m := decode(t, r)
	e, ok := m["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error envelope in %s", r.Body.String())
	}
	code, _ := e["code"].(string)
	return code
}

// firstMessage pulls messages[0] from a receive response.
func firstMessage(t *testing.T, r *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := decode(t, r)
	msgs, ok := m["messages"].([]any)
	if !ok || len(msgs) == 0 {
		t.Fatalf("no messages in %s", r.Body.String())
	}
	return msgs[0].(map[string]any)
}

// --- meta -------------------------------------------------------------------

func TestHealthz(t *testing.T) {
	s, _ := newTestServer(t)
	r := do(s, "GET", "/healthz", "")
	if r.Code != 200 || decode(t, r)["ok"] != true {
		t.Fatalf("healthz: %d %s", r.Code, r.Body.String())
	}
}

func TestVersionEndpoint(t *testing.T) {
	s, _ := newTestServer(t)
	r := do(s, "GET", "/version", "")
	if r.Code != 200 || decode(t, r)["version"] != "0.1.0" {
		t.Fatalf("version: %d %s", r.Code, r.Body.String())
	}
}

// --- queue lifecycle ---------------------------------------------------------

func TestCreateQueueReturns201ThenUpdate200(t *testing.T) {
	s, _ := newTestServer(t)
	if r := do(s, "PUT", "/q/jobs", ""); r.Code != 201 {
		t.Fatalf("create: %d %s", r.Code, r.Body.String())
	}
	if r := do(s, "PUT", "/q/jobs", `{"visibility_timeout":"5s"}`); r.Code != 200 {
		t.Fatalf("update: %d", r.Code)
	}
	r := do(s, "GET", "/q/jobs", "")
	cfg := decode(t, r)["config"].(map[string]any)
	if cfg["visibility_timeout"] != "5s" {
		t.Fatalf("config not applied: %s", r.Body.String())
	}
}

func TestCreateQueueBadNameIs400(t *testing.T) {
	s, _ := newTestServer(t)
	r := do(s, "PUT", "/q/bad%20name", "")
	if r.Code != 400 || errCode(t, r) != "bad_queue_name" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestCreateQueueBadConfigIs400(t *testing.T) {
	s, _ := newTestServer(t)
	if r := do(s, "PUT", "/q/jobs", `{"visibility_timeout": {}}`); r.Code != 400 {
		t.Fatalf("got %d", r.Code)
	}
	if r := do(s, "PUT", "/q/jobs", `not json`); r.Code != 400 {
		t.Fatalf("got %d", r.Code)
	}
	if r := do(s, "PUT", "/q/jobs", `{"max_receives": -1}`); r.Code != 400 {
		t.Fatalf("got %d", r.Code)
	}
}

func TestListQueuesSorted(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/zeta", "")
	do(s, "PUT", "/q/alpha", "")
	r := do(s, "GET", "/q", "")
	qs := decode(t, r)["queues"].([]any)
	if len(qs) != 2 {
		t.Fatalf("want 2 queues: %s", r.Body.String())
	}
	if qs[0].(map[string]any)["name"] != "alpha" {
		t.Fatalf("not sorted: %s", r.Body.String())
	}
}

func TestStatsUnknownQueueIs404(t *testing.T) {
	s, _ := newTestServer(t)
	r := do(s, "GET", "/q/ghost", "")
	if r.Code != 404 || errCode(t, r) != "queue_not_found" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestDeleteQueue(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	if r := do(s, "DELETE", "/q/jobs", ""); r.Code != 204 {
		t.Fatalf("delete: %d", r.Code)
	}
	if r := do(s, "GET", "/q/jobs", ""); r.Code != 404 {
		t.Fatalf("after delete: %d", r.Code)
	}
}

// --- produce / consume ---------------------------------------------------------

func TestSendReceiveAckLifecycle(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")

	r := do(s, "POST", "/q/jobs/messages", `{"task":"resize","width":128}`)
	if r.Code != 201 {
		t.Fatalf("send: %d %s", r.Code, r.Body.String())
	}
	id := decode(t, r)["id"].(string)

	r = do(s, "GET", "/q/jobs/messages", "")
	if r.Code != 200 {
		t.Fatalf("receive: %d", r.Code)
	}
	msg := firstMessage(t, r)
	if msg["id"] != id || msg["body"] != `{"task":"resize","width":128}` {
		t.Fatalf("delivery: %s", r.Body.String())
	}
	if msg["receives"] != float64(1) {
		t.Fatalf("receives: %v", msg["receives"])
	}

	receipt := msg["receipt"].(string)
	if r := do(s, "DELETE", "/q/jobs/messages/"+id+"?receipt="+receipt, ""); r.Code != 204 {
		t.Fatalf("ack: %d %s", r.Code, r.Body.String())
	}
	if r := do(s, "GET", "/q/jobs/messages", ""); r.Code != 204 {
		t.Fatalf("queue should be empty after ack: %d", r.Code)
	}
}

func TestSendToUnknownQueueIs404(t *testing.T) {
	s, _ := newTestServer(t)
	r := do(s, "POST", "/q/ghost/messages", "x")
	if r.Code != 404 || errCode(t, r) != "queue_not_found" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestDelayedSendIsInvisibleUntilDue(t *testing.T) {
	s, clk := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	do(s, "POST", "/q/jobs/messages?delay=10s", "later")
	if r := do(s, "GET", "/q/jobs/messages", ""); r.Code != 204 {
		t.Fatalf("delayed message leaked: %d", r.Code)
	}
	clk.Advance(11 * time.Second)
	r := do(s, "GET", "/q/jobs/messages", "")
	if r.Code != 200 || firstMessage(t, r)["body"] != "later" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestReceiveParamValidation(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	cases := []string{
		"/q/jobs/messages?max=0",
		"/q/jobs/messages?max=101",
		"/q/jobs/messages?max=lots",
		"/q/jobs/messages?wait=61", // above the 60s cap
		"/q/jobs/messages?wait=-1",
		"/q/jobs/messages?visibility=nope",
	}
	for _, target := range cases {
		if r := do(s, "GET", target, ""); r.Code != 400 {
			t.Fatalf("%s: got %d, want 400", target, r.Code)
		}
	}
}

func TestReceiveBatchViaMaxParam(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	for _, b := range []string{"a", "b", "c"} {
		do(s, "POST", "/q/jobs/messages", b)
	}
	r := do(s, "GET", "/q/jobs/messages?max=2", "")
	if len(decode(t, r)["messages"].([]any)) != 2 {
		t.Fatalf("batch: %s", r.Body.String())
	}
}

func TestBinaryBodyComesBackAsBase64(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	do(s, "POST", "/q/jobs/messages", "\xff\xfe\x00")
	r := do(s, "GET", "/q/jobs/messages", "")
	msg := firstMessage(t, r)
	if msg["body_b64"] != "//4A" {
		t.Fatalf("binary body wire format: %s", r.Body.String())
	}
	if _, hasPlain := msg["body"]; hasPlain {
		t.Fatalf("binary body must not claim to be a string: %s", r.Body.String())
	}
}

// --- lease operations ---------------------------------------------------------

func TestAckWithoutReceiptIs400(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	r := do(s, "DELETE", "/q/jobs/messages/0abc", "")
	if r.Code != 400 || errCode(t, r) != "missing_receipt" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestAckWithWrongReceiptIs409(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	do(s, "POST", "/q/jobs/messages", "x")
	r := do(s, "GET", "/q/jobs/messages", "")
	id := firstMessage(t, r)["id"].(string)
	r = do(s, "DELETE", "/q/jobs/messages/"+id+"?receipt=r0000000000009999", "")
	if r.Code != 409 || errCode(t, r) != "lease_lost" {
		t.Fatalf("got %d %s", r.Code, r.Body.String())
	}
}

func TestStaleReceiptAfterExpiryIs409(t *testing.T) {
	s, clk := newTestServer(t)
	do(s, "PUT", "/q/jobs", `{"visibility_timeout":"5s"}`)
	do(s, "POST", "/q/jobs/messages", "x")
	r := do(s, "GET", "/q/jobs/messages", "")
	msg := firstMessage(t, r)
	clk.Advance(6 * time.Second)
	r = do(s, "DELETE", "/q/jobs/messages/"+msg["id"].(string)+"?receipt="+msg["receipt"].(string), "")
	if r.Code != 409 {
		t.Fatalf("stale ack: %d %s", r.Code, r.Body.String())
	}
}

func TestNackRedelivers(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	do(s, "POST", "/q/jobs/messages", "retry me")
	r := do(s, "GET", "/q/jobs/messages", "")
	msg := firstMessage(t, r)
	url := "/q/jobs/messages/" + msg["id"].(string) + "/nack?receipt=" + msg["receipt"].(string)
	if r := do(s, "POST", url, ""); r.Code != 204 {
		t.Fatalf("nack: %d %s", r.Code, r.Body.String())
	}
	r = do(s, "GET", "/q/jobs/messages", "")
	if firstMessage(t, r)["receives"] != float64(2) {
		t.Fatalf("redelivery: %s", r.Body.String())
	}
}

func TestExtendLease(t *testing.T) {
	s, clk := newTestServer(t)
	do(s, "PUT", "/q/jobs", `{"visibility_timeout":"10s"}`)
	do(s, "POST", "/q/jobs/messages", "slow job")
	r := do(s, "GET", "/q/jobs/messages", "")
	msg := firstMessage(t, r)
	url := "/q/jobs/messages/" + msg["id"].(string) + "/extend?receipt=" + msg["receipt"].(string) + "&visibility=1m"
	if r := do(s, "POST", url, ""); r.Code != 204 {
		t.Fatalf("extend: %d %s", r.Code, r.Body.String())
	}
	clk.Advance(30 * time.Second) // past original 10s, inside the 1m extension
	if r := do(s, "GET", "/q/jobs/messages", ""); r.Code != 204 {
		t.Fatalf("extended message leaked: %d", r.Code)
	}
}

// --- dead-letter and redrive ---------------------------------------------------

func TestDeadLetterFlowOverHTTP(t *testing.T) {
	s, clk := newTestServer(t)
	do(s, "PUT", "/q/jobs.dlq", "")
	do(s, "PUT", "/q/jobs", `{"visibility_timeout":"5s","max_receives":1,"dead_letter":"jobs.dlq"}`)
	do(s, "POST", "/q/jobs/messages", "poison")

	do(s, "GET", "/q/jobs/messages", "") // receive 1: exhausts max_receives
	clk.Advance(6 * time.Second)

	if r := do(s, "GET", "/q/jobs/messages", ""); r.Code != 204 {
		t.Fatalf("poison still in main queue: %d", r.Code)
	}
	r := do(s, "GET", "/q/jobs.dlq/messages", "")
	if r.Code != 200 || firstMessage(t, r)["body"] != "poison" {
		t.Fatalf("DLQ: %d %s", r.Code, r.Body.String())
	}
	r = do(s, "GET", "/q/jobs", "")
	if decode(t, r)["total_dead_lettered"] != float64(1) {
		t.Fatalf("stats: %s", r.Body.String())
	}
}

func TestRedriveEndpoint(t *testing.T) {
	s, _ := newTestServer(t)
	do(s, "PUT", "/q/jobs", "")
	do(s, "PUT", "/q/jobs.dlq", "")
	do(s, "POST", "/q/jobs.dlq/messages", "revive me")
	r := do(s, "POST", "/q/jobs.dlq/redrive?to=jobs", "")
	if r.Code != 200 || decode(t, r)["moved"] != float64(1) {
		t.Fatalf("redrive: %d %s", r.Code, r.Body.String())
	}
	r = do(s, "GET", "/q/jobs/messages", "")
	if firstMessage(t, r)["body"] != "revive me" {
		t.Fatalf("moved message: %s", r.Body.String())
	}
}
