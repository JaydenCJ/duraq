// Tests for the NDJSON write-ahead log: round-trips, torn-tail recovery,
// corruption refusal, atomic compaction, and body encoding.
package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	log, recs, torn, err := Open(dir, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(recs) != 0 || torn {
		t.Fatalf("fresh dir should be empty, got %d recs torn=%v", len(recs), torn)
	}
	t.Cleanup(func() { log.Close() })
	return log, filepath.Join(dir, FileName)
}

func TestAppendThenReadAllRoundTrips(t *testing.T) {
	log, path := openTemp(t)
	want := []Record{
		{Op: OpQueueCreate, Queue: "jobs", Config: []byte(`{"visibility_timeout":"30s","max_receives":0}`)},
		{Op: OpSend, Queue: "jobs", ID: "0000000000000001", Body: "hello", TS: 1000},
		{Op: OpReceive, Queue: "jobs", ID: "0000000000000001", Receipt: "r0000000000000002", Deadline: 31000, Count: 1},
		{Op: OpAck, Queue: "jobs", ID: "0000000000000001", Receipt: "r0000000000000002"},
	}
	for _, r := range want {
		if err := log.Append(r); err != nil {
			t.Fatalf("Append(%s): %v", r.Op, err)
		}
	}
	got, _, torn, err := ReadAll(path)
	if err != nil || torn {
		t.Fatalf("ReadAll: %v torn=%v", err, torn)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].ID != want[i].ID || got[i].Body != want[i].Body {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestLogIsOneJSONObjectPerLine(t *testing.T) {
	log, path := openTemp(t)
	if err := log.Append(Record{Op: OpQueueCreate, Queue: "q1"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Op: OpSend, Queue: "q1", ID: "01", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), string(data))
	}
	for _, l := range lines {
		if !strings.HasPrefix(l, "{") || !strings.HasSuffix(l, "}") {
			t.Fatalf("line is not a JSON object: %q", l)
		}
	}
}

func TestReadAllMissingFileIsEmpty(t *testing.T) {
	recs, n, torn, err := ReadAll(filepath.Join(t.TempDir(), "nope.ndjson"))
	if err != nil || len(recs) != 0 || n != 0 || torn {
		t.Fatalf("missing file: recs=%d n=%d torn=%v err=%v", len(recs), n, torn, err)
	}
}

func TestTornTailWithoutNewlineIsRecovered(t *testing.T) {
	// Simulates a crash mid-append: the final line has no newline.
	log, path := openTemp(t)
	if err := log.Append(Record{Op: OpQueueCreate, Queue: "q1"}); err != nil {
		t.Fatal(err)
	}
	log.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"op":"send","q":"q1","id":"02","bo`)
	f.Close()
	recs, _, torn, err := ReadAll(path)
	if err != nil {
		t.Fatalf("torn tail must not be an error: %v", err)
	}
	if !torn || len(recs) != 1 {
		t.Fatalf("want torn=true recs=1, got torn=%v recs=%d", torn, len(recs))
	}
}

func TestTornTailWithGarbageLastLineIsRecovered(t *testing.T) {
	// A torn write that did land a newline but half a record before it.
	log, path := openTemp(t)
	log.Append(Record{Op: OpQueueCreate, Queue: "q1"})
	log.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("{\"op\":\"send\",\"q\n")
	f.Close()
	recs, _, torn, err := ReadAll(path)
	if err != nil || !torn || len(recs) != 1 {
		t.Fatalf("want recovery, got torn=%v recs=%d err=%v", torn, len(recs), err)
	}
}

func TestOpenTruncatesTornTailForAppend(t *testing.T) {
	dir := t.TempDir()
	log, _, _, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	log.Append(Record{Op: OpQueueCreate, Queue: "q1"})
	log.Close()
	path := filepath.Join(dir, FileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"op":"trunc`)
	f.Close()

	log2, recs, torn, err := Open(dir, true)
	if err != nil || !torn || len(recs) != 1 {
		t.Fatalf("reopen: torn=%v recs=%d err=%v", torn, len(recs), err)
	}
	// The next append must produce a parseable log, not splice into garbage.
	if err := log2.Append(Record{Op: OpSend, Queue: "q1", ID: "05", Body: "after crash"}); err != nil {
		t.Fatal(err)
	}
	log2.Close()
	recs2, _, torn2, err := ReadAll(path)
	if err != nil || torn2 || len(recs2) != 2 {
		t.Fatalf("after recovery append: recs=%d torn=%v err=%v", len(recs2), torn2, err)
	}
	if recs2[1].Body != "after crash" {
		t.Fatalf("appended record corrupted: %+v", recs2[1])
	}
}

func TestMidFileCorruptionIsAnError(t *testing.T) {
	// Damage anywhere except the tail means the file is untrustworthy;
	// replay must refuse rather than skip records silently.
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	content := "{\"op\":\"qcreate\",\"q\":\"q1\"}\nGARBAGE\n{\"op\":\"send\",\"q\":\"q1\",\"id\":\"01\"}\n"
	os.WriteFile(path, []byte(content), 0o644)
	if _, _, _, err := ReadAll(path); err == nil {
		t.Fatal("mid-file corruption should be an error")
	}
}

func TestInvalidRecordOnReplayIsAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	os.WriteFile(path, []byte("{\"op\":\"warp\",\"q\":\"q1\"}\n{\"op\":\"qcreate\",\"q\":\"q1\"}\n"), 0o644)
	if _, _, _, err := ReadAll(path); err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatalf("want unknown-op error, got %v", err)
	}
}

func TestAppendRejectsInvalidRecord(t *testing.T) {
	log, _ := openTemp(t)
	if err := log.Append(Record{Op: OpSend, Queue: "q1"}); err == nil {
		t.Fatal("send without id must be rejected before hitting disk")
	}
}

func TestSetBodyKeepsUTF8Readable(t *testing.T) {
	var r Record
	r.SetBody([]byte(`{"job":"resize","w":128}`))
	if r.Body == "" || r.BodyB64 != "" {
		t.Fatalf("UTF-8 body should stay plain: %+v", r)
	}
	got, err := r.GetBody()
	if err != nil || string(got) != `{"job":"resize","w":128}` {
		t.Fatalf("GetBody = %q, %v", got, err)
	}
}

func TestSetBodyBase64ForBinary(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 0x01}
	var r Record
	r.SetBody(raw)
	if r.Body != "" || r.BodyB64 == "" {
		t.Fatalf("binary body should be base64: %+v", r)
	}
	got, err := r.GetBody()
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("GetBody = %v, %v", got, err)
	}
}

func TestCompactReplacesLogAtomically(t *testing.T) {
	log, path := openTemp(t)
	for i := 0; i < 5; i++ {
		log.Append(Record{Op: OpQueueCreate, Queue: "q1"})
	}
	snap := []Record{
		{Op: OpQueueCreate, Queue: "q1"},
		{Op: OpSend, Queue: "q1", ID: "0a", Body: "live"},
	}
	if err := log.Compact(snap); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	recs, _, torn, err := ReadAll(path)
	if err != nil || torn || len(recs) != 2 {
		t.Fatalf("after compact: recs=%d torn=%v err=%v", len(recs), torn, err)
	}
	// The log must remain appendable through the swapped file handle.
	if err := log.Append(Record{Op: OpSend, Queue: "q1", ID: "0b", Body: "post"}); err != nil {
		t.Fatalf("append after compact: %v", err)
	}
	recs, _, _, _ = ReadAll(path)
	if len(recs) != 3 || recs[2].Body != "post" {
		t.Fatalf("post-compact append lost: %d records", len(recs))
	}
}

func TestCompactLeavesNoTempFiles(t *testing.T) {
	log, path := openTemp(t)
	log.Append(Record{Op: OpQueueCreate, Queue: "q1"})
	if err := log.Compact([]Record{{Op: OpQueueCreate, Queue: "q1"}}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != FileName {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("stray files after compact: %v", names)
	}
}

func TestValidateCatchesMissingFields(t *testing.T) {
	cases := []Record{
		{},                                   // no op
		{Op: OpQueueCreate},                  // no queue
		{Op: OpAck, Queue: "q"},              // no id
		{Op: OpReceive, Queue: "q", ID: "x"}, // no deadline
		{Op: OpMove, Queue: "q", ID: "x"},    // no target
	}
	for i, r := range cases {
		if err := r.Validate(); err == nil {
			t.Fatalf("case %d (%+v) should fail validation", i, r)
		}
	}
}
