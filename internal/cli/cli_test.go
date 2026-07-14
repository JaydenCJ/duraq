// CLI tests: usage errors, version output, and the offline compact / stats
// commands against a real WAL on disk. serve is covered by scripts/smoke.sh.
package cli

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JaydenCJ/duraq/internal/queue"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func run(args ...string) (code int, stdout, stderr string) {
	var out, errB bytes.Buffer
	code = Run(args, &out, &errB)
	return code, out.String(), errB.String()
}

// seedData builds a data dir with one queue, one acked and one live message.
func seedData(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	e, log, _, err := queue.Open(dir, true, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := e.CreateQueue("jobs", queue.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	e.Send("jobs", []byte("live"), 0)
	e.Send("jobs", []byte("done"), 0)
	ds, _ := e.Receive(context.Background(), "jobs", 1, 0, 0)
	e.Ack("jobs", ds[0].ID, ds[0].Receipt)
	return dir
}

func TestNoArgsPrintsUsageAndExits2(t *testing.T) {
	code, _, stderr := run()
	if code != ExitUsage || !strings.Contains(stderr, "Usage:") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestUnknownCommandExits2(t *testing.T) {
	code, _, stderr := run("frobnicate")
	if code != ExitUsage || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestVersionCommand(t *testing.T) {
	code, stdout, _ := run("version")
	if code != ExitOK || strings.TrimSpace(stdout) != "duraq 0.1.0" {
		t.Fatalf("code=%d stdout=%q", code, stdout)
	}
}

func TestServeRequiresDataDir(t *testing.T) {
	code, _, stderr := run("serve")
	if code != ExitUsage || !strings.Contains(stderr, "--data") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestStatsPrintsQueueTable(t *testing.T) {
	dir := seedData(t)
	code, stdout, _ := run("stats", "--data", dir)
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "jobs") || !strings.Contains(stdout, "queue") {
		t.Fatalf("stats output: %q", stdout)
	}
	// qcreate + 2 sends + recv + ack = 5 records.
	if !strings.Contains(stdout, "5 records in log") {
		t.Fatalf("record count missing: %q", stdout)
	}
}

func TestStatsOnEmptyDirSaysNoQueues(t *testing.T) {
	code, stdout, _ := run("stats", "--data", t.TempDir())
	if code != ExitOK || !strings.Contains(stdout, "no queues") {
		t.Fatalf("code=%d stdout=%q", code, stdout)
	}
}

func TestCompactShrinksLog(t *testing.T) {
	dir := seedData(t)
	code, stdout, _ := run("compact", "--data", dir)
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%q", code, stdout)
	}
	// 5 records of history compact to qcreate + 1 live message = 2.
	if !strings.Contains(stdout, "compacted 5 records to 2") {
		t.Fatalf("compact output: %q", stdout)
	}
	// The compacted log must still replay to the same live message.
	code, stdout, _ = run("stats", "--data", dir)
	if code != ExitOK || !strings.Contains(stdout, "2 records in log") {
		t.Fatalf("stats after compact: %q", stdout)
	}
}

func TestCompactOnCorruptLogFails(t *testing.T) {
	dir := t.TempDir()
	if err := writeFile(dir+"/wal.ndjson", "{\"op\":\"qcreate\",\"q\":\"a\"}\nJUNK\n{\"op\":\"qcreate\",\"q\":\"b\"}\n"); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := run("compact", "--data", dir)
	if code != ExitRuntime || !strings.Contains(stderr, "corrupt") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}
