// Package cli implements the duraq command line: serve, compact, stats,
// version. Parsing is pure and unit-testable; only serve touches the
// network, and only ever on the address the user passes (127.0.0.1 by
// default — duraq never listens publicly unless told to).
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/JaydenCJ/duraq/internal/queue"
	"github.com/JaydenCJ/duraq/internal/server"
	"github.com/JaydenCJ/duraq/internal/version"
	"github.com/JaydenCJ/duraq/internal/wal"
)

// Exit codes.
const (
	ExitOK      = 0
	ExitRuntime = 1
	ExitUsage   = 2
)

const usage = `duraq %s — durable message queue over plain HTTP

Usage:
  duraq serve   --data DIR [--addr HOST:PORT] [--no-fsync]
  duraq compact --data DIR
  duraq stats   --data DIR
  duraq version

Commands:
  serve     start the queue server (WAL replay, then listen)
  compact   rewrite the write-ahead log to just the live state
  stats     print per-queue counters from the log, offline
  version   print the version and exit

Run "duraq <command> -h" for command flags.
`

// Run executes the CLI and returns the process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, usage, version.Version)
		return ExitUsage
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:], stdout, stderr)
	case "compact":
		return runCompact(args[1:], stdout, stderr)
	case "stats":
		return runStats(args[1:], stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "duraq %s\n", version.Version)
		return ExitOK
	case "help", "--help", "-h":
		fmt.Fprintf(stdout, usage, version.Version)
		return ExitOK
	default:
		fmt.Fprintf(stderr, "duraq: unknown command %q\n\n", args[0])
		fmt.Fprintf(stderr, usage, version.Version)
		return ExitUsage
	}
}

// dataFlag registers the shared --data flag on fs.
func dataFlag(fs *flag.FlagSet) *string {
	return fs.String("data", "", "data directory holding "+wal.FileName+" (required)")
}

func parse(fs *flag.FlagSet, args []string, stderr io.Writer) (dataDir string, ok bool) {
	fs.SetOutput(stderr)
	data := dataFlag(fs)
	if err := fs.Parse(args); err != nil {
		return "", false
	}
	if *data == "" {
		fmt.Fprintf(stderr, "duraq %s: --data DIR is required\n", fs.Name())
		return "", false
	}
	return *data, true
}

// --- serve ------------------------------------------------------------------

func runServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := dataFlag(fs)
	addr := fs.String("addr", "127.0.0.1:7333", "listen address")
	noFsync := fs.Bool("no-fsync", false, "skip fsync per append (faster, loses the tail on power loss)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *data == "" {
		fmt.Fprintln(stderr, "duraq serve: --data DIR is required")
		return ExitUsage
	}

	eng, log, torn, err := queue.Open(*data, !*noFsync, time.Now)
	if err != nil {
		fmt.Fprintf(stderr, "duraq serve: %v\n", err)
		return ExitRuntime
	}
	defer log.Close()
	if torn {
		fmt.Fprintln(stderr, "duraq serve: recovered from a torn tail in the log (partial final record discarded)")
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(stderr, "duraq serve: %v\n", err)
		return ExitRuntime
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The sweeper is the only clock-driven goroutine: it settles delayed
	// deliveries and expired visibility timeouts, sleeping until the next
	// due deadline (capped at 1s so freshly created deadlines are noticed).
	go func() {
		for {
			next := eng.Sweep(time.Now())
			d := time.Second
			if !next.IsZero() {
				if until := time.Until(next); until < d {
					d = until
				}
			}
			if d < 10*time.Millisecond {
				d = 10 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
		}
	}()

	srv := &http.Server{Handler: server.New(eng)}
	fmt.Fprintf(stdout, "duraq %s serving on http://%s (data: %s, queues: %d)\n",
		version.Version, ln.Addr(), *data, len(eng.ListStats()))

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		fmt.Fprintln(stdout, "duraq: shut down cleanly")
		return ExitOK
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return ExitOK
		}
		fmt.Fprintf(stderr, "duraq serve: %v\n", err)
		return ExitRuntime
	}
}

// --- compact ------------------------------------------------------------------

func runCompact(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	data, ok := parse(fs, args, stderr)
	if !ok {
		return ExitUsage
	}
	log, recs, torn, err := wal.Open(data, true)
	if err != nil {
		fmt.Fprintf(stderr, "duraq compact: %v\n", err)
		return ExitRuntime
	}
	defer log.Close()
	if torn {
		fmt.Fprintln(stderr, "duraq compact: recovered from a torn tail in the log")
	}
	eng := queue.New(nil, time.Now)
	if err := eng.Load(recs); err != nil {
		fmt.Fprintf(stderr, "duraq compact: %v\n", err)
		return ExitRuntime
	}
	snap := eng.Snapshot()
	if err := log.Compact(snap); err != nil {
		fmt.Fprintf(stderr, "duraq compact: %v\n", err)
		return ExitRuntime
	}
	fmt.Fprintf(stdout, "compacted %d record%s to %d\n", len(recs), plural(len(recs)), len(snap))
	return ExitOK
}

// --- stats ------------------------------------------------------------------

func runStats(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	data, ok := parse(fs, args, stderr)
	if !ok {
		return ExitUsage
	}
	recs, _, torn, err := wal.ReadAll(dataPath(data))
	if err != nil {
		fmt.Fprintf(stderr, "duraq stats: %v\n", err)
		return ExitRuntime
	}
	if torn {
		fmt.Fprintln(stderr, "duraq stats: log has a torn tail (partial final record ignored)")
	}
	eng := queue.New(nil, time.Now)
	if err := eng.Load(recs); err != nil {
		fmt.Fprintf(stderr, "duraq stats: %v\n", err)
		return ExitRuntime
	}
	all := eng.ListStats()
	if len(all) == 0 {
		fmt.Fprintln(stdout, "no queues")
		return ExitOK
	}
	fmt.Fprintf(stdout, "%-24s %8s %8s %10s %8s\n", "queue", "ready", "delayed", "in-flight", "dead")
	for _, s := range all {
		fmt.Fprintf(stdout, "%-24s %8d %8d %10d %8d\n",
			s.Name, s.Ready, s.Delayed, s.InFlight, s.TotalDead)
	}
	fmt.Fprintf(stdout, "%d record%s in log\n", len(recs), plural(len(recs)))
	return ExitOK
}

// plural returns "s" unless n is exactly one, for grammatical counters.
func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func dataPath(dir string) string {
	return filepath.Join(dir, wal.FileName)
}
