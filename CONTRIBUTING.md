# Contributing to duraq

Issues, discussions and pull requests are all welcome.

## Getting started

You need Go ≥1.22 and bash with curl (for the smoke script); nothing else.

```bash
git clone https://github.com/JaydenCJ/duraq && cd duraq
go build ./...
go test ./...
bash scripts/smoke.sh
```

`scripts/smoke.sh` builds the binary, starts a real server on a loopback
port, and drives the whole lifecycle with curl — create, send, long-poll,
ack, dead-letter, redrive, crash-restart durability, and compaction; it must
finish by printing `SMOKE OK`.

## Before you open a pull request

1. `gofmt -l .` reports nothing (formatting is enforced).
2. `go vet ./...` passes with no findings.
3. `go test ./...` passes (90 deterministic tests, no network, no sleeps).
4. `bash scripts/smoke.sh` prints `SMOKE OK`.
5. Add tests for behavior changes; keep logic in pure, unit-testable
   modules (the engine never reads the wall clock — time is injected — and
   only `internal/wal` touches the filesystem).

## Ground rules

- Keep dependencies at zero; adding one needs strong justification in the PR.
- No network calls except serving the user's chosen listen address
  (127.0.0.1 by default). No telemetry.
- Durability invariants are sacred: a record is committed before its effect
  is observable, replay must reconstruct exactly, and torn tails are
  recovered while mid-file corruption fails loudly. Changes to
  `docs/wal-format.md` must come with replay tests for old logs.
- Code comments and doc comments are written in English.
- Determinism first: tests use a hand-cranked clock and in-process HTTP;
  keep them free of sleeps and real timers.

## Reporting bugs

Include the output of `duraq version`, the full command line you started the
server with, the relevant HTTP request/response pair (curl `-v` output is
ideal), and — for durability or replay issues — the smallest `wal.ndjson`
that reproduces the problem, since that file is exactly what the engine sees.

## Security

Please do not open public issues for security problems; use GitHub's
private vulnerability reporting on this repository instead.
