# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-13

### Added

- NDJSON write-ahead log with fsync-per-append durability (`--no-fsync` to
  trade the tail for speed), torn-tail detection and truncation on reopen,
  loud refusal on mid-file corruption, and atomic snapshot compaction via
  temp-file rename.
- Queue engine with FIFO ordering that survives redelivery (expired
  messages return to their original sequence position), per-message delays
  (≤15m), visibility-timeout leases with receipt validation, lease
  extension, and nack.
- Dead-letter policy: `max_receives` moves poison messages to the
  configured `dead_letter` queue (auto-created on first use) or drops them
  when none is set; `redrive` moves messages back with receive counts reset.
- Plain HTTP API: queue CRUD (`PUT/GET/DELETE /q/{name}`), raw-body send
  with `?delay=`, long-poll receive (`?wait=` ≤60s, `?max=` ≤100,
  per-request `?visibility=`), ack / nack / extend by receipt, redrive,
  `/healthz` and `/version`; uniform `{"error":{"code","message"}}`
  envelope with 404 / 409 / 400 semantics.
- Body handling for arbitrary bytes up to 1 MiB: UTF-8 payloads stored and
  served as plain JSON strings (greppable log), binary as base64.
- CLI: `serve` (loopback by default, graceful shutdown, adaptive sweeper),
  offline `stats` and `compact` that replay the log without a server, and
  `version`; exit codes 0/1/2.
- Human-friendly durations everywhere: `30s`, `1m30s`, or bare seconds.
- Runnable examples (`examples/producer.sh`, `examples/worker.sh`) and a
  WAL format reference (`docs/wal-format.md`).
- 90 deterministic offline tests (fake-clock engine tests, in-process HTTP
  tests, WAL crash-recovery tests) and `scripts/smoke.sh`.

[0.1.0]: https://github.com/JaydenCJ/duraq/releases/tag/v0.1.0
