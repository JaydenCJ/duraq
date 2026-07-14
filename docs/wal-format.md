# The duraq write-ahead log format

The entire durable state of a duraq server lives in one file:
`<data-dir>/wal.ndjson`. It is newline-delimited JSON — one object per line,
appended in commit order, never rewritten in place (compaction replaces the
whole file atomically). This document is the contract for that file: any
record sequence valid under these rules must replay to a working server.

## Ground rules

1. **One JSON object per line.** No pretty-printing, no continuation lines.
2. **Append-only.** A record is committed once its line and trailing `\n`
   are on disk (fsynced by default). Effects become observable only after
   the append succeeds.
3. **Torn tails are recoverable.** A crash mid-append can only damage the
   final line. On open, an unparseable final line (with or without its
   newline) is truncated and reported; damage anywhere earlier is treated
   as corruption and refuses to load.
4. **Replay is total.** Rebuilding state = applying every record in order.
   Message IDs are hex sequence numbers, so the ID counter is recovered
   from the records themselves.

## Common fields

| Field | Type | Meaning |
|---|---|---|
| `op` | string | record type (below) |
| `q` | string | queue name |
| `id` | string | message ID — 16 hex digits, globally sequential |
| `ts` | int | event time, Unix milliseconds |
| `body` / `body_b64` | string | payload: plain string when valid UTF-8, else base64 |
| `receipt` | string | lease token (`r` + 16 hex digits) |
| `deadline` | int | visibility deadline, Unix milliseconds |
| `nbf` | int | not-before time for delayed sends, Unix milliseconds |
| `count` | int | receive count |
| `to` | string | target queue for `dead` / `move` |
| `cfg` | object | queue config (same shape as the HTTP API) |

## Record types

| `op` | Written when | Required fields | Replay effect |
|---|---|---|---|
| `qcreate` | queue created or reconfigured | `q`, `cfg` | create queue or replace its config |
| `qdelete` | queue deleted | `q` | drop queue and all its messages |
| `send` | message enqueued | `q`, `id`, body | add message; delayed if `nbf` is in the future; `count` restores receive counts after compaction |
| `recv` | message leased to a consumer | `q`, `id`, `receipt`, `deadline`, `count` | mark leased with that receipt/deadline |
| `ack` | consumer confirmed completion | `q`, `id`, `receipt` | delete the message |
| `nack` | consumer returned the message | `q`, `id`, `receipt` | back to ready at its original position |
| `extend` | lease deadline pushed forward | `q`, `id`, `receipt`, `deadline` | update the deadline |
| `dead` | poison message dead-lettered | `q`, `id`, `to` | move to `to` (or drop when `to` is empty) |
| `move` | redrive between queues | `q`, `id`, `to` | move to `to`, reset receive count |

Note what is *not* logged: a lease expiring and the message returning to
ready. That transition is a pure function of `deadline` and the current
time, so replay recomputes it — the log stays small and the server never
races the clock to disk.

## Compaction

`duraq compact` (offline) replays the log, then rewrites it as the minimal
equivalent history: one `qcreate` per queue, one `send` per live message
(with `count` folded in), plus one `recv` for each message currently leased.
The rewrite goes to a temp file in the same directory, is fsynced, and is
`rename(2)`d over the log — a crash during compaction leaves either the old
complete file or the new complete file, never a hybrid.

## Auditing with standard tools

```bash
tail -f data/wal.ndjson                          # watch a queue live
grep '"op":"dead"' data/wal.ndjson               # every dead-lettered message
jq -r 'select(.op=="send") | .id' data/wal.ndjson # all message IDs ever sent
```
