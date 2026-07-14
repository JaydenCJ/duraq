#!/usr/bin/env bash
# End-to-end smoke test for duraq: builds the binary, runs a real server on
# a loopback port, and drives the whole lifecycle with curl — create, send,
# long-poll, ack, dead-letter, redrive, crash-restart durability, compact.
# Loopback only, idempotent, finishes in seconds.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

fail() {
  echo "SMOKE FAIL: $*" >&2
  [ -f "$WORKDIR/server.log" ] && sed 's/^/  server: /' "$WORKDIR/server.log" >&2
  exit 1
}

command -v curl >/dev/null || fail "curl is required for the smoke test"

BIN="$WORKDIR/duraq"
DATA="$WORKDIR/data"

start_server() {
  # --addr :0 picks a free port; the startup line tells us which.
  "$BIN" serve --data "$DATA" --addr 127.0.0.1:0 > "$WORKDIR/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    BASE="$(sed -n 's|.*serving on \(http://[0-9.:]*\).*|\1|p' "$WORKDIR/server.log")"
    if [ -n "$BASE" ] && curl -fsS "$BASE/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  fail "server did not become ready"
}

stop_server() {
  kill "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

echo "1. build"
(cd "$ROOT" && go build -o "$BIN" ./cmd/duraq) || fail "go build failed"

echo "2. version matches manifest"
"$BIN" version | grep -x "duraq 0.1.0" >/dev/null || fail "version mismatch"

echo "3. start the server"
start_server

echo "4. create a queue with a dead-letter policy"
curl -fsS -X PUT "$BASE/q/jobs" \
  -d '{"visibility_timeout":"1s","max_receives":1,"dead_letter":"jobs.dlq"}' \
  | grep '"name": "jobs"' >/dev/null || fail "queue create failed"

echo "5. send a message, receive it, ack it"
curl -fsS -X POST "$BASE/q/jobs/messages" -d '{"task":"resize","width":128}' \
  | grep '"id"' >/dev/null || fail "send failed"
RECV="$(curl -fsS "$BASE/q/jobs/messages")"
echo "$RECV" | grep '\\"task\\":\\"resize\\"' >/dev/null || fail "receive body wrong: $RECV"
ID="$(echo "$RECV" | sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p')"
RECEIPT="$(echo "$RECV" | sed -n 's/.*"receipt": "\(r[0-9a-f]*\)".*/\1/p')"
[ -n "$ID" ] && [ -n "$RECEIPT" ] || fail "no id/receipt in receive: $RECV"
STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$BASE/q/jobs/messages/$ID?receipt=$RECEIPT")"
[ "$STATUS" = "204" ] || fail "ack returned $STATUS"

echo "6. wrong receipt is a 409 conflict"
curl -fsS -X POST "$BASE/q/jobs/messages" -d 'job-two' >/dev/null
RECV="$(curl -fsS "$BASE/q/jobs/messages")"
ID="$(echo "$RECV" | sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p')"
RECEIPT="$(echo "$RECV" | sed -n 's/.*"receipt": "\(r[0-9a-f]*\)".*/\1/p')"
STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$BASE/q/jobs/messages/$ID?receipt=r0000000000009999")"
[ "$STATUS" = "409" ] || fail "wrong receipt returned $STATUS, want 409"
curl -fsS -X DELETE "$BASE/q/jobs/messages/$ID?receipt=$RECEIPT" >/dev/null

echo "7. long-poll is woken by a send"
curl -fsS "$BASE/q/jobs/messages?wait=10" > "$WORKDIR/poll.json" &
POLL_PID=$!
sleep 0.2
curl -fsS -X POST "$BASE/q/jobs/messages" -d 'wake the poller' >/dev/null
wait "$POLL_PID" || fail "long-poll curl failed"
grep -q 'wake the poller' "$WORKDIR/poll.json" || fail "long-poll missed the message"
RECEIPT="$(sed -n 's/.*"receipt": "\(r[0-9a-f]*\)".*/\1/p' "$WORKDIR/poll.json")"
ID="$(sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p' "$WORKDIR/poll.json")"
curl -fsS -X DELETE "$BASE/q/jobs/messages/$ID?receipt=$RECEIPT" >/dev/null

echo "8. unacked poison message dead-letters after max_receives"
curl -fsS -X POST "$BASE/q/jobs/messages" -d 'poison' >/dev/null
curl -fsS "$BASE/q/jobs/messages" >/dev/null   # receive 1 of max 1, never ack
sleep 1.3                                       # let the 1s visibility lease expire
DLQ="$(curl -fsS "$BASE/q/jobs.dlq/messages")"
echo "$DLQ" | grep 'poison' >/dev/null || fail "poison message not in DLQ: $DLQ"
RECEIPT="$(echo "$DLQ" | sed -n 's/.*"receipt": "\(r[0-9a-f]*\)".*/\1/p')"
ID="$(echo "$DLQ" | sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p')"
curl -fsS -X POST "$BASE/q/jobs.dlq/messages/$ID/nack?receipt=$RECEIPT" -o /dev/null

echo "9. redrive drains the DLQ back into the main queue"
curl -fsS -X POST "$BASE/q/jobs.dlq/redrive?to=jobs" | grep '"moved": 1' >/dev/null \
  || fail "redrive failed"

echo "10. messages survive a server restart (WAL replay)"
curl -fsS -X POST "$BASE/q/jobs/messages" -d 'durable across restarts' >/dev/null
stop_server
start_server
AFTER="$(curl -fsS "$BASE/q/jobs/messages?max=10")"
echo "$AFTER" | grep 'durable across restarts' >/dev/null || fail "message lost across restart"
echo "$AFTER" | grep 'poison' >/dev/null || fail "redriven message lost across restart"

echo "11. the log is plain NDJSON you can grep"
grep -q '"op":"send"' "$DATA/wal.ndjson" || fail "WAL is not greppable NDJSON"
grep -q 'durable across restarts' "$DATA/wal.ndjson" || fail "body not readable in WAL"

echo "12. offline stats and compact work on the same data"
stop_server
"$BIN" stats --data "$DATA" | grep 'jobs' >/dev/null || fail "stats failed"
"$BIN" compact --data "$DATA" | grep 'compacted' >/dev/null || fail "compact failed"
"$BIN" stats --data "$DATA" | grep 'jobs' >/dev/null || fail "stats after compact failed"

echo "13. usage errors exit 2"
set +e
"$BIN" serve >/dev/null 2>&1
[ $? -eq 2 ] || fail "serve without --data should exit 2"
"$BIN" frobnicate >/dev/null 2>&1
[ $? -eq 2 ] || fail "unknown command should exit 2"
set -e

echo "SMOKE OK"
