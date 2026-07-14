#!/usr/bin/env bash
# Worker example: a complete at-least-once consumer in a shell loop.
# Long-poll for a job, "process" it, ack on success, nack on failure.
# Kill this worker mid-job and the message redelivers to the next worker
# after the visibility timeout — that is the whole durability contract.
set -euo pipefail

BASE="${DURAQ_URL:-http://127.0.0.1:7333}"
Q="${DURAQ_QUEUE:-jobs}"

echo "worker consuming '$Q' from $BASE (Ctrl-C to stop)"
while true; do
  # Long-poll up to 20s. 204 = queue empty right now; just poll again.
  RESP="$(curl -fsS -w '\n%{http_code}' "$BASE/q/$Q/messages?wait=20")"
  CODE="$(echo "$RESP" | tail -1)"
  [ "$CODE" = "204" ] && continue

  BODY="$(echo "$RESP" | sed '$d')"
  ID="$(echo "$BODY" | sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p')"
  RECEIPT="$(echo "$BODY" | sed -n 's/.*"receipt": "\(r[0-9a-f]*\)".*/\1/p')"
  JOB="$(echo "$BODY" | sed -n 's/.*"body": "\(.*\)",$/\1/p' | head -1)"
  echo "processing $ID: $JOB"

  # A long job? Push the lease deadline out before it expires:
  #   curl -fsS -X POST "$BASE/q/$Q/messages/$ID/extend?receipt=$RECEIPT&visibility=2m"

  if sleep 0.1; then   # <- your real work goes here
    curl -fsS -X DELETE "$BASE/q/$Q/messages/$ID?receipt=$RECEIPT"
    echo "acked $ID"
  else
    # Failed: return it immediately so another worker retries it now.
    # After max_receives failures duraq moves it to the dead-letter queue.
    curl -fsS -X POST "$BASE/q/$Q/messages/$ID/nack?receipt=$RECEIPT"
    echo "nacked $ID"
  fi
done
