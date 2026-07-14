#!/usr/bin/env bash
# Producer example: create a queue with a dead-letter policy, then enqueue
# a batch of jobs — nothing but curl.
set -euo pipefail

BASE="${DURAQ_URL:-http://127.0.0.1:7333}"

# Create (or reconfigure) the queue. Idempotent: PUT is create-or-update.
curl -fsS -X PUT "$BASE/q/jobs" -d '{
  "visibility_timeout": "30s",
  "max_receives": 5,
  "dead_letter": "jobs.dlq"
}' > /dev/null
echo "queue 'jobs' ready (poison messages go to 'jobs.dlq' after 5 tries)"

# Enqueue ten jobs. The body is yours: JSON, text, anything up to 1 MiB.
for i in $(seq 1 10); do
  ID=$(curl -fsS -X POST "$BASE/q/jobs/messages" \
    -d "{\"task\":\"resize\",\"image\":\"photo-$i.png\",\"width\":128}" \
    | sed -n 's/.*"id": "\([0-9a-f]*\)".*/\1/p')
  echo "enqueued photo-$i.png as $ID"
done

# One delayed job: invisible for 30 seconds, then delivered like any other.
curl -fsS -X POST "$BASE/q/jobs/messages?delay=30s" \
  -d '{"task":"report","when":"later"}' > /dev/null
echo "enqueued 1 delayed job (+30s)"

curl -fsS "$BASE/q/jobs" | grep -E '"(ready|delayed)"'
