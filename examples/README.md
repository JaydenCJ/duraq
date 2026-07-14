# duraq examples

Everything here runs against a local server started with:

```bash
duraq serve --data ./data
```

| File | What it shows |
|---|---|
| [`producer.sh`](producer.sh) | create a queue with a dead-letter policy and enqueue jobs, plain curl |
| [`worker.sh`](worker.sh) | a complete at-least-once worker: long-poll, process, extend, ack/nack |

Run them in two terminals:

```bash
bash examples/producer.sh          # terminal 1: enqueue a batch of jobs
bash examples/worker.sh            # terminal 2: consume until the queue is drained
```

Both scripts talk only to `127.0.0.1:7333` and need nothing but bash and
curl — that is the point: if these ~40 lines are a full producer and a full
consumer, so is any HTTP client in any language.
