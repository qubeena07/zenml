---
description: Push live events from inside a step to subscribed clients
---

# Streaming Events

ZenML pipelines can push live events from inside a running step to any
subscriber listening on the server. This is the building block for LLM
token streaming, progress bars on long-running steps, real-time
dashboards, and any other "show what's happening right now" experience.

The feature is **opt-in on the server** (see [Enabling
streaming](#enabling-streaming-on-the-server)) and **dormant by
default** — pipelines that don't call `zenml.streams.publish()` are
unaffected.

{% hint style="warning" %}
Streaming is **best-effort, in-memory transport** for live events. It
is **not durable storage** — events are capped, can be dropped under
backpressure, and disappear when the broker's retention window
elapses. Use [run metadata](../artifacts/artifacts.md) or
[artifacts](../artifacts/artifacts.md) for anything you need to
persist.
{% endhint %}

## Quick start

### 1. Enable streaming on the server

Streaming is gated by the `event_broker_implementation_source` server
config (or the `streaming.eventBrokerImplementationSource` Helm value).
Pick a broker:

- `zenml.zen_server.streaming.brokers.memory.InMemoryBroker` — single
  replica only. Events live in the server process; everything is lost
  on restart. Good for local development.
- `zenml.zen_server.streaming.brokers.redis_streams.RedisStreamsBroker`
  — multi-replica safe. Requires Redis 5+. Install the optional
  dependency with `pip install 'zenml[server-streaming]'` and set
  `ZENML_REDIS_BROKER_URL` to point at your Redis.

For Helm:

```yaml
server:
  streaming:
    eventBrokerImplementationSource: zenml.zen_server.streaming.brokers.redis_streams.RedisStreamsBroker
  environment:
    ZENML_REDIS_BROKER_URL: redis://my-redis.svc.cluster.local:6379/0
```

### 2. Publish from inside a step

```python
from zenml import step
from zenml.streams import publish

@step
def my_streaming_step() -> str:
    publish({"phase": "warmup"})
    for i in range(10):
        publish({"i": i, "msg": f"working on item {i}"})
    publish({"phase": "done"})
    return "ok"
```

`publish()` discovers the current run and step from the step context, so
inside `@step`-decorated functions you don't need to pass any handle.

### 3. Subscribe over HTTP

Streams are served as
[Server-Sent Events (SSE)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
at:

```
GET /api/v1/runs/{pipeline_run_id}/events/stream
Accept: text/event-stream
Authorization: Bearer <token>
```

From a browser:

```javascript
const es = new EventSource(
  `/api/v1/runs/${runId}/events/stream`,
  { withCredentials: true }
);
es.addEventListener("event", (e) => console.log(JSON.parse(e.data)));
es.addEventListener("end", () => es.close());
```

From the command line:

```bash
curl -N -H "Accept: text/event-stream" \
  -H "Authorization: Bearer $ZENML_TOKEN" \
  "$ZENML_URL/api/v1/runs/$RUN_ID/events/stream"
```

## The `publish()` API

```python
from zenml.streams import publish, flush

publish(
    payload: Dict[str, Any],
    *,
    kind: str = "event",
    stream_id: Optional[str] = None,  # client-side correlation
    index: Optional[int] = None,       # client-side ordering
) -> None
```

- `payload`: any JSON-serializable dict. **Capped at ~64 KiB per event
  on the wire** — bigger payloads are rejected by the server.
- `kind`: a free-form label clients can filter on. Must match
  `[A-Za-z0-9._-]{1,64}`. The kinds `end`, `gap`, `error`, and `ping`
  are **reserved** for server-side control frames and will be rejected.
- `stream_id` / `index`: opaque to ZenML — exposed to consumers
  unchanged so clients can correlate or order events within a logical
  sub-stream (e.g., one `stream_id` per LLM generation).

`publish()` **never blocks** the caller. Events are queued and a
background thread ships them in small batches.

`flush(timeout=2.0)` waits (up to `timeout` seconds) for the queue and
all in-flight batches to drain. ZenML automatically flushes at step
end, so most users don't need to call this directly — call it only if
you want a stronger guarantee that a specific event has reached the
server before some side effect.

### Inside dynamic pipelines

Inside the body of a `@pipeline(dynamic=True)` (outside any `@step`),
`publish()` attributes events to the pipeline run but with no
`step_run_id`. See [Dynamic Pipelines](dynamic_pipelines.md).

## SSE wire format

Each frame the server emits looks like:

```
id: <broker-assigned id>
event: <kind>
data: <JSON-encoded StreamEvent>

```

Consumers should look for these well-known event names:

| `event:` | Meaning |
|----------|---------|
| `event` (default) or any custom `kind` | A normal payload. `data` is the JSON-serialized `StreamEvent`. |
| `end` | The run has reached a terminal state; the server will close the connection. |
| `gap` | The consumer may have missed events between the last `id` and now (reasons: `truncated`, `outage`, `overflow`, `broker_error`, `shutdown`). |
| `error` | A transient server-side error. The client should reconnect with `Last-Event-ID`. |

Heartbeats arrive as comment frames (`: ping\n\n`) every
`streaming_heartbeat_seconds` (default 30s) and require no client
handling.

### Filtering

Pass one or more `kinds` query parameters to receive only matching events:

```
GET /api/v1/runs/{run}/events/stream?kinds=token&kinds=progress
```

Filtered-out events still advance the server's cursor — clients can
safely reconnect with `Last-Event-ID` and won't be replayed.

### Resuming after a disconnect

The server honours the standard SSE `Last-Event-ID` request header on
reconnect. Browsers' `EventSource` sends it automatically; other
clients should track the last `id:` they received and send it back to
resume:

```
GET /api/v1/runs/{run}/events/stream
Last-Event-ID: <last id you received>
```

If too much time has passed and the event has been trimmed from the
broker, the server emits a `gap: truncated` frame instead of silently
skipping events.

## Delivery semantics

| Property | What you get |
|----------|--------------|
| Ordering | Per-run, monotonic by broker id. |
| Duplicates | At-most-once across a single connection; consumers should be idempotent across reconnects. |
| Loss | Best-effort. Events can be dropped by producer-side backpressure (queue cap is 4096 per process) or by broker-side cap (default 10 000 entries per run). |
| Retention | Redis: `streaming.streamTtlSeconds` (default 1 h after the last publish). In-memory: until the hub closes the session (`streaming_hub_idle_grace_seconds`, default 30 s after the last consumer leaves). |
| Multi-replica | Redis broker fans out across replicas via the deployment id. In-memory works only on a single-replica deployment. |
| Persistence | None. Use `log_metadata` or artifacts if you need durable storage. |

## Server configuration

| Field (`ServerConfiguration`) | Helm key (`server.streaming.*`) | Default | Notes |
|---|---|---|---|
| `event_broker_implementation_source` | `eventBrokerImplementationSource` | unset | Setting this enables streaming. |
| `streaming_heartbeat_seconds` | `heartbeatSeconds` | `30.0` | SSE heartbeat / idle keepalive interval. |
| `streaming_max_consumers_per_stream` | `maxConsumersPerStream` | `100` | Hard cap; the 101st gets a 503. |
| `streaming_hub_idle_grace_seconds` | `hubIdleGraceSeconds` | `30.0` | How long the hub keeps a stream's reader alive after the last consumer disconnects. |

### Redis-specific settings (env vars)

| Variable | Default | Notes |
|---|---|---|
| `ZENML_REDIS_BROKER_URL` | — | `redis://...` or `rediss://...`. Required. |
| `ZENML_REDIS_MAX_CONNECTIONS` | `10` | Pool size. Bump if you expect many active streams. |
| `ZENML_REDIS_SOCKET_TIMEOUT` | `2.0` | Per-call timeout in seconds. |
| `ZENML_REDIS_STREAMS_BROKER_MAX_STREAM_LENGTH` | `10000` | Per-run entry cap (XADD `MAXLEN ~`). |
| `ZENML_REDIS_STREAMS_BROKER_STREAM_TTL_SECONDS` | `3600` | TTL refreshed on every publish. |

### Helm ingress

The chart installs an SSE-only Gateway-API `HTTPRoute` rule that
disables Envoy's 15-second wall-clock request timeout for clients
sending `Accept: text/event-stream`. Browsers' `EventSource` and the
ZenML server's own emitted frames both qualify. Clients that send a
quality-list `Accept` header fall through to the default rule and get
capped at 15 s.

## Limitations

- Streaming events are **not persisted** to ZenML's database. They live
  only on the broker.
- The publisher-side queue is **bounded at 4 096 events per process**;
  events are dropped if a step publishes faster than the server can
  ingest.
- The broker stream is **capped per run** (default 10 000 entries on
  Redis; same default on the in-memory broker). Consumers that fall
  too far behind will see a `gap: truncated` frame.
- The `InMemoryBroker` is single-replica only. Multi-replica
  deployments must use the Redis broker.
- Publishing requires `UPDATE` permission on the run; consuming
  requires `READ`.
