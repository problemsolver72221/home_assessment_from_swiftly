# Device Control Pipeline Simulator

## 1. Setup and Run

Requires Docker Desktop with Compose v2.23+.

```bash
docker compose up --build
```

This starts mosquitto, three producers (bus-101, bus-102, bus-103), the consumer, and the sidecar. The consumer's health endpoint is at `localhost:8080/health`.

Unit tests (no Docker needed):
```bash
python tests/test_validation.py
```

**Fleet health demo:**
```bash
# All three vehicles should show healthy after a few seconds
curl -s localhost:8080/health

# Stop one producer and wait for it to go stale
docker compose stop producer-bus-102
sleep 6
curl -s localhost:8080/health   # bus-102: stale, others: healthy

# Sidecar resilience - queue grows while sidecar is down, drains when it comes back
docker compose stop sidecar
# ... wait a bit, watch consumer logs show backoff ...
docker compose start sidecar

# Consumer restart - queue persists since it's on a named Docker volume
docker compose restart consumer
# consumer picks up where it left off, no messages lost
```

---

## 2. Wire Protocol and Durability Design

**Protocol: newline-delimited JSON.**

The consumer sends the raw message payload (already valid JSON) with a `\n` appended. The sidecar responds with one JSON object + `\n`. Both sides enforce a 64 KB max frame size.

I chose newline-delimited over a length-prefixed binary protocol because it keeps the stream human-readable and debuggable with `nc localhost 9000`. For messages that are pure UTF-8 JSON, there's no real advantage to binary framing. The consumer uses `socket.makefile('rb')` to read responses, which handles buffering internally so a single `recv()` call is never assumed to carry a complete line.

The consumer maintains a persistent TCP connection to the sidecar and only reconnects on error, redialling per message would be wasteful.

**Durability: SQLite on a named Docker volume.**

The queue schema is exactly what the spec requires, stored in `/data/pending.db` (mounted via `consumer-data:/data`). `INSERT OR IGNORE` keyed on the message `id` makes MQTT's at-least-once delivery idempotent. WAL mode (`PRAGMA journal_mode=WAL`) allows the HTTP health handler to read the DB without blocking the TCP worker's writes.

I considered using a persistent MQTT session (QoS 1, `clean_session=False`) to let the broker hold messages during consumer downtime, but Mosquitto runs with `persistence false` in the provided config and would lose queued messages on restart. Owning the queue in the consumer is simpler to reason about.

**Backoff:** `min(2**attempts, 30) + random.uniform(0, 1)`, giving 1s, 2s, 4s, 8s, 16s, 30s with up to 1s of jitter. The jitter matters at fleet scale: without it, hundreds of vehicles coming back online after a shared outage would all retry simultaneously.

---

## 3. Sidecar Metrics

Every 30 seconds the sidecar logs:

```
METRICS total=N malformed=N valid=N invalid=N avg_lat_ms=N max_lat_ms=N active_connections=N
```

- **total**: if this stops incrementing, the pipeline is broken end-to-end.
- **malformed**: should always be 0 in normal operation. Non-zero means the consumer is sending invalid data or there's in-transit corruption.
- **valid/invalid split**: with random inputs the ratio should be stable. A sudden change could indicate a validation bug.
- **avg_lat_ms / max_lat_ms**: I track both because max is more actionable. A high max means some requests are consuming a significant chunk of the consumer's 3s timeout budget, which can cause spurious retries. Average alone hides this.
- **active_connections**: normally 1. Zero means the consumer is down; more than one means a reconnect happened before the old connection was reaped.

Things I'd add with more time: per-vehicle breakdown, latency histogram buckets, a Prometheus `/metrics` endpoint.

---

## 4. Extension: Fleet Health Visibility

I added a `GET /health` endpoint (stdlib `http.server`, no extra dependencies) on port 8080 that returns per-vehicle status:

```json
{
  "bus-101": {"status": "healthy", "last_seen": "2024-01-01T12:00:00+00:00", "error_count": 2},
  "bus-102": {"status": "stale",   "last_seen": "2024-01-01T11:59:55+00:00", "error_count": 0},
  "bus-103": {"status": "healthy", "last_seen": "2024-01-01T12:00:01+00:00", "error_count": 1}
}
```

The consumer tracks `last_seen[vehicle_id]` on every valid MQTT message and `error_count[vehicle_id]` each time a validation failure is published. A vehicle is marked stale if no message has been seen in more than 5 seconds (5x the 1s cadence; one missed beat could be scheduling noise, five in a row is a real signal).

**Why this direction:** the spec explicitly names fleet observability as the operational problem ("ops needs to be able to look at the fleet and quickly tell which vehicles are healthy and which are silently failing"). The floor implementation already addresses the reliability story (durable queue, retry with backoff). The remaining gap was visibility. Fleet health is also a useful hook for later; once `/health` exists, you can wire Prometheus scraping, alerting, and dashboards on top without touching the pipeline.

**Tradeoffs:**
- Health state is in-memory. Consumer restarts clear it; vehicles reappear after sending their next message.
- `error_count` is cumulative since startup, not a rolling rate. Ops would rather see "errors in the last 5 minutes."
- Single consumer is a SPOF for health monitoring. If the consumer is down, `/health` is too.

**What's next:** persist `last_seen`/`error_count` to SQLite alongside the queue, expose a Prometheus `/metrics` endpoint, add per-vehicle query params, push alerts when a vehicle transitions to stale.

---

## 5. Known Limitations

- **At-least-once delivery, not exactly-once.** If the consumer crashes after the sidecar responds but before `db_delete()` commits, the message is retried and a duplicate `control/error` may be published for the same id. This is a narrow window and acceptable here, but worth noting.
- **No TLS on the consumer-sidecar socket.** The spec notes the network is unmanaged and hardware is physically accessible. mTLS would be mandatory in production. (TODO in `consumer.py` and `sidecar.py`.)
- **No dead-letter table.** A message that consistently fails sidecar validation gets retried indefinitely with increasing backoff. A retry cap + quarantine table would be the fix. (TODO in `consumer.py`.)
- **No real serial bridging.** The sidecar's TCP server stands in for whatever hardware protocol the real sidecar would speak (RS485, J1708, CAN bus). (TODO in `sidecar.py`.)
- **Health state lost on consumer restart.** Covered in section 4.
- **Malformed messages cause silent retries.** When the sidecar discards a message without responding, the consumer times out after 3s, backs off, and retries. If a message consistently fails sidecar validation but passes the consumer's envelope check (e.g. letters contains a digit, which the consumer doesn't check), it will retry forever. The dead-letter TODO above is the fix.

---

## 6. AI Tool Usage

I used Claude (claude-sonnet-4-6 via Claude Code) throughout.

**What the AI generated:** all Python code and the docker-compose changes. I described the spec requirements and design decisions, and Claude wrote the implementation. I reviewed each file for correctness against the spec, specifically: that `INSERT OR IGNORE` is used correctly for idempotency, that `valid=false` deletes the row (not retries), that `letter_sum` in the error payload comes from the sidecar response and is never recomputed locally, and that the `isinstance(number, bool)` guard is present (Python's `isinstance(True, int)` returns `True`, so without it `True` would pass the int check).

**What I decided:** the extension direction (fleet health over deeper reliability), the stale threshold and its reasoning, newline-delimited vs length-prefixed protocol, SQLite vs in-memory queue, where to put the health endpoint (consumer not sidecar, since the consumer is the one that sees all vehicle messages), and the overall architecture.

**README:** Claude drafted the structure and prose; the reasoning sections reflect decisions I made during planning and I revised the content to make sure they're accurate.
#   h o m e _ a s s e s s m e n t _ f r o m _ s w i f t l y  
 