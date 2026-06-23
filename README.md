# Device Control Pipeline Simulator

## Setup

Requires Docker Desktop with Compose v2.23+.

```bash
docker compose up --build
```

Starts mosquitto, three producers (bus-101, bus-102, bus-103), the consumer, and the sidecar. Health endpoint is at `localhost:8080/health`.

Unit tests (no Docker needed):
```bash
python tests/test_validation.py
```

**Quick smoke test:**
```bash
# all three vehicles should be healthy within a few seconds
curl -s localhost:8080/health

# stop one producer and wait for it to go stale
docker compose stop producer-bus-102
sleep 6
curl -s localhost:8080/health   # bus-102: stale, others: healthy

# test sidecar downtime -- queue builds up, drains when it comes back
docker compose stop sidecar
sleep 5
docker compose start sidecar

# consumer restart -- queue survives on the named Docker volume
docker compose restart consumer
```

---

## Design Notes

**Queue:** I used SQLite on a named Docker volume instead of an in-memory structure so the queue survives consumer restarts. `INSERT OR IGNORE` on message `id` makes MQTT's at-least-once delivery safe -- duplicates silently do nothing. WAL mode (`PRAGMA journal_mode=WAL`) lets the health server read the DB while the TCP worker is writing, without them blocking each other.

I did look at using a persistent MQTT session (QoS 1) to let the broker hold messages during downtime, but Mosquitto has `persistence false` in the provided config so that wouldn't actually work.

**Wire format:** newline-delimited JSON over a persistent TCP connection. The main reason I picked this over a binary/length-prefixed format is that you can test the sidecar directly from a terminal with `nc localhost 9000`, which made debugging easier. The consumer uses `socket.makefile('rb')` so readline handles buffering internally.

**Backoff:** `min(2**attempts, 30) + random.uniform(0, 1)` -- gives 1s, 2s, 4s, 8s, 16s, 30s. The jitter is important: without it every vehicle in the fleet would retry at the exact same second after a shared outage.

---

## Fleet Health (`GET /health`)

The spec mentioned ops needing to spot which vehicles are silently failing. The floor implementation already handles durability so the more useful addition seemed to be visibility.

```json
{
  "bus-101": {"status": "healthy", "last_seen": "2024-01-01T12:00:00+00:00", "error_count": 2},
  "bus-102": {"status": "stale",   "last_seen": "2024-01-01T11:59:55+00:00", "error_count": 0},
  "bus-103": {"status": "healthy", "last_seen": "2024-01-01T12:00:01+00:00", "error_count": 1}
}
```

A vehicle is stale if nothing has been heard from it in over 5 seconds (5x the 1s publish cadence). The endpoint lives on the consumer rather than the sidecar because the consumer is the only component that sees traffic from every vehicle.

A couple of known gaps: health state is in-memory and resets on consumer restart (vehicles reappear on their next message), and `error_count` is cumulative since startup rather than a rolling window.

---

## Sidecar Metrics

Every 30 seconds the sidecar logs:
```
METRICS total=N malformed=N valid=N invalid=N avg_lat_ms=N max_lat_ms=N active_connections=N
```

I track both avg and max latency because a high max with a low avg means occasional slow requests eating into the consumer's 3s timeout budget. `active_connections` is normally 1 -- zero means the consumer is down.

---

## Known Limitations

- **At-least-once, not exactly-once.** If the consumer crashes between the sidecar's response arriving and `db_delete()` committing, the message is retried and a duplicate `control/error` can be published for the same id. Narrow window, but worth knowing.
- **No TLS on the consumer-sidecar socket.** Given the spec calls out an unmanaged network, mTLS would be mandatory in production. (TODO in both files.)
- **No dead-letter handling.** A message that consistently fails sidecar validation just retries forever with increasing backoff. A retry cap and quarantine table would fix this.
- **No real hardware bridging.** The sidecar's TCP server stands in for whatever the real protocol would be (RS485, CAN bus, etc.).

---

## AI Usage

I used Claude (claude-sonnet-4-6 via Claude Code) for all the Python code and docker-compose changes. I described the requirements and design decisions; Claude wrote the implementation. I reviewed each file against the spec -- the things I specifically checked were: `INSERT OR IGNORE` used correctly for idempotency, `valid=false` deletes the row rather than retrying, `letter_sum` in the error payload comes from the sidecar response and isn't recomputed locally, and the `isinstance(number, bool)` guard is present (since `isinstance(True, int)` returns `True` in Python, without it `True` would slip through as 1).

The extension direction, the stale threshold, protocol choice, queue storage, where to put the health endpoint -- those were my decisions. Claude drafted this README and I revised it.
