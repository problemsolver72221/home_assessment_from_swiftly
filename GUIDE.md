# Project Internals Guide

This document explains how the project works, why it was built the way it was, and what every important piece of code does. Written for someone who has not seen the codebase before.

---

## Table of Contents

1. [The Big Picture](#1-the-big-picture)
2. [How the Three Services Talk to Each Other](#2-how-the-three-services-talk-to-each-other)
3. [Key Concepts You Need to Know First](#3-key-concepts-you-need-to-know-first)
4. [producer.py — Explained Line by Line](#4-producerpy)
5. [consumer.py — Explained Line by Line](#5-consumerpy)
6. [sidecar.py — Explained Line by Line](#6-sidecarpy)
7. [docker-compose.yml — How Everything Starts](#7-docker-composeyml)
8. [tests/test_validation.py — The Tests](#8-teststest_validationpy)
9. [Why X Instead of Y — Design Decisions](#9-why-x-instead-of-y)

---

## 1. The Big Picture

Imagine a fleet of buses. Each bus has a computer onboard (the **producer**) that keeps sending sensor readings. A central server (the **consumer**) receives all those readings and passes each one to a validator (the **sidecar**). The sidecar checks if the reading is valid. If it is not, the bus gets notified.

```
Bus (producer)  --MQTT-->  Consumer  --TCP-->  Sidecar
                                |
                                v
                         SQLite queue
                                |
                                v
                        Bus (control/error topic)
```

There are three producers (bus-101, bus-102, bus-103) running at the same time. They all publish to the same MQTT topic. The consumer reads from that topic, stores messages in a queue, and sends them to the sidecar one by one.

---

## 2. How the Three Services Talk to Each Other

### Producer → Consumer: MQTT

MQTT is a lightweight messaging protocol designed for IoT devices. Think of it like a radio channel. Producers **publish** messages on channel `control/raw`. The consumer **subscribes** to that channel and receives every message.

There is a third piece called the **broker** (Mosquitto) that sits in the middle and routes messages. Producers do not talk directly to the consumer.

```
Producer  -->  Broker (Mosquitto)  -->  Consumer
              (topic: control/raw)
```

### Consumer → Sidecar: TCP (raw socket)

TCP is a basic internet connection, like a phone call between two programs. The consumer opens a socket connection to the sidecar and sends messages one at a time. The sidecar replies with a result. They keep the same connection open for all messages — there is no need to reconnect for every single message.

### Sidecar → Consumer → Producer: MQTT (control/error)

When the sidecar says a message is invalid, the consumer publishes a notification back on the `control/error` topic. The producer subscribes to that topic and logs the error. In a real system this would trigger an alert on the vehicle.

---

## 3. Key Concepts You Need to Know First

### What is a message queue?

A queue is a list of tasks waiting to be processed, in order. The consumer puts each incoming MQTT message into an SQLite database (the queue). A background worker pulls messages from the queue and sends them to the sidecar. The queue is important because:

- If the sidecar is down, messages pile up in the queue and get processed later. Nothing is lost.
- If the consumer itself restarts, the queue is still on disk and work continues.

Without a queue, any downtime in the sidecar means messages are lost forever.

### What is exponential backoff?

When the sidecar is unavailable, we do not want to retry every 0.1 seconds — that wastes CPU and network. Instead we wait longer and longer between retries:

- Attempt 1 fails → wait 1 second
- Attempt 2 fails → wait 2 seconds
- Attempt 3 fails → wait 4 seconds
- ...capped at 30 seconds

The formula is `min(2 ** attempts, 30)`. The `+ random.uniform(0, 1)` adds a small random number (jitter). Without jitter, if 100 vehicles all restart at the same time, they all retry at exactly the same second and flood the server together.

### What is idempotent?

An operation is idempotent if doing it twice gives the same result as doing it once. We use `INSERT OR IGNORE` in SQLite keyed on the message `id`. If the same message arrives twice from MQTT (MQTT guarantees "at least once", so duplicates are possible), the second insert does nothing. The queue stays clean.

### What is WAL mode in SQLite?

By default, SQLite locks the whole database when writing. WAL (Write-Ahead Log) mode allows one writer and multiple readers at the same time without locking each other out. We need this because the TCP worker writes to the DB and the health HTTP server reads from it simultaneously.

### What is asyncio vs threading?

**Threading** runs multiple tasks truly in parallel using OS threads. Python's GIL (Global Interpreter Lock) limits CPU parallelism in Python threads, but for I/O-bound work (waiting for network) threads work fine.

**asyncio** is single-threaded but switches between tasks whenever one is waiting for I/O. It is more efficient for handling many concurrent network connections.

- **consumer.py** uses **threading** — one thread for MQTT, one for the SQLite/TCP worker, one for the health server. Simple and straightforward.
- **sidecar.py** uses **asyncio** — it can handle many simultaneous TCP connections without spawning a thread per connection.

---

## 4. producer.py

The producer simulates a bus onboard computer. It generates a random message every second and publishes it to MQTT.

### The message format

```python
msg = {
    "id": str(uuid.uuid4()),          # unique ID for this message
    "vehicle_id": VEHICLE_ID,          # e.g. "bus-101"
    "letters": "ABZ",                  # 3-5 random uppercase letters
    "number": 142,                     # random int 1-200
    "timestamp": "2024-01-01T...",     # when it was sent
}
```

`uuid.uuid4()` generates a random unique string like `"f47ac10b-58cc-..."`. This is the message's identity. If the same message is processed twice, the consumer can detect it by ID.

### MQTT callbacks

```python
def on_connect(client, userdata, flags, reason_code, properties):
    log.info("Connected to broker, subscribing to control/error")
    client.subscribe("control/error")
```

paho-mqtt calls this function when the connection is established. We re-subscribe here (not just once at startup) because if the connection drops and reconnects, paho drops the subscription. Re-subscribing in `on_connect` handles reconnects automatically.

```python
def on_disconnect(client, userdata, flags, reason_code, properties):
    if reason_code != 0:
        log.warning("Unexpected disconnect (code=%s), paho will retry", reason_code)
```

If the disconnect was unexpected (code != 0), paho will automatically reconnect. We just log it.

### Initial connection retry loop

```python
# broker might not be up yet
while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        break
    except OSError as exc:
        log.warning("Cannot reach broker (%s), retrying in 5s", exc)
        time.sleep(5)
```

When Docker starts all containers simultaneously, the broker (Mosquitto) might not be ready yet. This loop keeps retrying until it connects. Without this, the producer would crash immediately on startup.

### loop_start() vs loop_forever()

```python
client.loop_start()   # used in producer
# vs
client.loop_forever() # used in consumer
```

`loop_start()` starts a background thread that handles the MQTT network connection. The main thread is then free to run the publish loop. `loop_forever()` blocks the current thread and handles MQTT there — used in the consumer because MQTT is the main event loop.

---

## 5. consumer.py

The consumer is the most complex file. It has four jobs running at the same time:

1. **MQTT callback** — receives messages from the broker, validates the envelope, inserts into SQLite
2. **TCP worker** — pulls messages from SQLite, sends to sidecar, handles responses
3. **Health server** — serves `GET /health` on port 8080
4. **MQTT network loop** — handled by paho in the main thread

### Global health state

```python
last_seen: dict[str, float] = {}   # vehicle_id -> unix timestamp
error_count: dict[str, int] = {}   # vehicle_id -> count
fleet_lock = threading.Lock()
```

These dictionaries are shared between the MQTT callback thread and the health server thread. `fleet_lock` prevents both threads from reading/writing at the same time, which could cause corrupted data (a race condition).

### SQLite setup

```python
_db = sqlite3.connect(QUEUE_DB_PATH, check_same_thread=False)
_db.execute("PRAGMA journal_mode=WAL")
```

`check_same_thread=False` tells SQLite to allow the same connection to be used from multiple threads. We manage safety ourselves with `_db_lock`. WAL mode (explained in section 3) lets the health server read the DB while the worker writes.

### The queue functions

```python
def db_enqueue(msg_id, payload_str):
    _db.execute("INSERT OR IGNORE INTO pending_messages ...")
```

`INSERT OR IGNORE` means: if a row with this `id` already exists, silently skip it. This makes duplicate messages from MQTT harmless.

```python
def db_next_due():
    return _db.execute(
        "SELECT ... WHERE next_attempt_at <= ? ORDER BY received_at ASC LIMIT 1",
        (time.time(),)
    ).fetchone()
```

This picks the oldest message that is ready to be processed now (its retry wait time has passed). `LIMIT 1` means we handle one at a time.

```python
def db_backoff(msg_id, attempts):
    delay = min(2 ** attempts, 30) + random.uniform(0, 1)
    _db.execute("UPDATE pending_messages SET attempts = attempts + 1, next_attempt_at = ?", ...)
```

On failure, we push the message's `next_attempt_at` forward by the backoff delay. The message stays in the queue but will not be retried until that time passes.

### Envelope validation

```python
_REQUIRED_FIELDS = {"id", "vehicle_id", "letters", "number", "timestamp"}

def _valid_envelope(msg):
    if not isinstance(msg, dict):
        return False
    return _REQUIRED_FIELDS.issubset(msg.keys())
```

The consumer only checks that the required fields exist. It does not validate their values (e.g. whether `letters` is uppercase). That is intentionally the sidecar's job. The consumer just needs enough to store and route the message.

### The MQTT callback

```python
def on_raw_message(client, userdata, message) -> None:
    raw = message.payload.decode(errors="replace")
    msg = json.loads(raw)
    ...
    with fleet_lock:
        last_seen[msg["vehicle_id"]] = time.time()
    db_enqueue(msg["id"], raw)
```

This function must return as fast as possible. paho-mqtt pauses all other message processing until this callback returns. So we just parse, update health state, and drop it in the queue. All the slow work (talking to the sidecar) happens in the TCP worker.

### The TCP worker

```python
def tcp_worker() -> None:
    sock = None
    conn_file = None

    while True:
        row = db_next_due()
        if row is None:
            time.sleep(0.2)
            continue
        ...
```

The worker loops forever. When there is nothing to do it sleeps 200ms rather than spinning at 100% CPU.

```python
sock = socket.create_connection((SIDECAR_HOST, SIDECAR_PORT), timeout=5)
sock.settimeout(3.0)
conn_file = sock.makefile("rb")
```

`create_connection` opens the TCP socket. `settimeout(3.0)` means any read or write that takes more than 3 seconds raises a `socket.timeout` exception. `makefile("rb")` wraps the socket so we can call `readline()` on it — without this we would have to manually handle the case where a single `recv()` call only returns part of a line.

```python
sock.sendall((payload_str.rstrip("\n") + "\n").encode("utf-8"))
raw = conn_file.readline(MAX_FRAME_BYTES + 1)
if not raw.endswith(b"\n"):
    raise ConnectionError("incomplete or oversized response")
```

We send the message with a newline at the end (our framing protocol). Then we read until we see a newline back. If we get more than 64KB without a newline, something is wrong and we treat it as an error.

```python
# delete whether valid or not -- valid=false means the sidecar made a decision
db_delete(msg_id)

if not response["valid"]:
    _publish_error(json.loads(payload_str), response)
```

Once the sidecar responds (whether valid or not), the message is done — delete it from the queue. `valid=false` means the sidecar looked at the message, computed the result, and decided it failed. That is a final answer, not a reason to retry. We only retry on connection errors, not on validation failures.

### The health server

```python
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        now = time.time()
        with fleet_lock:
            body = {
                vid: {
                    "status": "healthy" if now - ts <= STALE_THRESHOLD else "stale",
                    ...
                }
                for vid, ts in last_seen.items()
            }
```

`do_GET` is called by Python's built-in HTTP server every time a GET request arrives. We check each vehicle's last seen time. If it has been more than 5 seconds since we last heard from it, it is stale.

```python
def log_message(self, format, *args):
    pass
```

`BaseHTTPRequestHandler` prints every request to stdout by default. Overriding `log_message` with an empty function silences it to keep logs clean.

---

## 6. sidecar.py

The sidecar is an asyncio TCP server. It receives messages from the consumer, validates them, and sends back a result.

### Why asyncio here?

The sidecar can handle multiple connections at once (e.g. if you have multiple consumers). asyncio lets it handle many connections in a single thread by switching between them whenever one is waiting for data. Threading would need one thread per connection.

### Validation

```python
def letter_sum(letters: str) -> int:
    """A=1...Z=26."""
    return sum(ord(c) - 64 for c in letters)
```

`ord('A')` returns 65. `ord('A') - 64 = 1`. `ord('B') - 64 = 2`. And so on. This converts each letter to its alphabetical position and sums them.

```python
def validate_message(msg: object) -> tuple[bool, str]:
    ...
    # isinstance(True, int) is True in Python, so bool needs an explicit check
    if not isinstance(number, int) or isinstance(number, bool) or not (1 <= number <= 200):
        return False, f"invalid number: {number!r}"
```

In Python, `bool` is a subclass of `int`. That means `isinstance(True, int)` returns `True`. Without the extra `isinstance(number, bool)` check, a message with `"number": true` (JSON boolean) would pass the int check and get treated as 1. The explicit check rejects it.

### The connection handler

```python
async def handle_client(reader, writer) -> None:
    while True:
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=30.0)
        except asyncio.TimeoutError:
            break
        except asyncio.LimitOverrunError:
            break
```

`await` means "pause here until the data arrives, and let other connections run in the meantime". `wait_for(..., timeout=30.0)` automatically cancels the read after 30 seconds of silence, which closes stale connections from crashed consumers. `LimitOverrunError` is raised when a line exceeds 64KB — we close that connection too.

```python
lsum = letter_sum(msg["letters"])
valid = lsum > msg["number"]  # strictly greater than, equal fails

response = json.dumps({"id": msg["id"], "letter_sum": lsum, "valid": valid}) + "\n"
writer.write(response.encode("utf-8"))
await writer.drain()
```

`valid` is `True` only if the letter sum is strictly greater than the number. Equal is not valid. We always include `letter_sum` in the response so the consumer can put it in the error message without recomputing it.

`writer.drain()` flushes the send buffer. Without it, the response might sit in an internal buffer and never actually be sent.

### Metrics reporter

```python
async def _metrics_reporter() -> None:
    while True:
        await asyncio.sleep(METRICS_INTERVAL_S)
        ...
        log.info("METRICS total=%d malformed=%d valid=%d ...", ...)
```

Every 30 seconds this coroutine wakes up and logs statistics. The `_metrics` dict is safe to access without locking because asyncio is single-threaded — only one coroutine runs at a time.

---

## 7. docker-compose.yml

Docker Compose starts all services and connects them on a shared network.

```yaml
producer-bus-101:
  build: .
  command: python producer.py
  environment:
    - VEHICLE_ID=bus-101
    - MQTT_HOST=mosquitto
```

Each producer is the same Docker image but gets a different `VEHICLE_ID` environment variable. `MQTT_HOST=mosquitto` is the container name of the broker — Docker's internal DNS resolves container names automatically.

```yaml
consumer:
  environment:
    - QUEUE_DB_PATH=/data/pending.db
  volumes:
    - consumer-data:/data
  ports:
    - "8080:8080"
```

`QUEUE_DB_PATH=/data/pending.db` tells the consumer where to store the SQLite database. `consumer-data:/data` mounts a named Docker volume at `/data`. Named volumes persist even if the container is deleted and recreated — that is what gives the queue durability across restarts.

```yaml
volumes:
  consumer-data:
```

This declares the named volume. Without this block, `consumer-data:/data` would fail.

---

## 8. tests/test_validation.py

The tests copy the `letter_sum` and `validate_message` functions directly (no imports from sidecar.py) so they run without Docker and without installing paho-mqtt.

Key tests worth understanding:

```python
def test_number_as_bool_rejected(self):
    # bool is a subclass of int in Python, so isinstance(True, int) is True.
    # The explicit bool check is load-bearing: without it True (==1) would pass.
    ok, _ = validate_message({**self._base(), "number": True})
    self.assertFalse(ok)
```

This tests the edge case described in section 6 above. `True` looks like 1 to Python's type system, so we must explicitly reject it.

```python
def test_sum_equal_to_number_fails(self):
    # strictly greater than, not >=
    self.assertFalse(letter_sum("ABC") > 6)  # 6 == 6
```

Equal is a failure case. The rule is `letter_sum > number`, not `>=`.

---

## 9. Why X Instead of Y — Design Decisions

### Why MQTT instead of HTTP for producers?

MQTT is designed for IoT devices with unreliable connections and low power. It keeps a persistent connection to the broker and reconnects automatically. HTTP would require each bus to open a new connection for every message, which is slower and more fragile on unstable networks.

### Why SQLite instead of an in-memory queue (like a Python list)?

An in-memory list is lost when the process restarts. SQLite persists to disk. If the consumer crashes, all messages are still in the database when it comes back up.

### Why SQLite instead of Redis or Postgres?

SQLite needs zero infrastructure — no separate container, no configuration. For a single-consumer queue this is the simplest option that works. Redis or Postgres would be overkill and add operational complexity.

### Why keep a persistent TCP connection to the sidecar instead of reconnecting per message?

Opening a TCP connection involves a network round-trip (the TCP handshake). For a message arriving every second, reconnecting each time wastes time and resources. Keeping one connection open and reusing it is much more efficient.

### Why newline-delimited JSON instead of a binary protocol?

Binary protocols (like length-prefixed frames) are more efficient but harder to debug. With newline-delimited JSON you can test the sidecar directly from a terminal:

```bash
echo '{"id":"test","letters":"ABC","number":5}' | nc localhost 9000
```

For internal service communication where performance is not critical, human-readable is better.

### Why asyncio in sidecar but threading in consumer?

The sidecar is a server that could handle many connections. asyncio scales to thousands of connections in one thread. The consumer has exactly three things running (MQTT, TCP worker, health server) and threading is simpler to reason about for that case.

### Why put the health endpoint on the consumer instead of the sidecar?

The consumer sees every MQTT message from every vehicle. It is the only component that knows which vehicles are alive and which are not. The sidecar only sees messages after the consumer forwards them — it has no idea which vehicle sent each message.

### Why is valid=false a terminal result (delete from queue) instead of a retry?

A validation failure means the message content is wrong — wrong `letters`, wrong `number` range. Retrying would send the exact same bad data again and get the same `valid=false` back forever. It is not a temporary error like a network blip; it is a definitive decision from the sidecar. The right action is to log/notify (publish to `control/error`) and move on.
