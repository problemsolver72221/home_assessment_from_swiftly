import http.server
import json
import logging
import os
import random
import socket
import sqlite3
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
SIDECAR_HOST = os.environ.get("SIDECAR_HOST", "localhost")
SIDECAR_PORT = int(os.environ.get("SIDECAR_PORT", 9000))
QUEUE_DB_PATH = os.environ.get("QUEUE_DB_PATH", "/data/pending.db")

# 5x the 1s producer cadence: one missed beat could be noise, five in a row signals down
STALE_THRESHOLD = 5.0
MAX_FRAME_BYTES = 65_536

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("consumer")

# fleet health state, updated by the MQTT callback and TCP worker
last_seen: dict[str, float] = {}
error_count: dict[str, int] = {}
fleet_lock = threading.Lock()

# SQLite durable queue
# check_same_thread=False + _db_lock: MQTT callback and TCP worker share the connection
_db_dir = os.path.dirname(os.path.abspath(QUEUE_DB_PATH))
os.makedirs(_db_dir, exist_ok=True)

_db = sqlite3.connect(QUEUE_DB_PATH, check_same_thread=False)
_db.execute("PRAGMA journal_mode=WAL")
_db.execute("""
    CREATE TABLE IF NOT EXISTS pending_messages (
        id              TEXT PRIMARY KEY,
        payload         TEXT NOT NULL,
        received_at     TEXT NOT NULL,
        attempts        INTEGER NOT NULL DEFAULT 0,
        next_attempt_at REAL    NOT NULL
    )
""")
_db.commit()
_db_lock = threading.Lock()

# Assigned in main() before worker threads start
mqtt_client: mqtt.Client | None = None


def db_enqueue(msg_id: str, payload_str: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    with _db_lock:
        _db.execute(
            "INSERT OR IGNORE INTO pending_messages "
            "(id, payload, received_at, attempts, next_attempt_at) VALUES (?,?,?,0,?)",
            (msg_id, payload_str, now_iso, time.time()),
        )
        _db.commit()


def db_next_due() -> tuple[str, str, int] | None:
    with _db_lock:
        return _db.execute(
            "SELECT id, payload, attempts FROM pending_messages "
            "WHERE next_attempt_at <= ? ORDER BY received_at ASC LIMIT 1",
            (time.time(),),
        ).fetchone()


def db_delete(msg_id: str) -> None:
    with _db_lock:
        _db.execute("DELETE FROM pending_messages WHERE id = ?", (msg_id,))
        _db.commit()


def db_backoff(msg_id: str, attempts: int) -> None:
    # 1s, 2s, 4s, 8s, 16s, 30s (capped) + up to 1s jitter so a fleet doesn't
    # all reconnect in lockstep after a shared outage
    delay = min(2 ** attempts, 30) + random.uniform(0, 1)
    with _db_lock:
        _db.execute(
            "UPDATE pending_messages "
            "SET attempts = attempts + 1, next_attempt_at = ? "
            "WHERE id = ?",
            (time.time() + delay, msg_id),
        )
        _db.commit()
    log.debug("Backed off id=%s for %.1fs (attempt %d)", msg_id, delay, attempts + 1)


_REQUIRED_FIELDS = {"id", "vehicle_id", "letters", "number", "timestamp"}


def _valid_envelope(msg: object) -> bool:
    if not isinstance(msg, dict):
        return False
    return _REQUIRED_FIELDS.issubset(msg.keys())


def on_raw_message(client, userdata, message) -> None:
    raw = message.payload.decode(errors="replace")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("Malformed JSON on control/raw: %s", exc)
        return

    if not _valid_envelope(msg):
        log.warning("Invalid envelope, discarding: %.120s", raw)
        return

    with fleet_lock:
        last_seen[msg["vehicle_id"]] = time.time()

    db_enqueue(msg["id"], raw)
    log.debug("Queued id=%s vehicle=%s", msg["id"], msg["vehicle_id"])


def on_connect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code.is_failure:
        log.error("MQTT connect failed: %s", reason_code)
        return
    log.info("Connected to broker, subscribing to control/raw")
    client.subscribe("control/raw")


def on_disconnect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code != 0:
        log.warning(
            "MQTT unexpected disconnect (code=%s), paho will retry",
            reason_code,
        )


def _publish_error(payload: dict, response: dict) -> None:
    error = {
        "id": payload["id"],
        "letters": payload["letters"],
        "letter_sum": response["letter_sum"],  # from sidecar, not recomputed here
        "number": payload["number"],
        "original_timestamp": payload["timestamp"],
        "error_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    assert mqtt_client is not None  # set in main() before worker threads start
    mqtt_client.publish("control/error", json.dumps(error))
    log.warning(
        "Validation failed id=%s letters=%s sum=%d number=%d",
        payload["id"], payload["letters"], response["letter_sum"], payload["number"],
    )
    with fleet_lock:
        vid = payload.get("vehicle_id", "unknown")
        error_count[vid] = error_count.get(vid, 0) + 1


def tcp_worker() -> None:
    sock: socket.socket | None = None
    conn_file = None  # file-like view of sock for line-oriented reads

    while True:
        row = db_next_due()
        if row is None:
            time.sleep(0.2)
            continue

        msg_id, payload_str, attempts = row

        if sock is None:
            try:
                sock = socket.create_connection((SIDECAR_HOST, SIDECAR_PORT), timeout=5)
                sock.settimeout(3.0)  # per-request timeout
                conn_file = sock.makefile("rb")
                log.info("Connected to sidecar %s:%d", SIDECAR_HOST, SIDECAR_PORT)
            except OSError as exc:
                log.warning("Cannot connect to sidecar: %s", exc)
                db_backoff(msg_id, attempts)
                continue

        assert sock is not None and conn_file is not None

        try:
            sock.sendall((payload_str.rstrip("\n") + "\n").encode("utf-8"))

            # readline(n) reads at most n bytes, stopping at \n
            raw = conn_file.readline(MAX_FRAME_BYTES + 1)
            if not raw.endswith(b"\n"):
                raise ConnectionError("incomplete or oversized response")

            response = json.loads(raw)
            if not {"id", "letter_sum", "valid"}.issubset(response):
                raise ValueError(f"malformed sidecar response: {response!r}")

            # delete whether valid or not; valid=false is a final decision, not a retry
            db_delete(msg_id)

            if not response["valid"]:
                _publish_error(json.loads(payload_str), response)
            else:
                log.debug("Validated OK id=%s", msg_id)

        except (
            socket.timeout, OSError, ConnectionError, ValueError, json.JSONDecodeError
        ) as exc:
            log.warning("Delivery error id=%s: %s, backing off", msg_id, exc)
            try:
                sock.close()
            except OSError:
                pass
            sock = None
            conn_file = None
            db_backoff(msg_id, attempts)


class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        now = time.time()
        with fleet_lock:
            body = {
                vid: {
                    "status": "healthy" if now - ts <= STALE_THRESHOLD else "stale",
                    "last_seen": datetime.fromtimestamp(
                        ts, tz=timezone.utc
                    ).isoformat(),
                    "error_count": error_count.get(vid, 0),
                }
                for vid, ts in last_seen.items()
            }

        data = json.dumps(body, indent=2).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args) -> None:  # noqa: A002
        pass


def _health_server() -> None:
    httpd = http.server.HTTPServer(("0.0.0.0", 8080), HealthHandler)
    log.info("Health endpoint on :8080 (stale threshold %.0fs)", STALE_THRESHOLD)
    httpd.serve_forever()


def main() -> None:
    global mqtt_client

    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="consumer")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_raw_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client = client

    threading.Thread(target=tcp_worker, name="tcp-worker", daemon=True).start()
    threading.Thread(target=_health_server, name="health-server", daemon=True).start()

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except OSError as exc:
            log.warning("Cannot reach broker (%s), retrying in 5s", exc)
            time.sleep(5)

    log.info("Consumer started, queue at %s", QUEUE_DB_PATH)
    client.loop_forever()


if __name__ == "__main__":
    main()

# TODO: TLS on the consumer-sidecar socket
# TODO: dead-letter table for messages that keep failing sidecar validation
# NOTE: at-least-once delivery -- a crash between sidecar response and
# db_delete() can cause a duplicate to be published
