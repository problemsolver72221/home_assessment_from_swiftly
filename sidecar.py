import asyncio
import json
import logging
import os
import re
import time

TCP_PORT = int(os.environ.get("TCP_PORT", 9000))
MAX_FRAME_BYTES = 65_536
METRICS_INTERVAL_S = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [sidecar] %(message)s",
)
log = logging.getLogger("sidecar")

LETTERS_RE = re.compile(r"^[A-Z]{3,5}$")

# all accessed from the single asyncio event-loop thread, no locking needed
_metrics: dict = {
    "total_received": 0,
    "malformed": 0,
    "valid": 0,
    "invalid": 0,
    "active_connections": 0,
    "latencies_ms": [],
}


def letter_sum(letters: str) -> int:
    """A=1...Z=26."""
    return sum(ord(c) - 64 for c in letters)


def validate_message(msg: object) -> tuple[bool, str]:
    """Returns (ok, reason)."""
    if not isinstance(msg, dict):
        return False, "not a JSON object"
    msg_id = msg.get("id")
    letters = msg.get("letters")
    number = msg.get("number")
    if not isinstance(msg_id, str) or not msg_id:
        return False, f"invalid id: {msg_id!r}"
    if not isinstance(letters, str) or not LETTERS_RE.match(letters):
        return False, f"invalid letters: {letters!r}"
    # isinstance(True, int) is True in Python, so bool needs an explicit check
    if (
        not isinstance(number, int)
        or isinstance(number, bool)
        or not (1 <= number <= 200)
    ):
        return False, f"invalid number: {number!r}"
    return True, ""


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    addr = writer.get_extra_info("peername")
    log.info("Connection from %s", addr)
    _metrics["active_connections"] += 1

    try:
        while True:
            try:
                # 30s idle timeout; LimitOverrunError if line > MAX_FRAME_BYTES
                raw = await asyncio.wait_for(reader.readline(), timeout=30.0)
            except asyncio.TimeoutError:
                log.debug("Idle timeout from %s, closing", addr)
                break
            except asyncio.LimitOverrunError:
                log.warning("Oversized frame from %s, closing", addr)
                break

            if not raw:
                break

            t0 = time.perf_counter()

            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                log.warning("UTF-8 decode error from %s: %s", addr, exc)
                _metrics["malformed"] += 1
                continue

            try:
                msg = json.loads(text)
            except json.JSONDecodeError as exc:
                log.warning("JSON parse error from %s: %s", addr, exc)
                _metrics["malformed"] += 1
                # consumer will time out waiting for a response and back off
                continue

            ok, reason = validate_message(msg)
            if not ok:
                log.warning("Schema invalid from %s: %s", addr, reason)
                _metrics["malformed"] += 1
                continue

            lsum = letter_sum(msg["letters"])
            valid = lsum > msg["number"]  # strictly greater than, equal fails

            resp = {"id": msg["id"], "letter_sum": lsum, "valid": valid}
            response = json.dumps(resp) + "\n"
            writer.write(response.encode("utf-8"))
            await writer.drain()

            latency_ms = (time.perf_counter() - t0) * 1000
            _metrics["total_received"] += 1
            _metrics["latencies_ms"].append(latency_ms)
            if valid:
                _metrics["valid"] += 1
            else:
                _metrics["invalid"] += 1

            log.info(
                "id=%s letters=%s sum=%d number=%d valid=%s lat_ms=%.2f",
                msg["id"], msg["letters"], lsum, msg["number"], valid, latency_ms,
            )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        _metrics["active_connections"] -= 1
        log.info("Connection closed from %s", addr)


async def _metrics_reporter() -> None:
    while True:
        await asyncio.sleep(METRICS_INTERVAL_S)
        lats = _metrics["latencies_ms"]
        _metrics["latencies_ms"] = []
        avg_lat = sum(lats) / len(lats) if lats else 0.0
        max_lat = max(lats) if lats else 0.0
        log.info(
            "METRICS total=%d malformed=%d valid=%d invalid=%d "
            "avg_lat_ms=%.2f max_lat_ms=%.2f active_connections=%d",
            _metrics["total_received"],
            _metrics["malformed"],
            _metrics["valid"],
            _metrics["invalid"],
            avg_lat,
            max_lat,
            _metrics["active_connections"],
        )


async def _main() -> None:
    server = await asyncio.start_server(
        handle_client,
        "0.0.0.0",
        TCP_PORT,
        limit=MAX_FRAME_BYTES,
    )
    asyncio.create_task(_metrics_reporter())
    log.info("Sidecar listening on port %d", TCP_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_main())

# TODO: bridge to real onboard hardware (RS485/J1708/CAN bus)
# TODO: TLS on the TCP listener
