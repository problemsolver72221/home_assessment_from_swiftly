import json
import logging
import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
VEHICLE_ID = os.environ.get("VEHICLE_ID", "bus-000")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(f"producer.{VEHICLE_ID}")


def on_connect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code.is_failure:
        log.error("Connection failed: %s", reason_code)
        return
    log.info("Connected to broker, subscribing to control/error")
    # re-subscribe on reconnect, paho drops subs on disconnect
    client.subscribe("control/error")


def on_disconnect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code != 0:
        log.warning("Unexpected disconnect (code=%s), paho will retry", reason_code)


def on_message(client, userdata, message) -> None:
    log.warning("control/error received: %s", message.payload.decode(errors="replace"))


def main() -> None:
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"producer-{VEHICLE_ID}",
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    # paho retries automatically in loop_start(); these set the delay bounds
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # broker might not be up yet
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except OSError as exc:
            log.warning("Cannot reach broker (%s), retrying in 5s", exc)
            time.sleep(5)

    client.loop_start()  # background thread handles network I/O and reconnects

    while True:
        msg = {
            "id": str(uuid.uuid4()),
            "vehicle_id": VEHICLE_ID,
            "letters": "".join(
                random.choices(string.ascii_uppercase, k=random.randint(3, 5))
            ),
            "number": random.randint(1, 200),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        rc = client.publish("control/raw", json.dumps(msg))
        if rc.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("Publish failed (rc=%d) id=%s", rc.rc, msg["id"])
        else:
            log.info(
                "published id=%s vehicle=%s letters=%s number=%d",
                msg["id"], VEHICLE_ID, msg["letters"], msg["number"],
            )
        time.sleep(1)


if __name__ == "__main__":
    main()
