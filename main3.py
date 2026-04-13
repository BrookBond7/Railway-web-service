from __future__ import annotations

import json
import os
import ssl
import threading
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import paho.mqtt.client as mqtt

app = FastAPI(title="MES MQTT Bridge")

MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = int(os.getenv("MQTT_PORT", "15624"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "transport/yo/mes/#")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mes-fastapi-bridge")
MQTT_USE_TLS = os.getenv("MQTT_USE_TLS", "false").lower() == "true"

latest_by_topic: dict[str, dict[str, Any]] = {}
mqtt_connected = False
mqtt_last_error: str | None = None


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def extract_value(payload: Any) -> Any:
    if isinstance(payload, dict):
        return payload.get("v", payload)
    return payload


def topic_record(topic: str, item: dict[str, Any]) -> dict[str, Any]:
    payload = item.get("payload")
    return {
        "topic": topic,
        "node": topic.split("/")[-1],
        "value": extract_value(payload),
        "retain": item.get("retain"),
        "timestamp": item.get("timestamp"),
    }


def on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: dict,
    reason_code: Any,
    properties: Any = None,
) -> None:
    global mqtt_connected, mqtt_last_error

    if str(reason_code) == "Success":
        mqtt_connected = True
        mqtt_last_error = None
        print(f"Connected. Subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        mqtt_connected = False
        mqtt_last_error = f"MQTT connect failed with rc={reason_code}"
        print(mqtt_last_error)


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    disconnect_flags: Any,
    reason_code: Any,
    properties: Any = None,
) -> None:
    global mqtt_connected, mqtt_last_error
    mqtt_connected = False
    mqtt_last_error = f"Disconnected: {reason_code}"
    print(mqtt_last_error)


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    payload_raw = msg.payload.decode("utf-8", errors="replace")

    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = payload_raw

    latest_by_topic[msg.topic] = {
        "topic": msg.topic,
        "qos": msg.qos,
        "retain": msg.retain,
        "payload": payload,
        "payload_raw": payload_raw,
        "timestamp": utc_now(),
    }

    print(f"RECEIVED topic={msg.topic} retain={msg.retain} payload={payload_raw}")


def mqtt_worker() -> None:
    global mqtt_last_error

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311,
    )

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    if MQTT_USE_TLS:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        print(
            f"Connecting to MQTT broker host={MQTT_HOST} "
            f"port={MQTT_PORT} tls={MQTT_USE_TLS} topic={MQTT_TOPIC}"
        )
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_forever()
    except Exception as exc:
        mqtt_last_error = str(exc)
        print(f"MQTT worker error: {exc!r}")


@app.on_event("startup")
def startup_event() -> None:
    thread = threading.Thread(target=mqtt_worker, daemon=True)
    thread.start()


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "MES MQTT Bridge",
        "status": "running",
        "endpoints": [
            "/health",
            "/api/mes/yo",
            "/api/mes/yo/lld2",
            "/api/mes/yo/lld3",
            "/api/mes/yo/lld4",
            "/api/mes/yo/lld5",
            "/privacy",
        ],
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "mqtt_connected": mqtt_connected,
        "mqtt_last_error": mqtt_last_error,
        "topics_seen": len(latest_by_topic),
        "subscription": MQTT_TOPIC,
        "broker": MQTT_HOST,
        "port": MQTT_PORT,
        "tls": MQTT_USE_TLS,
    }


@app.get("/api/mes/yo")
def api_mes_yo() -> dict[str, Any]:
    prefix = "transport/yo/mes/"
    items = []

    for topic, item in latest_by_topic.items():
        if topic.startswith(prefix):
            items.append(topic_record(topic, item))

    return {
        "site": "yo",
        "count": len(items),
        "items": sorted(items, key=lambda x: x["node"]),
    }


@app.get("/api/mes/yo/{node}")
def api_mes_yo_node(node: str) -> dict[str, Any]:
    node = node.lower()
    topic = f"transport/yo/mes/{node}"

    if topic not in latest_by_topic:
        raise HTTPException(status_code=404, detail=f"No data for topic: {topic}")

    return {
        "site": "yo",
        **topic_record(topic, latest_by_topic[topic]),
    }


@app.get("/privacy", response_class=HTMLResponse)
def privacy() -> str:
    return """
    <html>
      <head>
        <title>Privacy Policy</title>
      </head>
      <body>
        <h1>Privacy Policy</h1>
        <p>This service provides read-only access to MES status data from a privately operated MQTT-to-API bridge.</p>
        <p>When a user asks for live MES status, the GPT may send a request to this service to retrieve current MES node messages.</p>
        <p>This service may process request metadata and query parameters needed to return MES status.</p>
        <p>No write operations are performed against MQTT, MES, or control systems.</p>
        <p>Data is used only to return MES status responses through the GPT action.</p>
        <p>Operator: Douglas Poole</p>
        <p>Contact: your-email@example.com</p>
      </body>
    </html>
    """
