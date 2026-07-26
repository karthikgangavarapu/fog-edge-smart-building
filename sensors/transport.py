"""
Pluggable transport between the sensor tier and the fog tier.

Two implementations are provided:

* ``MqttTransport``: real MQTT (paho-mqtt) against Mosquitto/Azure IoT Edge.
  This is what runs in the containerised demo.
* ``InProcTransport``: an in-process queue used by unit tests and by the
  single-process demo runner, so the whole pipeline can be exercised on a
  laptop with no broker installed.

Both expose the same ``publish``/``subscribe`` surface so the sensor and fog
code never needs to know which one is in use (dependency inversion).
"""
from __future__ import annotations

import json
import queue
import threading
from typing import Callable, Dict, List, Optional


class Transport:
    def publish(self, topic: str, payload: dict) -> None:
        raise NotImplementedError

    def subscribe(self, topic_filter: str, handler: Callable[[str, dict], None]) -> None:
        raise NotImplementedError

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class InProcTransport(Transport):
    """Thread-safe in-memory pub/sub with MQTT-style ``+``/``#`` wildcards."""

    def __init__(self) -> None:
        self._subs: List[tuple[str, Callable[[str, dict], None]]] = []
        self._q: "queue.Queue[tuple[str, dict]]" = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    @staticmethod
    def _matches(topic: str, filt: str) -> bool:
        t, f = topic.split("/"), filt.split("/")
        for i, part in enumerate(f):
            if part == "#":
                return True
            if i >= len(t):
                return False
            if part != "+" and part != t[i]:
                return False
        return len(t) == len(f)

    def publish(self, topic: str, payload: dict) -> None:
        self._q.put((topic, payload))

    def subscribe(self, topic_filter: str, handler) -> None:
        self._subs.append((topic_filter, handler))

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while self._running:
            try:
                topic, payload = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            for filt, handler in self._subs:
                if self._matches(topic, filt):
                    handler(topic, payload)

    def drain(self) -> None:
        """Process everything currently queued (used by tests)."""
        while not self._q.empty():
            topic, payload = self._q.get_nowait()
            for filt, handler in self._subs:
                if self._matches(topic, filt):
                    handler(topic, payload)

    def stop(self) -> None:
        self._running = False


class MqttTransport(Transport):
    """MQTT 3.1.1 transport. QoS 1 so the fog node cannot silently lose a batch."""

    def __init__(self, host: str = "localhost", port: int = 1883,
                 client_id: str = "", username: str = "", password: str = "") -> None:
        import paho.mqtt.client as mqtt  # imported lazily: optional dependency

        self._client = mqtt.Client(client_id=client_id or None, clean_session=True)
        if username:
            self._client.username_pw_set(username, password)
        self._client.on_message = self._on_message
        self._handlers: Dict[str, Callable[[str, dict], None]] = {}
        self._host, self._port = host, port

    def _on_message(self, _client, _userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode())
        except (ValueError, UnicodeDecodeError):
            return  # drop malformed frames at the edge rather than at the cloud
        for filt, handler in self._handlers.items():
            handler(msg.topic, payload)

    def publish(self, topic: str, payload: dict) -> None:
        self._client.publish(topic, json.dumps(payload), qos=1)

    def subscribe(self, topic_filter: str, handler) -> None:
        self._handlers[topic_filter] = handler
        self._client.subscribe(topic_filter, qos=1)

    def start(self) -> None:
        self._client.connect(self._host, self._port, keepalive=30)
        self._client.loop_start()

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


def build_transport(cfg: dict) -> Transport:
    kind = cfg.get("kind", "inproc")
    if kind == "mqtt":
        return MqttTransport(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 1883)),
            client_id=cfg.get("client_id", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
        )
    return InProcTransport()
