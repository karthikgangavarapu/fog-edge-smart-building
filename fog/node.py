"""
The virtual fog node.

Responsibilities (mapped to the assignment brief):
  * receive sensor data          -> subscribes to the MQTT topic tree
  * process sensor data          -> validate / smooth / aggregate / detect
  * dispatch payload to backend  -> batched, spooled, retried HTTPS POST

It is deliberately a single small process with no external dependencies beyond
the broker, so it can be containerised onto a real gateway (Raspberry Pi class
hardware) without change.
"""
from __future__ import annotations

import argparse
import logging
import threading
import time
from typing import Optional

from sensors.config import apply_env_overrides, load_config
from sensors.models import FogEnvelope, now_ms
from sensors.transport import Transport, build_transport

from .anomaly import AnomalyEngine
from .dispatcher import CloudDispatcher
from .pipeline import Smoother, Thinner, Validator, WindowAggregator
from .spool import Spool

log = logging.getLogger("fog.node")


class FogNode:
    def __init__(self, cfg: dict, transport: Transport,
                 dispatcher: Optional[CloudDispatcher] = None) -> None:
        fcfg = cfg["fog"]
        self.fog_id = fcfg.get("fog_id", "fog-01")
        self.prefix = cfg["transport"].get("topic_prefix", "building/hq")
        self.validator = Validator()
        self.smoother = Smoother(alpha=float(fcfg.get("ewma_alpha", 0.3)))
        self.aggregator = WindowAggregator(int(fcfg.get("window_seconds", 5)))
        self.thinner = Thinner(int(fcfg.get("thin_raw_to", 20)))
        self.anomalies = AnomalyEngine(
            zscore_threshold=float(fcfg.get("anomaly", {}).get("zscore_threshold", 3.0)),
            min_history=int(fcfg.get("anomaly", {}).get("min_history", 20)),
            cooldown_ms=int(fcfg.get("anomaly", {}).get("cooldown_s", 10)) * 1000,
        )
        self.transport = transport
        self.dispatcher = dispatcher or CloudDispatcher(
            url=cfg["cloud"]["ingest_url"],
            api_key=cfg["cloud"].get("api_key", ""),
            timeout=float(cfg["cloud"].get("timeout_s", 10)),
            max_retries=int(cfg["cloud"].get("max_retries", 5)),
            spool=Spool(fcfg.get("buffer_path", "./fog_spool.db")),
        )
        self.dispatch_interval = float(fcfg.get("dispatch_interval_s", 5))
        self._pending_anomalies: list = []
        self._lock = threading.Lock()
        self._running = False
        self.received = 0
        self.batches_built = 0

    # ---- ingest side ---------------------------------------------------
    def on_message(self, _topic: str, payload: dict) -> None:
        """Called by the transport for every sensor message."""
        for r in payload.get("readings", []):
            self.received += 1
            if not self.validator.accept(r):
                continue
            smoothed = self.smoother.smooth(r["sensor_id"], float(r["value"]))
            with self._lock:
                self.aggregator.add(r, smoothed)
                self.thinner.offer(r, smoothed)
                events = self.anomalies.evaluate(r, smoothed)
                if events:
                    self._pending_anomalies.extend(events)

    # ---- egress side ---------------------------------------------------
    def build_envelope(self) -> Optional[dict]:
        with self._lock:
            start = self.aggregator.window_start
            end = self.aggregator.window_end
            aggregates = self.aggregator.emit()
            anomalies, self._pending_anomalies = self._pending_anomalies, []
            raw = self.thinner.drain()
        if not aggregates and not anomalies:
            return None
        self.batches_built += 1
        env = FogEnvelope(
            fog_id=self.fog_id,
            window_start=start or now_ms(),
            window_end=end or now_ms(),
            aggregates=aggregates,
            anomalies=anomalies,
            raw_sample=raw,
            stats={
                "received": self.received,
                "dropped_invalid": self.validator.dropped,
                "sequence_gaps": self.validator.gaps,
                "anomalies_suppressed": self.anomalies.suppressed,
                "spool_depth": self.dispatcher.spool.depth(),
                "reduction_ratio": round(
                    1 - (len(aggregates) + len(raw)) / max(1, self.received), 4
                ),
            },
        )
        return env.to_dict()

    def _dispatch_loop(self) -> None:
        while self._running:
            time.sleep(self.dispatch_interval)
            env = self.build_envelope()
            if env:
                self.dispatcher.enqueue(env)
                log.info(
                    "batch %s: %d aggregates, %d anomalies, spool=%d",
                    env["batch_id"], len(env["aggregates"]),
                    len(env["anomalies"]), env["stats"]["spool_depth"],
                )

    def start(self) -> None:
        self._running = True
        self.transport.subscribe(f"{self.prefix}/#", self.on_message)
        self.dispatcher.start()
        threading.Thread(target=self._dispatch_loop, daemon=True).start()
        log.info("fog node %s listening on %s/#", self.fog_id, self.prefix)

    def stop(self) -> None:
        self._running = False
        env = self.build_envelope()
        if env:
            self.dispatcher.enqueue(env)
        self.dispatcher.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a virtual fog node")
    ap.add_argument("--config", default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cfg = apply_env_overrides(load_config(args.config))
    transport = build_transport(cfg["transport"])
    transport.start()
    node = FogNode(cfg, transport)
    node.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        transport.stop()


if __name__ == "__main__":
    main()
