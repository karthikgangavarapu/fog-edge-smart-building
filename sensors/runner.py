"""
Sensor tier runtime.

Each sensor runs on its own scheduling loop so that sensors with different
frequencies genuinely interleave (a single global tick would quantise them all
to the slowest rate and hide the effect we want to demonstrate).
"""
from __future__ import annotations

import argparse
import logging
import threading
import time
from typing import List

from .config import apply_env_overrides, load_config
from .models import SensorReading
from .sensor_types import build
from .transport import Transport, build_transport

log = logging.getLogger("sensors")


class SensorRunner:
    def __init__(self, cfg: dict, transport: Transport) -> None:
        self.cfg = cfg
        self.transport = transport
        self.prefix = cfg["transport"].get("topic_prefix", "building/hq")
        self.sensors = [
            build(
                s["type"],
                sensor_id=s["id"],
                zone=s["zone"],
                sample_hz=float(s.get("sample_hz", 1.0)),
                dispatch_every=int(s.get("dispatch_every", 1)),
                noise=float(s.get("noise", 0.0)),
                fault_rate=float(s.get("fault_rate", 0.0)),
            )
            for s in cfg["sensors"]
        ]
        self._threads: List[threading.Thread] = []
        self._running = False
        self.published = 0

    def _topic(self, reading: SensorReading) -> str:
        # Hierarchical topic => the fog node can subscribe per zone or per type.
        return f"{self.prefix}/{reading.zone}/{reading.sensor_type}/{reading.sensor_id}"

    def _publish_batch(self, batch: List[SensorReading]) -> None:
        payload = {"readings": [r.to_dict() for r in batch]}
        self.transport.publish(self._topic(batch[0]), payload)
        self.published += len(batch)

    def _sensor_loop(self, sensor) -> None:
        start = time.time()
        next_tick = start
        while self._running:
            now = time.time()
            if now < next_tick:
                time.sleep(min(0.05, next_tick - now))
                continue
            reading = sensor.read(now - start)
            batch = sensor.offer(reading)
            if batch:
                self._publish_batch(batch)
            next_tick += sensor.period

    def start(self) -> None:
        self._running = True
        for sensor in self.sensors:
            th = threading.Thread(target=self._sensor_loop, args=(sensor,), daemon=True)
            th.start()
            self._threads.append(th)
        log.info("started %d virtual sensors", len(self.sensors))

    def stop(self) -> None:
        self._running = False
        for sensor in self.sensors:
            remaining = sensor.flush()
            if remaining:
                self._publish_batch(remaining)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the virtual sensor fleet")
    ap.add_argument("--config", default=None)
    ap.add_argument("--seconds", type=float, default=0, help="0 = run forever")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cfg = apply_env_overrides(load_config(args.config))
    transport = build_transport(cfg["transport"])
    transport.start()
    runner = SensorRunner(cfg, transport)
    runner.start()
    try:
        if args.seconds:
            time.sleep(args.seconds)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        runner.stop()
        transport.stop()
        log.info("published %d readings", runner.published)


if __name__ == "__main__":
    main()
