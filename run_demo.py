"""
Single-process end-to-end demo: sensors -> fog -> cloud ingest.

Runs the sensor fleet and the fog node in one process using the in-process
transport, so a marker/demonstrator can see the whole pipeline working with
one command and no broker:

    python run_demo.py --seconds 60 --ingest http://localhost:8000/api/ingest

Use docker-compose (Mosquitto + MQTT transport) for the "realistic" run.
"""
from __future__ import annotations

import argparse
import logging
import time

from fog.node import FogNode
from sensors.config import apply_env_overrides, load_config
from sensors.runner import SensorRunner
from sensors.transport import build_transport


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--ingest", default=None, help="override cloud ingest URL")
    ap.add_argument("--api-key", default=None)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(name)-16s %(message)s")
    cfg = apply_env_overrides(load_config(args.config))
    if args.ingest:
        cfg["cloud"]["ingest_url"] = args.ingest
    if args.api_key:
        cfg["cloud"]["api_key"] = args.api_key

    transport = build_transport(cfg["transport"])
    transport.start()

    fog = FogNode(cfg, transport)
    fog.start()

    sensors = SensorRunner(cfg, transport)
    sensors.start()

    try:
        time.sleep(args.seconds)
    except KeyboardInterrupt:
        pass
    finally:
        sensors.stop()
        time.sleep(1)          # let in-flight messages reach the fog node
        fog.stop()
        transport.stop()
        print("\n=== demo summary ===")
        print(f"readings published by sensors : {sensors.published}")
        print(f"readings received by fog      : {fog.received}")
        print(f"invalid readings dropped      : {fog.validator.dropped}")
        print(f"batches built by fog          : {fog.batches_built}")
        print(f"batches delivered to cloud    : {fog.dispatcher.sent}")
        print(f"anomalies detected at fog     : {fog.anomalies.count}")
        print(f"spool depth at exit           : {fog.dispatcher.spool.depth()}")


if __name__ == "__main__":
    main()
