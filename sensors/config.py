"""Configuration loading. YAML if PyYAML is available, JSON fallback otherwise."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

DEFAULT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "sensors.yaml")


def load_config(path: str | None = None) -> Dict[str, Any]:
    path = path or os.environ.get("FOG_CONFIG", DEFAULT_PATH)
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    try:
        import yaml  # type: ignore
        return yaml.safe_load(text)
    except ImportError:
        # Allows the project to run in a bare container with no PyYAML.
        return json.loads(text)


def apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Twelve-factor style overrides so the same image can run locally and in the
    cloud without editing files: FOG_INGEST_URL, FOG_API_KEY, FOG_MQTT_HOST...
    """
    if os.environ.get("FOG_INGEST_URL"):
        cfg["cloud"]["ingest_url"] = os.environ["FOG_INGEST_URL"]
    if os.environ.get("FOG_API_KEY"):
        cfg["cloud"]["api_key"] = os.environ["FOG_API_KEY"]
    if os.environ.get("FOG_MQTT_HOST"):
        cfg["transport"]["kind"] = "mqtt"
        cfg["transport"]["host"] = os.environ["FOG_MQTT_HOST"]
    return cfg
