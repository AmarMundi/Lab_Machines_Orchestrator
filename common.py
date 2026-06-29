"""
common.py — shared schema and helpers used by every component.

The whole system speaks a single event vocabulary defined here.
"""
from __future__ import annotations
import json
import os
import time
import uuid
from dataclasses import dataclass, asdict, field
from typing import Optional

import redis

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Streams
EVENTS_STREAM = "events"        # Pipeline A and B publish here
ALERTS_STREAM = "alerts"        # NemoClaw orchestrator publishes here
STATE_KEY_PREFIX = "asset_state:"   # one HASH per asset for current state

# Asset registry — names match the Sandbox Loop diagram
ASSETS = {
    "SAMPLE_STORAGE_01": {"label": "Sample storage",   "color": "#378ADD", "row": 0, "col": 0},
    "ROBOTIC_VISION_01": {"label": "Robotic vision",   "color": "#D85A30", "row": 0, "col": 1},
    "ROVER_A":           {"label": "Rover A",          "color": "#5BAA72", "row": 0, "col": 2},
    "AMR_01":            {"label": "AMR",              "color": "#7F77DD", "row": 1, "col": 0},
    "PLATE_STORAGE_01":  {"label": "Plate storage",    "color": "#888780", "row": 1, "col": 1},
    "DA_01":             {"label": "Dilution / assay", "color": "#1F6F6F", "row": 1, "col": 2},
}

# Event-name contract (machine name → human label) — from section 5.2 of the SoW
EVENT_LABELS = {
    "ORDER_RECEIVED":               "Order received",
    "SAMPLE_PICKED_BY_STORAGE":     "Sample rack picked from storage",
    "SAMPLE_PICKED_BY_ROBOT_1":     "Robotic arm picked rack",
    "BARCODE_READ_OK":              "Rack barcode read successfully",
    "BARCODE_READ_LOW_CONF":        "Barcode read with low confidence",
    "ROVER_RESERVATION_OPEN":       "Rover assigned to rack",
    "ROVER_RESERVATION_HELD":       "Rover holding reservation longer than expected",
    "ROVER_STUCK_AND_EMPTY":        "Rover stationary with no rack",
    "PLATE_HANDOFF_TIMEOUT":        "Plate handoff did not complete",
    "ASSAY_RUN_STARTED":            "Assay run started",
    "ASSAY_STEP_SLOW":              "Assay step running slower than baseline",
    "ASSAY_RUN_COMPLETE":           "Assay run complete",
    "ASSAY_RUN_ABORTED":            "Assay run aborted",
    "ASSAY_RUN_INCOMPLETE":         "Assay run did not complete",
    "RACK_RETURNED_TO_STORAGE":     "Rack returned to storage",
    # Pipeline B (vision) events
    "VISION_DETECTION":             "Object(s) detected in zone",
    "VISION_NO_MOTION":             "Zone idle (no motion)",
    "VISION_OCCLUSION":             "Object occluded — view blocked",
}

SEVERITY_INFO  = "info"
SEVERITY_WARN  = "warn"
SEVERITY_ALERT = "alert"

# Each event name carries a default severity. The orchestrator can upgrade.
DEFAULT_SEVERITY = {
    "BARCODE_READ_LOW_CONF":   SEVERITY_WARN,
    "ROVER_RESERVATION_HELD":  SEVERITY_WARN,
    "ROVER_STUCK_AND_EMPTY":   SEVERITY_ALERT,
    "PLATE_HANDOFF_TIMEOUT":   SEVERITY_ALERT,
    "ASSAY_STEP_SLOW":         SEVERITY_WARN,
    "ASSAY_RUN_ABORTED":       SEVERITY_ALERT,
    "ASSAY_RUN_INCOMPLETE":    SEVERITY_ALERT,
}

# ---------------------------------------------------------------------------
# Event dataclass
# ---------------------------------------------------------------------------

@dataclass
class Event:
    """The single shape every component speaks."""
    event_id: str
    asset_id: str
    event_name: str
    ts: str                              # ISO-8601 with offset
    severity: str = SEVERITY_INFO
    payload: dict = field(default_factory=dict)
    pipeline: str = "A"                  # "A" = logs, "B" = video, "ORCH" = orchestrator
    correlation_id: Optional[str] = None # carried through fusion

    def to_redis(self) -> dict:
        """Redis Streams accepts only string fields, so we stringify."""
        return {
            "event_id": self.event_id,
            "asset_id": self.asset_id,
            "event_name": self.event_name,
            "ts": self.ts,
            "severity": self.severity,
            "payload": json.dumps(self.payload, separators=(",", ":")),
            "pipeline": self.pipeline,
            "correlation_id": self.correlation_id or "",
        }

    @classmethod
    def from_redis(cls, fields: dict) -> "Event":
        return cls(
            event_id=fields["event_id"],
            asset_id=fields["asset_id"],
            event_name=fields["event_name"],
            ts=fields["ts"],
            severity=fields.get("severity", SEVERITY_INFO),
            payload=json.loads(fields.get("payload") or "{}"),
            pipeline=fields.get("pipeline", "A"),
            correlation_id=fields.get("correlation_id") or None,
        )


def make_event(asset_id: str, event_name: str, payload: dict | None = None,
               severity: str | None = None, pipeline: str = "A",
               correlation_id: str | None = None, ts: str | None = None) -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        asset_id=asset_id,
        event_name=event_name,
        ts=ts or _now_iso(),
        severity=severity or DEFAULT_SEVERITY.get(event_name, SEVERITY_INFO),
        payload=payload or {},
        pipeline=pipeline,
        correlation_id=correlation_id,
    )


def _now_iso() -> str:
    """ISO-8601 with the system's local offset."""
    import datetime as _dt
    return _dt.datetime.now().astimezone().isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def get_redis() -> redis.Redis:
    """A decoded-string Redis client. Decode at the boundary for sanity."""
    return redis.from_url(REDIS_URL, decode_responses=True)


def publish_event(r: redis.Redis, event: Event) -> str:
    """Publish to the events stream and update the asset's current-state hash."""
    msg_id = r.xadd(EVENTS_STREAM, event.to_redis(), maxlen=10000, approximate=True)
    # Update the asset's current state — the dashboard reads this for the live tile.
    r.hset(STATE_KEY_PREFIX + event.asset_id, mapping={
        "asset_id": event.asset_id,
        "last_event": event.event_name,
        "last_ts": event.ts,
        "last_severity": event.severity,
        "last_payload": json.dumps(event.payload),
    })
    return msg_id


def publish_alert(r: redis.Redis, event: Event) -> str:
    """Alerts are a separate stream so the dashboard can filter cleanly."""
    return r.xadd(ALERTS_STREAM, event.to_redis(), maxlen=2000, approximate=True)


def get_all_asset_state(r: redis.Redis) -> dict:
    """Return current state for every asset, used to render the dashboard initially."""
    out = {}
    for asset_id in ASSETS:
        state = r.hgetall(STATE_KEY_PREFIX + asset_id)
        out[asset_id] = state or {
            "asset_id": asset_id,
            "last_event": "—",
            "last_ts": "",
            "last_severity": "info",
            "last_payload": "{}",
        }
    return out
