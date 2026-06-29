"""
pipeline_logs/tailer.py — Pipeline A.

For each asset's log file, parse line-by-line and emit Event objects on the
shared Redis stream. In demo mode, we replay the file at a chosen speed so
the dashboard shows live activity even though the fixture is a recording.

Usage:
  python -m pipeline_logs.tailer --speed 60   # 60x — 24h replays in 24min
  python -m pipeline_logs.tailer --speed 240  # 240x — fits in ~6min

Parser strategy: each asset has its own line-shape, so we use small per-asset
regexes. This is intentionally simple and replaceable; a real deployment would
use a Flink job or similar.
"""
from __future__ import annotations
import argparse
import os
import re
import sys
import time
import datetime as dt
from typing import Optional, Iterator

# Allow running both as `python -m pipeline_logs.tailer` and `python tailer.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import (
    get_redis, publish_event, make_event,
    SEVERITY_INFO, SEVERITY_WARN, SEVERITY_ALERT, ASSETS,
)

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures", "logs")

# ---------------------------------------------------------------------------
# Parsers — one per asset class
# ---------------------------------------------------------------------------

# Common ISO8601 prefix
ISO_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2})\s+")
# Hamilton-Venus bracket prefix: [2026-04-24 08:25:30.114]
BRACK_RE = re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})\]\s+")

def _parse_iso_ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)

def _parse_brack_ts(s: str) -> dt.datetime:
    # Treat as local time with the system offset (matches generator)
    naive = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    return naive.astimezone()


def parse_sample_storage(line: str) -> Optional[dict]:
    """Sample storage: PICK_ORD, PICK_OUT, RETURN_IN."""
    m = ISO_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_iso_ts(m.group("ts"))

    if "PICK_ORD" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ORDER_RECEIVED",
                "payload": {"order_id": d.get("order"), "rack_id": d.get("rack"),
                            "barcode": d.get("barcode")}}
    if "PICK_OUT" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "SAMPLE_PICKED_BY_STORAGE",
                "payload": {"order_id": d.get("order"), "rack_id": d.get("rack"),
                            "duration_ms": _int(d.get("duration_ms"))}}
    if "RETURN_IN" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "RACK_RETURNED_TO_STORAGE",
                "payload": {"rack_id": d.get("rack"), "barcode": d.get("barcode")}}
    return None


def parse_robotic_vision(line: str) -> Optional[dict]:
    """Robotic vision: BARCODE reads, PLACE, REST_OUT."""
    m = ISO_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_iso_ts(m.group("ts"))

    if "BARCODE" in rest and "WARN" in line:
        d = _kv(rest)
        return {"ts": ts, "event_name": "BARCODE_READ_LOW_CONF",
                "payload": {"barcode": d.get("read"), "conf": _float(d.get("conf"))},
                "severity": SEVERITY_WARN}
    if "BARCODE" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "BARCODE_READ_OK",
                "payload": {"barcode": d.get("read"), "conf": _float(d.get("conf"))}}
    if "PLACE" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "SAMPLE_PICKED_BY_ROBOT_1",
                "payload": {"rack_id": d.get("rack"), "dest": d.get("dest")}}
    return None


def parse_rover(line: str) -> Optional[dict]:
    """Rover: RES_OPEN, RES_TIMEOUT, TELE."""
    m = ISO_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_iso_ts(m.group("ts"))

    if "RES_OPEN" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ROVER_RESERVATION_OPEN",
                "payload": {"res_id": d.get("res_id"), "rack_id": d.get("rack")}}
    if "RES_TIMEOUT" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ROVER_RESERVATION_HELD",
                "payload": {"res_id": d.get("res_id"), "reason": d.get("reason")},
                "severity": SEVERITY_WARN}
    if "TELE" in rest:
        d = _kv(rest)
        # We emit a low-level event only when the rover is stuck (speed=0 + EN_ROUTE).
        # Otherwise telemetry is just stored as the asset's last_event but not alerted.
        if d.get("speed") == "0.00" and d.get("task_state") == "EN_ROUTE":
            return {"ts": ts, "event_name": "ROVER_STUCK_AND_EMPTY",
                    "payload": {"pos": d.get("pos"), "battery": _int(d.get("battery"))},
                    "severity": SEVERITY_ALERT}
        # otherwise emit as a generic info heartbeat — useful for dashboard "alive" indicator
        return {"ts": ts, "event_name": "ROVER_RESERVATION_OPEN",  # reuse — keeps event vocab small
                "payload": {"pos": d.get("pos"), "speed": d.get("speed"),
                            "task_state": d.get("task_state"), "battery": _int(d.get("battery"))},
                "severity": SEVERITY_INFO}
    return None


def parse_amr(line: str) -> Optional[dict]:
    """AMR: PLATE_ORD."""
    m = ISO_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_iso_ts(m.group("ts"))
    if "PLATE_ORD" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ORDER_RECEIVED",
                "payload": {"order_id": d.get("order"), "plate_id": d.get("plate"),
                            "barcode": d.get("barcode"), "src": d.get("src"), "dst": d.get("dst")}}
    return None


def parse_plate_storage(line: str) -> Optional[dict]:
    """Plate storage: RETRIEVE_REQ, RETRIEVE_OK."""
    m = ISO_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_iso_ts(m.group("ts"))
    if "RETRIEVE_REQ" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ORDER_RECEIVED",
                "payload": {"plate_id": d.get("plate"), "slot": d.get("slot")}}
    if "RETRIEVE_OK" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "SAMPLE_PICKED_BY_STORAGE",
                "payload": {"plate_id": d.get("plate"), "duration_ms": _int(d.get("duration_ms"))}}
    return None


def parse_da(line: str) -> Optional[dict]:
    """Dilution/assay: RUN_START / STEP_BEGIN / STEP_END / RUN_END / RUN_ABORT / STEP_SLOW / BARCODE_MISMATCH."""
    m = BRACK_RE.match(line)
    if not m: return None
    rest = line[m.end():]
    ts = _parse_brack_ts(m.group("ts"))

    if "RUN_START" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ASSAY_RUN_STARTED",
                "payload": {"run_id": d.get("run_id"), "method": d.get("method")}}
    if "RUN_ABORT" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ASSAY_RUN_ABORTED",
                "payload": {"run_id": d.get("run_id"), "reason": d.get("reason")},
                "severity": SEVERITY_ALERT}
    if "STEP_SLOW" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ASSAY_STEP_SLOW",
                "payload": {"step": d.get("step"),
                            "elapsed_ms": _int(d.get("elapsed_ms"))},
                "severity": SEVERITY_WARN}
    if "RUN_END" in rest:
        d = _kv(rest)
        return {"ts": ts, "event_name": "ASSAY_RUN_COMPLETE",
                "payload": {"run_id": d.get("run_id"), "status": d.get("status")}}
    return None


PARSERS = {
    "SAMPLE_STORAGE_01": parse_sample_storage,
    "ROBOTIC_VISION_01": parse_robotic_vision,
    "ROVER_A":           parse_rover,
    "AMR_01":            parse_amr,
    "PLATE_STORAGE_01":  parse_plate_storage,
    "DA_01":             parse_da,
}


# ---------------------------------------------------------------------------
# Tiny key=value parser (handles the loose log shape we generated)
# ---------------------------------------------------------------------------

KV_RE = re.compile(r"(\w+)=([^\s]+)")
def _kv(s: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in KV_RE.finditer(s)}

def _int(s: Optional[str]) -> Optional[int]:
    try: return int(s) if s is not None else None
    except (TypeError, ValueError): return None

def _float(s: Optional[str]) -> Optional[float]:
    try: return float(s) if s is not None else None
    except (TypeError, ValueError): return None


# ---------------------------------------------------------------------------
# Replay loop
# ---------------------------------------------------------------------------

def iter_log(path: str) -> Iterator[str]:
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line:
                yield line


def replay(speed: float = 60.0) -> None:
    """Replay all asset logs in time order at `speed` × real time."""
    r = get_redis()

    # Build a unified, time-sorted iterator of (ts, asset_id, event_dict)
    print(f"[Pipeline A] reading log files from {LOG_DIR}")
    all_events: list[tuple[dt.datetime, str, dict]] = []
    for asset_id, parser in PARSERS.items():
        # Find the log file for this asset (any date)
        for fname in os.listdir(LOG_DIR):
            if fname.startswith(asset_id):
                path = os.path.join(LOG_DIR, fname)
                count = 0
                for line in iter_log(path):
                    parsed = parser(line)
                    if parsed:
                        all_events.append((parsed["ts"], asset_id, parsed))
                        count += 1
                print(f"  {asset_id:22s} {count:5d} events from {fname}")
    all_events.sort(key=lambda x: x[0])
    print(f"[Pipeline A] total {len(all_events)} events to replay at {speed}× speed")

    if not all_events:
        print("[Pipeline A] no events parsed — did you run generate_fixtures.py?")
        return

    start_real = time.monotonic()
    start_log = all_events[0][0]
    for ts, asset_id, parsed in all_events:
        # Sleep until ts maps to wall-clock
        log_elapsed = (ts - start_log).total_seconds()
        target_real = start_real + (log_elapsed / speed)
        sleep_for = target_real - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

        ev = make_event(
            asset_id=asset_id,
            event_name=parsed["event_name"],
            payload=parsed["payload"],
            severity=parsed.get("severity"),
            pipeline="A",
            ts=ts.isoformat(timespec="milliseconds"),
        )
        publish_event(r, ev)
    print("[Pipeline A] replay complete")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--speed", type=float, default=60.0,
                   help="Replay speed multiplier (60 = 24h-in-24min, 240 = ~6min)")
    args = p.parse_args()
    replay(speed=args.speed)


if __name__ == "__main__":
    main()
