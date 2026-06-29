"""
nemoclaw_orchestrator/agent.py

This is the "NemoClaw orchestrator" in the SoW — the fusion layer.

It consumes the unified event stream (logs + video) and emits alerts.
Two reasoning paths:

1. RULES — deterministic rules that catch the three anchor scenarios.
           Always on; runs in milliseconds.
2. LLM   — calls a local Ollama model to write a one-sentence root-cause
           summary for each alert. Optional; if Ollama is not running, the
           orchestrator just emits the rule-based alert without the summary.

Why this design?
- Rules give us speed, predictability, and easy auditing.
- The LLM gives us readable summaries operators can scan in 2 seconds.
- This matches the SoW: deterministic detectors fire, then RCA agent narrates.

Emitted alerts go to the "alerts" stream. The dashboard subscribes to it.
Every alert is also written to a small SQLite file as the audit trail.
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict, deque
from typing import Optional

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import (
    get_redis, publish_alert, make_event, Event,
    EVENTS_STREAM, ASSETS,
    SEVERITY_INFO, SEVERITY_WARN, SEVERITY_ALERT,
)

# ---------------------------------------------------------------------------
# Audit log (SQLite)
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True)
AUDIT_DB = os.path.join(DATA_DIR, "audit.db")

def init_audit() -> sqlite3.Connection:
    conn = sqlite3.connect(AUDIT_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            asset_id TEXT NOT NULL,
            event_name TEXT NOT NULL,
            severity TEXT NOT NULL,
            payload TEXT NOT NULL,
            llm_summary TEXT,
            evidence_event_ids TEXT
        )
    """)
    conn.commit()
    return conn

def write_audit(conn: sqlite3.Connection, ev: Event, llm_summary: Optional[str], evidence_ids: list[str]):
    conn.execute(
        "INSERT INTO alerts(ts, asset_id, event_name, severity, payload, llm_summary, evidence_event_ids) VALUES (?,?,?,?,?,?,?)",
        (ev.ts, ev.asset_id, ev.event_name, ev.severity, json.dumps(ev.payload), llm_summary, json.dumps(evidence_ids))
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Sliding-window memory of recent events (per asset)
# ---------------------------------------------------------------------------

class EventMemory:
    """Keeps the last N events per asset for fusion logic."""
    def __init__(self, n: int = 50):
        self.window: dict[str, deque] = defaultdict(lambda: deque(maxlen=n))
        self.last_event_id_by_pair: dict[tuple, str] = {}

    def add(self, ev: Event):
        self.window[ev.asset_id].append(ev)

    def recent(self, asset_id: str) -> list[Event]:
        return list(self.window[asset_id])

    def recent_for_assets(self, asset_ids: list[str]) -> list[Event]:
        out = []
        for a in asset_ids:
            out.extend(self.window[a])
        out.sort(key=lambda e: e.ts)
        return out


# ---------------------------------------------------------------------------
# Rule layer — anchor scenarios
# ---------------------------------------------------------------------------

def rule_assay_aborted(ev: Event, mem: EventMemory) -> Optional[Event]:
    """Anchor scenario: ASSAY_RUN_ABORTED → tier-1 alert with evidence from recent DA + rover events."""
    if ev.event_name != "ASSAY_RUN_ABORTED":
        return None
    return make_event(
        asset_id=ev.asset_id,
        event_name="ALERT_ASSAY_RUN_ABORTED",
        payload={
            "rule": "rule_assay_aborted",
            "trigger_event_id": ev.event_id,
            "run_id": ev.payload.get("run_id"),
            "reason": ev.payload.get("reason"),
        },
        severity=SEVERITY_ALERT,
        pipeline="ORCH",
        correlation_id=ev.payload.get("run_id") or ev.event_id,
    )


def rule_rover_stuck_and_empty(ev: Event, mem: EventMemory) -> Optional[Event]:
    """Anchor scenario: log says ROVER_STUCK_AND_EMPTY (speed=0 + EN_ROUTE).

    Strengthen this if Pipeline B also reports VISION_NO_MOTION on the same asset
    in the recent window — that's the cross-pipeline fusion the SoW promises.
    """
    if ev.event_name != "ROVER_STUCK_AND_EMPTY":
        return None
    # Fusion: did Pipeline B see no motion in this asset's zone recently?
    fused = False
    for past in reversed(mem.recent(ev.asset_id)):
        if past.event_name == "VISION_NO_MOTION" and past.pipeline == "B":
            fused = True
            break
    severity = SEVERITY_ALERT if fused else SEVERITY_WARN
    return make_event(
        asset_id=ev.asset_id,
        event_name="ALERT_ROVER_STUCK_AND_EMPTY" + ("_FUSED" if fused else ""),
        payload={
            "rule": "rule_rover_stuck_and_empty",
            "trigger_event_id": ev.event_id,
            "fused_with_vision": fused,
            "pos": ev.payload.get("pos"),
        },
        severity=severity,
        pipeline="ORCH",
        correlation_id=ev.event_id,
    )


def rule_assay_step_slow(ev: Event, mem: EventMemory) -> Optional[Event]:
    """Tier-2 advisory: assay step slower than baseline.

    Upgrade to alert if 2+ consecutive STEP_SLOW events in this asset's window.
    """
    if ev.event_name != "ASSAY_STEP_SLOW":
        return None
    recent = [e for e in mem.recent(ev.asset_id) if e.event_name == "ASSAY_STEP_SLOW"]
    severity = SEVERITY_ALERT if len(recent) >= 2 else SEVERITY_WARN
    return make_event(
        asset_id=ev.asset_id,
        event_name="ALERT_ASSAY_STEP_SLOW",
        payload={
            "rule": "rule_assay_step_slow",
            "trigger_event_id": ev.event_id,
            "step": ev.payload.get("step"),
            "consecutive": len(recent),
        },
        severity=severity,
        pipeline="ORCH",
    )


def rule_barcode_low_conf(ev: Event, mem: EventMemory) -> Optional[Event]:
    """Tier-2 warn: low-confidence barcode read."""
    if ev.event_name != "BARCODE_READ_LOW_CONF":
        return None
    return make_event(
        asset_id=ev.asset_id,
        event_name="ALERT_BARCODE_READ_LOW_CONF",
        payload={
            "rule": "rule_barcode_low_conf",
            "trigger_event_id": ev.event_id,
            "barcode": ev.payload.get("barcode"),
            "conf": ev.payload.get("conf"),
        },
        severity=SEVERITY_WARN,
        pipeline="ORCH",
    )


RULES = [
    rule_assay_aborted,
    rule_rover_stuck_and_empty,
    rule_assay_step_slow,
    rule_barcode_low_conf,
]


# ---------------------------------------------------------------------------
# LLM layer — Ollama
# ---------------------------------------------------------------------------

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

PROMPT_TMPL = """You are a lab automation root-cause analyst. Given the following alert and the recent event history from the affected machine, write ONE concise sentence (max 25 words) that an operator can act on. Do not invent facts — stick to what the events show.

ALERT:
{alert_json}

RECENT EVENTS ON THIS MACHINE (most recent last):
{events_block}

ONE SENTENCE:"""

def llm_summarise(alert: Event, recent: list[Event], timeout: float = 4.0) -> Optional[str]:
    """Call Ollama for a one-sentence summary. Returns None on any failure (offline-safe)."""
    try:
        events_block = "\n".join(
            f"- {e.ts}  {e.pipeline}  {e.event_name}  {json.dumps(e.payload, separators=(',', ':'))}"
            for e in recent[-12:]
        )
        prompt = PROMPT_TMPL.format(
            alert_json=json.dumps({
                "asset_id": alert.asset_id,
                "event_name": alert.event_name,
                "severity": alert.severity,
                "payload": alert.payload,
            }, indent=2),
            events_block=events_block,
        )
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False,
                  "options": {"num_predict": 64, "temperature": 0.2}},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None
        text = (resp.json().get("response") or "").strip().split("\n")[0]
        return text or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main loop — consume events, run rules, optionally summarise, publish alerts
# ---------------------------------------------------------------------------

def run(use_llm: bool = True, from_beginning: bool = False) -> None:
    r = get_redis()
    conn = init_audit()
    mem = EventMemory(n=50)

    print(f"[NemoClaw] starting orchestrator (use_llm={use_llm}, model={OLLAMA_MODEL}, from_beginning={from_beginning})")
    print(f"[NemoClaw] audit DB: {AUDIT_DB}")

    # "$" = only events that arrive after we subscribe.
    # "0" = read from the beginning of the stream (demo replay mode).
    last_id = "0" if from_beginning else "$"
    while True:
        try:
            resp = r.xread({EVENTS_STREAM: last_id}, count=20, block=2000)
        except Exception as e:
            print(f"[NemoClaw] redis read error: {e}; retrying…")
            time.sleep(1.0)
            continue

        if not resp:
            continue
        # resp = [(stream_name, [(msg_id, fields), …])]
        for _, msgs in resp:
            for msg_id, fields in msgs:
                last_id = msg_id
                try:
                    ev = Event.from_redis(fields)
                except Exception as e:
                    print(f"[NemoClaw] bad event {msg_id}: {e}")
                    continue
                mem.add(ev)
                # Run all rules
                for rule in RULES:
                    alert = rule(ev, mem)
                    if not alert:
                        continue
                    summary = None
                    if use_llm:
                        summary = llm_summarise(alert, mem.recent(ev.asset_id))
                        if summary:
                            alert.payload["summary"] = summary
                    # Publish alert and audit
                    publish_alert(r, alert)
                    write_audit(conn, alert, summary, [ev.event_id])
                    print(f"[NemoClaw] ALERT  {alert.severity:5s}  {alert.asset_id:18s}  "
                          f"{alert.event_name}  {summary or '(no summary)'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--no-llm", action="store_true",
                   help="Skip Ollama calls; rules-only mode (still emits alerts).")
    p.add_argument("--from-beginning", action="store_true",
                   help="Process the events stream from the beginning (demo replay mode).")
    args = p.parse_args()
    run(use_llm=not args.no_llm, from_beginning=args.from_beginning)


if __name__ == "__main__":
    main()
