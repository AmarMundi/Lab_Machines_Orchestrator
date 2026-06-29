"""
dashboard/app.py - Streamlit Cloud entrypoint.

This app runs the NemoClaw demo in one Streamlit process using fakeredis as the
shared event bus. The original FastAPI dashboard remains available as
dashboard.fastapi_app for local uvicorn runs.
"""
from __future__ import annotations

import base64
import datetime as dt
import html
import json
import os
import subprocess
import sys
import threading
import time
import tempfile
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any

import fakeredis
import redis as _redis
import streamlit as st


st.set_page_config(
    page_title="NemoClaw Sandbox Loop",
    layout="wide",
    initial_sidebar_state="collapsed",
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ.setdefault("NEMOCLAW_AUDIT_DB", os.path.join(tempfile.gettempdir(), "nemoclaw_audit.db"))


@st.cache_resource(show_spinner=False)
def _get_fake_redis():
    return fakeredis.FakeStrictRedis(decode_responses=True)


_FAKE_REDIS = _get_fake_redis()


def _fake_from_url(url: str, **kwargs: Any):
    return _FAKE_REDIS


_redis.from_url = _fake_from_url  # type: ignore[assignment]

from common import (  # noqa: E402
    ALERTS_STREAM,
    ASSETS,
    EVENTS_STREAM,
    EVENT_LABELS,
    get_all_asset_state,
    get_redis,
    make_event,
    publish_event,
)


@dataclass
class DemoRuntime:
    started_at: float
    threads: dict[str, threading.Thread] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def _thread(runtime: DemoRuntime, name: str, target, **kwargs: Any) -> None:
    def wrapped() -> None:
        try:
            target(**kwargs)
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            runtime.errors.append(f"{name}: {exc}")

    th = threading.Thread(target=wrapped, name=name, daemon=True)
    th.start()
    runtime.threads[name] = th


def _ensure_fixtures(runtime: DemoRuntime) -> None:
    video_path = os.path.join(ROOT, "fixtures", "video", "cell.mp4")
    logs_dir = os.path.join(ROOT, "fixtures", "logs")
    logs_missing = not os.path.isdir(logs_dir) or not os.listdir(logs_dir)
    if os.path.exists(video_path) and not logs_missing:
        return

    try:
        subprocess.run(
            [sys.executable, os.path.join(ROOT, "scripts", "generate_fixtures.py")],
            check=True,
            cwd=ROOT,
        )
    except Exception as exc:
        runtime.errors.append(f"fixture generation: {exc}")


def _fallback_pipeline_b() -> None:
    """Pure-Python Pipeline B fallback when OpenCV is unavailable."""
    r = get_redis()
    frame = 0

    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        Image = ImageDraw = ImageFont = None

    while True:
        x = 20 + ((frame * 13) % 240)
        detections = [{
            "label": "moving_object",
            "conf": 0.99,
            "bbox": [x, 88, x + 54, 138],
        }]
        publish_event(r, make_event(
            asset_id="ROVER_A",
            event_name="VISION_DETECTION",
            payload={"n": 1, "objects": detections, "engine": "StreamlitFallback"},
            pipeline="B",
        ))

        if Image is not None:
            img = Image.new("RGB", (320, 180), (18, 18, 24))
            draw = ImageDraw.Draw(img)
            draw.rectangle((0, 0, 319, 179), outline=(52, 52, 68))
            draw.rectangle((32, 48, 96, 72), fill=(130, 90, 220))
            draw.text((34, 30), "plate", fill=(220, 220, 220))
            draw.rectangle((210, 42, 270, 78), fill=(80, 180, 100))
            draw.text((200, 24), "rack R-2007", fill=(220, 220, 220))
            draw.rectangle((x, 88, x + 54, 138), fill=(200, 100, 30), outline=(50, 200, 50), width=2)
            draw.text((x, 72), "ROVER_A", fill=(235, 235, 235))
            draw.text((226, 156), dt.datetime.now().strftime("%H:%M:%S"), fill=(150, 150, 160))
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=72)
            r.set("video:latest_frame", base64.b64encode(buf.getvalue()).decode("ascii"), ex=30)
            r.set("video:latest_ts", dt.datetime.now().astimezone().isoformat(timespec="milliseconds"))

        frame += 1
        time.sleep(1.0)


@st.cache_resource(show_spinner=False)
def start_demo_runtime() -> DemoRuntime:
    runtime = DemoRuntime(started_at=time.time())
    _ensure_fixtures(runtime)

    from nemoclaw_orchestrator.agent import run as run_orchestrator
    from pipeline_logs.tailer import replay as run_pipeline_a

    _thread(runtime, "orchestrator", run_orchestrator, use_llm=False, from_beginning=True)

    try:
        from pipeline_video.analyzer import run as run_pipeline_b

        _thread(
            runtime,
            "pipeline_b",
            run_pipeline_b,
            video_path=os.path.join(ROOT, "fixtures", "video", "cell.mp4"),
            engine_name=os.environ.get("VIDEO_ENGINE", "mock"),
            loop=True,
            zone_asset="ROVER_A",
            speed=1.0,
        )
    except Exception as exc:
        runtime.errors.append(f"pipeline_b OpenCV unavailable; using fallback: {exc}")
        _thread(runtime, "pipeline_b_fallback", _fallback_pipeline_b)

    log_speed = float(os.environ.get("LOG_SPEED", "720"))
    _thread(runtime, "pipeline_a", run_pipeline_a, speed=log_speed)
    return runtime


def _decode_redis_event(fields: dict[str, str]) -> dict[str, Any]:
    payload_str = fields.get("payload") or "{}"
    try:
        payload = json.loads(payload_str)
    except Exception:
        payload = {"_raw": payload_str}

    return {
        "event_id": fields.get("event_id", ""),
        "asset_id": fields.get("asset_id", ""),
        "event_name": fields.get("event_name", ""),
        "label": EVENT_LABELS.get(fields.get("event_name"), fields.get("event_name", "")),
        "ts": fields.get("ts", ""),
        "severity": fields.get("severity", "info"),
        "pipeline": fields.get("pipeline", ""),
        "payload": payload,
    }


def _recent_stream(stream: str, count: int) -> list[dict[str, Any]]:
    r = get_redis()
    return [_decode_redis_event(fields) for _, fields in r.xrevrange(stream, count=count)]


def _short_ts(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(value)
        return parsed.strftime("%H:%M:%S")
    except Exception:
        return value[-12:]


def _payload_preview(value: str | dict[str, Any]) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value or "{}")
        except Exception:
            return value
    return json.dumps(value or {}, separators=(",", ": "))[:220]


def _asset_tile(asset_id: str, meta: dict[str, Any], state: dict[str, str]) -> str:
    severity = state.get("last_severity", "info") or "info"
    event_name = state.get("last_event", "-") or "-"
    event_label = EVENT_LABELS.get(event_name, event_name)
    payload = _payload_preview(state.get("last_payload", "{}"))
    color = html.escape(meta["color"])
    return f"""
    <div class="asset-tile {html.escape(severity)}">
      <div class="tile-head">
        <span class="asset-dot" style="background:{color};"></span>
        <span class="asset-title">{html.escape(meta["label"])}</span>
        <span class="asset-id">{html.escape(asset_id)}</span>
      </div>
      <div class="event-label">{html.escape(event_label)}</div>
      <div class="event-name">{html.escape(event_name)}</div>
      <div class="event-meta">{html.escape(_short_ts(state.get("last_ts", "")))} · {html.escape(severity)}</div>
      <pre>{html.escape(payload)}</pre>
    </div>
    """


def _alert_card(alert: dict[str, Any]) -> str:
    severity = alert.get("severity", "info") or "info"
    payload = alert.get("payload") or {}
    summary = payload.get("summary") or EVENT_LABELS.get(alert.get("event_name"), alert.get("event_name", ""))
    return f"""
    <div class="alert-card {html.escape(severity)}">
      <div class="alert-top">
        <span>{html.escape(alert.get("event_name", ""))}</span>
        <code>{html.escape(_short_ts(alert.get("ts", "")))}</code>
      </div>
      <div class="alert-asset">{html.escape(alert.get("asset_id", ""))}</div>
      <div class="alert-summary">{html.escape(summary)}</div>
    </div>
    """


def _event_rows(events: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [{
        "time": _short_ts(event.get("ts", "")),
        "asset": event.get("asset_id", ""),
        "pipeline": event.get("pipeline", ""),
        "severity": event.get("severity", ""),
        "event": event.get("event_name", ""),
    } for event in events]


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        :root {
          --bg: #0f1014;
          --panel: #181a22;
          --panel2: #20232d;
          --text: #ece8df;
          --muted: #a5a0a0;
          --border: #303342;
          --green: #5baa72;
          --warn: #d6a04a;
          --alert: #c75252;
          --blue: #378add;
        }
        .stApp { background: var(--bg); color: var(--text); }
        [data-testid="stHeader"] { background: rgba(15, 16, 20, 0.86); }
        h1, h2, h3 { letter-spacing: 0; }
        .block-container { padding-top: 1.4rem; padding-bottom: 1rem; max-width: 1480px; }
        .status-strip {
          display: grid;
          grid-template-columns: repeat(4, minmax(120px, 1fr));
          gap: 10px;
          margin: 8px 0 18px;
        }
        .metric-box {
          background: var(--panel);
          border: 1px solid var(--border);
          border-radius: 8px;
          padding: 10px 12px;
        }
        .metric-label {
          color: var(--muted);
          font-size: 0.72rem;
          text-transform: uppercase;
          letter-spacing: 0;
        }
        .metric-value {
          color: var(--text);
          font-size: 1.05rem;
          font-weight: 700;
          margin-top: 2px;
        }
        .asset-tile {
          min-height: 178px;
          background: var(--panel);
          border: 1px solid var(--border);
          border-radius: 8px;
          padding: 13px 14px;
          margin-bottom: 12px;
          box-shadow: inset 0 0 0 1px rgba(255,255,255,0.015);
        }
        .asset-tile.warn { border-color: rgba(214, 160, 74, 0.72); }
        .asset-tile.alert { border-color: rgba(199, 82, 82, 0.82); }
        .tile-head {
          display: flex;
          align-items: center;
          gap: 8px;
          min-width: 0;
        }
        .asset-dot {
          width: 10px;
          height: 10px;
          border-radius: 50%;
          flex: 0 0 10px;
        }
        .asset-title {
          font-weight: 700;
          font-size: 0.93rem;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .asset-id {
          margin-left: auto;
          color: var(--muted);
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.68rem;
        }
        .event-label {
          color: var(--text);
          font-size: 0.86rem;
          margin-top: 14px;
          min-height: 22px;
        }
        .event-name {
          color: #9f98ff;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.74rem;
          margin-top: 2px;
          overflow-wrap: anywhere;
        }
        .event-meta {
          color: var(--muted);
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.7rem;
          margin-top: 6px;
        }
        .asset-tile pre {
          margin: 8px 0 0;
          white-space: pre-wrap;
          overflow-wrap: anywhere;
          max-height: 48px;
          overflow: hidden;
          background: var(--panel2);
          color: var(--muted);
          border-radius: 6px;
          padding: 7px 8px;
          font-size: 0.68rem;
          line-height: 1.35;
        }
        .alert-card {
          background: var(--panel);
          border: 1px solid var(--border);
          border-left: 4px solid var(--blue);
          border-radius: 8px;
          padding: 10px 12px;
          margin-bottom: 10px;
        }
        .alert-card.warn { border-left-color: var(--warn); }
        .alert-card.alert { border-left-color: var(--alert); }
        .alert-top {
          display: flex;
          gap: 8px;
          align-items: center;
          justify-content: space-between;
          color: var(--text);
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.75rem;
        }
        .alert-top code {
          color: var(--muted);
          background: transparent;
          font-size: 0.68rem;
        }
        .alert-asset {
          color: var(--muted);
          font-size: 0.74rem;
          margin-top: 4px;
        }
        .alert-summary {
          color: var(--text);
          font-size: 0.78rem;
          line-height: 1.4;
          margin-top: 7px;
        }
        [data-testid="stDataFrame"] {
          border: 1px solid var(--border);
          border-radius: 8px;
          overflow: hidden;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


runtime = start_demo_runtime()
r = get_redis()
_inject_css()

state = get_all_asset_state(r)
events = _recent_stream(EVENTS_STREAM, 60)
alerts = _recent_stream(ALERTS_STREAM, 20)
uptime = max(0, int(time.time() - runtime.started_at))
running_threads = sum(1 for thread in runtime.threads.values() if thread.is_alive())

st.title("NemoClaw Sandbox Loop")
st.markdown(
    f"""
    <div class="status-strip">
      <div class="metric-box"><div class="metric-label">events</div><div class="metric-value">{r.xlen(EVENTS_STREAM)}</div></div>
      <div class="metric-box"><div class="metric-label">alerts</div><div class="metric-value">{r.xlen(ALERTS_STREAM)}</div></div>
      <div class="metric-box"><div class="metric-label">services</div><div class="metric-value">{running_threads}/3</div></div>
      <div class="metric-box"><div class="metric-label">uptime</div><div class="metric-value">{uptime // 60:02d}:{uptime % 60:02d}</div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

for error in runtime.errors[-3:]:
    st.warning(error)

assets_by_position = sorted(ASSETS.items(), key=lambda item: (item[1]["row"], item[1]["col"]))
for start in range(0, len(assets_by_position), 3):
    cols = st.columns(3)
    for col, (asset_id, meta) in zip(cols, assets_by_position[start:start + 3]):
        with col:
            st.markdown(_asset_tile(asset_id, meta, state.get(asset_id, {})), unsafe_allow_html=True)

left, right = st.columns([1.15, 0.85], gap="large")

with left:
    st.subheader("Pipeline B Preview")
    frame_b64 = r.get("video:latest_frame")
    frame_ts = r.get("video:latest_ts")
    if frame_b64:
        st.image(base64.b64decode(frame_b64), caption=f"last frame {frame_ts or ''}", width="stretch")
    else:
        st.info("Awaiting first frame.")

    st.subheader("Live Event Stream")
    rows = _event_rows(events)
    if rows:
        st.dataframe(rows, hide_index=True, width="stretch", height=360)
    else:
        st.info("Awaiting events.")

with right:
    st.subheader("NemoClaw Alerts")
    if alerts:
        for alert in alerts:
            st.markdown(_alert_card(alert), unsafe_allow_html=True)
    else:
        st.info("No alerts yet.")

with st.sidebar:
    auto_refresh = st.toggle("Live refresh", value=True)
    refresh_interval = st.slider("Refresh interval", min_value=1, max_value=5, value=2)

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
