"""
dashboard/app.py — single visual monitoring layer.

Endpoints:
- GET  /                  — single HTML page
- GET  /api/state         — current state for all assets (initial render)
- GET  /api/video-frame   — latest annotated frame from Pipeline B (base64 JPEG)
- WS   /ws                — pushes events and alerts as they happen

Run with:
  uvicorn dashboard.app:app --host 0.0.0.0 --port 8080 --reload
"""
from __future__ import annotations
import asyncio
import json
import os
import sys

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import (
    get_redis, get_all_asset_state, ASSETS, EVENT_LABELS,
    EVENTS_STREAM, ALERTS_STREAM,
)

ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATES = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))

app = FastAPI(title="NemoClaw monitoring")
app.mount("/static", StaticFiles(directory=os.path.join(ROOT, "static")), name="static")


# ---------------------------------------------------------------------------
# Pages and APIs
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    tmpl = TEMPLATES.get_template("index.html")
    return tmpl.render(assets=ASSETS, event_labels=EVENT_LABELS)


@app.get("/api/state")
async def api_state():
    r = get_redis()
    return JSONResponse(get_all_asset_state(r))


@app.get("/api/video-frame")
async def api_video_frame():
    r = get_redis()
    b64 = r.get("video:latest_frame")
    ts = r.get("video:latest_ts")
    return JSONResponse({"frame_b64": b64 or "", "ts": ts or ""})


@app.get("/api/alerts/recent")
async def api_alerts_recent(n: int = 20):
    r = get_redis()
    msgs = r.xrevrange(ALERTS_STREAM, count=n)
    out = []
    for _, fields in msgs:
        out.append(_decode_redis_event(fields))
    return JSONResponse(out)


def _decode_redis_event(fields: dict) -> dict:
    """Turn raw Redis hash back into a JSON-safe dict for the client."""
    payload_str = fields.get("payload") or "{}"
    try:
        payload = json.loads(payload_str)
    except Exception:
        payload = {"_raw": payload_str}
    return {
        "event_id": fields.get("event_id"),
        "asset_id": fields.get("asset_id"),
        "event_name": fields.get("event_name"),
        "ts": fields.get("ts"),
        "severity": fields.get("severity"),
        "pipeline": fields.get("pipeline"),
        "payload": payload,
        "label": EVENT_LABELS.get(fields.get("event_name"), fields.get("event_name")),
    }


# ---------------------------------------------------------------------------
# WebSocket: pushes events and alerts in real time
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    r = get_redis()

    last_event_id = "$"
    last_alert_id = "$"

    try:
        while True:
            # Use a non-blocking xread with a small block; gather both streams
            resp = await asyncio.to_thread(
                r.xread,
                {EVENTS_STREAM: last_event_id, ALERTS_STREAM: last_alert_id},
                count=50, block=1500,
            )
            if not resp:
                # Heartbeat keeps the connection alive through corporate proxies
                await ws.send_text(json.dumps({"type": "heartbeat"}))
                continue

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    decoded = _decode_redis_event(fields)
                    if stream_name == EVENTS_STREAM:
                        last_event_id = msg_id
                        await ws.send_text(json.dumps({"type": "event", "data": decoded}))
                    elif stream_name == ALERTS_STREAM:
                        last_alert_id = msg_id
                        await ws.send_text(json.dumps({"type": "alert", "data": decoded}))
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await ws.send_text(json.dumps({"type": "error", "message": str(e)}))
        except Exception:
            pass
