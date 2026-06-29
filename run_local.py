"""
run_local.py — single-process launcher that needs NO Redis install.

Boots all four services (dashboard, orchestrator, Pipeline A, Pipeline B) in
threads, sharing one in-process fakeredis instance. Same code paths as the
multi-process version, just no external Redis daemon required.

Run:
    python run_local.py

Then open: http://localhost:8080
Stop:     Ctrl-C
"""
from __future__ import annotations
import os
import sys
import threading
import time
import signal

# ---------------------------------------------------------------------------
# Patch redis BEFORE anyone imports common.py
# ---------------------------------------------------------------------------
import fakeredis
import redis as _redis

_singleton = fakeredis.FakeStrictRedis(decode_responses=True)
def _fake_from_url(url, **kw):
    return _singleton
_redis.from_url = _fake_from_url   # type: ignore

print("=" * 64)
print(" NemoClaw demo — single-process mode (fakeredis, no daemon needed)")
print("=" * 64)

# ---------------------------------------------------------------------------
# Make sure fixtures exist; generate if not.
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

if not os.path.exists(os.path.join(ROOT, "fixtures", "video", "cell.mp4")) \
   or not os.listdir(os.path.join(ROOT, "fixtures", "logs")):
    print("\n[setup] generating fixtures (one-time)…")
    import subprocess
    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "generate_fixtures.py")],
                   check=True)

# ---------------------------------------------------------------------------
# Import the four services
# ---------------------------------------------------------------------------
from pipeline_logs.tailer import replay as run_pipeline_a
from pipeline_video.analyzer import run as run_pipeline_b
from nemoclaw_orchestrator.agent import run as run_orchestrator
import dashboard.app   # noqa: E402 (FastAPI app)
import uvicorn

# ---------------------------------------------------------------------------
# Threaded launchers
# ---------------------------------------------------------------------------
_stop_event = threading.Event()

def _t(name, target, **kwargs):
    def wrapped():
        try:
            target(**kwargs)
        except Exception as e:
            print(f"[{name}] crashed: {e}")
            import traceback; traceback.print_exc()
    th = threading.Thread(target=wrapped, name=name, daemon=True)
    th.start()
    print(f"[boot] started thread: {name}")
    return th

print("\n[boot] starting services…")

# Dashboard first — it serves the HTML
def _run_dashboard():
    config = uvicorn.Config(
        dashboard.app.app,
        host="0.0.0.0",
        port=8080,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server.run()

_t("dashboard",   _run_dashboard)
time.sleep(1.0)

# Orchestrator — rules-only by default for offline operation. Set USE_LLM=1 if Ollama is running.
use_llm = os.environ.get("USE_LLM", "0") == "1"
_t("orchestrator", run_orchestrator, use_llm=use_llm, from_beginning=True)

# Pipeline B — motion detector by default; set VIDEO_ENGINE=yolo if you have torch+ultralytics
video_engine = os.environ.get("VIDEO_ENGINE", "mock")
_t("pipeline_b", run_pipeline_b,
   video_path=os.path.join(ROOT, "fixtures", "video", "cell.mp4"),
   engine_name=video_engine,
   loop=True,
   zone_asset="ROVER_A",
   speed=1.0)

# Pipeline A — log replayer
log_speed = float(os.environ.get("LOG_SPEED", "120"))
_t("pipeline_a", run_pipeline_a, speed=log_speed)

print(f"""
{'='*64}
 Dashboard:  http://localhost:8080
 Press Ctrl-C to stop.

 Settings:
   USE_LLM       = {use_llm}            (1 to enable Ollama summaries)
   VIDEO_ENGINE  = {video_engine}    (mock = motion detector, yolo = YOLOv8n)
   LOG_SPEED     = {log_speed}      (60 = 24h-in-24min, 240 = ~6min)
{'='*64}
""")

# ---------------------------------------------------------------------------
# Wait for Ctrl-C
# ---------------------------------------------------------------------------
def _shutdown(signum, frame):
    print("\n[shutdown] stopping…")
    _stop_event.set()
    sys.exit(0)

signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# Block forever
while not _stop_event.is_set():
    time.sleep(1.0)
