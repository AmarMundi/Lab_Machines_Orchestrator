#!/usr/bin/env bash
# run_all.sh — boots the demo locally on macOS / Linux.
# Strategy:
#   1. Verify redis is up (start brew service if not).
#   2. Activate venv if present.
#   3. If fixtures missing, generate them.
#   4. Launch the four services as background processes, write PIDs to data/pids/.
#   5. Tail the logs.
#
# Run ./scripts/stop_all.sh to bring everything down.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/data/logs"
PID_DIR="$ROOT/data/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

# ------------- helpers -------------
say() { printf "\033[1;35m[run_all]\033[0m %s\n" "$*"; }

# ------------- 1. redis -------------
if ! redis-cli ping >/dev/null 2>&1; then
  say "Redis not running — attempting to start via brew services…"
  if command -v brew >/dev/null 2>&1; then
    brew services start redis || true
    sleep 2
  fi
  if ! redis-cli ping >/dev/null 2>&1; then
    echo "Redis is not running and could not be started automatically."
    echo "Please run: brew install redis && brew services start redis"
    echo "or:        redis-server &"
    exit 1
  fi
fi
say "Redis is up."

# ------------- 2. venv -------------
if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
  say "Activated .venv"
else
  say "No .venv directory found — relying on system Python (consider running 'python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt')."
fi

# ------------- 3. fixtures -------------
if [[ ! -f fixtures/video/cell.mp4 ]] || [[ -z "$(ls -A fixtures/logs 2>/dev/null)" ]]; then
  say "Generating fixtures (one-time)…"
  python scripts/generate_fixtures.py
fi

# ------------- 4. services -------------
# Defaults: rules-only orchestrator (so the demo runs with NO ollama install),
# motion-detection video engine (so the demo runs without YOLO weights).
# Override via env: USE_LLM=1 VIDEO_ENGINE=yolo ./scripts/run_all.sh
USE_LLM="${USE_LLM:-0}"
VIDEO_ENGINE="${VIDEO_ENGINE:-mock}"
LOG_SPEED="${LOG_SPEED:-120}"

start_bg() {
  local name="$1"; shift
  local cmd="$*"
  say "Starting $name → $LOG_DIR/$name.log"
  # `nohup` so closing the terminal doesn't kill the process; redirect both streams.
  nohup bash -c "$cmd" >"$LOG_DIR/$name.log" 2>&1 &
  echo $! >"$PID_DIR/$name.pid"
}

# Stop anything left over from a previous run
if [[ -f scripts/stop_all.sh ]]; then
  bash scripts/stop_all.sh --quiet || true
fi

start_bg "dashboard"   "uvicorn dashboard.fastapi_app:app --host 0.0.0.0 --port 8080"
sleep 1
ORCH_FLAG=""
if [[ "$USE_LLM" != "1" ]]; then ORCH_FLAG="--no-llm"; fi
start_bg "orchestrator" "python -m nemoclaw_orchestrator.agent $ORCH_FLAG"
start_bg "pipeline_b"  "python -m pipeline_video.analyzer --engine $VIDEO_ENGINE"
start_bg "pipeline_a"  "python -m pipeline_logs.tailer --speed $LOG_SPEED"

say ""
say "All services launched."
say "Dashboard:        http://localhost:8080"
say "Tail logs:        tail -F $LOG_DIR/*.log"
say "Stop everything:  ./scripts/stop_all.sh"
say ""
say "PIDs:"
ls "$PID_DIR" | while read f; do
  echo "  $f → $(cat "$PID_DIR/$f")"
done
