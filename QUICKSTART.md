# QUICKSTART — 60 seconds, no Redis install required

```bash
# 1. unzip and cd
cd nemoclaw_demo

# 2. create a venv and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. boot the Streamlit app
streamlit run dashboard/app.py

# 4. Streamlit opens the dashboard in your browser
```

That's it. **No Redis, no Docker, no brew install.** Everything runs in one
Python process using `fakeredis` as an in-process event bus. Stop with Ctrl-C.

For a faster demo, replay the synthetic 24h shift in ~2 minutes:

```bash
LOG_SPEED=720 streamlit run dashboard/app.py
```

## (Optional) Original local FastAPI dashboard

```bash
python run_local.py
open http://localhost:8080
```

## (Optional) Multi-process mode with real Redis

Use this if you want the production-shaped deployment with separate processes
and a real event bus.

```bash
brew install redis              # if you can — needs Xcode CLT
brew services start redis
./scripts/run_all.sh
```

This boots four background processes that talk over real Redis. Identical code
paths and dashboard.

## (Optional) Add Ollama for natural-language alert summaries

```bash
# Install: https://ollama.com/download/mac
ollama pull qwen2.5:3b      # ~2 GB, runs on CPU
USE_LLM=1 python run_local.py
```

Each alert in the right panel will then carry a one-sentence summary like
"Run RUN-00408-A aborted because read barcode RB0000064383 did not match
expected RB0000002008."

## Stop

`Ctrl-C` (single-process mode) or `./scripts/stop_all.sh` (multi-process mode).

## What you should see after boot

Within 10 seconds, the dashboard at http://localhost:8080 shows:
- 6 asset tiles (Sample storage, Robotic vision, Rover A, AMR, Plate storage,
  Dilution/assay) with live event names changing every 1–2 seconds.
- A live video preview at the bottom-left with green detection boxes around
  moving objects.
- An "Alerts" panel on the right that fills up with ~14 alerts over the first
  2 minutes (faster if you raised `LOG_SPEED`) — the seeded failures from the
  synthetic shift.
- A live event stream below the alerts, showing every parsed event from both
  pipelines.

## Files in 30 seconds

```
run_local.py                       ← single-process launcher (no Redis needed)
common.py                          ← shared event schema + Redis helpers
scripts/generate_fixtures.py       ← creates 24h synthetic logs + 30s video
pipeline_logs/tailer.py            ← Pipeline A: parse logs → publish events
pipeline_video/analyzer.py         ← Pipeline B: video → detection events
nemoclaw_orchestrator/agent.py     ← the fusion agent: rules + (optional) LLM
dashboard/app.py                   ← Streamlit Cloud app
dashboard/fastapi_app.py           ← FastAPI + WebSocket server for local uvicorn
dashboard/templates/index.html     ← FastAPI HTML page
scripts/run_all.sh / stop_all.sh   ← multi-process service boot/stop
```

## Tunables (env vars)

```
USE_LLM=1                     # default 0 (rules only)
VIDEO_ENGINE=mock|yolo        # default mock; yolo needs ultralytics + ~6MB weights
LOG_SPEED=120                 # 60 = 24h-in-24min, 240 = 6min, 720 = 2min
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=qwen2.5:3b
REDIS_URL=redis://localhost:6379/0   # only used in multi-process mode
```
