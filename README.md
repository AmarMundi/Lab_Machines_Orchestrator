# NemoClaw Sandbox Loop — single-pane monitoring demo

A runnable, on-prem-edge demonstration of the architecture proposed in the SoW.
Runs entirely on a **MacBook (M1/M2/M3 or Intel 2018+)**, no GPU required,
no internet required after dependency install. All open-source.

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Single dashboard (8080)                        │
│                       FastAPI + WebSocket                            │
└────────────────────────────────▲────────────────────────────────────┘
                                 │ WS push
┌────────────────────────────────┴────────────────────────────────────┐
│                    NemoClaw orchestrator agent                       │
│         OpenClaw-style fusion · Ollama (local LLM) or rule-only      │
│         consumes Redis stream "events", emits "alerts"               │
└──────────────────▲────────────────────────────────▲─────────────────┘
                   │                                │
        ┌──────────┴──────────┐         ┌──────────┴───────────┐
        │ Pipeline A (logs)   │         │ Pipeline B (video)   │
        │ watchdog + tailer   │         │ OpenCV + YOLOv8n CPU │
        │ parses 6 asset logs │         │ scans video clip     │
        └──────────▲──────────┘         └──────────▲───────────┘
                   │                                │
        ┌──────────┴──────────┐         ┌──────────┴───────────┐
        │ fixtures/logs/      │         │ fixtures/video/      │
        │ synthetic 24h shift │         │ synthetic clip       │
        └─────────────────────┘         └──────────────────────┘
```

## What this demo proves

1. Two independent pipelines (logs + video) feeding one event bus.
2. A NemoClaw-style orchestrator agent that fuses both streams and emits alerts.
3. A single visual layer showing all 6 machines in real time.
4. The whole thing runs on a 2018 Intel Mac with no GPU, no cloud egress.

## Prerequisites (one-time)

```bash
# 1. Python 3.10+
python3 --version

# 2. Python deps (in a venv)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

That's all. The default mode uses `fakeredis` (an in-process Redis stand-in)
so you do **not** need Redis installed.

If you want the production-shaped multi-process mode, also install Redis:

```bash
brew install redis            # only if you want multi-process mode
brew services start redis     # only if you want multi-process mode
```

If `brew install redis` fails on macOS 13 with Xcode CLT issues, just skip it
and use the single-process mode below — it does the same thing.

## Run the demo

**Single-process mode (recommended, no Redis required):**

```bash
# 1. Generate synthetic fixtures (auto-runs on first launch if missing)
# 2. Start everything in one process
python run_local.py

# 3. Open the dashboard
open http://localhost:8080
```

**Multi-process mode (requires Redis):**

```bash
python scripts/generate_fixtures.py       # one-time
./scripts/run_all.sh                      # boots 4 background processes
open http://localhost:8080
```

You should see all six machines come alive within ~10 seconds, with events
flowing in, alerts surfacing in the side panel, and the small video preview
showing detection boxes.

## Stop everything

```bash
./scripts/stop_all.sh
```

## Project layout

```
nemoclaw_demo/
├── agents/                       # the 26-agent mesh skeleton (per-asset agents)
├── pipeline_logs/                # Pipeline A — log watcher + parser
├── pipeline_video/               # Pipeline B — OpenCV + YOLOv8n
├── nemoclaw_orchestrator/        # NemoClaw-style fusion agent
├── dashboard/                    # FastAPI + WebSocket + single HTML page
├── fixtures/                     # synthetic 24h logs + sample video
├── scripts/                      # generate_fixtures, run_all, stop_all
└── requirements.txt
```

## Trade-offs we made for the 2018 Intel Mac

| Constraint                     | Choice                                                                  |
| ------------------------------ | ----------------------------------------------------------------------- |
| No GPU / no MPS                | YOLOv8n on CPU at 320×240, 5 fps. Detections only, no VLM inline.       |
| 8–16 GB RAM typical            | Ollama qwen2.5:3b, optional. Without it, deterministic rules.           |
| Single-machine demo            | Redis instead of Kafka; SQLite instead of Postgres; one process group.  |
| No internet on prod cell       | All deps installable offline once cached. Ollama runs locally.          |

## How this maps to the SoW

| SoW element                     | This demo                                              |
| ------------------------------- | ------------------------------------------------------ |
| Layer 1 asset agents            | `agents/asset_agent.py` (one process per asset)        |
| Layer 2 edge collectors         | folded into Pipeline A & B for the demo                |
| Layer 3 Pipeline A              | `pipeline_logs/`                                       |
| Layer 3 Pipeline B              | `pipeline_video/`                                      |
| Layer 4 NemoClaw orchestrator   | `nemoclaw_orchestrator/agent.py`                       |
| Layer 5 dashboard               | `dashboard/app.py` + `dashboard/templates/index.html`  |
| A2A bus                         | Redis Streams (`events` and `alerts`)                  |
| Audit log                       | SQLite at `data/audit.db`                              |

## What this demo deliberately does NOT do

- Real OCR on barcodes (use the SoW build for that — needs a real labeled set)
- Closed-loop writeback to assets (shadow-mode only, per SoW MVP 1)
- GxP-grade signing of audit traces (uses ordinary SHA256, not a real KMS)
- Multi-cell or multi-tenant
