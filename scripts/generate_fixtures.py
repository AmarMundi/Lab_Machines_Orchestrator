"""
generate_fixtures.py — produce realistic synthetic fixtures so the demo runs
without requiring real AbbVie data.

Outputs:
- fixtures/logs/SAMPLE_STORAGE_01_RUN_<date>.log    (file-drop + REST mix)
- fixtures/logs/ROBOTIC_VISION_01_<date>.log        (controller log)
- fixtures/logs/ROVER_A_<date>.log                  (telemetry + reservations)
- fixtures/logs/AMR_01_<date>.log                   (plate-order events)
- fixtures/logs/PLATE_STORAGE_01_<date>.log         (retrievals)
- fixtures/logs/DA_01_RUN_<date>.log                (Hamilton-Venus-style)
- fixtures/video/cell.mp4                           (synthetic 30s clip with moving rectangles)

Time-compressed: 24h of lab activity is squashed into a 60-second wall-clock
playback when the log tailer reads them. The tailer's --speed flag controls this.
"""
from __future__ import annotations
import os
import random
import datetime as dt
import math
import json

import cv2
import numpy as np

random.seed(42)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(ROOT, "fixtures", "logs")
VIDEO_DIR = os.path.join(ROOT, "fixtures", "video")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)

# Use a fixed base date so logs are deterministic and easy to diff.
BASE = dt.datetime(2026, 4, 24, 8, 0, 0).astimezone()
DATE_STR = BASE.strftime("%Y%m%d")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt(ts: dt.datetime) -> str:
    """ISO-8601 with milliseconds and timezone."""
    return ts.isoformat(timespec="milliseconds")

def fmt_bracket(ts: dt.datetime) -> str:
    """Hamilton-Venus-style timestamp."""
    return ts.strftime("[%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}]"

# ---------------------------------------------------------------------------
# Generate one full "shift": ~30 orders, with a few intentionally bad ones
# ---------------------------------------------------------------------------

# Each order is a tuple (order_id, rack_id, plate_id, outcome)
# outcome ∈ {"normal", "failed", "delayed", "incomplete", "rover_stuck"}
ORDERS = []
for i in range(30):
    order_id = f"ORD-2026-04-24-{400 + i:05d}"
    rack_id = f"R-{2000 + i}"
    plate_id = f"P-{900 + i}"
    # 20 normal, 3 delayed, 2 failed, 1 incomplete, 1 rover-stuck, plus filler
    outcome = "normal"
    if i in (5, 12, 19): outcome = "delayed"
    if i in (8, 22):     outcome = "failed"
    if i == 15:          outcome = "incomplete"
    if i == 25:          outcome = "rover_stuck"
    ORDERS.append((order_id, rack_id, plate_id, outcome))

print(f"Generating fixtures for {len(ORDERS)} orders across {len(os.listdir(LOG_DIR)) if os.path.exists(LOG_DIR) else 0} prior files (will overwrite)…")

# Each log file we will write line-by-line as a list, then sort by timestamp at end.
log_buffers: dict[str, list[tuple[dt.datetime, str]]] = {
    "SAMPLE_STORAGE_01": [],
    "ROBOTIC_VISION_01": [],
    "ROVER_A":           [],
    "AMR_01":            [],
    "PLATE_STORAGE_01":  [],
    "DA_01":             [],
}

def add(asset: str, ts: dt.datetime, line: str):
    log_buffers[asset].append((ts, line))

# ---------------------------------------------------------------------------
# Walk each order through the 5-step journey
# ---------------------------------------------------------------------------

# We'll spread orders across ~24h compressed; for the demo, real intervals are
# preserved, but the tailer plays them back fast.
cursor = BASE
for order_id, rack_id, plate_id, outcome in ORDERS:
    rack_bc = f"RB{int(rack_id.split('-')[1]):010d}"
    plate_bc = f"PB{int(plate_id.split('-')[1]):010d}"

    # ===== Step 1: Pick from sample storage =====
    t = cursor
    add("SAMPLE_STORAGE_01", t,
        f"{fmt(t)}  INFO  PICK_ORD     order={order_id} rack={rack_id} barcode={rack_bc}")
    t += dt.timedelta(seconds=random.uniform(20, 35))
    if outcome == "rover_stuck":
        # Pick still completes; trouble is downstream
        add("SAMPLE_STORAGE_01", t,
            f"{fmt(t)}  INFO  PICK_OUT     order={order_id} rack={rack_id} status=PICKED duration_ms={random.randint(20000, 35000)}")
    else:
        add("SAMPLE_STORAGE_01", t,
            f"{fmt(t)}  INFO  PICK_OUT     order={order_id} rack={rack_id} status=PICKED duration_ms={random.randint(20000, 35000)}")

    # ===== Step 2: Vision-confirmed handoff =====
    t += dt.timedelta(seconds=random.uniform(0.5, 1.5))
    add("ROBOTIC_VISION_01", t, f"{fmt(t)}  INFO  CMD_RECV     cmd=pick rack={rack_id}")
    t += dt.timedelta(seconds=random.uniform(2, 4))
    if outcome == "failed" and random.random() < 0.5:
        add("ROBOTIC_VISION_01", t,
            f"{fmt(t)}  WARN  BARCODE      read={rack_bc} conf=0.42 attempts=3 status=LOW_CONF")
    else:
        add("ROBOTIC_VISION_01", t,
            f"{fmt(t)}  INFO  BARCODE      read={rack_bc} conf={random.uniform(0.94, 0.99):.2f} attempts=1")
    t += dt.timedelta(seconds=random.uniform(3, 6))
    add("ROBOTIC_VISION_01", t,
        f"{fmt(t)}  INFO  PLACE        rack={rack_id} dest=ROVER_A")
    t += dt.timedelta(seconds=random.uniform(0.1, 0.3))
    add("ROBOTIC_VISION_01", t,
        f"{fmt(t)}  INFO  REST_OUT     POST /rover/notify barcode={rack_bc} status=BARCODE_OK rover=ROVER_A")

    # ===== Step 3: Rover transit =====
    res_id = f"RES-2026-04-24-{9000 + ORDERS.index((order_id, rack_id, plate_id, outcome))}"
    t += dt.timedelta(seconds=random.uniform(0.2, 0.5))
    add("ROVER_A", t,
        f"{fmt(t)}  INFO  RES_OPEN     res_id={res_id} rack={rack_id} barcode={rack_bc} from=STORAGE_OUT to=ASSAY_DOCK status=ACTIVE")

    if outcome == "rover_stuck":
        # Rover takes a snapshot, then nothing — generates ROVER_RESERVATION_HELD via timeout
        t += dt.timedelta(seconds=2)
        add("ROVER_A", t, f"{fmt(t)}  INFO  TELE         pos=14.32,6.81 speed=0.40 task_state=EN_ROUTE battery=78")
        t += dt.timedelta(seconds=5)
        add("ROVER_A", t, f"{fmt(t)}  INFO  TELE         pos=14.32,6.81 speed=0.00 task_state=EN_ROUTE battery=78")
        t += dt.timedelta(seconds=15)
        add("ROVER_A", t, f"{fmt(t)}  WARN  TELE         pos=14.32,6.81 speed=0.00 task_state=EN_ROUTE battery=77")
        # Reservation gets manually closed later
        t += dt.timedelta(seconds=60)
        add("ROVER_A", t, f"{fmt(t)}  WARN  RES_TIMEOUT  res_id={res_id} reason=NO_PROGRESS")
        cursor = t + dt.timedelta(seconds=random.uniform(30, 90))
        continue  # this order does not progress further
    else:
        # Normal transit
        for k in range(random.randint(8, 15)):
            t += dt.timedelta(seconds=random.uniform(0.8, 1.2))
            x = 14.32 - 0.4 * k
            add("ROVER_A", t,
                f"{fmt(t)}  INFO  TELE         pos={x:.2f},6.81 speed={random.uniform(0.35, 0.50):.2f} task_state=EN_ROUTE battery={random.randint(72, 82)}")
        t += dt.timedelta(seconds=random.uniform(2, 4))
        add("ROVER_A", t,
            f"{fmt(t)}  INFO  TELE         pos=8.10,6.81 speed=0.00 task_state=DOCKED battery=72")

    # ===== Step 4: Plate handoff + assay run =====
    t += dt.timedelta(seconds=random.uniform(1, 3))
    add("AMR_01", t,
        f"{fmt(t)}  INFO  PLATE_ORD    order={order_id} plate={plate_id} barcode={plate_bc} src=PLATE_STORAGE_01_SLOT_{(int(plate_id.split('-')[1])) % 20 + 1} dst=ASSAY_DECK_LANE_2")
    t += dt.timedelta(seconds=random.uniform(2, 5))
    add("PLATE_STORAGE_01", t,
        f"{fmt(t)}  INFO  RETRIEVE_REQ plate={plate_id} barcode={plate_bc} slot={(int(plate_id.split('-')[1])) % 20 + 1}")
    t += dt.timedelta(seconds=random.uniform(3, 6))
    add("PLATE_STORAGE_01", t,
        f"{fmt(t)}  INFO  RETRIEVE_OK  plate={plate_id} barcode={plate_bc} duration_ms={random.randint(3000, 6000)} status=PRESENTED")

    # Assay run — Hamilton-Venus-style log
    run_id = f"RUN-{order_id.split('-')[-1]}-A"
    t += dt.timedelta(seconds=random.uniform(2, 4))
    add("DA_01", t, f"{fmt_bracket(t)}  INFO  RUN_START    method=DilAssay_v3.2 run_id={run_id} operator=auto")
    t += dt.timedelta(milliseconds=random.randint(100, 300))
    add("DA_01", t, f"{fmt_bracket(t)}  INFO  PLATE_LOAD   plate={plate_id} barcode={plate_bc} lane=2")
    t += dt.timedelta(seconds=random.uniform(1, 2))
    add("DA_01", t, f"{fmt_bracket(t)}  INFO  RACK_PRESENT rack={rack_id} barcode={rack_bc} lane=2")

    if outcome == "failed":
        # Barcode mismatch abort
        t += dt.timedelta(seconds=random.uniform(0.5, 1.0))
        wrong_bc = f"RB{random.randint(0, 99999):010d}"
        add("DA_01", t, f"{fmt_bracket(t)}  WARN  BARCODE_MISMATCH expected={rack_bc} read={wrong_bc}")
        t += dt.timedelta(milliseconds=random.randint(20, 80))
        add("DA_01", t, f"{fmt_bracket(t)}  ERROR RUN_ABORT    run_id={run_id} reason=BARCODE_MISMATCH")
        t += dt.timedelta(milliseconds=random.randint(100, 200))
        add("DA_01", t, f"{fmt_bracket(t)}  INFO  REST_OUT     POST /amr/notify status=PROCESS_FAIL run_id={run_id}")
        cursor = t + dt.timedelta(seconds=random.uniform(60, 180))
        continue

    # Step sequence
    steps = [("Aspirate", 16, 18), ("Dispense", 13, 15), ("Mix", 18, 22)]
    for step, base_lo, base_hi in steps:
        t += dt.timedelta(milliseconds=random.randint(100, 400))
        add("DA_01", t, f"{fmt_bracket(t)}  INFO  STEP_BEGIN   step={step}")
        if outcome == "delayed":
            duration_s = random.uniform(base_hi + 5, base_hi + 10)
            t += dt.timedelta(seconds=duration_s - 5)
            add("DA_01", t, f"{fmt_bracket(t)}  WARN  STEP_SLOW    step={step} elapsed_ms={int((duration_s-5)*1000)} expected_ms<={base_hi*1000}")
            t += dt.timedelta(seconds=5)
        else:
            duration_s = random.uniform(base_lo, base_hi)
            t += dt.timedelta(seconds=duration_s)
        add("DA_01", t, f"{fmt_bracket(t)}  INFO  STEP_END     step={step} duration_ms={int(duration_s*1000)}")

        if outcome == "incomplete" and step == "Dispense":
            # Power blip — log just stops here. No RUN_END will be emitted.
            cursor = t + dt.timedelta(seconds=random.uniform(120, 240))
            break
    else:
        t += dt.timedelta(milliseconds=random.randint(100, 300))
        status = "OK_DELAYED" if outcome == "delayed" else "OK"
        add("DA_01", t, f"{fmt_bracket(t)}  INFO  RUN_END      run_id={run_id} status={status}")
        t += dt.timedelta(milliseconds=random.randint(150, 300))
        add("DA_01", t, f"{fmt_bracket(t)}  INFO  REST_OUT     POST /rover/notify status=PROCESS_OK rover=ROVER_A run_id={run_id}")

        # ===== Step 5: Return to storage =====
        t += dt.timedelta(seconds=random.uniform(2, 4))
        add("ROVER_A", t,
            f"{fmt(t)}  INFO  RES_RETURN   res_id={res_id} rack={rack_id} barcode={rack_bc} from=ASSAY_DOCK to=STORAGE_IN")
        for k in range(random.randint(6, 10)):
            t += dt.timedelta(seconds=random.uniform(0.8, 1.2))
            x = 8.10 + 0.4 * k
            add("ROVER_A", t,
                f"{fmt(t)}  INFO  TELE         pos={x:.2f},6.81 speed=0.40 task_state=EN_ROUTE battery={random.randint(70, 80)}")
        t += dt.timedelta(seconds=random.uniform(2, 4))
        add("SAMPLE_STORAGE_01", t,
            f"{fmt(t)}  INFO  RETURN_IN    rack={rack_id} barcode={rack_bc} status=RETURNED")
        cursor = t + dt.timedelta(seconds=random.uniform(60, 180))
        continue

    cursor = t + dt.timedelta(seconds=random.uniform(60, 180))

# ---------------------------------------------------------------------------
# Sort each buffer by timestamp and write
# ---------------------------------------------------------------------------

written = []
for asset, buf in log_buffers.items():
    buf.sort(key=lambda x: x[0])
    fname = f"{asset}_{DATE_STR}.log"
    path = os.path.join(LOG_DIR, fname)
    with open(path, "w") as f:
        for _, line in buf:
            f.write(line + "\n")
    written.append((path, len(buf)))

print("\nLog fixtures written:")
for path, n in written:
    print(f"  {path}  ({n} lines)")

# ---------------------------------------------------------------------------
# Synthetic video (30 seconds, 320x240, 5 fps, ~150 frames)
# Simple coloured rectangles representing rover, racks, AMR — enough for
# YOLOv8n to detect "things" and for the dashboard to show motion.
# ---------------------------------------------------------------------------

print("\nGenerating synthetic video clip…")
W, H, FPS, SECS = 320, 240, 5, 30
N_FRAMES = FPS * SECS
out_path = os.path.join(VIDEO_DIR, "cell.mp4")

# Try codecs in order — mp4v works in most opencv-python wheels; avc1 is the
# fallback that the M-series macOS wheels prefer. If neither works, we fall
# back to MJPG inside a .avi container, which always works.
def _open_writer():
    for fourcc_code, ext in [("mp4v", ".mp4"), ("avc1", ".mp4"), ("MJPG", ".avi")]:
        path = out_path if ext == ".mp4" else out_path.replace(".mp4", ext)
        w = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*fourcc_code), FPS, (W, H))
        if w.isOpened():
            return w, path, fourcc_code
        w.release()
    raise RuntimeError("Could not open any video codec — install ffmpeg via 'brew install ffmpeg' and retry.")

writer, out_path, codec_used = _open_writer()
print(f"  video codec: {codec_used} → {out_path}")

for i in range(N_FRAMES):
    frame = np.full((H, W, 3), 245, dtype=np.uint8)  # off-white background

    # Draw a rover (blue rectangle) moving left to right, then back
    t_norm = (i / N_FRAMES) * 2.0
    if t_norm < 1.0:
        x = int(20 + (W - 80) * t_norm)
    else:
        x = int(20 + (W - 80) * (2.0 - t_norm))
    cv2.rectangle(frame, (x, 110), (x + 50, 160), (200, 100, 30), -1)
    cv2.putText(frame, "ROVER_A", (x, 105), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (50, 50, 50), 1)

    # A "rack" sitting still on the AMR side
    cv2.rectangle(frame, (220, 40), (270, 80), (80, 180, 100), -1)
    cv2.putText(frame, "rack R-2007", (200, 35), cv2.FONT_HERSHEY_SIMPLEX, 0.35, (50, 50, 50), 1)

    # A "plate" near the assay deck
    cv2.rectangle(frame, (40, 50), (80, 75), (130, 90, 220), -1)
    cv2.putText(frame, "plate", (40, 45), cv2.FONT_HERSHEY_SIMPLEX, 0.35, (50, 50, 50), 1)

    # Lab-cell timestamp overlay (top-right) — gives the demo a "live feed" feel
    overlay_ts = (BASE + dt.timedelta(seconds=i * 8)).strftime("%H:%M:%S")
    cv2.putText(frame, overlay_ts, (W - 70, 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (90, 90, 90), 1)

    writer.write(frame)

writer.release()
print(f"  {out_path}  ({N_FRAMES} frames, {SECS}s @ {FPS}fps)")

print("\nDone. Run ./scripts/run_all.sh to start the demo.")
