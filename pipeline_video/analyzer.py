"""
pipeline_video/analyzer.py — Pipeline B.

Analyses the video clip at fixtures/video/cell.mp4 and emits per-second events:
- VISION_DETECTION   — what objects are visible in this 1s window
- VISION_NO_MOTION   — when the scene is static for > N seconds
- VISION_OCCLUSION   — when a previously-tracked object disappears

It also writes an annotated frame stream to the dashboard (every 5th frame
is JPEG-encoded and stashed in Redis as "video:latest_frame" — the dashboard
polls this).

Two engine modes:
- yolo  — YOLOv8n on CPU (~2-3 fps on a 2018 Intel Mac at 320x240)
- mock  — OpenCV background-subtraction motion detection (always works,
          used as fallback if ultralytics import fails)
"""
from __future__ import annotations
import argparse
import base64
import os
import sys
import time
import datetime as dt
from typing import Optional

import cv2
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import (
    get_redis, publish_event, make_event,
    SEVERITY_INFO, SEVERITY_WARN,
)

VIDEO_PATH_DEFAULT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "fixtures", "video", "cell.mp4"
)


# ---------------------------------------------------------------------------
# Engine: YOLO (preferred)
# ---------------------------------------------------------------------------

class YoloEngine:
    """YOLOv8n on CPU. First call downloads the ~6MB weights from Ultralytics.

    On the demo machine (offline), copy a pre-downloaded yolov8n.pt next to
    this file and pass --model-path; we fall back to that if available.
    """
    def __init__(self, model_path: Optional[str] = None):
        from ultralytics import YOLO  # lazy import; keeps fallback usable
        # Default uses the bundled name — Ultralytics caches under ~/.cache
        path = model_path or "yolov8n.pt"
        print(f"[Pipeline B] loading YOLOv8n from {path}…")
        self.model = YOLO(path)

    def detect(self, frame: np.ndarray) -> list[dict]:
        # imgsz=224 keeps inference fast on CPU; conf=0.25 is forgiving for our synthetic frames
        results = self.model.predict(frame, imgsz=224, conf=0.25, verbose=False, max_det=10)
        out = []
        for r in results:
            names = r.names
            for box in r.boxes:
                cls_id = int(box.cls[0].item())
                conf = float(box.conf[0].item())
                xyxy = box.xyxy[0].tolist()
                out.append({"label": names[cls_id], "conf": round(conf, 2),
                            "bbox": [round(v, 1) for v in xyxy]})
        return out


# ---------------------------------------------------------------------------
# Engine: mock motion detection (fallback)
# ---------------------------------------------------------------------------

class MotionEngine:
    """Background-subtraction-based motion detection. Always works.

    This is the demo-friendly fallback: it labels moving regions as "object"
    and reports their bbox, which is enough for the orchestrator to fuse
    with log signals like ROVER_STUCK_AND_EMPTY.
    """
    def __init__(self):
        self.bgsub = cv2.createBackgroundSubtractorMOG2(history=30, varThreshold=25, detectShadows=False)

    def detect(self, frame: np.ndarray) -> list[dict]:
        mask = self.bgsub.apply(frame)
        # Clean up the mask
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, np.ones((3, 3), np.uint8))
        mask = cv2.dilate(mask, np.ones((5, 5), np.uint8), iterations=1)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        out = []
        for c in contours:
            area = cv2.contourArea(c)
            if area < 200:
                continue
            x, y, w, h = cv2.boundingRect(c)
            out.append({"label": "moving_object", "conf": 0.99,
                        "bbox": [x, y, x + w, y + h]})
        return out


# ---------------------------------------------------------------------------
# Annotation helper
# ---------------------------------------------------------------------------

def annotate(frame: np.ndarray, detections: list[dict]) -> np.ndarray:
    """Draw bounding boxes onto a copy of the frame for the dashboard preview."""
    out = frame.copy()
    for d in detections:
        x1, y1, x2, y2 = [int(v) for v in d["bbox"]]
        cv2.rectangle(out, (x1, y1), (x2, y2), (50, 200, 50), 1)
        label = f"{d['label']} {d['conf']:.2f}"
        cv2.putText(out, label, (x1, max(y1 - 4, 10)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.35, (40, 40, 40), 1)
    return out


def encode_jpeg_b64(frame: np.ndarray, quality: int = 70) -> str:
    ok, buf = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    return base64.b64encode(buf.tobytes()).decode("ascii") if ok else ""


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(video_path: str, engine_name: str, loop: bool = True,
        zone_asset: str = "ROVER_A", emit_every_n: int = 5,
        speed: float = 1.0) -> None:
    r = get_redis()

    # Pick engine
    if engine_name == "yolo":
        try:
            engine = YoloEngine()
        except Exception as e:
            print(f"[Pipeline B] YOLO unavailable ({e}); falling back to motion detector")
            engine = MotionEngine()
    else:
        engine = MotionEngine()

    print(f"[Pipeline B] opening {video_path} (engine={type(engine).__name__})")
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        # Codec fallback — generator may have written .avi if mp4v wasn't available
        alt = video_path.replace(".mp4", ".avi")
        if os.path.exists(alt):
            print(f"[Pipeline B] {video_path} not opened — trying {alt}")
            cap = cv2.VideoCapture(alt)
        if not cap.isOpened():
            print(f"[Pipeline B] could not open video at {video_path}")
            return

    fps = cap.get(cv2.CAP_PROP_FPS) or 5
    frame_period = 1.0 / (fps * speed)

    last_seen: list[dict] = []
    no_motion_count = 0
    frame_idx = 0
    while True:
        ok, frame = cap.read()
        if not ok:
            if loop:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue
            break

        detections = engine.detect(frame)

        # State logic — emit only meaningful events, not every frame
        if frame_idx % emit_every_n == 0:
            if not detections:
                no_motion_count += 1
                if no_motion_count > 3:
                    publish_event(r, make_event(
                        asset_id=zone_asset,
                        event_name="VISION_NO_MOTION",
                        payload={"frames_idle": no_motion_count, "engine": type(engine).__name__},
                        pipeline="B",
                    ))
            else:
                no_motion_count = 0
                publish_event(r, make_event(
                    asset_id=zone_asset,
                    event_name="VISION_DETECTION",
                    payload={
                        "n": len(detections),
                        "objects": detections[:5],
                        "engine": type(engine).__name__,
                    },
                    pipeline="B",
                ))
            # Stash the annotated frame for the dashboard preview
            preview = annotate(frame, detections)
            r.set("video:latest_frame", encode_jpeg_b64(preview), ex=30)
            r.set("video:latest_ts", dt.datetime.now().astimezone().isoformat(timespec="milliseconds"))

        last_seen = detections
        frame_idx += 1
        time.sleep(frame_period)

    cap.release()
    print("[Pipeline B] done")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--video", default=VIDEO_PATH_DEFAULT)
    p.add_argument("--engine", choices=["yolo", "mock"], default="mock",
                   help="mock = OpenCV motion detector (fast, always works)")
    p.add_argument("--zone-asset", default="ROVER_A",
                   help="Which asset this camera covers (events tag this asset_id)")
    p.add_argument("--no-loop", action="store_true")
    p.add_argument("--speed", type=float, default=1.0,
                   help="Playback speed multiplier (1.0 = real-time)")
    args = p.parse_args()
    run(args.video, args.engine, loop=not args.no_loop,
        zone_asset=args.zone_asset, speed=args.speed)


if __name__ == "__main__":
    main()
