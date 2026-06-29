#!/usr/bin/env bash
# stop_all.sh — kills the four demo services started by run_all.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_DIR="$ROOT/data/pids"
QUIET=0
[[ "${1:-}" == "--quiet" ]] && QUIET=1

if [[ ! -d "$PID_DIR" ]]; then
  [[ "$QUIET" == 0 ]] && echo "No PID directory at $PID_DIR — nothing to do."
  exit 0
fi

for pidfile in "$PID_DIR"/*.pid; do
  [[ -f "$pidfile" ]] || continue
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  name="$(basename "$pidfile" .pid)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    [[ "$QUIET" == 0 ]] && echo "  stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null || true
    # Give it a moment, then force
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pidfile"
done

[[ "$QUIET" == 0 ]] && echo "All demo processes stopped."
