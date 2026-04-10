#!/usr/bin/env bash
# Stop the Meridian HUB server started by dev.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="${PROJECT_ROOT}/.meridian.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No .meridian.pid found — server may not be running."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  echo "Stopping Meridian HUB (PID $PID)..."
  kill -TERM "$PID"
  echo "Done."
else
  echo "Process $PID is not running. Cleaning up stale PID file."
fi
rm -f "$PID_FILE"
