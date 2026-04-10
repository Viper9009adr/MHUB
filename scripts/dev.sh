#!/usr/bin/env bash
# =============================================================================
# Meridian HUB — One-Command Bring-Up
#
# Usage:
#   bash scripts/dev.sh          # Start with defaults
#   bash scripts/dev.sh --port 9000  # Custom API port
#
# Checks:
#   - Port availability (API and gRPC)
#   - Python venv activation
#   - .env file presence
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
API_PORT="${API_PORT:-8123}"
GRPC_PORT="${MERIDIAN_GRPC_PORT:-50052}"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      API_PORT="$2"
      shift 2
      ;;
    --grpc-port)
      GRPC_PORT="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: bash scripts/dev.sh [--port PORT] [--grpc-port PORT]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

check_port() {
  local port=$1
  local name=$2
  if command -v lsof &>/dev/null; then
    if lsof -i ":${port}" &>/dev/null; then
      echo "ERROR: ${name} port ${port} is already in use."
      echo "  Kill the process or use a different port: --port ${port}"
      exit 1
    fi
  elif command -v nc &>/dev/null; then
    if nc -z localhost "${port}" 2>/dev/null; then
      echo "ERROR: ${name} port ${port} is already in use."
      exit 1
    fi
  fi
  echo "  ${name} port ${port} is available."
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

echo "=== Meridian HUB — Bring-Up ==="
echo ""

# Check .env
if [[ ! -f "${PROJECT_ROOT}/.env" ]]; then
  echo "WARNING: No .env file found. Copy .env.example to .env and configure."
  echo "  cp .env.example .env"
fi

# Check port availability
echo "Checking port availability..."
check_port "${API_PORT}" "API"
check_port "${GRPC_PORT}" "gRPC"
echo ""

# Check Python venv
if [[ ! -d "${PROJECT_ROOT}/.venv" ]]; then
  echo "Creating Python virtual environment..."
  cd "${PROJECT_ROOT}"
  python3 -m venv .venv
fi

# Activate venv
# shellcheck disable=SC1091
source "${PROJECT_ROOT}/.venv/bin/activate"

# Install dependencies if needed
if ! python3 -c "import fastapi" 2>/dev/null; then
  echo "Installing Python dependencies..."
  pip install -q -r "${PROJECT_ROOT}/requirements.txt" 2>/dev/null || true
fi

echo ""
echo "Starting Meridian HUB..."
echo "  API:  http://localhost:${API_PORT}"
echo "  gRPC: localhost:${GRPC_PORT}"
echo "  PID file: ${PROJECT_ROOT}/.meridian.pid"
echo "  Stop with: bash scripts/stop.sh  OR  kill \$(cat .meridian.pid)"
echo ""

# Export config for the app
export API_PORT
export MERIDIAN_GRPC_PORT="${GRPC_PORT}"

PID_FILE="${PROJECT_ROOT}/.meridian.pid"

cleanup() {
  echo ""
  echo "Shutting down Meridian HUB..."
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "Done."
  exit 0
}
trap cleanup SIGINT SIGTERM

# Start the API server in the background so the trap can catch signals
cd "${PROJECT_ROOT}"
PYTHONPATH="${PROJECT_ROOT}" uvicorn src.api.app:create_app \
  --host 0.0.0.0 \
  --port "${API_PORT}" \
  --factory \
  --reload 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"

wait "$SERVER_PID"
rm -f "$PID_FILE"
