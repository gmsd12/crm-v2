#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
CELERY_BIN="$ROOT_DIR/.venv/bin/celery"

WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8000}"
BROKER_URL="${CELERY_BROKER_URL:-redis://127.0.0.1:6379/0}"
RUN_DIR="$ROOT_DIR/.run/dev-stack"
mkdir -p "$RUN_DIR"

PIDS=()
REDIS_STARTED_BY_SCRIPT=0

kill_group() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -TERM -- "-$pid" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
  fi
}

start_service() {
  local name="$1"
  shift
  local pid_file="$RUN_DIR/${name}.pid"

  if [[ -f "$pid_file" ]]; then
    local existing_pid
    existing_pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
      echo "Service '$name' is already running (PID: $existing_pid)."
      echo "Stop it first: scripts/dev-down.sh"
      exit 1
    fi
    rm -f "$pid_file"
  fi

  setsid "$@" &
  local pid=$!
  echo "$pid" > "$pid_file"
  PIDS+=("$pid")
}

cleanup() {
  local code=$?
  for pid in "${PIDS[@]:-}"; do
    kill_group "$pid"
  done

  if [[ "$REDIS_STARTED_BY_SCRIPT" == "1" ]]; then
    redis-cli -u "$BROKER_URL" shutdown nosave >/dev/null 2>&1 || true
  fi

  rm -f "$RUN_DIR"/*.pid >/dev/null 2>&1 || true

  exit "$code"
}
trap cleanup EXIT INT TERM

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing virtualenv python: $PYTHON_BIN"
  exit 1
fi

if [[ ! -x "$CELERY_BIN" ]]; then
  echo "Missing celery binary in venv: $CELERY_BIN"
  echo "Install with: .venv/bin/pip install celery redis"
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli is not installed."
  exit 1
fi

if ! redis-cli -u "$BROKER_URL" ping >/dev/null 2>&1; then
  if ! command -v redis-server >/dev/null 2>&1; then
    echo "Redis is not running and redis-server is not installed."
    exit 1
  fi
  echo "Starting local Redis..."
  redis-server --daemonize yes
  REDIS_STARTED_BY_SCRIPT=1
  sleep 0.5
fi

if ! redis-cli -u "$BROKER_URL" ping >/dev/null 2>&1; then
  echo "Redis is not reachable at $BROKER_URL"
  exit 1
fi

echo "Applying migrations..."
"$PYTHON_BIN" manage.py migrate --noinput

echo "Starting Django on ${WEB_HOST}:${WEB_PORT}..."
start_service django "$PYTHON_BIN" manage.py runserver "${WEB_HOST}:${WEB_PORT}" --noreload

echo "Starting Celery worker..."
start_service celery_worker "$CELERY_BIN" -A config worker -l info

echo "Starting Celery beat..."
start_service celery_beat "$CELERY_BIN" -A config beat -l info

echo "Dev stack is up. Press Ctrl+C to stop all services."

wait -n "${PIDS[@]}"
