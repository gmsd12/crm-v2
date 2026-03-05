#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run/dev-stack"

if [[ ! -d "$RUN_DIR" ]]; then
  echo "No runtime directory found: $RUN_DIR"
  exit 0
fi

stopped=0
for pid_file in "$RUN_DIR"/*.pid; do
  [[ -e "$pid_file" ]] || continue
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  name="$(basename "$pid_file" .pid)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "Stopping $name (PID: $pid)..."
    kill -TERM -- "-$pid" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
    stopped=1
  fi
  rm -f "$pid_file"
done

if [[ "$stopped" -eq 0 ]]; then
  echo "No running dev-stack processes found."
else
  echo "Stop signal sent."
fi
