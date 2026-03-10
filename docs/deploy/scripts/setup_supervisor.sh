#!/usr/bin/env bash
set -euo pipefail

# Backend process setup for CRM:
# - gunicorn (ASGI) on unix socket
# - celery worker
# - celery beat

APP_DIR="${APP_DIR:-/opt/crm/crm}"
VENV_DIR="${VENV_DIR:-$APP_DIR/.venv}"
RUN_DIR="${RUN_DIR:-/run/crm}"
SOCKET_PATH="${SOCKET_PATH:-$RUN_DIR/gunicorn.sock}"
SUPERVISOR_CONF="${SUPERVISOR_CONF:-/etc/supervisor/conf.d/crm.conf}"
WORKERS="${WORKERS:-3}"
RUN_USER="${RUN_USER:-www-data}"
RUN_GROUP="${RUN_GROUP:-www-data}"

if [[ ! -d "$APP_DIR" ]]; then
  echo "APP_DIR does not exist: $APP_DIR" >&2
  exit 1
fi

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "Python not found in venv: $VENV_DIR/bin/python" >&2
  exit 1
fi

echo "[1/5] Installing supervisor"
apt-get update
apt-get install -y supervisor

echo "[2/5] Creating runtime dir for socket"
cat >/etc/tmpfiles.d/crm.conf <<EOF
d $RUN_DIR 0755 $RUN_USER $RUN_GROUP -
EOF
systemd-tmpfiles --create /etc/tmpfiles.d/crm.conf

echo "[3/5] Writing supervisor config: $SUPERVISOR_CONF"
cat >"$SUPERVISOR_CONF" <<EOF
[program:crm_api]
directory=$APP_DIR
command=$VENV_DIR/bin/gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker --workers $WORKERS --bind unix:$SOCKET_PATH --timeout 120 --access-logfile - --error-logfile -
user=$RUN_USER
autostart=true
autorestart=true
stopsignal=TERM
stopasgroup=true
killasgroup=true
stdout_logfile=/var/log/supervisor/crm_api.log
stderr_logfile=/var/log/supervisor/crm_api.err.log
environment=PYTHONUNBUFFERED="1"

[program:crm_celery_worker]
directory=$APP_DIR
command=$VENV_DIR/bin/celery -A config worker -l info
user=$RUN_USER
autostart=true
autorestart=true
stopsignal=TERM
stopasgroup=true
killasgroup=true
stdout_logfile=/var/log/supervisor/crm_celery_worker.log
stderr_logfile=/var/log/supervisor/crm_celery_worker.err.log
environment=PYTHONUNBUFFERED="1"

[program:crm_celery_beat]
directory=$APP_DIR
command=$VENV_DIR/bin/celery -A config beat -l info
user=$RUN_USER
autostart=true
autorestart=true
stopsignal=TERM
stopasgroup=true
killasgroup=true
stdout_logfile=/var/log/supervisor/crm_celery_beat.log
stderr_logfile=/var/log/supervisor/crm_celery_beat.err.log
environment=PYTHONUNBUFFERED="1"
EOF

echo "[4/5] Restarting supervisor"
systemctl enable --now supervisor
supervisorctl reread
supervisorctl update

echo "[5/5] Current status"
supervisorctl status

echo
echo "Done. Gunicorn socket: $SOCKET_PATH"
