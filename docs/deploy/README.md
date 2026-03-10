# CRM Deployment Runbook

## 1) Prerequisites

- Ubuntu 22.04+ (or similar Linux)
- Python 3.12+, `venv`
- Redis 6+
- Nginx
- Supervisor
- TLS certificate (Let's Encrypt)
- Frontend runtime: `bun` + `pm2` (for `crm-web`)

Optional:
- PostgreSQL (recommended for production instead of sqlite)

## 2) Backend Environment

Create `/opt/crm/crm/.env` from `.env.example` and set at least:

- `DJANGO_ENV=prod`
- `DJANGO_SECRET_KEY=<strong-secret>`
- `DJANGO_DEBUG=false`
- `DJANGO_ALLOWED_HOSTS=<api-domain>,<server-ip>`
- `DATABASE_URL=<postgres://...>` (recommended)
- `DJANGO_STATIC_ROOT=/opt/crm/crm/staticfiles`
- `CORS_ALLOWED_ORIGINS=https://<frontend-domain>`
- `CSRF_TRUSTED_ORIGINS=https://<frontend-domain>`
- `JWT_REFRESH_COOKIE_SECURE=true`
- `DJANGO_SECURE_SSL_REDIRECT=true`
- `DJANGO_SESSION_COOKIE_SECURE=true`
- `DJANGO_CSRF_COOKIE_SECURE=true`
- `DJANGO_SECURE_PROXY_SSL_HEADER=true`
- `CELERY_BROKER_URL=redis://127.0.0.1:6379/0`
- `CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/1`

## 3) Backend First Deploy

```bash
cd /opt/crm/crm
python3 -m venv .venv
.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install -r requirements.txt
mkdir -p /opt/crm/crm/staticfiles
.venv/bin/python manage.py migrate
.venv/bin/python manage.py collectstatic --noinput
.venv/bin/python manage.py check --deploy
```

Create admin user:

```bash
cd /opt/crm/crm
.venv/bin/python manage.py createsuperuser
```

## 4) Config Layout

Canonical rule:

- Templates live in the repo under `docs/deploy/`
- Active configs live in `/etc/...`
- Runtime-only files live in `/run/...`
- Edit the repo template first, then copy/apply it to the system path, then reload the service

Canonical backend config map:

| Purpose | Repo template | Active system path |
|---|---|---|
| supervisor main config | `docs/deploy/supervisor/supervisord.conf.example` | `/etc/supervisor/supervisord.conf` |
| API process | `docs/deploy/supervisor/conf.d/crm-api.conf.example` | `/etc/supervisor/conf.d/crm-api.conf` |
| Celery worker | `docs/deploy/supervisor/conf.d/crm_celery_worker.conf.example` | `/etc/supervisor/conf.d/crm_celery_worker.conf` |
| Celery beat | `docs/deploy/supervisor/conf.d/crm_celery_beat.conf.example` | `/etc/supervisor/conf.d/crm_celery_beat.conf` |
| runtime socket dir | `docs/deploy/tmpfiles/crm.conf` | `/etc/tmpfiles.d/crm.conf` |
| nginx site | `docs/deploy/nginx.crm.example.conf` | `/etc/nginx/sites-available/crm.conf` and `/etc/nginx/sites-enabled/crm.conf` |

Runtime path:

- API unix socket: `/run/crm/gunicorn.sock`

Notes:

- `/run/crm/gunicorn.sock` is not stored in git and is not copied manually; it is created at runtime by gunicorn
- `/run/crm` must exist before the API starts, so keep `/etc/tmpfiles.d/crm.conf`
- `supervisor` is the only supported production process manager for this project

## 5) Backend Processes (supervisor + unix socket)

Install/update canonical supervisor files:

```bash
cd /opt/crm/crm
sudo bash docs/deploy/scripts/setup_supervisor.sh
sudo supervisorctl status
```

`gunicorn` runs in ASGI mode (`config.asgi:application` + `uvicorn_worker.UvicornWorker`) and binds to:

- `unix:/run/crm/gunicorn.sock`

## 6) Frontend (crm-web) with PM2

```bash
cd /opt/crm/crm-web
bun install
bun run build
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

Required frontend env (`/opt/crm/crm-web/.env`):

- `NODE_ENV=production`
- `PORT=3000`
- `NUXT_PUBLIC_API_BASE=https://<api-domain>`
- `NUXT_PUBLIC_SSE_URL=https://<api-domain>/api/v1/notifications/stream/`

## 7) Nginx

Use canonical paths:

- repo template: `docs/deploy/nginx.crm.example.conf`
- active files: `/etc/nginx/sites-available/crm.conf`, `/etc/nginx/sites-enabled/crm.conf`

Apply:

```bash
sudo cp docs/deploy/nginx.crm.example.conf /etc/nginx/sites-available/crm.conf
sudo ln -sf /etc/nginx/sites-available/crm.conf /etc/nginx/sites-enabled/crm.conf
sudo nginx -t
sudo systemctl reload nginx
```

Important:

- Keep SSE buffering disabled on `/api/v1/notifications/stream/`
- Forward `X-Forwarded-Proto` header
- Enable TLS
- Serve `/static/` from `DJANGO_STATIC_ROOT` path
- Proxy API via unix socket `/run/crm/gunicorn.sock`

## 8) Post-deploy checks

- `GET https://<api-domain>/api/health/` returns `{"status":"ок"}`
- Login works, refresh works
- Notifications stream works
- Celery worker and beat are active
- `supervisorctl status` is healthy for all services
- `ps -ef | grep gunicorn` shows `config.asgi:application`
- `grep -R "/run/crm/gunicorn.sock" /etc/nginx/sites-enabled/crm.conf /etc/supervisor/conf.d/crm-api.conf` matches both files

## 9) Backup minimum

- DB backup schedule (daily + retention)
- Media directory backup (`/opt/crm/crm/media`)
- `.env` secure copy in secret manager/vault

## 10) Config Update Workflow

When you change deploy config, use this order:

1. Edit the template in `docs/deploy/...`
2. Copy the template into the matching `/etc/...` path
3. Reload the owning service:
   - `supervisorctl reread && supervisorctl update` for `supervisor`
   - `systemctl reload nginx` for `nginx`
   - `systemd-tmpfiles --create /etc/tmpfiles.d/crm.conf` after tmpfiles changes
4. Verify with `supervisorctl status`, `nginx -t`, and `/api/health/`

Do not treat `/run/crm/gunicorn.sock` as a config file.
It is a runtime artifact created by gunicorn and recreated after restart/reboot.

## 11) Recommended hardening

- Fail2ban + UFW
- Restrict DB/Redis to private network only
- Monitor with basic alerts: process down, disk usage, 5xx spikes
