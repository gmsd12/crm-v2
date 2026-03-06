# CRM Deployment Runbook

## 1) Prerequisites

- Ubuntu 22.04+ (or similar Linux)
- Python 3.12+, `venv`
- Redis 6+
- Nginx
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

## 4) Backend Processes (systemd)

Use templates from:

- `docs/deploy/systemd/crm-api.service`
- `docs/deploy/systemd/crm-celery-worker.service`
- `docs/deploy/systemd/crm-celery-beat.service`

Install:

```bash
sudo cp docs/deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crm-api crm-celery-worker crm-celery-beat
sudo systemctl status crm-api crm-celery-worker crm-celery-beat
```

## 5) Frontend (crm-web) with PM2

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

## 6) Nginx

Use template:

- `docs/deploy/nginx.crm.example.conf`

Important:

- Keep SSE buffering disabled on `/api/v1/notifications/stream/`
- Forward `X-Forwarded-Proto` header
- Enable TLS
- Serve `/static/` from `DJANGO_STATIC_ROOT` path

## 7) Post-deploy checks

- `GET https://<api-domain>/api/health/` returns `{"status":"ок"}`
- Login works, refresh works
- Notifications stream works
- Celery worker and beat are active
- `systemctl status` is healthy for all services

## 8) Backup minimum

- DB backup schedule (daily + retention)
- Media directory backup (`/opt/crm/crm/media`)
- `.env` secure copy in secret manager/vault

## 9) Recommended hardening

- Fail2ban + UFW
- Restrict DB/Redis to private network only
- Monitor with basic alerts: process down, disk usage, 5xx spikes
