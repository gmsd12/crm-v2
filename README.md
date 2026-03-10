# CRM API (Django + DRF)

Backend for CRM v2 with:
- IAM auth (JWT access + refresh cookie)
- Partner API token authentication
- Lead ingestion with duplicate rejection by `phone`

## Quick start

1. Create env file:
   - copy `.env.example` to `.env`
2. Run migrations:
   - `./.venv/bin/python manage.py migrate`
3. Create superuser:
   - `./.venv/bin/python manage.py createsuperuser`
4. Run server:
   - `./.venv/bin/python manage.py runserver`

## Useful URLs

- API docs: `/api/docs/`
- OpenAPI schema: `/api/schema/`
- Health check: `/api/health/`

## Tests

- Run all tests:
  - `./.venv/bin/python manage.py test`
- Run focused suites:
  - `./.venv/bin/python manage.py test apps.core apps.iam apps.partners`

## Demo data

- Seed readable demo dataset (partner, pipeline, statuses, users, leads, comments):
  - `./.venv/bin/python manage.py seed_demo_crm`
- Top up only leads/comments at any time:
  - `./.venv/bin/python manage.py seed_demo_crm --leads 25 --comments 0`
  - `./.venv/bin/python manage.py seed_demo_crm --leads 0 --comments 60`
- Change demo users password:
  - `./.venv/bin/python manage.py seed_demo_crm --password newpass123`

## Operational notes

- Partner lead list endpoint uses pagination (`page`, `page_size`).
- Partner token requests are rate-limited by token id.
- Refresh tokens are rotated and blacklisted after rotation/logout.

## Production deploy

- See deployment runbook and templates in `docs/deploy/`:
  - `docs/deploy/README.md`
  - `docs/deploy/nginx.crm.example.conf`
  - `docs/deploy/supervisor/supervisord.conf.example`
  - `docs/deploy/supervisor/conf.d/crm-api.conf.example`
  - `docs/deploy/supervisor/conf.d/crm_celery_worker.conf.example`
  - `docs/deploy/supervisor/conf.d/crm_celery_beat.conf.example`
  - `docs/deploy/tmpfiles/crm.conf`
  - `docs/deploy/scripts/setup_supervisor.sh`
