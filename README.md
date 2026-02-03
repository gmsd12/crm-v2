# CRM API (Django + DRF)

Backend for CRM v2 with:
- IAM auth (JWT access + refresh cookie)
- Partner API token authentication
- Lead ingestion with idempotency by `external_id`

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

## Operational notes

- Partner lead list endpoint uses pagination (`page`, `page_size`).
- Partner token requests are rate-limited by token id.
- Refresh tokens are rotated and blacklisted after rotation/logout.
