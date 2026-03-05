try:
    from .celery import app as celery_app
except Exception:  # pragma: no cover - celery is optional for local/test environments
    celery_app = None

__all__ = ("celery_app",)
