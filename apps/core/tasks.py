from __future__ import annotations

try:
    from celery import shared_task
except Exception:  # pragma: no cover - fallback when celery package is absent
    def shared_task(*_args, **_kwargs):
        def decorator(func):
            func.delay = func
            return func

        return decorator

from apps.core.notifications import (
    deliver_notification,
    emit_next_contact_overdue_notifications,
    emit_manager_no_activity_notifications,
    process_due_notifications,
)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def deliver_notification_task(self, notification_id: int):
    return deliver_notification(notification_id=notification_id)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def process_due_notifications_task(self, limit: int = 500):
    return process_due_notifications(limit=limit)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def emit_overdue_notifications_task(self, limit: int = 500):
    return emit_next_contact_overdue_notifications(limit=limit)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def emit_manager_no_activity_notifications_task(self, limit: int = 500):
    return emit_manager_no_activity_notifications(limit=limit)
