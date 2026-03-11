from __future__ import annotations

try:
    from celery import shared_task
except Exception:  # pragma: no cover - fallback when celery package is absent
    def shared_task(*_args, **_kwargs):
        def decorator(func):
            func.delay = func
            return func

        return decorator

from apps.notifications.orchestrator import (
    process_notification_outbox_event,
    process_pending_notification_outbox_events,
)
from apps.notifications.runtime import deliver_notification, process_due_notifications
from apps.notifications.handlers.followups import (
    emit_manager_no_activity_notifications,
    emit_next_contact_overdue_notifications,
)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def process_notification_outbox_event_task(self, event_id: int):
    return process_notification_outbox_event(event_id=event_id)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def process_pending_notification_outbox_events_task(self, limit: int = 100):
    return process_pending_notification_outbox_events(limit=limit)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def deliver_notification_task(self, delivery_id: int):
    return deliver_notification(delivery_id=delivery_id)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def process_due_notifications_task(self, limit: int = 500):
    return process_due_notifications(limit=limit)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def emit_overdue_notifications_task(self, limit: int = 500):
    return emit_next_contact_overdue_notifications(limit=limit)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def emit_manager_no_activity_notifications_task(self, limit: int = 500):
    return emit_manager_no_activity_notifications(limit=limit)
