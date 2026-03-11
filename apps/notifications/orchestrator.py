from __future__ import annotations

import time

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.notifications.models import NotificationOutbox
from apps.notifications.registry import NotificationDomainEvent

_BROKER_HEALTH = {"checked_at": 0.0, "ok": False}


def enqueue_outbox_event_processing(event_id: int) -> None:
    if not _celery_broker_is_available():
        process_notification_outbox_event(event_id=event_id)
        return
    try:
        from apps.notifications.tasks import process_notification_outbox_event_task

        process_notification_outbox_event_task.delay(event_id=event_id)
    except Exception:
        process_notification_outbox_event(event_id=event_id)


def _celery_broker_is_available() -> bool:
    now_monotonic = time.monotonic()
    checked_at = _BROKER_HEALTH.get("checked_at", 0.0)
    if now_monotonic - checked_at < 5.0:
        return bool(_BROKER_HEALTH.get("ok", False))

    broker_url = getattr(settings, "CELERY_BROKER_URL", "").strip()
    if not broker_url:
        _BROKER_HEALTH.update({"checked_at": now_monotonic, "ok": False})
        return False

    try:
        import redis  # type: ignore

        client = redis.Redis.from_url(
            broker_url,
            socket_connect_timeout=0.2,
            socket_timeout=0.2,
            retry_on_timeout=False,
        )
        ok = bool(client.ping())
    except Exception:
        ok = False

    _BROKER_HEALTH.update({"checked_at": now_monotonic, "ok": ok})
    return ok


def _optional_int(value):
    if value in (None, "", 0, "0"):
        return None
    return int(value)


def _handle_lead_assigned(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_lead_assigned_notification

    emit_lead_assigned_notification(
        lead_id=int(payload["lead_id"]),
        to_manager_id=int(payload["to_manager_id"]),
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        from_manager_id=_optional_int(payload.get("from_manager_id")),
        suppress_actor_watcher=bool(payload.get("suppress_actor_watcher")),
    )


def _handle_bulk_lead_assigned(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_bulk_lead_assigned_notification

    emit_bulk_lead_assigned_notification(
        lead_ids=[int(lead_id) for lead_id in payload.get("lead_ids") or []],
        to_manager_id=int(payload["to_manager_id"]),
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        from_manager_ids=[int(manager_id) for manager_id in payload.get("from_manager_ids") or []],
        batch_id=(payload.get("batch_id") or "").strip() or None,
        suppress_actor_watcher=bool(payload.get("suppress_actor_watcher")),
    )


def _handle_lead_unassigned(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_lead_unassigned_notification

    emit_lead_unassigned_notification(
        lead_id=int(payload["lead_id"]),
        from_manager_id=_optional_int(payload.get("from_manager_id")),
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        audit_log_id=_optional_int(payload.get("audit_log_id")),
        suppress_actor_watcher=bool(payload.get("suppress_actor_watcher")),
    )


def _handle_bulk_lead_unassigned(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_bulk_lead_unassigned_notification

    emit_bulk_lead_unassigned_notification(
        lead_to_from_manager=[
            (int(item[0]), int(item[1]))
            for item in payload.get("lead_to_from_manager") or []
            if len(item) == 2
        ],
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        batch_id=(payload.get("batch_id") or "").strip() or None,
        suppress_actor_watcher=bool(payload.get("suppress_actor_watcher")),
    )


def _handle_lead_status_changed(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_lead_status_changed_notification

    emit_lead_status_changed_notification(
        lead_id=int(payload["lead_id"]),
        from_status_id=_optional_int(payload.get("from_status_id")),
        to_status_id=_optional_int(payload.get("to_status_id")),
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        audit_log_id=_optional_int(payload.get("audit_log_id")),
    )


def _handle_bulk_lead_status_changed(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_bulk_lead_status_changed_notification

    emit_bulk_lead_status_changed_notification(
        lead_status_items=[
            (int(item[0]), _optional_int(item[1]), _optional_int(item[2]))
            for item in payload.get("lead_status_items") or []
            if len(item) == 3
        ],
        actor_user_id=_optional_int(payload.get("actor_user_id")),
        batch_id=(payload.get("batch_id") or "").strip() or None,
    )


def _handle_deposit_created(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_deposit_created_notification

    emit_deposit_created_notification(
        deposit_id=int(payload["deposit_id"]),
        actor_user_id=_optional_int(payload.get("actor_user_id")),
    )


def _handle_comment_added(payload: dict) -> None:
    from apps.notifications.handlers.lead_events import emit_comment_added_notification

    emit_comment_added_notification(comment_id=int(payload["comment_id"]))


def _handle_partner_duplicate_attempt(payload: dict) -> None:
    from apps.notifications.handlers.alerts import emit_partner_duplicate_attempt_notification

    emit_partner_duplicate_attempt_notification(attempt_id=int(payload["attempt_id"]))


def _handle_next_contact_planned_sync(payload: dict) -> None:
    from apps.notifications.handlers.followups import reschedule_next_contact_planned_notifications

    reschedule_next_contact_planned_notifications(
        lead_id=int(payload["lead_id"]),
        remind_before_minutes=int(payload.get("remind_before_minutes") or 15),
    )


EVENT_HANDLERS = {
    NotificationDomainEvent.LEAD_ASSIGNED: _handle_lead_assigned,
    NotificationDomainEvent.LEAD_ASSIGNED_BULK: _handle_bulk_lead_assigned,
    NotificationDomainEvent.LEAD_UNASSIGNED: _handle_lead_unassigned,
    NotificationDomainEvent.LEAD_UNASSIGNED_BULK: _handle_bulk_lead_unassigned,
    NotificationDomainEvent.LEAD_STATUS_CHANGED: _handle_lead_status_changed,
    NotificationDomainEvent.LEAD_STATUS_CHANGED_BULK: _handle_bulk_lead_status_changed,
    NotificationDomainEvent.DEPOSIT_CREATED: _handle_deposit_created,
    NotificationDomainEvent.COMMENT_ADDED: _handle_comment_added,
    NotificationDomainEvent.PARTNER_DUPLICATE_ATTEMPT: _handle_partner_duplicate_attempt,
    NotificationDomainEvent.NEXT_CONTACT_PLANNED_SYNC: _handle_next_contact_planned_sync,
}


def process_notification_outbox_event(event_id: int) -> bool:
    now = timezone.now()
    with transaction.atomic():
        event = (
            NotificationOutbox.objects.select_for_update()
            .filter(id=event_id)
            .first()
        )
        if event is None:
            return False
        if event.status == NotificationOutbox.Status.PROCESSED:
            return False
        if event.status == NotificationOutbox.Status.PROCESSING:
            return False
        if event.available_at and event.available_at > now:
            return False
        event.mark_processing(at=now)

    handler = EVENT_HANDLERS.get(event.event_type)
    if handler is None:
        event.mark_failed(error_message=f"Unsupported notification event type: {event.event_type}")
        return False

    try:
        handler(event.payload or {})
    except Exception as exc:
        event.mark_failed(error_message=str(exc))
        return False

    event.mark_processed(at=timezone.now())
    return True


def process_pending_notification_outbox_events(*, limit: int = 100) -> int:
    now = timezone.now()
    event_ids = list(
        NotificationOutbox.objects.filter(
            status__in=[NotificationOutbox.Status.PENDING, NotificationOutbox.Status.FAILED],
            available_at__lte=now,
        )
        .order_by("available_at", "id")
        .values_list("id", flat=True)[: max(1, int(limit or 100))]
    )
    processed = 0
    for event_id in event_ids:
        if process_notification_outbox_event(event_id=event_id):
            processed += 1
    return processed
