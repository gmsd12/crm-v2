from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from apps.notifications.events import NotificationEvent
from apps.notifications.models import Notification, NotificationDelivery, NotificationDeliveryAttempt

User = get_user_model()


@dataclass(frozen=True)
class NotificationEmitPayload:
    event_type: str
    recipient_id: int
    title: str
    body: str = ""
    actor_user_id: int | None = None
    lead_id: int | None = None
    payload: dict | None = None
    dedupe_key: str | None = None
    scheduled_for: datetime | None = None


_BROKER_HEALTH = {"checked_at": 0.0, "ok": False}


def emit(payload: NotificationEmitPayload) -> Notification | None:
    recipient = User.objects.filter(id=payload.recipient_id, is_active=True).first()
    if recipient is None:
        return None

    scheduled_for = payload.scheduled_for or timezone.now()
    if payload.dedupe_key:
        exists = NotificationDelivery.objects.filter(
            notification__recipient_id=payload.recipient_id,
            dedupe_key=payload.dedupe_key,
            status__in=[NotificationDelivery.Status.PENDING, NotificationDelivery.Status.SENT],
        ).exists()
        if exists:
            return None

    notification = Notification.objects.create(
        event_type=payload.event_type,
        status=Notification.Status.PENDING,
        scheduled_for=scheduled_for,
        recipient_id=payload.recipient_id,
        actor_user_id=payload.actor_user_id,
        lead_id=payload.lead_id,
        title=payload.title,
        body=payload.body,
        payload=payload.payload or {},
    )
    delivery = NotificationDelivery.objects.create(
        notification=notification,
        status=NotificationDelivery.Status.PENDING,
        scheduled_for=scheduled_for,
        dedupe_key=payload.dedupe_key or "",
    )
    if scheduled_for <= timezone.now():
        enqueue_notification_delivery(delivery.id)
    return notification


def deliver_notification(delivery_id: int, *, now=None) -> bool:
    now = now or timezone.now()
    with transaction.atomic():
        delivery = (
            NotificationDelivery.objects.select_for_update()
            .select_related("notification")
            .filter(id=delivery_id)
            .first()
        )
        if delivery is None:
            return False
        if delivery.status in {NotificationDelivery.Status.SENT, NotificationDelivery.Status.CANCELLED}:
            return False
        if delivery.scheduled_for and delivery.scheduled_for > now:
            return False
        notification = delivery.notification
        last_sequence = (
            delivery.attempt_logs.aggregate(max_sequence=Max("sequence")).get("max_sequence") or 0
        )
        attempt = NotificationDeliveryAttempt.objects.create(
            delivery=delivery,
            sequence=max(delivery.attempts or 0, last_sequence) + 1,
            status=NotificationDeliveryAttempt.Status.STARTED,
            started_at=now,
        )
        if notification.event_type == NotificationEvent.NEXT_CONTACT_PLANNED:
            from apps.notifications.handlers.followups import can_deliver_next_contact_planned

            if not can_deliver_next_contact_planned(notification=notification):
                cancel_notification(delivery)
                attempt.mark_cancelled(
                    at=now,
                    error_message="Delivery precondition failed",
                )
                return False
        try:
            delivery.mark_sent(at=now)
            attempt.mark_sent(at=now)
            _sync_notification_delivery_state(notification=notification, delivery=delivery)
        except Exception as exc:
            delivery.mark_failed(error_message=str(exc))
            attempt.mark_failed(error_message=str(exc), at=now)
            _sync_notification_delivery_state(notification=notification, delivery=delivery)
            return False
        return True


def enqueue_notification_delivery(delivery_id: int) -> None:
    if not _celery_broker_is_available():
        deliver_notification(delivery_id=delivery_id)
        return
    try:
        from apps.notifications.tasks import deliver_notification_task

        deliver_notification_task.delay(delivery_id=delivery_id)
    except Exception:
        deliver_notification(delivery_id=delivery_id)


def process_due_notifications(*, now=None, limit: int = 500) -> int:
    now = now or timezone.now()
    if limit <= 0:
        return 0
    queryset = (
        NotificationDelivery.objects.filter(
            status=NotificationDelivery.Status.PENDING,
            scheduled_for__lte=now,
        )
        .order_by("scheduled_for", "id")
        .values_list("id", flat=True)[:limit]
    )
    delivered = 0
    for delivery_id in queryset:
        if deliver_notification(delivery_id=delivery_id, now=now):
            delivered += 1
    return delivered


def cancel_notification(delivery: NotificationDelivery) -> None:
    delivery.mark_cancelled()
    _sync_notification_delivery_state(notification=delivery.notification, delivery=delivery)


def _sync_notification_delivery_state(*, notification: Notification, delivery: NotificationDelivery) -> None:
    notification.status = delivery.status
    notification.scheduled_for = delivery.scheduled_for
    notification.sent_at = delivery.sent_at
    notification.save(
        update_fields=[
            "status",
            "scheduled_for",
            "sent_at",
            "updated_at",
        ]
    )


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
