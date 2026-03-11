from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import Count, Q
from django.utils import timezone

from apps.leads.models import Lead, LeadStatus
from apps.notifications.events import NotificationEvent
from apps.notifications.models import Notification
from apps.notifications.policies import resolve_user_settings_cached
from apps.notifications.recipients import resolve_lead_event_recipients, resolve_manager_event_recipients
from apps.notifications.renderers import format_human_datetime, format_user_short
from apps.notifications.runtime import NotificationEmitPayload, emit

User = get_user_model()


def slot_for_overdue(*, now, next_contact_at, repeat_minutes: int) -> int:
    if now < next_contact_at:
        return -1
    delay_seconds = (now - next_contact_at).total_seconds()
    return int(delay_seconds // (repeat_minutes * 60))


def is_within_overdue_notification_window(*, now) -> bool:
    local_now = timezone.localtime(now)
    return 8 <= local_now.hour < 18


def emit_next_contact_overdue_notifications(*, now=None, limit: int | None = None) -> int:
    now = now or timezone.now()
    if not is_within_overdue_notification_window(now=now):
        return 0
    queryset = (
        Lead.objects.select_related("manager")
        .filter(
            is_deleted=False,
            manager__isnull=False,
            manager__is_active=True,
            next_contact_at__isnull=False,
            next_contact_at__lt=now,
        )
        .filter(Q(last_contacted_at__isnull=True) | Q(last_contacted_at__lt=models.F("next_contact_at")))
        .filter(status__work_bucket__in=[LeadStatus.WorkBucket.WORKING, LeadStatus.WorkBucket.RETURN])
        .order_by("next_contact_at", "id")
    )
    if limit:
        queryset = queryset[:limit]

    created = 0
    settings_cache: dict[tuple[int, str], dict] = {}
    for lead in queryset:
        if lead.manager_id is None:
            continue
        if lead.last_contacted_at and lead.next_contact_at and lead.last_contacted_at >= lead.next_contact_at:
            continue

        recipients = resolve_lead_event_recipients(
            event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
            lead=lead,
            primary_recipient_ids=[lead.manager_id],
        )

        lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
        for recipient in recipients:
            settings = resolve_user_settings_cached(
                user=recipient,
                event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
                cache=settings_cache,
            )
            repeat_minutes = max(1, int(settings["repeat_minutes"]))
            slot = slot_for_overdue(now=now, next_contact_at=lead.next_contact_at, repeat_minutes=repeat_minutes)
            if slot < 0:
                continue
            notification = emit(
                NotificationEmitPayload(
                    event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
                    recipient_id=recipient.id,
                    actor_user_id=None,
                    lead_id=lead.id,
                    title=f"Просрочен: {lead_label}",
                    body=f"Следующий контакт просрочен с {format_human_datetime(lead.next_contact_at)}",
                    payload={
                        "notification_kind": "overdue",
                        "lead_id": str(lead.id),
                        "next_contact_at": lead.next_contact_at.isoformat() if lead.next_contact_at else None,
                        "slot": slot,
                        "repeat_minutes": repeat_minutes,
                    },
                    dedupe_key=f"followup_overdue:{lead.id}:{recipient.id}:{lead.next_contact_at.isoformat()}:{slot}",
                )
            )
            if notification is not None:
                created += 1
    return created


def slot_for_repeat_minutes(*, now, repeat_minutes: int) -> int:
    repeat_minutes = max(1, int(repeat_minutes))
    return int(now.timestamp() // (repeat_minutes * 60))


def emit_manager_no_activity_notifications(*, now=None, threshold: int | None = None, limit: int | None = None) -> int:
    now = now or timezone.now()
    threshold = max(1, int(threshold or getattr(settings, "NOTIFICATIONS_MANAGER_NO_ACTIVITY_THRESHOLD", 5)))
    overdue = (
        Lead.objects.filter(
            is_deleted=False,
            manager__isnull=False,
            manager__is_active=True,
            next_contact_at__isnull=False,
            next_contact_at__lt=now,
        )
        .filter(Q(last_contacted_at__isnull=True) | Q(last_contacted_at__lt=models.F("next_contact_at")))
        .filter(status__work_bucket__in=[LeadStatus.WorkBucket.WORKING, LeadStatus.WorkBucket.RETURN])
        .values("manager_id")
        .annotate(overdue_count=Count("id"))
        .filter(overdue_count__gte=threshold)
        .order_by("-overdue_count", "manager_id")
    )
    if limit:
        overdue = overdue[:limit]

    manager_ids = [row["manager_id"] for row in overdue if row.get("manager_id")]
    managers = {
        user.id: user
        for user in User.objects.filter(id__in=manager_ids, is_active=True).only("id", "username", "first_name", "last_name", "role")
    }
    settings_cache: dict[tuple[int, str], dict] = {}
    created = 0
    for row in overdue:
        manager_id = row.get("manager_id")
        if not manager_id:
            continue
        manager = managers.get(manager_id)
        if manager is None:
            continue
        recipients = resolve_manager_event_recipients(event_type=NotificationEvent.MANAGER_NO_ACTIVITY, manager=manager)
        if not recipients:
            continue
        overdue_count = int(row.get("overdue_count") or 0)
        for recipient in recipients:
            user_settings = resolve_user_settings_cached(
                user=recipient,
                event_type=NotificationEvent.MANAGER_NO_ACTIVITY,
                cache=settings_cache,
            )
            slot = slot_for_repeat_minutes(now=now, repeat_minutes=user_settings["repeat_minutes"])
            notification = emit(
                NotificationEmitPayload(
                    event_type=NotificationEvent.MANAGER_NO_ACTIVITY,
                    recipient_id=recipient.id,
                    actor_user_id=None,
                    lead_id=None,
                    title=f"Нет активности менеджера: {format_user_short(manager)}",
                    body=f"{overdue_count} просроченных лидов без контакта",
                    payload={
                        "manager_id": str(manager.id),
                        "overdue_count": overdue_count,
                        "threshold": threshold,
                        "slot": slot,
                    },
                    dedupe_key=f"manager_no_activity:{manager.id}:{recipient.id}:{slot}",
                )
            )
            if notification is not None:
                created += 1
    return created


def can_deliver_next_contact_planned(*, notification: Notification) -> bool:
    lead = (
        Lead.objects.select_related("manager", "status")
        .filter(id=notification.lead_id, is_deleted=False, manager__isnull=False, manager__is_active=True)
        .first()
    )
    if lead is None or lead.next_contact_at is None:
        return False
    if lead.last_contacted_at and lead.last_contacted_at >= lead.next_contact_at:
        return False
    if lead.status_id is None or getattr(lead.status, "work_bucket", None) not in {
        LeadStatus.WorkBucket.WORKING,
        LeadStatus.WorkBucket.RETURN,
    }:
        return False

    expected_next_contact_at = (notification.payload or {}).get("next_contact_at")
    current_next_contact_at = lead.next_contact_at.isoformat()
    if expected_next_contact_at and expected_next_contact_at != current_next_contact_at:
        return False

    recipients = resolve_lead_event_recipients(
        event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
        lead=lead,
        primary_recipient_ids=[lead.manager_id] if lead.manager_id else [],
    )
    return any(recipient.id == notification.recipient_id for recipient in recipients)


def reschedule_next_contact_planned_notifications(*, lead_id: int, remind_before_minutes: int = 15) -> int:
    lead = Lead.objects.select_related("manager", "status").filter(id=lead_id).first()
    if lead is None:
        return 0

    Notification.objects.filter(
        event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
        lead_id=lead.id,
        status=Notification.Status.PENDING,
    ).delete()

    if lead.is_deleted or lead.manager_id is None or lead.next_contact_at is None:
        return 0
    if lead.status_id is None or getattr(lead.status, "work_bucket", None) not in {
        LeadStatus.WorkBucket.WORKING,
        LeadStatus.WorkBucket.RETURN,
    }:
        return 0
    if lead.last_contacted_at and lead.last_contacted_at >= lead.next_contact_at:
        return 0

    recipients = resolve_lead_event_recipients(
        event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
        lead=lead,
        primary_recipient_ids=[lead.manager_id],
    )
    if not recipients:
        return 0

    now = timezone.now()
    remind_before_minutes = max(0, int(remind_before_minutes))
    scheduled_for = lead.next_contact_at - timedelta(minutes=remind_before_minutes)
    if scheduled_for < now:
        scheduled_for = now

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    next_contact_iso = lead.next_contact_at.isoformat()
    created = 0
    for recipient in recipients:
        notification = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
                recipient_id=recipient.id,
                actor_user_id=None,
                lead_id=lead.id,
                title=f"Напоминание о контакте: {lead_label}",
                body=f"Контакт запланирован на {format_human_datetime(lead.next_contact_at)}",
                payload={
                    "notification_kind": "planned_reminder",
                    "lead_id": str(lead.id),
                    "next_contact_at": next_contact_iso,
                    "remind_before_minutes": remind_before_minutes,
                },
                dedupe_key=f"planned_next_contact:{lead.id}:{recipient.id}:{next_contact_iso}",
                scheduled_for=scheduled_for,
            )
        )
        if notification is not None:
            created += 1
    return created


def schedule_next_contact_overdue_notifications(*, lead_ids: list[int], delay_minutes: int = 0) -> int:
    if not lead_ids:
        return 0
    created = 0
    for lead_id in lead_ids:
        created += reschedule_next_contact_planned_notifications(
            lead_id=lead_id,
            remind_before_minutes=max(0, int(delay_minutes or 15)),
        )
    return created
