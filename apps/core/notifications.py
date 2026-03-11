from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import time

from django.contrib.auth import get_user_model
from django.conf import settings
from django.db import models
from django.db.models import Count, Q
from django.utils import timezone

from apps.core.models import Notification, NotificationPolicy, NotificationPreference
from apps.iam.models import UserRole
from apps.leads.models import Lead, LeadComment, LeadDeposit, LeadDuplicateAttempt, LeadStatus

User = get_user_model()


class NotificationEvent:
    NEXT_CONTACT_PLANNED = "next_contact_planned"
    LEAD_ASSIGNED = "lead_assigned"
    LEAD_UNASSIGNED = "lead_unassigned"
    LEAD_STATUS_CHANGED = "lead_status_changed"
    DEPOSIT_CREATED = "deposit_created"
    MANAGER_NO_ACTIVITY = "manager_no_activity"
    PARTNER_DUPLICATE_ATTEMPT = "partner_duplicate_attempt"
    NEXT_CONTACT_OVERDUE = "next_contact_overdue"
    COMMENT_ADDED = "comment_added"


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


DEFAULT_POLICY_CONFIG = {
    NotificationEvent.NEXT_CONTACT_PLANNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_ASSIGNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.COMMENT_ADDED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_UNASSIGNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_STATUS_CHANGED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.DEPOSIT_CREATED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.MANAGER_NO_ACTIVITY: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.PARTNER_DUPLICATE_ATTEMPT: {
        "enabled_by_default": True,
        "default_repeat_minutes": 60,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.NEXT_CONTACT_OVERDUE: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
}
_BROKER_HEALTH = {"checked_at": 0.0, "ok": False}


def _format_human_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return timezone.localtime(value).strftime("%d-%m-%Y %H:%M")


def _format_user_short(user) -> str:
    if not user:
        return ""
    first_name = (getattr(user, "first_name", "") or "").strip()
    last_name = (getattr(user, "last_name", "") or "").strip()
    username = (getattr(user, "username", "") or "").strip()
    if last_name and first_name:
        return f"{first_name[0].upper()}. {last_name}"
    if last_name:
        return last_name
    return username or f"пользователь {getattr(user, 'id', '')}"


def event_types_for_user(*, user) -> list[str]:
    role = getattr(user, "role", None)
    if role in {UserRole.TEAMLEADER, UserRole.ADMIN, UserRole.SUPERUSER}:
        return list(DEFAULT_POLICY_CONFIG.keys())
    allowed = {
        NotificationEvent.NEXT_CONTACT_PLANNED,
        NotificationEvent.LEAD_ASSIGNED,
        NotificationEvent.COMMENT_ADDED,
        NotificationEvent.NEXT_CONTACT_OVERDUE,
        NotificationEvent.LEAD_UNASSIGNED,
    }
    return [event_type for event_type in DEFAULT_POLICY_CONFIG.keys() if event_type in allowed]


def get_or_create_policy(event_type: str) -> NotificationPolicy:
    defaults = DEFAULT_POLICY_CONFIG.get(
        event_type,
        {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": NotificationPolicy.WatchScope.OWN,
            "apply_to_teamleaders": False,
            "apply_to_admins": False,
            "apply_to_superusers": False,
        },
    )
    policy, _created = NotificationPolicy.objects.get_or_create(event_type=event_type, defaults=defaults)
    return policy


def resolve_user_notification_settings(*, user, event_type: str) -> dict:
    policy = get_or_create_policy(event_type)
    pref = NotificationPreference.objects.filter(user_id=user.id, event_type=event_type).first()
    enabled = policy.enabled_by_default if pref is None or pref.enabled is None else pref.enabled
    repeat_minutes = (
        policy.default_repeat_minutes if pref is None or pref.repeat_minutes is None else pref.repeat_minutes
    )
    watch_scope = policy.default_watch_scope if pref is None or not pref.watch_scope else pref.watch_scope
    return {
        "policy": policy,
        "enabled": bool(enabled),
        "repeat_minutes": max(1, int(repeat_minutes or 1)),
        "watch_scope": watch_scope,
    }


def _resolve_user_settings_cached(*, user, event_type: str, cache: dict[tuple[int, str], dict]) -> dict:
    key = (user.id, event_type)
    settings = cache.get(key)
    if settings is None:
        settings = resolve_user_notification_settings(user=user, event_type=event_type)
        cache[key] = settings
    return settings


def emit(payload: NotificationEmitPayload) -> Notification | None:
    recipient = User.objects.filter(id=payload.recipient_id, is_active=True).first()
    if recipient is None:
        return None

    scheduled_for = payload.scheduled_for or timezone.now()
    if payload.dedupe_key:
        exists = Notification.objects.filter(
            recipient_id=payload.recipient_id,
            dedupe_key=payload.dedupe_key,
            status__in=[Notification.Status.PENDING, Notification.Status.SENT],
        ).exists()
        if exists:
            return None

    notification = Notification.objects.create(
        event_type=payload.event_type,
        channel=Notification.Channel.IN_APP,
        status=Notification.Status.PENDING,
        scheduled_for=scheduled_for,
        recipient_id=payload.recipient_id,
        actor_user_id=payload.actor_user_id,
        lead_id=payload.lead_id,
        title=payload.title,
        body=payload.body,
        payload=payload.payload or {},
        dedupe_key=payload.dedupe_key or "",
    )
    if scheduled_for <= timezone.now():
        enqueue_notification_delivery(notification.id)
    return notification


def deliver_notification(notification_id: int, *, now=None) -> bool:
    now = now or timezone.now()
    notification = Notification.objects.filter(id=notification_id).first()
    if notification is None:
        return False
    if notification.status in {Notification.Status.SENT, Notification.Status.CANCELLED}:
        return False
    if notification.scheduled_for and notification.scheduled_for > now:
        return False
    if notification.event_type == NotificationEvent.NEXT_CONTACT_PLANNED:
        if not _can_deliver_next_contact_planned(notification=notification):
            _cancel_notification(notification)
            return False
    try:
        notification.mark_sent(at=now)
    except Exception as exc:
        notification.mark_failed(error_message=str(exc))
        return False
    return True


def enqueue_notification_delivery(notification_id: int) -> None:
    if not _celery_broker_is_available():
        deliver_notification(notification_id=notification_id)
        return
    try:
        from apps.core.tasks import deliver_notification_task

        deliver_notification_task.delay(notification_id=notification_id)
    except Exception:
        deliver_notification(notification_id=notification_id)


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


def process_due_notifications(*, now=None, limit: int = 500) -> int:
    now = now or timezone.now()
    if limit <= 0:
        return 0
    queryset = (
        Notification.objects.filter(
            status=Notification.Status.PENDING,
            scheduled_for__lte=now,
        )
        .order_by("scheduled_for", "id")
        .values_list("id", flat=True)[:limit]
    )
    delivered = 0
    for notification_id in queryset:
        if deliver_notification(notification_id=notification_id, now=now):
            delivered += 1
    return delivered


def emit_lead_assigned_notification(*, lead_id: int, to_manager_id: int, actor_user_id: int | None, from_manager_id: int | None) -> Notification | None:
    lead = Lead.objects.select_related("manager").filter(id=lead_id).first()
    if lead is None:
        return None

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_ASSIGNED,
        lead=lead,
        primary_recipient_ids=[to_manager_id],
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    manager_label = _format_user_short(lead.manager) if lead.manager else "менеджер"

    created_notification = None
    for recipient in recipients:
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_ASSIGNED,
                recipient_id=recipient.id,
                actor_user_id=actor_user_id,
                lead_id=lead.id,
                title=f"Назначен новый лид: {lead_label}",
                body=f"Лид #{lead.id} назначен пользователю {manager_label}",
                payload={
                    "lead_id": str(lead.id),
                    "from_manager_id": str(from_manager_id) if from_manager_id else None,
                    "to_manager_id": str(to_manager_id),
                },
                dedupe_key=f"lead_assigned:{lead.id}:{to_manager_id}:{recipient.id}:{from_manager_id or 'none'}",
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def emit_bulk_lead_assigned_notification(
    *,
    lead_ids: list[int],
    to_manager_id: int,
    actor_user_id: int | None,
    from_manager_ids: list[int] | None = None,
    batch_id: str | None = None,
) -> int:
    unique_lead_ids = [int(lead_id) for lead_id in dict.fromkeys(lead_ids)]
    if not unique_lead_ids:
        return 0

    sample_lead = (
        Lead.objects.select_related("manager")
        .filter(id__in=unique_lead_ids)
        .order_by("id")
        .first()
    )
    if sample_lead is None:
        return 0

    to_manager = User.objects.filter(id=to_manager_id, is_active=True).only("id", "username", "first_name", "last_name").first()
    to_manager_name = _format_user_short(to_manager) if to_manager else f"пользователь {to_manager_id}"
    changed_from_count = len({int(manager_id) for manager_id in (from_manager_ids or []) if manager_id})
    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_ASSIGNED,
        lead=sample_lead,
        primary_recipient_ids=[to_manager_id],
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )

    lead_count = len(unique_lead_ids)
    sample_ids = unique_lead_ids[:20]
    status_breakdown = _collect_status_breakdown_for_leads(unique_lead_ids)
    status_counts = {
        (row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0)
        for row in status_breakdown
    }
    batch_key = batch_id or f"{to_manager_id}:{lead_count}:{sample_ids[0]}:{sample_ids[-1]}"
    created = 0
    for recipient in recipients:
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_ASSIGNED,
                recipient_id=recipient.id,
                actor_user_id=actor_user_id,
                lead_id=None,
                title=f"Массовое назначение: {lead_count} лидов",
                body=f"Назначено на {to_manager_name}",
                payload={
                    "batch_id": batch_id,
                    "mode": "bulk_summary",
                    "lead_count": lead_count,
                    "to_manager_id": str(to_manager_id),
                    "from_manager_count": changed_from_count,
                    "status_counts": status_counts,
                    "status_breakdown": status_breakdown,
                },
                dedupe_key=f"lead_assigned_bulk:{batch_key}:{recipient.id}",
            )
        )
        if item is not None:
            created += 1
    return created


def emit_lead_unassigned_notification(
    *,
    lead_id: int,
    from_manager_id: int | None,
    actor_user_id: int | None,
    audit_log_id: int | None = None,
) -> Notification | None:
    if from_manager_id is None:
        return None
    lead = Lead.objects.select_related("manager").filter(id=lead_id).first()
    if lead is None:
        return None

    from_manager = User.objects.filter(id=from_manager_id, is_active=True).only("id", "username", "first_name", "last_name", "role").first()
    manager_role = getattr(from_manager, "role", None)
    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_UNASSIGNED,
        lead=lead,
        primary_recipient_ids=[from_manager_id],
        manager_id_for_scope=from_manager_id,
        manager_role_for_scope=manager_role,
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    if not recipients:
        return None

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    from_username = _format_user_short(from_manager) if from_manager else "менеджер"
    created_notification = None
    for recipient in recipients:
        dedupe_key = (
            f"lead_unassigned:{audit_log_id}:{recipient.id}"
            if audit_log_id
            else f"lead_unassigned:{lead.id}:{from_manager_id}:{recipient.id}:{int(timezone.now().timestamp())}"
        )
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_UNASSIGNED,
                recipient_id=recipient.id,
                actor_user_id=actor_user_id,
                lead_id=lead.id,
                title=f"Лид снят с менеджера: {lead_label}",
                body=f"Лид #{lead.id} снят с пользователя {from_username}",
                payload={
                    "lead_id": str(lead.id),
                    "from_manager_id": str(from_manager_id),
                },
                dedupe_key=dedupe_key,
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def emit_bulk_lead_unassigned_notification(
    *,
    lead_to_from_manager: list[tuple[int, int]],
    actor_user_id: int | None,
    batch_id: str | None = None,
) -> int:
    unique_pairs = [(int(lead_id), int(from_manager_id)) for lead_id, from_manager_id in dict.fromkeys(lead_to_from_manager) if from_manager_id]
    if not unique_pairs:
        return 0

    leads_map = {
        lead.id: lead
        for lead in Lead.objects.select_related("manager").filter(id__in=[lead_id for lead_id, _from_manager_id in unique_pairs])
    }
    managers = {
        manager.id: manager
        for manager in User.objects.filter(
            id__in=[from_manager_id for _lead_id, from_manager_id in unique_pairs],
            is_active=True,
        ).only("id", "username", "first_name", "last_name", "role")
    }

    recipient_map: dict[int, dict] = {}
    for lead_id, from_manager_id in unique_pairs:
        lead = leads_map.get(lead_id)
        manager = managers.get(from_manager_id)
        if lead is None or manager is None:
            continue

        recipients = _resolve_lead_event_recipients(
            event_type=NotificationEvent.LEAD_UNASSIGNED,
            lead=lead,
            primary_recipient_ids=[from_manager_id],
            manager_id_for_scope=from_manager_id,
            manager_role_for_scope=getattr(manager, "role", None),
            excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
        )
        for recipient in recipients:
            entry = recipient_map.setdefault(
                recipient.id,
                {
                    "recipient": recipient,
                    "lead_ids": set(),
                    "from_manager_ids": set(),
                },
            )
            entry["lead_ids"].add(lead_id)
            entry["from_manager_ids"].add(from_manager_id)

    created = 0
    for entry in recipient_map.values():
        lead_ids = sorted(entry["lead_ids"])
        from_manager_ids = sorted(entry["from_manager_ids"])
        if not lead_ids or not from_manager_ids:
            continue

        lead_count = len(lead_ids)
        status_breakdown = _collect_status_breakdown_for_leads(lead_ids)
        status_counts = {
            (row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0)
            for row in status_breakdown
        }
        if len(from_manager_ids) == 1:
            from_manager = managers.get(from_manager_ids[0])
            body = f"Снято с {_format_user_short(from_manager) if from_manager else f'пользователя {from_manager_ids[0]}'}"
        else:
            body = f"Снято у {len(from_manager_ids)} пользователей"
        batch_key = batch_id or f"{lead_count}:{lead_ids[0]}:{lead_ids[-1]}"

        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_UNASSIGNED,
                recipient_id=entry["recipient"].id,
                actor_user_id=actor_user_id,
                lead_id=None,
                title=f"Массовое снятие: {lead_count} лидов",
                body=body,
                payload={
                    "batch_id": batch_id,
                    "mode": "bulk_summary",
                    "lead_count": lead_count,
                    "from_manager_id": str(from_manager_ids[0]) if len(from_manager_ids) == 1 else None,
                    "from_manager_ids": [str(manager_id) for manager_id in from_manager_ids],
                    "from_manager_count": len(from_manager_ids),
                    "status_counts": status_counts,
                    "status_breakdown": status_breakdown,
                },
                dedupe_key=f"lead_unassigned_bulk:{batch_key}:{entry['recipient'].id}",
            )
        )
        if item is not None:
            created += 1
    return created


def emit_bulk_lead_status_changed_notification(
    *,
    lead_status_items: list[tuple[int, int | None, int | None]],
    actor_user_id: int | None,
    batch_id: str | None = None,
) -> int:
    unique_items = [
        (int(lead_id), int(from_status_id) if from_status_id else None, int(to_status_id) if to_status_id else None)
        for lead_id, from_status_id, to_status_id in dict.fromkeys(lead_status_items)
        if to_status_id
    ]
    if not unique_items:
        return 0

    leads_map = {
        lead.id: lead
        for lead in Lead.objects.select_related("manager", "status").filter(id__in=[lead_id for lead_id, _from_status_id, _to_status_id in unique_items])
    }
    status_ids = {
        status_id
        for _lead_id, from_status_id, to_status_id in unique_items
        for status_id in (from_status_id, to_status_id)
        if status_id
    }
    statuses_map = {
        status.id: status
        for status in LeadStatus.objects.filter(id__in=status_ids).only(
            "id",
            "code",
            "name",
            "is_valid",
            "work_bucket",
            "conversion_bucket",
        )
    }

    recipient_map: dict[int, dict] = {}
    for lead_id, from_status_id, to_status_id in unique_items:
        lead = leads_map.get(lead_id)
        to_status = statuses_map.get(to_status_id) if to_status_id else None
        from_status = statuses_map.get(from_status_id) if from_status_id else None
        if lead is None or to_status is None:
            continue
        if not _is_important_status_change(from_status=from_status, to_status=to_status):
            continue

        recipients = _resolve_lead_event_recipients(
            event_type=NotificationEvent.LEAD_STATUS_CHANGED,
            lead=lead,
            primary_recipient_ids=[],
            excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
        )
        for recipient in recipients:
            entry = recipient_map.setdefault(
                recipient.id,
                {
                    "recipient": recipient,
                    "lead_ids": set(),
                    "to_status_ids": set(),
                },
            )
            entry["lead_ids"].add(lead_id)
            entry["to_status_ids"].add(to_status.id)

    created = 0
    for entry in recipient_map.values():
        lead_ids = sorted(entry["lead_ids"])
        to_status_ids = sorted(entry["to_status_ids"])
        if not lead_ids or not to_status_ids:
            continue

        lead_count = len(lead_ids)
        status_breakdown = _collect_status_breakdown_for_leads(lead_ids)
        status_counts = {
            (row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0)
            for row in status_breakdown
        }
        if len(to_status_ids) == 1:
            to_status = statuses_map.get(to_status_ids[0])
            body = f"Переведено в {(getattr(to_status, 'code', None) or 'НЕТ')}"
        else:
            body = f"Изменены статусы у {lead_count} лидов"
        batch_key = batch_id or f"{lead_count}:{lead_ids[0]}:{lead_ids[-1]}"

        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_STATUS_CHANGED,
                recipient_id=entry["recipient"].id,
                actor_user_id=actor_user_id,
                lead_id=None,
                title=f"Массовая смена статуса: {lead_count} лидов",
                body=body,
                payload={
                    "batch_id": batch_id,
                    "mode": "bulk_summary",
                    "lead_count": lead_count,
                    "to_status_id": str(to_status_ids[0]) if len(to_status_ids) == 1 else None,
                    "to_status_ids": [str(status_id) for status_id in to_status_ids],
                    "status_counts": status_counts,
                    "status_breakdown": status_breakdown,
                },
                dedupe_key=f"lead_status_changed_bulk:{batch_key}:{entry['recipient'].id}",
            )
        )
        if item is not None:
            created += 1
    return created


def _collect_status_breakdown_for_leads(lead_ids: list[int]) -> list[dict]:
    unique_lead_ids = [int(lead_id) for lead_id in dict.fromkeys(lead_ids)]
    if not unique_lead_ids:
        return []
    rows = (
        Lead.objects.filter(id__in=unique_lead_ids)
        .values("status_id", "status__code", "status__name")
        .annotate(count=Count("id"))
        .order_by("-count", "status__code")
    )
    result: list[dict] = []
    for row in rows:
        status_id = row.get("status_id")
        status_code = row.get("status__code") or "NO_STATUS"
        status_name = row.get("status__name") or "Без статуса"
        result.append(
            {
                "status_id": str(status_id) if status_id else None,
                "status_code": status_code,
                "status_name": status_name,
                "count": int(row.get("count") or 0),
            }
        )
    return result


def _status_conversion_bucket(status_obj: LeadStatus | None) -> str:
    if not status_obj:
        return LeadStatus.ConversionBucket.IGNORE
    bucket = getattr(status_obj, "conversion_bucket", None)
    if bucket in {
        LeadStatus.ConversionBucket.WON,
        LeadStatus.ConversionBucket.LOST,
        LeadStatus.ConversionBucket.IGNORE,
    }:
        if bucket == LeadStatus.ConversionBucket.IGNORE:
            status_code = (getattr(status_obj, "code", "") or "").upper()
            if status_code == "WON":
                return LeadStatus.ConversionBucket.WON
            if status_code == "LOST":
                return LeadStatus.ConversionBucket.LOST
        return bucket
    return LeadStatus.ConversionBucket.IGNORE


def _is_important_status_change(*, from_status: LeadStatus | None, to_status: LeadStatus | None) -> bool:
    if to_status is None:
        return False
    to_bucket = _status_conversion_bucket(to_status)
    if to_bucket in {LeadStatus.ConversionBucket.WON, LeadStatus.ConversionBucket.LOST}:
        return True
    if getattr(to_status, "work_bucket", None) == LeadStatus.WorkBucket.RETURN:
        return True
    from_is_valid = bool(getattr(from_status, "is_valid", False))
    to_is_valid = bool(getattr(to_status, "is_valid", False))
    return from_is_valid != to_is_valid


def emit_lead_status_changed_notification(
    *,
    lead_id: int,
    from_status_id: int | None,
    to_status_id: int | None,
    actor_user_id: int | None,
    audit_log_id: int | None = None,
) -> Notification | None:
    lead = Lead.objects.select_related("manager", "status").filter(id=lead_id).first()
    if lead is None:
        return None
    from_status = LeadStatus.objects.filter(id=from_status_id).only(
        "id",
        "code",
        "name",
        "is_valid",
        "work_bucket",
        "conversion_bucket",
    ).first()
    to_status = LeadStatus.objects.filter(id=to_status_id).only(
        "id",
        "code",
        "name",
        "is_valid",
        "work_bucket",
        "conversion_bucket",
    ).first()
    if not _is_important_status_change(from_status=from_status, to_status=to_status):
        return None

    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_STATUS_CHANGED,
        lead=lead,
        primary_recipient_ids=[],
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    if not recipients or to_status is None:
        return None

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    from_code = getattr(from_status, "code", None) or "НЕТ"
    to_code = getattr(to_status, "code", None) or "НЕТ"
    created_notification = None
    for recipient in recipients:
        dedupe_key = (
            f"lead_status_changed:{audit_log_id}:{recipient.id}"
            if audit_log_id
            else f"lead_status_changed:{lead.id}:{from_status_id or 'none'}:{to_status.id}:{recipient.id}:{int(timezone.now().timestamp())}"
        )
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.LEAD_STATUS_CHANGED,
                recipient_id=recipient.id,
                actor_user_id=actor_user_id,
                lead_id=lead.id,
                title=f"Изменен статус лида: {lead_label}",
                body=f"{from_code} -> {to_code}",
                payload={
                    "lead_id": str(lead.id),
                    "from_status": {
                        "id": str(from_status.id) if from_status else None,
                        "code": getattr(from_status, "code", None),
                        "is_valid": bool(getattr(from_status, "is_valid", False)),
                    },
                    "to_status": {
                        "id": str(to_status.id),
                        "code": to_code,
                        "is_valid": bool(getattr(to_status, "is_valid", False)),
                        "work_bucket": getattr(to_status, "work_bucket", None),
                        "conversion_bucket": _status_conversion_bucket(to_status),
                    },
                },
                dedupe_key=dedupe_key,
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def emit_deposit_created_notification(*, deposit_id: int, actor_user_id: int | None = None) -> Notification | None:
    deposit = (
        LeadDeposit.objects.select_related("lead", "creator", "lead__manager")
        .filter(id=deposit_id)
        .only(
            "id",
            "type",
            "amount",
            "lead_id",
            "lead__id",
            "lead__full_name",
            "lead__phone",
            "lead__manager_id",
            "creator_id",
        )
        .first()
    )
    if deposit is None or deposit.lead is None:
        return None
    if int(deposit.type) != int(LeadDeposit.Type.FTD):
        return None

    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.DEPOSIT_CREATED,
        lead=deposit.lead,
        primary_recipient_ids=[],
        excluded_recipient_ids=[actor_user_id or deposit.creator_id],
    )
    if not recipients:
        return None

    lead_label = (deposit.lead.full_name or deposit.lead.phone or f"Лид #{deposit.lead_id}").strip()
    created_notification = None
    for recipient in recipients:
        event_actor_id = actor_user_id or deposit.creator_id
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.DEPOSIT_CREATED,
                recipient_id=recipient.id,
                actor_user_id=event_actor_id,
                lead_id=deposit.lead_id,
                title=f"Создан FTD: {lead_label}",
                body=f"Сумма FTD: {deposit.amount}",
                payload={
                    "lead_id": str(deposit.lead_id),
                    "deposit_id": str(deposit.id),
                    "deposit_type": int(deposit.type),
                    "amount": str(deposit.amount),
                },
                dedupe_key=f"deposit_created:{deposit.id}:{recipient.id}",
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def emit_comment_added_notification(*, comment_id: int) -> Notification | None:
    comment = (
        LeadComment.objects.select_related("lead", "author", "lead__manager")
        .filter(id=comment_id)
        .only(
            "id",
            "body",
            "lead_id",
            "author_id",
            "lead__id",
            "lead__full_name",
            "lead__phone",
            "lead__manager_id",
        )
        .first()
    )
    if comment is None or comment.lead is None or comment.lead.manager_id is None:
        return None

    lead_label = (comment.lead.full_name or comment.lead.phone or f"Лид #{comment.lead_id}").strip()
    comment_preview = (comment.body or "").strip()
    if len(comment_preview) > 160:
        comment_preview = f"{comment_preview[:157]}..."
    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.COMMENT_ADDED,
        lead=comment.lead,
        primary_recipient_ids=[comment.lead.manager_id],
        excluded_recipient_ids=[comment.author_id] if comment.author_id else None,
    )
    created_notification = None
    for recipient in recipients:
        item = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.COMMENT_ADDED,
                recipient_id=recipient.id,
                actor_user_id=comment.author_id,
                lead_id=comment.lead_id,
                title=f"Новый комментарий по лиду {lead_label}",
                body=comment_preview,
                payload={
                    "lead_id": str(comment.lead_id),
                    "comment_id": str(comment.id),
                },
                dedupe_key=f"comment_added:{comment.id}:{recipient.id}",
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def _lead_matches_scope(
    *,
    lead: Lead,
    watcher,
    watch_scope: str,
    manager_id: int | None = None,
    manager_role: str | None = None,
) -> bool:
    if watch_scope == NotificationPolicy.WatchScope.ALL:
        return True
    resolved_manager_id = manager_id if manager_id is not None else lead.manager_id
    if watch_scope == NotificationPolicy.WatchScope.OWN:
        return resolved_manager_id == watcher.id
    # TEAM scope follows current lead RBAC shape for teamleaders.
    resolved_manager_role = manager_role if manager_role is not None else getattr(lead.manager, "role", None)
    if resolved_manager_id is None:
        return False
    if resolved_manager_id == watcher.id:
        return True
    if resolved_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
        return True
    return False


def _role_policy_candidates(*, policy: NotificationPolicy):
    query = User.objects.filter(is_active=True)
    role_q = Q()
    if policy.apply_to_teamleaders:
        role_q |= Q(role=UserRole.TEAMLEADER)
    if policy.apply_to_admins:
        role_q |= Q(role=UserRole.ADMIN)
    if policy.apply_to_superusers:
        role_q |= Q(role=UserRole.SUPERUSER)
    if not role_q:
        return query.none()
    return query.filter(role_q)


def _resolve_lead_event_recipients(
    *,
    event_type: str,
    lead: Lead,
    primary_recipient_ids: list[int] | None = None,
    manager_id_for_scope: int | None = None,
    manager_role_for_scope: str | None = None,
    excluded_recipient_ids: list[int] | None = None,
) -> list:
    policy = get_or_create_policy(event_type)
    recipients_map = {}
    excluded_ids = {int(recipient_id) for recipient_id in (excluded_recipient_ids or []) if recipient_id}

    primary_recipient_ids = primary_recipient_ids or []
    if primary_recipient_ids:
        for user in User.objects.filter(id__in=primary_recipient_ids, is_active=True):
            recipients_map[user.id] = user

    for watcher in _role_policy_candidates(policy=policy):
        recipients_map[watcher.id] = watcher

    settings_cache: dict[tuple[int, str], dict] = {}
    recipients = []
    for recipient in recipients_map.values():
        if recipient.id in excluded_ids:
            continue
        settings = _resolve_user_settings_cached(user=recipient, event_type=event_type, cache=settings_cache)
        if not settings["enabled"]:
            continue
        if not _lead_matches_scope(
            lead=lead,
            watcher=recipient,
            watch_scope=settings["watch_scope"],
            manager_id=manager_id_for_scope,
            manager_role=manager_role_for_scope,
        ):
            continue
        recipients.append(recipient)
    return recipients


def _manager_matches_scope(*, manager, watcher, watch_scope: str) -> bool:
    if watch_scope == NotificationPolicy.WatchScope.ALL:
        return True
    if watch_scope == NotificationPolicy.WatchScope.OWN:
        return manager.id == watcher.id
    if manager.id == watcher.id:
        return True
    return getattr(manager, "role", None) in {UserRole.MANAGER, UserRole.TEAMLEADER}


def _resolve_manager_event_recipients(*, event_type: str, manager) -> list:
    policy = get_or_create_policy(event_type)
    settings_cache: dict[tuple[int, str], dict] = {}
    recipients = []
    for watcher in _role_policy_candidates(policy=policy):
        settings = _resolve_user_settings_cached(user=watcher, event_type=event_type, cache=settings_cache)
        if not settings["enabled"]:
            continue
        if not _manager_matches_scope(manager=manager, watcher=watcher, watch_scope=settings["watch_scope"]):
            continue
        recipients.append(watcher)
    return recipients


def _slot_for_overdue(*, now, next_contact_at, repeat_minutes: int) -> int:
    if now < next_contact_at:
        return -1
    delay_seconds = (now - next_contact_at).total_seconds()
    return int(delay_seconds // (repeat_minutes * 60))


def _is_within_overdue_notification_window(*, now) -> bool:
    local_now = timezone.localtime(now)
    return 8 <= local_now.hour < 18


def emit_next_contact_overdue_notifications(*, now=None, limit: int | None = None) -> int:
    now = now or timezone.now()
    if not _is_within_overdue_notification_window(now=now):
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

        recipients = _resolve_lead_event_recipients(
            event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
            lead=lead,
            primary_recipient_ids=[lead.manager_id],
        )

        lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
        for recipient in recipients:
            settings = _resolve_user_settings_cached(
                user=recipient,
                event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
                cache=settings_cache,
            )
            repeat_minutes = max(1, int(settings["repeat_minutes"]))
            slot = _slot_for_overdue(now=now, next_contact_at=lead.next_contact_at, repeat_minutes=repeat_minutes)
            if slot < 0:
                continue
            notification = emit(
                NotificationEmitPayload(
                    event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
                    recipient_id=recipient.id,
                    actor_user_id=None,
                    lead_id=lead.id,
                    title=f"Просрочен: {lead_label}",
                    body=f"Следующий контакт просрочен с {_format_human_datetime(lead.next_contact_at)}",
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


def _slot_for_repeat_minutes(*, now, repeat_minutes: int) -> int:
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
        for user in User.objects.filter(id__in=manager_ids, is_active=True).only(
            "id",
            "username",
            "first_name",
            "last_name",
            "role",
        )
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
        recipients = _resolve_manager_event_recipients(
            event_type=NotificationEvent.MANAGER_NO_ACTIVITY,
            manager=manager,
        )
        if not recipients:
            continue
        overdue_count = int(row.get("overdue_count") or 0)
        for recipient in recipients:
            user_settings = _resolve_user_settings_cached(
                user=recipient,
                event_type=NotificationEvent.MANAGER_NO_ACTIVITY,
                cache=settings_cache,
            )
            slot = _slot_for_repeat_minutes(now=now, repeat_minutes=user_settings["repeat_minutes"])
            notification = emit(
                NotificationEmitPayload(
                    event_type=NotificationEvent.MANAGER_NO_ACTIVITY,
                    recipient_id=recipient.id,
                    actor_user_id=None,
                    lead_id=None,
                    title=f"Нет активности менеджера: {_format_user_short(manager)}",
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


def emit_partner_duplicate_attempt_notification(*, attempt_id: int, now=None) -> int:
    now = now or timezone.now()
    attempt = (
        LeadDuplicateAttempt.objects.select_related("partner", "existing_lead", "existing_lead__manager")
        .filter(id=attempt_id)
        .only(
            "id",
            "partner_id",
            "source",
            "existing_lead_id",
            "created_at",
            "phone",
            "full_name",
            "email",
            "partner__id",
            "partner__code",
            "partner__name",
        )
        .first()
    )
    if attempt is None:
        return 0

    window_minutes = max(1, int(getattr(settings, "NOTIFICATIONS_PARTNER_DUPLICATE_WINDOW_MINUTES", 60)))
    threshold = max(1, int(getattr(settings, "NOTIFICATIONS_PARTNER_DUPLICATE_THRESHOLD", 10)))
    window_start = now - timedelta(minutes=window_minutes)
    attempts_count = LeadDuplicateAttempt.objects.filter(
        partner_id=attempt.partner_id,
        created_at__gte=window_start,
        created_at__lte=now,
    ).count()
    if attempts_count < threshold:
        return 0

    threshold_block = attempts_count // threshold
    if threshold_block <= 0:
        return 0

    policy = get_or_create_policy(NotificationEvent.PARTNER_DUPLICATE_ATTEMPT)
    recipients = []
    settings_cache: dict[tuple[int, str], dict] = {}
    for watcher in _role_policy_candidates(policy=policy):
        user_settings = _resolve_user_settings_cached(
            user=watcher,
            event_type=NotificationEvent.PARTNER_DUPLICATE_ATTEMPT,
            cache=settings_cache,
        )
        if not user_settings["enabled"]:
            continue
        watch_scope = user_settings["watch_scope"]
        if attempt.existing_lead_id is not None and attempt.existing_lead is not None:
            if not _lead_matches_scope(lead=attempt.existing_lead, watcher=watcher, watch_scope=watch_scope):
                continue
        elif watch_scope != NotificationPolicy.WatchScope.ALL:
            continue
        recipients.append(watcher)

    if not recipients:
        return 0

    window_slot = int(now.timestamp() // (window_minutes * 60))
    created = 0
    partner_label = getattr(attempt.partner, "name", "") or getattr(attempt.partner, "code", "") or f"Партнер #{attempt.partner_id}"
    for recipient in recipients:
        notification = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.PARTNER_DUPLICATE_ATTEMPT,
                recipient_id=recipient.id,
                actor_user_id=None,
                lead_id=attempt.existing_lead_id,
                title=f"Алерт по дублям от партнера: {partner_label}",
                body=f"{attempts_count} дублей загрузки за последние {window_minutes} минут",
                payload={
                    "attempt_id": str(attempt.id),
                    "partner_id": str(attempt.partner_id),
                    "source": attempt.source or "",
                    "existing_lead_id": str(attempt.existing_lead_id) if attempt.existing_lead_id else None,
                    "phone": attempt.phone,
                    "attempts_count": attempts_count,
                    "threshold": threshold,
                    "window_minutes": window_minutes,
                    "threshold_block": threshold_block,
                },
                dedupe_key=(
                    f"partner_duplicate_attempt:{attempt.partner_id}:{recipient.id}:{window_slot}:{threshold_block}"
                ),
            )
        )
        if notification is not None:
            created += 1
    return created


def _cancel_notification(notification: Notification) -> None:
    if notification.status == Notification.Status.CANCELLED:
        return
    notification.status = Notification.Status.CANCELLED
    notification.save(update_fields=["status", "updated_at"])


def _can_deliver_next_contact_planned(*, notification: Notification) -> bool:
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

    recipients = _resolve_lead_event_recipients(
        event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
        lead=lead,
        primary_recipient_ids=[lead.manager_id] if lead.manager_id else [],
    )
    return any(recipient.id == notification.recipient_id for recipient in recipients)


def reschedule_next_contact_planned_notifications(*, lead_id: int, remind_before_minutes: int = 15) -> int:
    lead = (
        Lead.objects.select_related("manager", "status")
        .filter(id=lead_id)
        .first()
    )
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

    recipients = _resolve_lead_event_recipients(
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
                body=f"Контакт запланирован на {_format_human_datetime(lead.next_contact_at)}",
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
