from __future__ import annotations

import time

from django.contrib.auth import get_user_model
from django.db.models import Count
from django.utils import timezone

from apps.leads.models import Lead, LeadComment, LeadDeposit, LeadStatus
from apps.notifications.events import NotificationEvent
from apps.notifications.recipients import resolve_lead_event_recipients
from apps.notifications.renderers import format_user_short
from apps.notifications.runtime import NotificationEmitPayload, emit

User = get_user_model()


def emit_lead_assigned_notification(
    *,
    lead_id: int,
    to_manager_id: int,
    actor_user_id: int | None,
    from_manager_id: int | None,
    suppress_actor_watcher: bool = False,
):
    lead = Lead.objects.select_related("manager").filter(id=lead_id).first()
    if lead is None:
        return None

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    recipients = resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_ASSIGNED,
        lead=lead,
        primary_recipient_ids=[to_manager_id],
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    if suppress_actor_watcher and actor_user_id:
        recipients = [recipient for recipient in recipients if recipient.id != actor_user_id]
    manager_label = format_user_short(lead.manager) if lead.manager else "менеджер"

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
    suppress_actor_watcher: bool = False,
) -> int:
    unique_lead_ids = [int(lead_id) for lead_id in dict.fromkeys(lead_ids)]
    if not unique_lead_ids:
        return 0

    sample_lead = Lead.objects.select_related("manager").filter(id__in=unique_lead_ids).order_by("id").first()
    if sample_lead is None:
        return 0

    to_manager = User.objects.filter(id=to_manager_id, is_active=True).only("id", "username", "first_name", "last_name").first()
    to_manager_name = format_user_short(to_manager) if to_manager else f"пользователь {to_manager_id}"
    changed_from_count = len({int(manager_id) for manager_id in (from_manager_ids or []) if manager_id})
    recipients = resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_ASSIGNED,
        lead=sample_lead,
        primary_recipient_ids=[to_manager_id],
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    if suppress_actor_watcher and actor_user_id:
        recipients = [recipient for recipient in recipients if recipient.id != actor_user_id]

    lead_count = len(unique_lead_ids)
    sample_ids = unique_lead_ids[:20]
    status_breakdown = collect_status_breakdown_for_leads(unique_lead_ids)
    status_counts = {(row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0) for row in status_breakdown}
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
    suppress_actor_watcher: bool = False,
):
    if from_manager_id is None:
        return None
    lead = Lead.objects.select_related("manager").filter(id=lead_id).first()
    if lead is None:
        return None

    from_manager = User.objects.filter(id=from_manager_id, is_active=True).only("id", "username", "first_name", "last_name", "role").first()
    manager_role = getattr(from_manager, "role", None)
    recipients = resolve_lead_event_recipients(
        event_type=NotificationEvent.LEAD_UNASSIGNED,
        lead=lead,
        primary_recipient_ids=[from_manager_id],
        manager_id_for_scope=from_manager_id,
        manager_role_for_scope=manager_role,
        excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
    )
    if suppress_actor_watcher and actor_user_id:
        recipients = [recipient for recipient in recipients if recipient.id != actor_user_id]
    if not recipients:
        return None

    lead_label = (lead.full_name or lead.phone or f"Лид #{lead.id}").strip()
    from_username = format_user_short(from_manager) if from_manager else "менеджер"
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
    suppress_actor_watcher: bool = False,
) -> int:
    unique_pairs = [(int(lead_id), int(from_manager_id)) for lead_id, from_manager_id in dict.fromkeys(lead_to_from_manager) if from_manager_id]
    if not unique_pairs:
        return 0

    leads_map = {lead.id: lead for lead in Lead.objects.select_related("manager").filter(id__in=[lead_id for lead_id, _ in unique_pairs])}
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

        recipients = resolve_lead_event_recipients(
            event_type=NotificationEvent.LEAD_UNASSIGNED,
            lead=lead,
            primary_recipient_ids=[from_manager_id],
            manager_id_for_scope=from_manager_id,
            manager_role_for_scope=getattr(manager, "role", None),
            excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
        )
        if suppress_actor_watcher and actor_user_id:
            recipients = [recipient for recipient in recipients if recipient.id != actor_user_id]
        for recipient in recipients:
            entry = recipient_map.setdefault(recipient.id, {"recipient": recipient, "lead_ids": set(), "from_manager_ids": set()})
            entry["lead_ids"].add(lead_id)
            entry["from_manager_ids"].add(from_manager_id)

    created = 0
    for entry in recipient_map.values():
        lead_ids = sorted(entry["lead_ids"])
        from_manager_ids = sorted(entry["from_manager_ids"])
        if not lead_ids or not from_manager_ids:
            continue

        lead_count = len(lead_ids)
        status_breakdown = collect_status_breakdown_for_leads(lead_ids)
        status_counts = {(row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0) for row in status_breakdown}
        if len(from_manager_ids) == 1:
            from_manager = managers.get(from_manager_ids[0])
            body = f"Снято с {format_user_short(from_manager) if from_manager else f'пользователя {from_manager_ids[0]}'}"
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
        for lead in Lead.objects.select_related("manager", "status").filter(id__in=[lead_id for lead_id, _from, _to in unique_items])
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
        if not is_important_status_change(from_status=from_status, to_status=to_status):
            continue

        recipients = resolve_lead_event_recipients(
            event_type=NotificationEvent.LEAD_STATUS_CHANGED,
            lead=lead,
            primary_recipient_ids=[],
            excluded_recipient_ids=[actor_user_id] if actor_user_id else None,
        )
        for recipient in recipients:
            entry = recipient_map.setdefault(recipient.id, {"recipient": recipient, "lead_ids": set(), "to_status_ids": set()})
            entry["lead_ids"].add(lead_id)
            entry["to_status_ids"].add(to_status.id)

    created = 0
    for entry in recipient_map.values():
        lead_ids = sorted(entry["lead_ids"])
        to_status_ids = sorted(entry["to_status_ids"])
        if not lead_ids or not to_status_ids:
            continue

        lead_count = len(lead_ids)
        status_breakdown = collect_status_breakdown_for_leads(lead_ids)
        status_counts = {(row.get("status_code") or "NO_STATUS"): int(row.get("count") or 0) for row in status_breakdown}
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


def collect_status_breakdown_for_leads(lead_ids: list[int]) -> list[dict]:
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


def status_conversion_bucket(status_obj: LeadStatus | None) -> str:
    if not status_obj:
        return LeadStatus.ConversionBucket.IGNORE
    bucket = getattr(status_obj, "conversion_bucket", None)
    if bucket in {LeadStatus.ConversionBucket.WON, LeadStatus.ConversionBucket.LOST, LeadStatus.ConversionBucket.IGNORE}:
        if bucket == LeadStatus.ConversionBucket.IGNORE:
            status_code = (getattr(status_obj, "code", "") or "").upper()
            if status_code == "WON":
                return LeadStatus.ConversionBucket.WON
            if status_code == "LOST":
                return LeadStatus.ConversionBucket.LOST
        return bucket
    return LeadStatus.ConversionBucket.IGNORE


def is_important_status_change(*, from_status: LeadStatus | None, to_status: LeadStatus | None) -> bool:
    if to_status is None:
        return False
    to_bucket = status_conversion_bucket(to_status)
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
):
    lead = Lead.objects.select_related("manager", "status").filter(id=lead_id).first()
    if lead is None:
        return None
    from_status = LeadStatus.objects.filter(id=from_status_id).only("id", "code", "name", "is_valid", "work_bucket", "conversion_bucket").first()
    to_status = LeadStatus.objects.filter(id=to_status_id).only("id", "code", "name", "is_valid", "work_bucket", "conversion_bucket").first()
    if not is_important_status_change(from_status=from_status, to_status=to_status):
        return None

    recipients = resolve_lead_event_recipients(
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
                        "conversion_bucket": status_conversion_bucket(to_status),
                    },
                },
                dedupe_key=dedupe_key,
            )
        )
        if item is not None and created_notification is None:
            created_notification = item
    return created_notification


def emit_deposit_created_notification(*, deposit_id: int, actor_user_id: int | None = None):
    deposit = (
        LeadDeposit.objects.select_related("lead", "creator", "lead__manager")
        .filter(id=deposit_id)
        .only("id", "type", "amount", "lead_id", "lead__id", "lead__full_name", "lead__phone", "lead__manager_id", "creator_id")
        .first()
    )
    if deposit is None or deposit.lead is None:
        return None
    if int(deposit.type) != int(LeadDeposit.Type.FTD):
        return None

    recipients = resolve_lead_event_recipients(
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


def emit_comment_added_notification(*, comment_id: int):
    comment = (
        LeadComment.objects.select_related("lead", "author", "lead__manager")
        .filter(id=comment_id)
        .only("id", "body", "lead_id", "author_id", "lead__id", "lead__full_name", "lead__phone", "lead__manager_id")
        .first()
    )
    if comment is None or comment.lead is None or comment.lead.manager_id is None:
        return None

    lead_label = (comment.lead.full_name or comment.lead.phone or f"Лид #{comment.lead_id}").strip()
    comment_preview = (comment.body or "").strip()
    if len(comment_preview) > 160:
        comment_preview = f"{comment_preview[:157]}..."
    recipients = resolve_lead_event_recipients(
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
