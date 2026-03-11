from __future__ import annotations

from django.db import transaction
from django.utils import timezone

from apps.notifications.models import NotificationOutbox
from apps.notifications.orchestrator import enqueue_outbox_event_processing
from apps.notifications.registry import NotificationDomainEvent, get_event_definition


def publish_event(
    *,
    event_type: str,
    aggregate_id: str | int | None,
    payload: dict | None = None,
    actor_user_id: int | None = None,
    dedupe_key: str | None = None,
    available_at=None,
) -> NotificationOutbox:
    definition = get_event_definition(event_type)
    available_at = available_at or timezone.now()
    dedupe_key = (dedupe_key or "").strip()
    defaults = {
        "event_type": event_type,
        "aggregate_type": definition.aggregate_type,
        "aggregate_id": str(aggregate_id or ""),
        "actor_user_id": actor_user_id,
        "payload": payload or {},
        "available_at": available_at,
        "status": NotificationOutbox.Status.PENDING,
    }

    if dedupe_key:
        event, created = NotificationOutbox.objects.get_or_create(
            dedupe_key=dedupe_key,
            defaults={**defaults, "dedupe_key": dedupe_key},
        )
        if created or event.status in {NotificationOutbox.Status.PENDING, NotificationOutbox.Status.FAILED}:
            transaction.on_commit(lambda event_id=event.id: enqueue_outbox_event_processing(event_id))
        return event

    event = NotificationOutbox.objects.create(dedupe_key="", **defaults)
    transaction.on_commit(lambda event_id=event.id: enqueue_outbox_event_processing(event_id))
    return event


def publish_lead_assigned(
    *,
    lead_id: int,
    to_manager_id: int,
    actor_user_id: int | None,
    from_manager_id: int | None,
    audit_log_id: int | None = None,
    suppress_actor_watcher: bool = False,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_ASSIGNED,
        aggregate_id=lead_id,
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_assigned:{audit_log_id}" if audit_log_id else None,
        payload={
            "lead_id": lead_id,
            "to_manager_id": to_manager_id,
            "actor_user_id": actor_user_id,
            "from_manager_id": from_manager_id,
            "suppress_actor_watcher": suppress_actor_watcher,
        },
    )


def publish_bulk_lead_assigned(
    *,
    lead_ids: list[int],
    to_manager_id: int,
    actor_user_id: int | None,
    from_manager_ids: list[int] | None = None,
    batch_id: str | None = None,
    suppress_actor_watcher: bool = False,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_ASSIGNED_BULK,
        aggregate_id=batch_id or f"{to_manager_id}:{len(lead_ids)}",
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_assigned_bulk:{batch_id}" if batch_id else None,
        payload={
            "lead_ids": [int(lead_id) for lead_id in lead_ids],
            "to_manager_id": int(to_manager_id),
            "actor_user_id": actor_user_id,
            "from_manager_ids": [int(manager_id) for manager_id in (from_manager_ids or []) if manager_id],
            "batch_id": batch_id,
            "suppress_actor_watcher": suppress_actor_watcher,
        },
    )


def publish_lead_unassigned(
    *,
    lead_id: int,
    from_manager_id: int | None,
    actor_user_id: int | None,
    audit_log_id: int | None = None,
    suppress_actor_watcher: bool = False,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_UNASSIGNED,
        aggregate_id=lead_id,
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_unassigned:{audit_log_id}" if audit_log_id else None,
        payload={
            "lead_id": lead_id,
            "from_manager_id": from_manager_id,
            "actor_user_id": actor_user_id,
            "audit_log_id": audit_log_id,
            "suppress_actor_watcher": suppress_actor_watcher,
        },
    )


def publish_bulk_lead_unassigned(
    *,
    lead_to_from_manager: list[tuple[int, int]],
    actor_user_id: int | None,
    batch_id: str | None = None,
    suppress_actor_watcher: bool = False,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_UNASSIGNED_BULK,
        aggregate_id=batch_id or f"{len(lead_to_from_manager)}",
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_unassigned_bulk:{batch_id}" if batch_id else None,
        payload={
            "lead_to_from_manager": [(int(lead_id), int(from_manager_id)) for lead_id, from_manager_id in lead_to_from_manager],
            "actor_user_id": actor_user_id,
            "batch_id": batch_id,
            "suppress_actor_watcher": suppress_actor_watcher,
        },
    )


def publish_lead_status_changed(
    *,
    lead_id: int,
    from_status_id: int | None,
    to_status_id: int,
    actor_user_id: int | None,
    audit_log_id: int | None = None,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_STATUS_CHANGED,
        aggregate_id=lead_id,
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_status_changed:{audit_log_id}" if audit_log_id else None,
        payload={
            "lead_id": lead_id,
            "from_status_id": from_status_id,
            "to_status_id": to_status_id,
            "actor_user_id": actor_user_id,
            "audit_log_id": audit_log_id,
        },
    )


def publish_bulk_lead_status_changed(
    *,
    lead_status_items: list[tuple[int, int | None, int | None]],
    actor_user_id: int | None,
    batch_id: str | None = None,
) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.LEAD_STATUS_CHANGED_BULK,
        aggregate_id=batch_id or f"{len(lead_status_items)}",
        actor_user_id=actor_user_id,
        dedupe_key=f"lead_status_changed_bulk:{batch_id}" if batch_id else None,
        payload={
            "lead_status_items": [
                (int(lead_id), int(from_status_id) if from_status_id else None, int(to_status_id) if to_status_id else None)
                for lead_id, from_status_id, to_status_id in lead_status_items
            ],
            "actor_user_id": actor_user_id,
            "batch_id": batch_id,
        },
    )


def publish_deposit_created(*, deposit_id: int, actor_user_id: int | None = None) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.DEPOSIT_CREATED,
        aggregate_id=deposit_id,
        actor_user_id=actor_user_id,
        dedupe_key=f"deposit_created:{deposit_id}",
        payload={
            "deposit_id": deposit_id,
            "actor_user_id": actor_user_id,
        },
    )


def publish_comment_added(*, comment_id: int) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.COMMENT_ADDED,
        aggregate_id=comment_id,
        dedupe_key=f"comment_added:{comment_id}",
        payload={"comment_id": comment_id},
    )


def publish_partner_duplicate_attempt(*, attempt_id: int) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.PARTNER_DUPLICATE_ATTEMPT,
        aggregate_id=attempt_id,
        dedupe_key=f"partner_duplicate_attempt:{attempt_id}",
        payload={"attempt_id": attempt_id},
    )


def publish_next_contact_planned_resync(*, lead_id: int, remind_before_minutes: int = 15) -> NotificationOutbox:
    return publish_event(
        event_type=NotificationDomainEvent.NEXT_CONTACT_PLANNED_SYNC,
        aggregate_id=lead_id,
        payload={
            "lead_id": lead_id,
            "remind_before_minutes": max(0, int(remind_before_minutes or 15)),
        },
    )
