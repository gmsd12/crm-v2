from __future__ import annotations

from dataclasses import dataclass


class NotificationDomainEvent:
    LEAD_ASSIGNED = "lead_assigned"
    LEAD_ASSIGNED_BULK = "lead_assigned_bulk"
    LEAD_UNASSIGNED = "lead_unassigned"
    LEAD_UNASSIGNED_BULK = "lead_unassigned_bulk"
    LEAD_STATUS_CHANGED = "lead_status_changed"
    LEAD_STATUS_CHANGED_BULK = "lead_status_changed_bulk"
    DEPOSIT_CREATED = "deposit_created"
    COMMENT_ADDED = "comment_added"
    PARTNER_DUPLICATE_ATTEMPT = "partner_duplicate_attempt"
    NEXT_CONTACT_PLANNED_SYNC = "next_contact_planned_sync"


@dataclass(frozen=True)
class NotificationEventDefinition:
    event_type: str
    aggregate_type: str
    description: str


EVENT_DEFINITIONS: dict[str, NotificationEventDefinition] = {
    NotificationDomainEvent.LEAD_ASSIGNED: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_ASSIGNED,
        aggregate_type="lead",
        description="Single lead assignment notification publication",
    ),
    NotificationDomainEvent.LEAD_ASSIGNED_BULK: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_ASSIGNED_BULK,
        aggregate_type="lead_batch",
        description="Bulk lead assignment notification publication",
    ),
    NotificationDomainEvent.LEAD_UNASSIGNED: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_UNASSIGNED,
        aggregate_type="lead",
        description="Single lead unassignment notification publication",
    ),
    NotificationDomainEvent.LEAD_UNASSIGNED_BULK: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_UNASSIGNED_BULK,
        aggregate_type="lead_batch",
        description="Bulk lead unassignment notification publication",
    ),
    NotificationDomainEvent.LEAD_STATUS_CHANGED: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_STATUS_CHANGED,
        aggregate_type="lead",
        description="Single lead status change notification publication",
    ),
    NotificationDomainEvent.LEAD_STATUS_CHANGED_BULK: NotificationEventDefinition(
        event_type=NotificationDomainEvent.LEAD_STATUS_CHANGED_BULK,
        aggregate_type="lead_batch",
        description="Bulk lead status change notification publication",
    ),
    NotificationDomainEvent.DEPOSIT_CREATED: NotificationEventDefinition(
        event_type=NotificationDomainEvent.DEPOSIT_CREATED,
        aggregate_type="deposit",
        description="Deposit-created notification publication",
    ),
    NotificationDomainEvent.COMMENT_ADDED: NotificationEventDefinition(
        event_type=NotificationDomainEvent.COMMENT_ADDED,
        aggregate_type="lead_comment",
        description="Comment-added notification publication",
    ),
    NotificationDomainEvent.PARTNER_DUPLICATE_ATTEMPT: NotificationEventDefinition(
        event_type=NotificationDomainEvent.PARTNER_DUPLICATE_ATTEMPT,
        aggregate_type="lead_duplicate_attempt",
        description="Partner duplicate-attempt alert publication",
    ),
    NotificationDomainEvent.NEXT_CONTACT_PLANNED_SYNC: NotificationEventDefinition(
        event_type=NotificationDomainEvent.NEXT_CONTACT_PLANNED_SYNC,
        aggregate_type="lead",
        description="Lead reminder rescheduling publication",
    ),
}


def get_event_definition(event_type: str) -> NotificationEventDefinition:
    try:
        return EVENT_DEFINITIONS[event_type]
    except KeyError as exc:
        raise ValueError(f"Unknown notification event type: {event_type}") from exc

