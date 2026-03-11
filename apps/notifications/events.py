from __future__ import annotations


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
