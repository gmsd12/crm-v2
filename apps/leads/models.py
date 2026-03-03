from __future__ import annotations

from django.db import models
from django.utils import timezone
from django.db.models import Q
from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator

from apps.core.models import BaseModel
from apps.partners.models import Partner, PartnerSource


class LeadStatus(BaseModel):
    class ConversionBucket(models.TextChoices):
        WON = "WON", "Won"
        LOST = "LOST", "Lost"
        IGNORE = "IGNORE", "Ignore"

    class WorkBucket(models.TextChoices):
        WORKING = "WORKING", "Working"
        RETURN = "RETURN", "Return"
        NON_WORKING = "NON_WORKING", "Non-working"

    code = models.SlugField(max_length=64)
    name = models.CharField(max_length=255)
    order = models.PositiveIntegerField(default=100)
    color = models.CharField(max_length=16, default="#6B7280")
    is_default_for_new_leads = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    is_valid = models.BooleanField(default=False, db_index=True)
    work_bucket = models.CharField(
        max_length=16,
        choices=WorkBucket.choices,
        default=WorkBucket.WORKING,
        db_index=True,
    )
    conversion_bucket = models.CharField(
        max_length=16,
        choices=ConversionBucket.choices,
        default=ConversionBucket.IGNORE,
        db_index=True,
    )

    class Meta:
        db_table = "lead_statuses"
        indexes = [
            models.Index(fields=["order"]),
            models.Index(fields=["is_active"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["code"],
                condition=Q(is_deleted=False),
                name="uniq_status_code_alive",
            ),
            models.UniqueConstraint(
                fields=["is_default_for_new_leads"],
                condition=Q(is_default_for_new_leads=True, is_deleted=False),
                name="uniq_default_status_alive",
            ),
        ]

    def __str__(self) -> str:
        return self.code


class Lead(BaseModel):
    class Priority(models.IntegerChoices):
        LOW = 10, "Low"
        NORMAL = 20, "Normal"
        HIGH = 30, "High"
        URGENT = 40, "Urgent"

    partner = models.ForeignKey(Partner, on_delete=models.PROTECT, related_name="leads")
    manager = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="managed_leads",
    )
    first_manager = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="first_managed_leads",
    )
    source = models.ForeignKey(PartnerSource, null=True, blank=True, on_delete=models.SET_NULL, related_name="leads")
    status = models.ForeignKey("leads.LeadStatus", null=True, blank=True, on_delete=models.PROTECT, related_name="leads")

    geo = models.CharField(max_length=2, blank=True, default="", db_index=True)
    full_name = models.CharField(max_length=255, blank=True, default="")
    phone = models.CharField(max_length=32, blank=True, default="", db_index=True)
    email = models.EmailField(blank=True, default="", db_index=True)
    age = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        db_index=True,
        validators=[MinValueValidator(0), MaxValueValidator(99)],
    )
    priority = models.PositiveSmallIntegerField(choices=Priority.choices, default=Priority.NORMAL, null=True, blank=True)
    next_contact_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_contacted_at = models.DateTimeField(null=True, blank=True)
    assigned_at = models.DateTimeField(null=True, blank=True)
    first_assigned_at = models.DateTimeField(null=True, blank=True, db_index=True)
    custom_fields = models.JSONField(default=dict, blank=True, null=True)

    received_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        db_table = "leads"
        indexes = [
            models.Index(fields=["partner", "received_at"]),
            models.Index(fields=["manager", "received_at"]),
            models.Index(fields=["partner", "source", "received_at"]),
            models.Index(fields=["partner", "phone"]),
            models.Index(fields=["partner", "email"]),
            models.Index(fields=["partner", "priority", "received_at"]),
            models.Index(fields=["first_manager", "first_assigned_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["phone"],
                condition=Q(is_deleted=False) & ~Q(phone=""),
                name="uniq_lead_phone_alive_nonempty",
            ),
        ]

    def __str__(self) -> str:
        return f"Lead {self.pk} partner={self.partner_id}"


class LeadDeposit(BaseModel):
    class Type(models.IntegerChoices):
        FTD = 1, "FTD"
        RELOAD = 2, "Reload"
        DEPOSIT = 3, "Deposit"

    lead = models.ForeignKey(Lead, on_delete=models.PROTECT, related_name="deposits")
    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="created_lead_deposits",
    )
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    type = models.PositiveSmallIntegerField(choices=Type.choices, default=Type.DEPOSIT, db_index=True)

    class Meta:
        db_table = "lead_deposits"
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(fields=["lead", "created_at"]),
            models.Index(fields=["creator", "created_at"]),
            models.Index(fields=["type", "created_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["lead"],
                condition=Q(type=1, is_deleted=False),
                name="uniq_lead_ftd_alive",
            ),
            models.UniqueConstraint(
                fields=["lead"],
                condition=Q(type=2, is_deleted=False),
                name="uniq_lead_reload_alive",
            ),
        ]

    def __str__(self) -> str:
        return f"LeadDeposit {self.pk} lead={self.lead_id} type={self.type}"


class LeadComment(BaseModel):
    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name="comments")
    author = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="lead_comments",
    )
    body = models.TextField()
    is_pinned = models.BooleanField(default=False, db_index=True)

    class Meta:
        db_table = "lead_comments"
        indexes = [
            models.Index(fields=["lead", "created_at"]),
            models.Index(fields=["author", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"LeadComment {self.pk} lead={self.lead_id}"


class LeadDuplicateAttempt(models.Model):
    id = models.BigAutoField(primary_key=True)
    partner = models.ForeignKey(Partner, on_delete=models.CASCADE, related_name="lead_duplicate_attempts")
    source = models.ForeignKey(PartnerSource, null=True, blank=True, on_delete=models.SET_NULL, related_name="lead_duplicate_attempts")
    existing_lead = models.ForeignKey(Lead, null=True, blank=True, on_delete=models.SET_NULL, related_name="duplicate_attempts")
    phone = models.CharField(max_length=32, db_index=True)
    full_name = models.CharField(max_length=255, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "lead_duplicate_attempts"
        indexes = [
            models.Index(fields=["partner", "phone", "created_at"]),
            models.Index(fields=["existing_lead", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"LeadDuplicateAttempt {self.pk} partner={self.partner_id} phone={self.phone}"


class LeadAuditEvent(models.TextChoices):
    STATUS_CHANGED = "status_changed", "Status Changed"
    STATUS_CREATED = "status_created", "Status Created"
    STATUS_UPDATED = "status_updated", "Status Updated"
    STATUS_DELETED_SOFT = "status_deleted_soft", "Status Soft Deleted"
    STATUS_DELETED_HARD = "status_deleted_hard", "Status Hard Deleted"
    MANAGER_ASSIGNED = "manager_assigned", "Manager Assigned"
    MANAGER_REASSIGNED = "manager_reassigned", "Manager Reassigned"
    MANAGER_UNASSIGNED = "manager_unassigned", "Manager Unassigned"
    LEAD_CREATED = "lead_created", "Lead Created"
    LEAD_UPDATED = "lead_updated", "Lead Updated"
    LEAD_SOFT_DELETED = "lead_soft_deleted", "Lead Soft Deleted"
    LEAD_RESTORED = "lead_restored", "Lead Restored"
    LEAD_HARD_DELETED = "lead_hard_deleted", "Lead Hard Deleted"
    COMMENT_CREATED = "comment_created", "Comment Created"
    COMMENT_UPDATED = "comment_updated", "Comment Updated"
    COMMENT_SOFT_DELETED = "comment_soft_deleted", "Comment Soft Deleted"
    COMMENT_RESTORED = "comment_restored", "Comment Restored"
    COMMENT_PINNED = "comment_pinned", "Comment Pinned"
    COMMENT_UNPINNED = "comment_unpinned", "Comment Unpinned"
    DUPLICATE_REJECTED = "duplicate_rejected", "Duplicate Rejected"
    DEPOSIT_CREATED = "deposit_created", "Deposit Created"
    DEPOSIT_UPDATED = "deposit_updated", "Deposit Updated"
    DEPOSIT_SOFT_DELETED = "deposit_soft_deleted", "Deposit Soft Deleted"
    DEPOSIT_RESTORED = "deposit_restored", "Deposit Restored"
    DEPOSIT_HARD_DELETED = "deposit_hard_deleted", "Deposit Hard Deleted"


class LeadAuditEntity(models.TextChoices):
    LEAD = "lead", "Lead"
    LEAD_STATUS = "lead_status", "Lead Status"
    LEAD_COMMENT = "lead_comment", "Lead Comment"
    LEAD_DEPOSIT = "lead_deposit", "Lead Deposit"
    DUPLICATE_ATTEMPT = "duplicate_attempt", "Duplicate Attempt"


class LeadAuditSource(models.TextChoices):
    API = "api", "API"
    ADMIN = "admin", "Admin"
    SYSTEM = "system", "System"
    IMPORT = "import", "Import"


class LeadStatusIdempotencyEndpoint(models.TextChoices):
    CHANGE_STATUS = "change_status", "Change Status"
    BULK_CHANGE_STATUS = "bulk_change_status", "Bulk Change Status"
    ASSIGN_MANAGER = "assign_manager", "Assign Manager"
    BULK_ASSIGN_MANAGER = "bulk_assign_manager", "Bulk Assign Manager"
    UNASSIGN_MANAGER = "unassign_manager", "Unassign Manager"
    BULK_UNASSIGN_MANAGER = "bulk_unassign_manager", "Bulk Unassign Manager"


# Canonical alias for cleaner naming in app code.
LeadIdempotencyEndpoint = LeadStatusIdempotencyEndpoint


class LeadAuditLog(models.Model):
    id = models.BigAutoField(primary_key=True)
    lead = models.ForeignKey(Lead, null=True, blank=True, on_delete=models.SET_NULL, related_name="status_audit_logs")
    event_type = models.CharField(max_length=64, choices=LeadAuditEvent.choices)
    entity_type = models.CharField(max_length=32, choices=LeadAuditEntity.choices, default=LeadAuditEntity.LEAD)
    entity_id = models.CharField(max_length=64, blank=True, default="", db_index=True)
    from_status = models.ForeignKey(
        LeadStatus,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="audit_from_status_events",
    )
    to_status = models.ForeignKey(
        LeadStatus,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="audit_to_status_events",
    )
    actor_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="lead_status_audit_events",
    )
    source = models.CharField(max_length=32, choices=LeadAuditSource.choices, default=LeadAuditSource.API)
    reason = models.TextField(blank=True, default="")
    batch_id = models.CharField(max_length=64, blank=True, default="", db_index=True)
    payload_before = models.JSONField(null=True, blank=True)
    payload_after = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "lead_audit_logs"
        indexes = [
            models.Index(fields=["event_type", "created_at"]),
            models.Index(fields=["lead", "created_at"]),
            models.Index(fields=["entity_type", "created_at"]),
            models.Index(fields=["batch_id", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.event_type} lead={self.lead_id} at={self.created_at.isoformat()}"

class LeadStatusIdempotencyKey(models.Model):
    id = models.BigAutoField(primary_key=True)
    actor_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="lead_status_idempotency_keys",
    )
    endpoint = models.CharField(max_length=64, choices=LeadIdempotencyEndpoint.choices)
    key = models.CharField(max_length=128)
    request_hash = models.CharField(max_length=64)
    response_status = models.PositiveSmallIntegerField(default=0)
    response_body = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "lead_status_idempotency_keys"
        constraints = [
            models.UniqueConstraint(
                fields=["actor_user", "endpoint", "key"],
                name="uniq_lead_status_idempotency_per_user_endpoint_key",
            ),
        ]
        indexes = [
            models.Index(fields=["endpoint", "created_at"]),
            models.Index(fields=["actor_user", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.endpoint}:{self.actor_user_id}:{self.key}"


# Canonical alias for cleaner naming in app code.
LeadIdempotencyKey = LeadStatusIdempotencyKey
