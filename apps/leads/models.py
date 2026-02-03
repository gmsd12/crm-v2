from __future__ import annotations

from django.db import models
from django.utils import timezone
from django.db.models import Q
from django.conf import settings
from django.core.exceptions import ValidationError

from apps.core.models import BaseModel
from apps.partners.models import Partner, PartnerSource


class Pipeline(BaseModel):
    code = models.SlugField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    is_default = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "lead_pipelines"
        indexes = [
            models.Index(fields=["is_default", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.code}:{self.name}"


class LeadStatus(BaseModel):
    pipeline = models.ForeignKey(Pipeline, on_delete=models.PROTECT, related_name="statuses")
    code = models.SlugField(max_length=64)
    name = models.CharField(max_length=255)
    order = models.PositiveIntegerField(default=100)
    color = models.CharField(max_length=16, default="#6B7280")
    is_default_for_new_leads = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    is_terminal = models.BooleanField(default=False)
    counts_for_conversion = models.BooleanField(default=False)

    class Meta:
        db_table = "lead_statuses"
        indexes = [
            models.Index(fields=["pipeline", "order"]),
            models.Index(fields=["pipeline", "is_active"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["pipeline", "code"],
                condition=Q(is_deleted=False),
                name="uniq_status_code_per_pipeline_alive",
            ),
            models.UniqueConstraint(
                fields=["pipeline"],
                condition=Q(is_default_for_new_leads=True, is_deleted=False),
                name="uniq_default_status_per_pipeline_alive",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.pipeline.code}:{self.code}"


class LeadStatusTransition(BaseModel):
    pipeline = models.ForeignKey(Pipeline, on_delete=models.PROTECT, related_name="transitions")
    from_status = models.ForeignKey(LeadStatus, on_delete=models.PROTECT, related_name="outgoing_transitions")
    to_status = models.ForeignKey(LeadStatus, on_delete=models.PROTECT, related_name="incoming_transitions")
    is_active = models.BooleanField(default=True)
    requires_comment = models.BooleanField(default=False)

    class Meta:
        db_table = "lead_status_transitions"
        indexes = [
            models.Index(fields=["pipeline", "is_active"]),
            models.Index(fields=["from_status", "to_status"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["pipeline", "from_status", "to_status"],
                condition=Q(is_deleted=False),
                name="uniq_status_transition_alive",
            ),
            models.CheckConstraint(
                condition=~Q(from_status=models.F("to_status")),
                name="check_transition_not_self",
            ),
        ]

    def clean(self):
        if self.from_status_id and self.pipeline_id and self.from_status.pipeline_id != self.pipeline_id:
            raise ValidationError("from_status must belong to pipeline")
        if self.to_status_id and self.pipeline_id and self.to_status.pipeline_id != self.pipeline_id:
            raise ValidationError("to_status must belong to pipeline")

    def __str__(self) -> str:
        return f"{self.pipeline.code}:{self.from_status.code}->{self.to_status.code}"


class Lead(BaseModel):
    partner = models.ForeignKey(Partner, on_delete=models.PROTECT, related_name="leads")
    manager = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="managed_leads",
    )
    source = models.ForeignKey(PartnerSource, null=True, blank=True, on_delete=models.SET_NULL, related_name="leads")
    pipeline = models.ForeignKey("leads.Pipeline", null=True, blank=True, on_delete=models.PROTECT, related_name="leads")
    status = models.ForeignKey("leads.LeadStatus", null=True, blank=True, on_delete=models.PROTECT, related_name="leads")

    # idempotency: partner can push same external_id safely
    external_id = models.CharField(max_length=128, null=True, blank=True)

    # пока гибко: сырой payload (потом нормализуем поля)
    payload = models.JSONField(default=dict, blank=True)

    received_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        db_table = "leads"
        indexes = [
            models.Index(fields=["partner", "received_at"]),
            models.Index(fields=["manager", "received_at"]),
            models.Index(fields=["partner", "source", "received_at"]),
            models.Index(fields=["partner", "external_id"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["partner", "external_id"],
                condition=Q(external_id__isnull=False),
                name="uniq_partner_external_id_notnull",
            )
        ]

    def __str__(self) -> str:
        return f"Lead {self.pk} partner={self.partner_id}"


class LeadStatusAuditEvent(models.TextChoices):
    STATUS_CHANGED = "status_changed", "Status Changed"
    STATUS_CREATED = "status_created", "Status Created"
    STATUS_UPDATED = "status_updated", "Status Updated"
    STATUS_DELETED_SOFT = "status_deleted_soft", "Status Soft Deleted"
    STATUS_DELETED_HARD = "status_deleted_hard", "Status Hard Deleted"
    MANAGER_ASSIGNED = "manager_assigned", "Manager Assigned"
    MANAGER_REASSIGNED = "manager_reassigned", "Manager Reassigned"
    MANAGER_UNASSIGNED = "manager_unassigned", "Manager Unassigned"


class LeadStatusAuditSource(models.TextChoices):
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


class LeadStatusAuditLog(models.Model):
    id = models.BigAutoField(primary_key=True)
    lead = models.ForeignKey(Lead, null=True, blank=True, on_delete=models.SET_NULL, related_name="status_audit_logs")
    event_type = models.CharField(max_length=64, choices=LeadStatusAuditEvent.choices)
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
    source = models.CharField(max_length=32, choices=LeadStatusAuditSource.choices, default=LeadStatusAuditSource.API)
    reason = models.TextField(blank=True, default="")
    payload_before = models.JSONField(null=True, blank=True)
    payload_after = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "lead_status_audit_logs"
        indexes = [
            models.Index(fields=["event_type", "created_at"]),
            models.Index(fields=["lead", "created_at"]),
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
    endpoint = models.CharField(max_length=64, choices=LeadStatusIdempotencyEndpoint.choices)
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
