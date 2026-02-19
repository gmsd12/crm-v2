from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, time, timedelta

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Count, F, Q
from django.db.models.functions import TruncDate
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend
from django.utils import timezone
from rest_framework import serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.iam.api.rbac_mixins import RBACActionMixin
from apps.iam.api.rbac_permissions import RBACPermission
from apps.iam.models import UserRole
from apps.iam.rbac import Perm
from apps.leads.models import (
    Lead,
    LeadAuditEntity,
    LeadDeposit,
    LeadRetTransfer,
    LeadComment,
    LeadStatus,
    LeadStatusAuditEvent,
    LeadStatusAuditLog,
    LeadStatusAuditSource,
    LeadStatusIdempotencyEndpoint,
    LeadStatusIdempotencyKey,
    LeadStatusTransition,
    Pipeline,
)
from apps.partners.models import Partner

User = get_user_model()

from .serializers import (
    BulkLeadAssignManagerSerializer,
    LeadCloseWonTransferSerializer,
    LeadDepositCreateSerializer,
    LeadDepositSerializer,
    LeadRollbackRetTransferSerializer,
    BulkLeadUnassignManagerSerializer,
    BulkLeadStatusChangeSerializer,
    LeadAssignManagerSerializer,
    LeadChangeFirstManagerSerializer,
    LeadFunnelMetricsQuerySerializer,
    LeadWriteSerializer,
    LeadCommentSerializer,
    LeadSerializer,
    LeadStatusChangeSerializer,
    LeadStatusAuditLogSerializer,
    LeadStatusSerializer,
    LeadStatusTransitionSerializer,
    LeadUnassignManagerSerializer,
    PipelineSerializer,
)


def _status_payload(status_obj: LeadStatus | None) -> dict | None:
    if not status_obj:
        return None
    return {
        "id": str(status_obj.id),
        "pipeline": str(status_obj.pipeline_id),
        "code": status_obj.code,
        "name": status_obj.name,
        "order": status_obj.order,
        "color": status_obj.color,
        "is_default_for_new_leads": status_obj.is_default_for_new_leads,
        "is_active": status_obj.is_active,
        "is_terminal": status_obj.is_terminal,
        "is_valid": status_obj.is_valid,
        "conversion_bucket": status_obj.conversion_bucket,
        "is_deleted": status_obj.is_deleted,
        "created_at": status_obj.created_at.isoformat() if status_obj.created_at else None,
        "updated_at": status_obj.updated_at.isoformat() if status_obj.updated_at else None,
    }


def _manager_payload(manager_obj) -> dict | None:
    if not manager_obj:
        return None
    return {
        "id": str(manager_obj.id),
        "username": manager_obj.username,
        "role": manager_obj.role,
        "is_active": manager_obj.is_active,
    }


def _lead_payload(lead: Lead | None) -> dict | None:
    if not lead:
        return None
    return {
        "id": str(lead.id),
        "partner_id": str(lead.partner_id) if lead.partner_id else None,
        "manager_id": str(lead.manager_id) if lead.manager_id else None,
        "first_manager_id": str(lead.first_manager_id) if lead.first_manager_id else None,
        "source_id": str(lead.source_id) if lead.source_id else None,
        "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
        "status_id": str(lead.status_id) if lead.status_id else None,
        "geo": lead.geo,
        "full_name": lead.full_name,
        "phone": lead.phone,
        "email": lead.email,
        "priority": lead.priority,
        "next_contact_at": lead.next_contact_at.isoformat() if lead.next_contact_at else None,
        "last_contacted_at": lead.last_contacted_at.isoformat() if lead.last_contacted_at else None,
        "assigned_at": lead.assigned_at.isoformat() if lead.assigned_at else None,
        "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        "manager_outcome": lead.manager_outcome,
        "manager_outcome_at": lead.manager_outcome_at.isoformat() if lead.manager_outcome_at else None,
        "manager_outcome_by_id": str(lead.manager_outcome_by_id) if lead.manager_outcome_by_id else None,
        "transferred_to_ret_at": lead.transferred_to_ret_at.isoformat() if lead.transferred_to_ret_at else None,
        "received_at": lead.received_at.isoformat() if lead.received_at else None,
        "custom_fields": lead.custom_fields or {},
        "is_deleted": bool(getattr(lead, "is_deleted", False)),
        "deleted_at": lead.deleted_at.isoformat() if getattr(lead, "deleted_at", None) else None,
    }


def _comment_payload(comment: LeadComment | None) -> dict | None:
    if not comment:
        return None
    return {
        "id": str(comment.id),
        "lead_id": str(comment.lead_id) if comment.lead_id else None,
        "author_id": str(comment.author_id) if comment.author_id else None,
        "body": comment.body,
        "is_pinned": comment.is_pinned,
        "is_deleted": bool(getattr(comment, "is_deleted", False)),
        "deleted_at": comment.deleted_at.isoformat() if getattr(comment, "deleted_at", None) else None,
    }


def _deposit_payload(dep: LeadDeposit | None) -> dict | None:
    if not dep:
        return None
    return {
        "id": str(dep.id),
        "lead_id": str(dep.lead_id) if dep.lead_id else None,
        "creator_id": str(dep.creator_id) if dep.creator_id else None,
        "amount": str(dep.amount),
        "type": dep.type,
        "is_deleted": bool(getattr(dep, "is_deleted", False)),
        "deleted_at": dep.deleted_at.isoformat() if getattr(dep, "deleted_at", None) else None,
        "created_at": dep.created_at.isoformat() if dep.created_at else None,
    }


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


def _set_first_manager_if_needed(lead: Lead, *, update_fields: list[str]) -> None:
    manager = getattr(lead, "manager", None)
    if manager is None:
        return
    if lead.first_manager_id is None:
        lead.first_manager = manager
        update_fields.append("first_manager")
    if lead.first_assigned_at is None:
        lead.first_assigned_at = timezone.now()
        update_fields.append("first_assigned_at")


def _apply_manager_outcome_if_needed(lead: Lead, *, to_status: LeadStatus, actor_user, update_fields: list[str]) -> None:
    bucket = _status_conversion_bucket(to_status)
    if lead.manager_outcome != Lead.StageOutcome.PENDING:
        return
    if bucket not in {LeadStatus.ConversionBucket.WON, LeadStatus.ConversionBucket.LOST}:
        return
    lead.manager_outcome = Lead.StageOutcome.WON if bucket == LeadStatus.ConversionBucket.WON else Lead.StageOutcome.LOST
    lead.manager_outcome_at = timezone.now()
    lead.manager_outcome_by = actor_user
    update_fields.extend(["manager_outcome", "manager_outcome_at", "manager_outcome_by"])


def _is_manager_won_locked(lead: Lead) -> bool:
    return lead.manager_outcome == Lead.StageOutcome.WON


def _next_deposit_type(lead: Lead) -> int:
    count = LeadDeposit.objects.filter(lead=lead).count()
    if count <= 0:
        return LeadDeposit.Type.FTD
    if count == 1:
        return LeadDeposit.Type.RELOAD
    return LeadDeposit.Type.DEPOSIT


def _first_won_status_for_pipeline(pipeline_id: int | None) -> LeadStatus | None:
    if not pipeline_id:
        return None
    return (
        LeadStatus.objects.filter(
            pipeline_id=pipeline_id,
            is_active=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        .order_by("order", "id")
        .first()
    )


def _log_status_audit(
    *,
    event_type: str,
    actor_user,
    source: str,
    entity_type: str = LeadAuditEntity.LEAD,
    entity_id: str = "",
    batch_id: str = "",
    reason: str = "",
    lead=None,
    from_status: LeadStatus | None = None,
    to_status: LeadStatus | None = None,
    payload_before: dict | None = None,
    payload_after: dict | None = None,
) -> None:
    LeadStatusAuditLog.objects.create(
        lead=lead,
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        from_status=from_status,
        to_status=to_status,
        actor_user=actor_user,
        source=source,
        reason=reason,
        batch_id=batch_id,
        payload_before=payload_before,
        payload_after=payload_after,
    )


def _log_manager_audit(
    *, lead: Lead, actor_user, source: str, reason: str, from_manager, to_manager, batch_id: str = ""
) -> None:
    from_id = getattr(from_manager, "id", None)
    to_id = getattr(to_manager, "id", None)
    if from_id == to_id:
        return
    if to_manager is None:
        event_type = LeadStatusAuditEvent.MANAGER_UNASSIGNED
    elif from_manager is None:
        event_type = LeadStatusAuditEvent.MANAGER_ASSIGNED
    else:
        event_type = LeadStatusAuditEvent.MANAGER_REASSIGNED

    _log_status_audit(
        event_type=event_type,
        actor_user=actor_user,
        source=source,
        entity_type=LeadAuditEntity.LEAD,
        entity_id=str(lead.id),
        batch_id=batch_id,
        reason=reason,
        lead=lead,
        payload_before={"lead_id": str(lead.id), "manager": _manager_payload(from_manager)},
        payload_after={"lead_id": str(lead.id), "manager": _manager_payload(to_manager)},
    )


def _transition_error_for_lead(
    *,
    lead: Lead,
    to_status: LeadStatus,
    reason: str,
    transition_map: dict | None = None,
    force: bool = False,
) -> str | None:
    if to_status.id == lead.status_id:
        return "Lead already has this status"
    if force:
        return None
    if not lead.pipeline_id or not lead.status_id:
        return "Lead has no current workflow status"
    if to_status.pipeline_id != lead.pipeline_id:
        return "to_status must belong to lead pipeline"

    requires_comment = None
    if transition_map is not None:
        requires_comment = transition_map.get(lead.status_id)
    else:
        transition = (
            LeadStatusTransition.objects.filter(
                pipeline_id=lead.pipeline_id,
                from_status_id=lead.status_id,
                to_status_id=to_status.id,
                is_active=True,
            )
            .only("id", "requires_comment")
            .first()
        )
        if transition:
            requires_comment = transition.requires_comment

    if requires_comment is None:
        return "Transition is not allowed"
    if requires_comment and not reason:
        return "Comment is required for this transition"
    return None


def _single_transition_error_as_validation_error(error: str) -> serializers.ValidationError:
    if error == "Comment is required for this transition":
        return serializers.ValidationError({"reason": error})
    if error in {
        "to_status must belong to lead pipeline",
        "Lead already has this status",
        "Transition is not allowed",
    }:
        return serializers.ValidationError({"to_status": error})
    return serializers.ValidationError(error)


def _request_hash(payload: dict) -> str:
    dumped = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def _get_idempotency_key(request) -> str | None:
    key = (request.headers.get("Idempotency-Key") or "").strip()
    if not key:
        return None
    if len(key) > 128:
        raise serializers.ValidationError({"idempotency_key": "Idempotency-Key must be 128 chars or less"})
    return key


def _acquire_idempotency_record(*, request, endpoint: str, payload_hash: str):
    key = _get_idempotency_key(request)
    if not key:
        return None, None

    record, created = LeadStatusIdempotencyKey.objects.select_for_update().get_or_create(
        actor_user=request.user,
        endpoint=endpoint,
        key=key,
        defaults={"request_hash": payload_hash},
    )
    if not created:
        if record.request_hash != payload_hash:
            raise serializers.ValidationError(
                {"idempotency_key": "This Idempotency-Key was already used with a different payload"}
            )
        if record.response_status == 0:
            raise serializers.ValidationError({"idempotency_key": "Request with this Idempotency-Key is in progress"})
        return record, Response(record.response_body, status=record.response_status)
    return record, None


def _save_idempotency_response(record, *, response_status: int, response_body):
    if not record:
        return
    record.response_status = response_status
    record.response_body = response_body
    record.save(update_fields=["response_status", "response_body", "updated_at"])


def _assert_status_not_used(status_obj: LeadStatus, *, action: str):
    if Lead.all_objects.filter(status_id=status_obj.id).exists():
        raise serializers.ValidationError({"status": f"Cannot {action} status that is assigned to leads"})


class BaseStatusCatalogViewSet(RBACActionMixin, viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.LEAD_STATUSES_READ,),
        "retrieve": (Perm.LEAD_STATUSES_READ,),
        "create": (Perm.LEAD_STATUSES_WRITE,),
        "update": (Perm.LEAD_STATUSES_WRITE,),
        "partial_update": (Perm.LEAD_STATUSES_WRITE,),
        "soft_delete": (Perm.LEAD_STATUSES_WRITE,),
        "restore": (Perm.LEAD_STATUSES_WRITE,),
        "destroy": (Perm.LEAD_STATUSES_HARD_DELETE,),
    }

    def perform_destroy(self, instance):
        instance.hard_delete()

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        instance = self.get_object()
        instance.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        instance = self.get_object()
        instance.restore()
        return Response(status=status.HTTP_200_OK)


class PipelineViewSet(BaseStatusCatalogViewSet):
    queryset = Pipeline.objects.all().order_by("code")
    serializer_class = PipelineSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["is_default", "is_active"]


class LeadStatusViewSet(BaseStatusCatalogViewSet):
    queryset = LeadStatus.objects.select_related("pipeline").all().order_by("pipeline__code", "order", "code")
    serializer_class = LeadStatusSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = [
        "pipeline",
        "is_active",
        "is_terminal",
        "is_valid",
        "conversion_bucket",
        "is_default_for_new_leads",
    ]

    def perform_create(self, serializer):
        status_obj = serializer.save()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.STATUS_CREATED,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(status_obj.id),
            to_status=status_obj,
            payload_after=_status_payload(status_obj),
        )

    def perform_update(self, serializer):
        will_deactivate = (
            "is_active" in serializer.validated_data
            and serializer.instance.is_active
            and serializer.validated_data["is_active"] is False
        )
        if will_deactivate:
            _assert_status_not_used(serializer.instance, action="deactivate")

        before = _status_payload(serializer.instance)
        status_obj = serializer.save()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.STATUS_UPDATED,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(status_obj.id),
            to_status=status_obj,
            payload_before=before,
            payload_after=_status_payload(status_obj),
        )

    def perform_destroy(self, instance):
        _assert_status_not_used(instance, action="hard-delete")
        before = _status_payload(instance)
        super().perform_destroy(instance)
        _log_status_audit(
            event_type=LeadStatusAuditEvent.STATUS_DELETED_HARD,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(instance.id),
            payload_before=before,
        )

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        instance = self.get_object()
        _assert_status_not_used(instance, action="soft-delete")
        before = _status_payload(instance)
        instance.delete()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.STATUS_DELETED_SOFT,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(instance.id),
            from_status=instance,
            payload_before=before,
            payload_after=_status_payload(instance),
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        instance = self.get_object()
        before = _status_payload(instance)
        instance.restore()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.STATUS_UPDATED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(instance.id),
            to_status=instance,
            payload_before=before,
            payload_after=_status_payload(instance),
        )
        return Response(status=status.HTTP_200_OK)


class LeadStatusTransitionViewSet(BaseStatusCatalogViewSet):
    queryset = LeadStatusTransition.objects.select_related("pipeline", "from_status", "to_status").all().order_by(
        "pipeline__code",
        "from_status__order",
        "to_status__order",
    )
    serializer_class = LeadStatusTransitionSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["pipeline", "from_status", "to_status", "is_active", "requires_comment"]


class LeadStatusAuditLogViewSet(RBACActionMixin, viewsets.ReadOnlyModelViewSet):
    queryset = LeadStatusAuditLog.objects.select_related("lead", "from_status", "to_status", "actor_user").all().order_by(
        "-created_at"
    )
    serializer_class = LeadStatusAuditLogSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.LEAD_STATUSES_READ,),
        "retrieve": (Perm.LEAD_STATUSES_READ,),
    }
    filter_backends = [DjangoFilterBackend]
    filterset_fields = [
        "lead",
        "entity_type",
        "entity_id",
        "event_type",
        "from_status",
        "to_status",
        "actor_user",
        "source",
        "batch_id",
    ]


class NumberInFilter(django_filters.BaseInFilter, django_filters.NumberFilter):
    pass


class IdInFilter(django_filters.BaseInFilter, django_filters.NumberFilter):
    pass


class RoleInFilter(django_filters.BaseInFilter, django_filters.CharFilter):
    pass


class LeadRecordFilter(django_filters.FilterSet):
    partner__in = IdInFilter(field_name="partner_id", lookup_expr="in")
    manager__in = NumberInFilter(field_name="manager_id", lookup_expr="in")
    manager_role = django_filters.ChoiceFilter(field_name="manager__role", choices=UserRole.choices)
    manager_role__in = RoleInFilter(field_name="manager__role", lookup_expr="in")
    source__in = IdInFilter(field_name="source_id", lookup_expr="in")
    pipeline__in = IdInFilter(field_name="pipeline_id", lookup_expr="in")
    status__in = IdInFilter(field_name="status_id", lookup_expr="in")
    priority__in = NumberInFilter(field_name="priority", lookup_expr="in")

    class Meta:
        model = Lead
        fields = [
            "partner",
            "partner__in",
            "manager",
            "manager__in",
            "manager_role",
            "manager_role__in",
            "source",
            "source__in",
            "pipeline",
            "pipeline__in",
            "status",
            "status__in",
            "phone",
            "email",
            "priority",
            "priority__in",
        ]


class LeadCommentFilter(django_filters.FilterSet):
    authors = NumberInFilter(field_name="author_id", lookup_expr="in")

    class Meta:
        model = LeadComment
        fields = ["lead", "author", "authors"]


class LeadCommentViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = LeadComment.objects.select_related("lead", "author").all().order_by("-is_pinned", "-created_at")
    serializer_class = LeadCommentSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "create": (Perm.LEADS_WRITE,),
        "update": (Perm.LEADS_WRITE,),
        "partial_update": (Perm.LEADS_WRITE,),
        "destroy": (Perm.LEADS_WRITE,),
        "restore": (Perm.LEADS_WRITE,),
    }
    filter_backends = [DjangoFilterBackend]
    filterset_class = LeadCommentFilter

    def _assert_write_allowed(self, instance: LeadComment):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        if instance.author_id != self.request.user.id:
            raise PermissionDenied("You can modify only your own comments")

    def perform_create(self, serializer):
        comment = serializer.save(author=self.request.user)
        _log_status_audit(
            event_type=LeadStatusAuditEvent.COMMENT_CREATED,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(comment.id),
            lead=comment.lead,
            payload_after=_comment_payload(comment),
        )

    def perform_update(self, serializer):
        self._assert_write_allowed(serializer.instance)
        before = _comment_payload(serializer.instance)
        comment = serializer.save()
        event_type = LeadStatusAuditEvent.COMMENT_UPDATED
        if "is_pinned" in serializer.validated_data and len(serializer.validated_data) == 1:
            event_type = (
                LeadStatusAuditEvent.COMMENT_PINNED
                if comment.is_pinned
                else LeadStatusAuditEvent.COMMENT_UNPINNED
            )
        _log_status_audit(
            event_type=event_type,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(comment.id),
            lead=comment.lead,
            payload_before=before,
            payload_after=_comment_payload(comment),
        )

    def perform_destroy(self, instance):
        self._assert_write_allowed(instance)
        before = _comment_payload(instance)
        instance.delete()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.COMMENT_SOFT_DELETED,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(instance.id),
            lead=instance.lead,
            payload_before=before,
            payload_after=_comment_payload(instance),
        )

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        comment = get_object_or_404(LeadComment.all_objects.select_related("lead", "author"), id=pk)
        self._assert_write_allowed(comment)
        before = _comment_payload(comment)
        comment.restore()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.COMMENT_RESTORED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(comment.id),
            lead=comment.lead,
            payload_before=before,
            payload_after=_comment_payload(comment),
        )
        return Response(status=status.HTTP_200_OK)


class LeadViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = Lead.objects.select_related(
        "partner",
        "manager",
        "first_manager",
        "source",
        "pipeline",
        "status",
    ).all().order_by("-received_at")
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    manager_ret_protected_fields = {
        "partner",
        "manager",
        "received_at",
    }
    superuser_only_update_fields = {
        "partner",
        "geo",
        "full_name",
        "phone",
        "email",
        "source",
        "pipeline",
    }
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "create": (Perm.LEADS_WRITE,),
        "update": (Perm.LEADS_WRITE,),
        "partial_update": (Perm.LEADS_WRITE,),
        "soft_delete": (Perm.LEADS_WRITE,),
        "restore": (Perm.LEADS_WRITE,),
        "destroy": (Perm.LEADS_HARD_DELETE,),
        "metrics": (Perm.LEADS_READ,),
        "assign_manager": (Perm.LEADS_ASSIGN_MANAGER,),
        "change_first_manager": (Perm.LEADS_ASSIGN_MANAGER,),
        "bulk_assign_manager": (Perm.LEADS_ASSIGN_MANAGER,),
        "unassign_manager": (Perm.LEADS_ASSIGN_MANAGER,),
        "bulk_unassign_manager": (Perm.LEADS_ASSIGN_MANAGER,),
        "change_status": (Perm.LEADS_STATUS_WRITE,),
        "bulk_change_status": (Perm.LEADS_STATUS_WRITE,),
        "close_won_transfer": (Perm.LEADS_WRITE,),
        "rollback_ret_transfer": (Perm.LEADS_WRITE,),
        "deposits": (Perm.LEADS_WRITE,),
    }
    filter_backends = [DjangoFilterBackend]
    filterset_class = LeadRecordFilter

    def get_queryset(self):
        queryset = super().get_queryset()
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET}:
            queryset = queryset.filter(manager_id=self.request.user.id)
        return queryset

    def get_serializer_class(self):
        if self.action in {"create", "update", "partial_update"}:
            return LeadWriteSerializer
        return LeadSerializer

    def _assert_can_create(self):
        role = getattr(self.request.user, "role", None)
        if role not in {UserRole.SUPERUSER, UserRole.ADMIN}:
            raise PermissionDenied("Only admins and superusers can create leads")

    def _assert_can_edit(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET} and lead.manager_id != self.request.user.id:
            raise PermissionDenied("You can edit only your own leads")
        if role == UserRole.TEAMLEADER:
            if lead.manager_id == self.request.user.id:
                return
            lead_manager_role = getattr(getattr(lead, "manager", None), "role", None)
            if lead_manager_role == UserRole.MANAGER:
                return
            raise PermissionDenied("Teamleaders can edit only own leads and manager leads")

    def _assert_can_manage_assignment(self, lead: Lead, *, operation: str):
        role = getattr(self.request.user, "role", None)
        if role != UserRole.TEAMLEADER:
            return

        lead_manager = getattr(lead, "manager", None)
        lead_manager_role = getattr(lead_manager, "role", None)

        if operation == "assign":
            if lead.manager_id is None or lead_manager_role == UserRole.MANAGER:
                return
            raise PermissionDenied("Teamleaders can assign only unassigned leads or manager leads")

        if operation == "unassign":
            if lead_manager_role == UserRole.MANAGER:
                return
            raise PermissionDenied("Teamleaders can unassign only leads assigned to managers")

        if lead.manager_id is None:
            return
        raise PermissionDenied("Unsupported assignment operation")

    def _assert_can_change_first_manager(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        raise PermissionDenied("Only admins and superusers can change first_manager")

    def _assert_update_payload_allowed(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        payload_fields = set(self.request.data.keys())

        if role != UserRole.SUPERUSER:
            forbidden_sensitive = sorted(payload_fields & self.superuser_only_update_fields)
            if forbidden_sensitive:
                raise PermissionDenied(
                    f"Only superusers can edit sensitive fields: {', '.join(forbidden_sensitive)}"
                )

        if role in {UserRole.MANAGER, UserRole.RET}:
            forbidden = sorted(payload_fields & self.manager_ret_protected_fields)
            if forbidden:
                raise PermissionDenied(
                    f"Managers and RET cannot edit protected fields: {', '.join(forbidden)}"
                )

        if "next_contact_at" in payload_fields:
            if role in {UserRole.MANAGER, UserRole.RET} and lead.manager_id != self.request.user.id:
                raise PermissionDenied("You can set next_contact_at only for your own leads")
            if role == UserRole.TEAMLEADER:
                if lead.manager_id == self.request.user.id:
                    return
                manager = getattr(lead, "manager", None)
                manager_role = getattr(manager, "role", None)
                if manager is None or manager_role != UserRole.MANAGER:
                    raise PermissionDenied("Teamleaders can set next_contact_at only for own and manager leads")

    def _assert_create_payload_allowed(self):
        role = getattr(self.request.user, "role", None)
        payload_fields = set(self.request.data.keys())
        if role != UserRole.SUPERUSER and "geo" in payload_fields:
            raise PermissionDenied("Only superusers can set geo on create")

    def _assert_bulk_status_change_allowed(self, leads: list[Lead]):
        role = getattr(self.request.user, "role", None)
        if role not in {UserRole.MANAGER, UserRole.RET, UserRole.TEAMLEADER}:
            return
        if role in {UserRole.MANAGER, UserRole.RET}:
            foreign_ids = [str(lead.id) for lead in leads if lead.manager_id != self.request.user.id]
            if foreign_ids:
                raise PermissionDenied("You can change status only for your own leads")
            return

        forbidden_ids = []
        for lead in leads:
            if lead.manager_id == self.request.user.id:
                continue
            lead_manager_role = getattr(getattr(lead, "manager", None), "role", None)
            if lead_manager_role == UserRole.MANAGER:
                continue
            forbidden_ids.append(str(lead.id))
        if forbidden_ids:
            raise PermissionDenied("Teamleaders can change status only for own and manager leads")

    def _teamleader_can_manage_manager_scope(self, lead: Lead) -> bool:
        if lead.manager_id == self.request.user.id:
            return True
        lead_manager_role = getattr(getattr(lead, "manager", None), "role", None)
        return lead_manager_role == UserRole.MANAGER

    def _assert_can_force_status_change(self, *, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER and self._teamleader_can_manage_manager_scope(lead):
            return
        raise PermissionDenied("Only teamleaders (own/manager leads), admins, and superusers can use force status change")

    def _assert_can_transfer_to_ret(self, *, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER:
            return
        if role == UserRole.MANAGER and lead.manager_id == self.request.user.id:
            return
        raise PermissionDenied("You cannot transfer this lead to RET")

    def _assert_can_rollback_ret_transfer(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        raise PermissionDenied("Only teamleaders, admins and superusers can rollback RET transfer")

    def _assert_can_create_deposit(self, *, lead: Lead, manual_type: bool):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER:
            if manual_type:
                raise PermissionDenied("Only admins and superusers can choose deposit type manually")
            return
        if role == UserRole.MANAGER:
            if manual_type:
                raise PermissionDenied("Only admins and superusers can choose deposit type manually")
            if lead.manager_id != self.request.user.id:
                raise PermissionDenied("Managers can create deposits only for own leads")
            return
        if role == UserRole.RET:
            if manual_type:
                raise PermissionDenied("Only admins and superusers can choose deposit type manually")
            if lead.manager_id != self.request.user.id:
                raise PermissionDenied("RET can create deposits only for own leads")
            return
        raise PermissionDenied("You cannot create deposits for this lead")

    def create(self, request, *args, **kwargs):
        self._assert_can_create()
        self._assert_create_payload_allowed()
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        lead = serializer.save()
        post_save_updates: list[str] = []
        _set_first_manager_if_needed(lead, update_fields=post_save_updates)
        if post_save_updates:
            lead.save(update_fields=sorted(set(post_save_updates + ["updated_at"])))
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_CREATED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            payload_after=_lead_payload(lead),
        )
        return Response(LeadSerializer(lead).data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        lead = self.get_object()
        self._assert_can_edit(lead)
        self._assert_update_payload_allowed(lead)
        before = _lead_payload(lead)
        serializer = self.get_serializer(lead, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        lead = serializer.save()
        post_save_updates: list[str] = []
        _set_first_manager_if_needed(lead, update_fields=post_save_updates)
        if post_save_updates:
            lead.save(update_fields=sorted(set(post_save_updates + ["updated_at"])))
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_UPDATED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            payload_before=before,
            payload_after=_lead_payload(lead),
        )
        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    def partial_update(self, request, *args, **kwargs):
        kwargs["partial"] = True
        return self.update(request, *args, **kwargs)

    def perform_destroy(self, instance):
        lead_id = str(instance.id)
        before = _lead_payload(instance)
        instance.hard_delete()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_HARD_DELETED,
            actor_user=self.request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=lead_id,
            payload_before=before,
        )

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        lead = self.get_object()
        self._assert_can_edit(lead)
        before = _lead_payload(lead)
        lead.delete()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_SOFT_DELETED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            payload_before=before,
            payload_after=_lead_payload(lead),
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        lead = Lead.all_objects.select_related("manager").get(id=pk)
        self._assert_can_edit(lead)
        before = _lead_payload(lead)
        lead.restore()
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_RESTORED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            payload_before=before,
            payload_after=_lead_payload(lead),
        )
        return Response(status=status.HTTP_200_OK)

    @action(detail=True, methods=["get", "post"], url_path="deposits")
    def deposits(self, request, pk=None):
        lead = self.get_object()
        if request.method.upper() == "GET":
            items = LeadDeposit.objects.filter(lead=lead).select_related("creator").order_by("-created_at")
            return Response(LeadDepositSerializer(items, many=True).data, status=status.HTTP_200_OK)

        serializer = LeadDepositCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        manual_type = "type" in serializer.validated_data
        self._assert_can_create_deposit(lead=lead, manual_type=manual_type)

        requested_type = serializer.validated_data.get("type")
        if requested_type is not None:
            dep_type = int(requested_type)
        else:
            dep_type = _next_deposit_type(lead)

        role = getattr(request.user, "role", None)
        if role == UserRole.MANAGER and dep_type != LeadDeposit.Type.FTD:
            raise serializers.ValidationError({"type": "Managers can create only FTD"})
        if role == UserRole.RET and dep_type == LeadDeposit.Type.FTD:
            raise serializers.ValidationError({"type": "RET cannot create FTD"})

        dep = LeadDeposit.objects.create(
            lead=lead,
            creator=request.user,
            amount=serializer.validated_data["amount"],
            type=dep_type,
        )
        _log_status_audit(
            event_type=LeadStatusAuditEvent.DEPOSIT_CREATED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            reason=serializer.validated_data.get("reason", ""),
            payload_after=_deposit_payload(dep),
        )
        return Response(LeadDepositSerializer(dep).data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"], url_path="close-won-transfer")
    def close_won_transfer(self, request, pk=None):
        serializer = LeadCloseWonTransferSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        with transaction.atomic():
            lead = (
                Lead.objects.select_for_update()
                .select_related("manager", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_transfer_to_ret(lead=lead)
            if LeadRetTransfer.objects.select_for_update().filter(lead=lead, is_active=True).exists():
                raise serializers.ValidationError("Lead already has an active RET transfer")

            if LeadDeposit.objects.filter(lead=lead).exists():
                raise serializers.ValidationError("Cannot create transfer FTD: lead already has deposits")

            from_manager = lead.manager
            if from_manager is None:
                raise serializers.ValidationError("Lead must have assigned manager before transfer to RET")
            if getattr(from_manager, "role", None) == UserRole.RET:
                raise serializers.ValidationError("Lead is already assigned to RET")

            won_status = None
            if _status_conversion_bucket(lead.status) != LeadStatus.ConversionBucket.WON:
                won_status = _first_won_status_for_pipeline(lead.pipeline_id)
                if won_status is None:
                    raise serializers.ValidationError(
                        {"status": "Lead must be in WON status or pipeline must have active WON status"}
                    )

            now = timezone.now()
            dep = LeadDeposit.objects.create(
                lead=lead,
                creator=request.user,
                amount=serializer.validated_data["amount"],
                type=LeadDeposit.Type.FTD,
            )
            transfer = LeadRetTransfer.objects.create(
                lead=lead,
                from_manager=from_manager,
                to_ret=serializer.validated_data["ret_manager"],
                transferred_by=request.user,
                transferred_at=now,
                reason=serializer.validated_data.get("reason", ""),
            )

            before = _lead_payload(lead)
            lead.manager_outcome = Lead.StageOutcome.WON
            lead.manager_outcome_at = now
            lead.manager_outcome_by = request.user
            lead.manager = serializer.validated_data["ret_manager"]
            lead.assigned_at = now
            lead.transferred_to_ret_at = now

            update_fields = [
                "manager_outcome",
                "manager_outcome_at",
                "manager_outcome_by",
                "manager",
                "assigned_at",
                "transferred_to_ret_at",
                "updated_at",
            ]
            if won_status is not None:
                lead.status = won_status
                lead.pipeline = won_status.pipeline
                update_fields.extend(["status", "pipeline"])
            lead.save(update_fields=sorted(set(update_fields)))

            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                reason=serializer.validated_data.get("reason", ""),
                from_manager=from_manager,
                to_manager=lead.manager,
            )
            _log_status_audit(
                event_type=LeadStatusAuditEvent.RET_TRANSFERRED,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=serializer.validated_data.get("reason", ""),
                payload_before=before,
                payload_after={
                    "lead": _lead_payload(lead),
                    "transfer": {
                        "id": str(transfer.id),
                        "from_manager_id": str(transfer.from_manager_id) if transfer.from_manager_id else None,
                        "to_ret_id": str(transfer.to_ret_id) if transfer.to_ret_id else None,
                        "transferred_at": transfer.transferred_at.isoformat() if transfer.transferred_at else None,
                        "is_active": transfer.is_active,
                    },
                    "ftd": _deposit_payload(dep),
                },
            )

        lead.refresh_from_db()
        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="rollback-ret-transfer")
    def rollback_ret_transfer(self, request, pk=None):
        serializer = LeadRollbackRetTransferSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        self._assert_can_rollback_ret_transfer()

        with transaction.atomic():
            lead = (
                Lead.objects.select_for_update()
                .select_related("manager")
                .get(id=pk)
            )
            transfer = (
                LeadRetTransfer.objects.select_for_update()
                .select_related("from_manager", "to_ret")
                .filter(lead=lead, is_active=True)
                .order_by("-transferred_at")
                .first()
            )
            if not transfer:
                raise serializers.ValidationError("Active RET transfer not found")

            if LeadDeposit.objects.filter(lead=lead).exclude(type=LeadDeposit.Type.FTD).exists():
                raise serializers.ValidationError("Cannot rollback transfer: lead already has non-FTD deposits")

            ftd = LeadDeposit.objects.filter(lead=lead, type=LeadDeposit.Type.FTD).order_by("-created_at").first()
            if not ftd:
                raise serializers.ValidationError("Cannot rollback transfer: FTD not found")

            before_lead = _lead_payload(lead)
            before_ftd = _deposit_payload(ftd)
            ftd.delete()

            transfer.is_active = False
            transfer.rolled_back_at = timezone.now()
            transfer.rolled_back_by = request.user
            transfer.rollback_reason = serializer.validated_data.get("reason", "")
            transfer.save(
                update_fields=["is_active", "rolled_back_at", "rolled_back_by", "rollback_reason", "updated_at"]
            )

            previous_manager = lead.manager
            lead.manager = transfer.from_manager
            lead.assigned_at = timezone.now()
            lead.transferred_to_ret_at = None
            lead.manager_outcome = Lead.StageOutcome.PENDING
            lead.manager_outcome_at = None
            lead.manager_outcome_by = None
            lead.save(
                update_fields=[
                    "manager",
                    "assigned_at",
                    "transferred_to_ret_at",
                    "manager_outcome",
                    "manager_outcome_at",
                    "manager_outcome_by",
                    "updated_at",
                ]
            )

            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                reason=serializer.validated_data.get("reason", ""),
                from_manager=previous_manager,
                to_manager=lead.manager,
            )
            _log_status_audit(
                event_type=LeadStatusAuditEvent.DEPOSIT_REVERSED,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=serializer.validated_data.get("reason", ""),
                payload_before=before_ftd,
                payload_after=_deposit_payload(ftd),
            )
            _log_status_audit(
                event_type=LeadStatusAuditEvent.RET_TRANSFER_ROLLBACK,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=serializer.validated_data.get("reason", ""),
                payload_before=before_lead,
                payload_after={
                    "lead": _lead_payload(lead),
                    "transfer_id": str(transfer.id),
                    "rolled_back_at": transfer.rolled_back_at.isoformat() if transfer.rolled_back_at else None,
                },
            )

        lead.refresh_from_db()
        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="metrics")
    def metrics(self, request):
        if "manager" in request.query_params:
            raise serializers.ValidationError({"manager": "manager filter is not supported in this endpoint"})
        query = LeadFunnelMetricsQuerySerializer(data=request.query_params)
        query.is_valid(raise_exception=True)

        date_to = query.validated_data.get("date_to") or timezone.localdate()
        date_from = query.validated_data.get("date_from") or (date_to - timedelta(days=30))
        if date_from > date_to:
            raise serializers.ValidationError({"date_from": "date_from must be less than or equal to date_to"})

        tz = timezone.get_current_timezone()
        period_start = timezone.make_aware(datetime.combine(date_from, time.min), tz)
        period_end = timezone.make_aware(datetime.combine(date_to + timedelta(days=1), time.min), tz)

        pipeline = query.validated_data.get("pipeline")
        partner = query.validated_data.get("partner")
        group_by = query.validated_data.get("group_by")
        requester_role = getattr(request.user, "role", None)
        manager_scope = request.user if requester_role in {UserRole.MANAGER, UserRole.RET} else None
        pipeline_id = str(pipeline.id) if pipeline else None

        leads_received_qs = Lead.objects.filter(received_at__gte=period_start, received_at__lt=period_end)
        if pipeline:
            leads_received_qs = leads_received_qs.filter(pipeline=pipeline)
        if partner:
            leads_received_qs = leads_received_qs.filter(partner=partner)
        if manager_scope:
            leads_received_qs = leads_received_qs.filter(first_manager=manager_scope)

        valid_status_filter = Q(status__is_valid=True)
        won_manager_outcome_filter = Q(manager_outcome=Lead.StageOutcome.WON)
        lost_manager_outcome_filter = Q(manager_outcome=Lead.StageOutcome.LOST)
        won_status_fallback_filter = Q(
            manager_outcome=Lead.StageOutcome.PENDING,
            status__conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lost_status_fallback_filter = Q(
            manager_outcome=Lead.StageOutcome.PENDING,
            status__conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )

        def _rate(numerator: int, denominator: int) -> float:
            if not denominator:
                return 0.0
            return round((numerator / denominator), 4)

        def _percent(rate: float) -> float:
            return round(rate * 100, 2)

        def _build_metrics_bundle(*, partner_filter: Partner | None):
            partner_leads_received_qs = leads_received_qs

            if partner_filter:
                partner_leads_received_qs = partner_leads_received_qs.filter(partner=partner_filter)

            total = partner_leads_received_qs.count()
            leads_in_status = list(
                partner_leads_received_qs.exclude(status__isnull=True)
                .values("status_id", "status__code", "status__name")
                .annotate(count=Count("id"))
                .order_by("status__code")
            )
            valid_count = partner_leads_received_qs.filter(valid_status_filter).count()
            invalid_count = max(total - valid_count, 0)
            won_count = partner_leads_received_qs.filter(won_manager_outcome_filter | won_status_fallback_filter).count()
            lost_count = partner_leads_received_qs.filter(lost_manager_outcome_filter | lost_status_fallback_filter).count()
            same_day_won_count = (
                partner_leads_received_qs.filter(
                    won_manager_outcome_filter & Q(manager_outcome_at__isnull=False)
                )
                .annotate(
                    received_date=TruncDate("received_at"),
                    outcome_date=TruncDate("manager_outcome_at"),
                )
                .filter(received_date=F("outcome_date"))
                .count()
            )

            valid_rate = _rate(valid_count, total)
            invalid_rate = _rate(invalid_count, total)
            won_rate = _rate(won_count, total)
            lost_rate = _rate(lost_count, total)
            conversion_same_day_rate = _rate(same_day_won_count, total)
            conversion_cohort_rate = _rate(won_count, total)

            status_breakdown = []
            for row in leads_in_status:
                status_rate = _rate(row["count"], total)
                status_breakdown.append(
                    {
                        "status_id": str(row["status_id"]),
                        "status_code": row["status__code"],
                        "status_name": row["status__name"],
                        "count": row["count"],
                        "rate": status_rate,
                        "percent": _percent(status_rate),
                    }
                )

            return {
                "overview": {
                    "total": total,
                    "valid_total": valid_count,
                    "valid_rate": valid_rate,
                    "valid_percent": _percent(valid_rate),
                    "invalid_total": invalid_count,
                    "invalid_rate": invalid_rate,
                    "invalid_percent": _percent(invalid_rate),
                    "won_total": won_count,
                    "won_rate": won_rate,
                    "won_percent": _percent(won_rate),
                    "lost_total": lost_count,
                    "lost_rate": lost_rate,
                    "lost_percent": _percent(lost_rate),
                },
                "conversion": {
                    "same_day": {
                        "count": same_day_won_count,
                        "rate": conversion_same_day_rate,
                        "percent": _percent(conversion_same_day_rate),
                    },
                    "cohort": {
                        "count": won_count,
                        "rate": conversion_cohort_rate,
                        "percent": _percent(conversion_cohort_rate),
                    },
                },
                "status_breakdown": status_breakdown,
            }

        payload = {
            "period": {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
            "pipeline": (
                {
                    "id": pipeline_id,
                    "code": pipeline.code,
                    "name": pipeline.name,
                }
                if pipeline
                else None
            ),
        }
        if partner:
            payload["partner"] = {"id": str(partner.id), "code": partner.code, "name": partner.name}
        if manager_scope:
            payload["manager"] = {"id": str(manager_scope.id), "username": manager_scope.username}

        if group_by == "partner":
            partner_ids = set(leads_received_qs.values_list("partner_id", flat=True))
            partner_ids.discard(None)
            partners = list(Partner.objects.filter(id__in=partner_ids).order_by("code"))
            payload["group_by"] = "partner"
            payload["items"] = [
                {
                    "partner": {"id": str(item.id), "code": item.code, "name": item.name},
                    **_build_metrics_bundle(partner_filter=item),
                }
                for item in partners
            ]
        else:
            payload.update(_build_metrics_bundle(partner_filter=None))

        return Response(payload, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="change-first-manager")
    def change_first_manager(self, request, pk=None):
        self._assert_can_change_first_manager()
        serializer = LeadChangeFirstManagerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        manager = serializer.validated_data["manager"]
        reason = serializer.validated_data.get("reason", "")

        lead = (
            Lead.objects.select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
            .get(id=pk)
        )
        before = {
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(getattr(lead, "first_manager", None)),
            "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        }
        lead.first_manager = manager
        if lead.first_assigned_at is None:
            lead.first_assigned_at = timezone.now()
            lead.save(update_fields=["first_manager", "first_assigned_at", "updated_at"])
        else:
            lead.save(update_fields=["first_manager", "updated_at"])
        after = {
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(getattr(lead, "first_manager", None)),
            "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        }
        _log_status_audit(
            event_type=LeadStatusAuditEvent.LEAD_UPDATED,
            actor_user=request.user,
            source=LeadStatusAuditSource.API,
            entity_type=LeadAuditEntity.LEAD,
            entity_id=str(lead.id),
            lead=lead,
            reason=reason,
            payload_before=before,
            payload_after=after,
        )
        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="assign-manager")
    def assign_manager(self, request, pk=None):
        payload_hash = _request_hash(
            {
                "lead_id": str(pk),
                "manager": str(request.data.get("manager") or ""),
                "reason": (request.data.get("reason") or "").strip(),
            }
        )
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.ASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            serializer = LeadAssignManagerSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            manager = serializer.validated_data["manager"]
            reason = serializer.validated_data.get("reason", "")

            lead = (
                Lead.objects.select_for_update()
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="assign")
            if manager.role == UserRole.RET and not _is_manager_won_locked(lead):
                raise serializers.ValidationError(
                    {"manager": "Lead can be transferred to RET only after manager outcome is WON"}
                )
            previous_manager = lead.manager
            update_fields = ["manager", "updated_at"]
            if getattr(previous_manager, "id", None) != manager.id:
                lead.assigned_at = timezone.now()
                update_fields.append("assigned_at")
            if manager.role == UserRole.RET and lead.transferred_to_ret_at is None:
                lead.transferred_to_ret_at = timezone.now()
                update_fields.append("transferred_to_ret_at")
            lead.manager = manager
            _set_first_manager_if_needed(lead, update_fields=update_fields)
            lead.save(update_fields=update_fields)
            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                reason=reason,
                from_manager=previous_manager,
                to_manager=manager,
            )

            response_payload = LeadSerializer(lead).data
            _save_idempotency_response(
                idempotency_record,
                response_status=status.HTTP_200_OK,
                response_body=response_payload,
            )

        lead.refresh_from_db()
        return Response(response_payload, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="unassign-manager")
    def unassign_manager(self, request, pk=None):
        payload_hash = _request_hash({"lead_id": str(pk), "reason": (request.data.get("reason") or "").strip()})
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.UNASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            serializer = LeadUnassignManagerSerializer(data=request.data or {})
            serializer.is_valid(raise_exception=True)
            reason = serializer.validated_data.get("reason", "")

            lead = (
                Lead.objects.select_for_update()
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="unassign")
            previous_manager = lead.manager
            lead.manager = None
            lead.save(update_fields=["manager", "updated_at"])
            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                reason=reason,
                from_manager=previous_manager,
                to_manager=None,
            )

            response_payload = LeadSerializer(lead).data
            _save_idempotency_response(
                idempotency_record,
                response_status=status.HTTP_200_OK,
                response_body=response_payload,
            )

        lead.refresh_from_db()
        return Response(response_payload, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"], url_path="bulk-assign-manager")
    def bulk_assign_manager(self, request):
        serializer = BulkLeadAssignManagerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        lead_ids = serializer.validated_data["_lead_ids"]
        manager = serializer.validated_data["manager"]
        reason = serializer.validated_data.get("reason", "")
        allow_partial = serializer.validated_data.get("allow_partial", False)
        payload_hash = _request_hash(
            {
                "lead_ids": [str(lead_id) for lead_id in lead_ids],
                "manager": str(manager.id),
                "reason": reason,
                "allow_partial": allow_partial,
            }
        )
        batch_id = uuid.uuid4().hex

        updated_ids = []
        failed: dict[str, str] = {}
        response_payload = None
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.BULK_ASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            locked_leads = list(
                Lead.objects.select_for_update()
                .select_related("manager")
                .filter(id__in=lead_ids)
            )
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Unknown lead ids: {', '.join(missing)}"})
                failed.update({lead_id: "Unknown lead id" for lead_id in missing})

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                self._assert_can_manage_assignment(lead, operation="assign")
                if manager.role == UserRole.RET and not _is_manager_won_locked(lead):
                    error_text = "Lead can be transferred to RET only after manager outcome is WON"
                    if not allow_partial:
                        raise serializers.ValidationError({"lead_ids": {str(lead.id): error_text}})
                    failed[str(lead.id)] = error_text
                    continue
                previous_manager = lead.manager
                update_fields = ["manager", "updated_at"]
                if getattr(previous_manager, "id", None) != manager.id:
                    lead.assigned_at = timezone.now()
                    update_fields.append("assigned_at")
                if manager.role == UserRole.RET and lead.transferred_to_ret_at is None:
                    lead.transferred_to_ret_at = timezone.now()
                    update_fields.append("transferred_to_ret_at")
                lead.manager = manager
                _set_first_manager_if_needed(lead, update_fields=update_fields)
                lead.save(update_fields=update_fields)
                _log_manager_audit(
                    lead=lead,
                    actor_user=request.user,
                    source=LeadStatusAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=manager,
                    batch_id=batch_id,
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .order_by("-received_at")
            )
            response_payload = {
                "batch_id": batch_id,
                "updated_count": len(refreshed_leads),
                "updated_ids": updated_ids,
                "failed_count": len(failed),
                "failed": failed,
                "results": LeadSerializer(refreshed_leads, many=True).data,
            }
            _save_idempotency_response(
                idempotency_record,
                response_status=status.HTTP_200_OK,
                response_body=response_payload,
            )

        return Response(response_payload, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"], url_path="bulk-unassign-manager")
    def bulk_unassign_manager(self, request):
        serializer = BulkLeadUnassignManagerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        lead_ids = serializer.validated_data["_lead_ids"]
        reason = serializer.validated_data.get("reason", "")
        allow_partial = serializer.validated_data.get("allow_partial", False)
        payload_hash = _request_hash(
            {
                "lead_ids": [str(lead_id) for lead_id in lead_ids],
                "reason": reason,
                "allow_partial": allow_partial,
            }
        )
        batch_id = uuid.uuid4().hex

        updated_ids = []
        failed: dict[str, str] = {}
        response_payload = None
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.BULK_UNASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            locked_leads = list(
                Lead.objects.select_for_update()
                .select_related("manager")
                .filter(id__in=lead_ids)
            )
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Unknown lead ids: {', '.join(missing)}"})
                failed.update({lead_id: "Unknown lead id" for lead_id in missing})

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                self._assert_can_manage_assignment(lead, operation="unassign")
                previous_manager = lead.manager
                lead.manager = None
                lead.save(update_fields=["manager", "updated_at"])
                _log_manager_audit(
                    lead=lead,
                    actor_user=request.user,
                    source=LeadStatusAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=None,
                    batch_id=batch_id,
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .order_by("-received_at")
            )
            response_payload = {
                "batch_id": batch_id,
                "updated_count": len(refreshed_leads),
                "updated_ids": updated_ids,
                "failed_count": len(failed),
                "failed": failed,
                "results": LeadSerializer(refreshed_leads, many=True).data,
            }
            _save_idempotency_response(
                idempotency_record,
                response_status=status.HTTP_200_OK,
                response_body=response_payload,
            )

        return Response(response_payload, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="change-status")
    def change_status(self, request, pk=None):
        payload_hash = _request_hash(
            {
                "lead_id": str(pk),
                "to_status": str(request.data.get("to_status") or ""),
                "reason": (request.data.get("reason") or "").strip(),
                "force": request.data.get("force", False),
            }
        )
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.CHANGE_STATUS,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            serializer = LeadStatusChangeSerializer(data=request.data, context={"lead": self.get_object()})
            serializer.is_valid(raise_exception=True)
            to_status = serializer.validated_data["to_status"]
            reason = serializer.validated_data.get("reason", "")
            force = serializer.validated_data.get("force", False)

            lead = (
                Lead.objects.select_for_update()
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_edit(lead)
            if force:
                self._assert_can_force_status_change(lead=lead)
            error = _transition_error_for_lead(lead=lead, to_status=to_status, reason=reason, force=force)
            if error:
                raise _single_transition_error_as_validation_error(error)

            from_status = lead.status
            from_bucket = _status_conversion_bucket(from_status)
            payload_before = {
                "lead_id": str(lead.id),
                "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                "status_id": str(from_status.id) if from_status else None,
                "status_code": from_status.code if from_status else None,
                "status_bucket": from_bucket,
                "manager_outcome": lead.manager_outcome,
                "manager_outcome_at": lead.manager_outcome_at.isoformat() if lead.manager_outcome_at else None,
                "manager_outcome_by_id": str(lead.manager_outcome_by_id) if lead.manager_outcome_by_id else None,
            }

            lead.status = to_status
            lead.pipeline = to_status.pipeline
            update_fields = ["status", "pipeline", "updated_at"]
            _apply_manager_outcome_if_needed(lead, to_status=to_status, actor_user=request.user, update_fields=update_fields)
            lead.save(update_fields=sorted(set(update_fields)))

            payload_after = {
                "lead_id": str(lead.id),
                "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                "status_id": str(lead.status_id) if lead.status_id else None,
                "status_code": to_status.code,
                "status_bucket": _status_conversion_bucket(to_status),
                "manager_outcome": lead.manager_outcome,
                "manager_outcome_at": lead.manager_outcome_at.isoformat() if lead.manager_outcome_at else None,
                "manager_outcome_by_id": str(lead.manager_outcome_by_id) if lead.manager_outcome_by_id else None,
            }

            _log_status_audit(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                actor_user=request.user,
                source=LeadStatusAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                reason=reason,
                lead=lead,
                from_status=from_status,
                to_status=to_status,
                payload_before=payload_before,
                payload_after=payload_after,
            )

            response_payload = LeadSerializer(lead).data
            _save_idempotency_response(idempotency_record, response_status=status.HTTP_200_OK, response_body=response_payload)

        lead.refresh_from_db()
        return Response(response_payload, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"], url_path="bulk-change-status")
    def bulk_change_status(self, request):
        serializer = BulkLeadStatusChangeSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        lead_ids = serializer.validated_data["_lead_ids"]
        to_status = serializer.validated_data["to_status"]
        reason = serializer.validated_data.get("reason", "")
        allow_partial = serializer.validated_data.get("allow_partial", False)
        force = serializer.validated_data.get("force", False)
        payload_hash = _request_hash(
            {
                "lead_ids": [str(lead_id) for lead_id in lead_ids],
                "to_status": str(to_status.id),
                "reason": reason,
                "allow_partial": allow_partial,
                "force": force,
            }
        )
        batch_id = uuid.uuid4().hex

        updated_ids = []
        failed: dict[str, str] = {}
        response_payload = None
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadStatusIdempotencyEndpoint.BULK_CHANGE_STATUS,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            locked_leads = list(
                Lead.objects.select_for_update()
                .select_related("status", "pipeline")
                .filter(id__in=lead_ids)
            )
            self._assert_bulk_status_change_allowed(locked_leads)
            if force:
                for lead in locked_leads:
                    self._assert_can_force_status_change(lead=lead)
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Unknown lead ids: {', '.join(missing)}"})
                failed.update({lead_id: "Unknown lead id" for lead_id in missing})

            transition_map = None
            if not force:
                from_status_ids = {
                    lead.status_id
                    for lead in locked_leads
                    if lead.status_id and lead.pipeline_id and lead.pipeline_id == to_status.pipeline_id
                }
                transition_map = {
                    row["from_status_id"]: row["requires_comment"]
                    for row in LeadStatusTransition.objects.filter(
                        pipeline_id=to_status.pipeline_id,
                        to_status_id=to_status.id,
                        from_status_id__in=from_status_ids,
                        is_active=True,
                    ).values("from_status_id", "requires_comment")
                }

            errors = {}
            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                error = _transition_error_for_lead(
                    lead=lead,
                    to_status=to_status,
                    reason=reason,
                    transition_map=transition_map,
                    force=force,
                )
                if error:
                    errors[str(lead.id)] = error
            if errors:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": errors})
                failed.update(errors)

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                if str(lead.id) in failed:
                    continue
                from_status = lead.status
                from_bucket = _status_conversion_bucket(from_status)
                payload_before = {
                    "lead_id": str(lead.id),
                    "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                    "status_id": str(from_status.id) if from_status else None,
                    "status_code": from_status.code if from_status else None,
                    "status_bucket": from_bucket,
                    "manager_outcome": lead.manager_outcome,
                    "manager_outcome_at": lead.manager_outcome_at.isoformat() if lead.manager_outcome_at else None,
                    "manager_outcome_by_id": str(lead.manager_outcome_by_id) if lead.manager_outcome_by_id else None,
                }

                lead.status = to_status
                lead.pipeline = to_status.pipeline
                update_fields = ["status", "pipeline", "updated_at"]
                _apply_manager_outcome_if_needed(lead, to_status=to_status, actor_user=request.user, update_fields=update_fields)
                lead.save(update_fields=sorted(set(update_fields)))

                payload_after = {
                    "lead_id": str(lead.id),
                    "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                    "status_id": str(lead.status_id) if lead.status_id else None,
                    "status_code": to_status.code,
                    "status_bucket": _status_conversion_bucket(to_status),
                    "manager_outcome": lead.manager_outcome,
                    "manager_outcome_at": lead.manager_outcome_at.isoformat() if lead.manager_outcome_at else None,
                    "manager_outcome_by_id": str(lead.manager_outcome_by_id) if lead.manager_outcome_by_id else None,
                }

                _log_status_audit(
                    event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                    actor_user=request.user,
                    source=LeadStatusAuditSource.API,
                    entity_type=LeadAuditEntity.LEAD,
                    entity_id=str(lead.id),
                    batch_id=batch_id,
                    reason=reason,
                    lead=lead,
                    from_status=from_status,
                    to_status=to_status,
                    payload_before=payload_before,
                    payload_after=payload_after,
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "source", "pipeline", "status")
                .order_by("-received_at")
            )
            response_payload = {
                "batch_id": batch_id,
                "updated_count": len(refreshed_leads),
                "updated_ids": updated_ids,
                "failed_count": len(failed),
                "failed": failed,
                "results": LeadSerializer(refreshed_leads, many=True).data,
            }
            _save_idempotency_response(
                idempotency_record,
                response_status=status.HTTP_200_OK,
                response_body=response_payload,
            )

        return Response(response_payload, status=status.HTTP_200_OK)
