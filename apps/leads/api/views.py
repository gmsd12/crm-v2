from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, time, timedelta
from statistics import median

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Count, Exists, F, OuterRef, Q
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
        "counts_for_conversion": status_obj.counts_for_conversion,
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
        "won_by_manager_id": str(lead.won_by_manager_id) if lead.won_by_manager_id else None,
        "source_id": str(lead.source_id) if lead.source_id else None,
        "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
        "status_id": str(lead.status_id) if lead.status_id else None,
        "external_id": lead.external_id,
        "geo": lead.geo,
        "full_name": lead.full_name,
        "phone": lead.phone,
        "email": lead.email,
        "priority": lead.priority,
        "next_contact_at": lead.next_contact_at.isoformat() if lead.next_contact_at else None,
        "last_contacted_at": lead.last_contacted_at.isoformat() if lead.last_contacted_at else None,
        "assigned_at": lead.assigned_at.isoformat() if lead.assigned_at else None,
        "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        "won_at": lead.won_at.isoformat() if lead.won_at else None,
        "sales_closed": lead.sales_closed,
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


def _status_conversion_bucket(status_obj: LeadStatus | None) -> str:
    if not status_obj:
        return LeadStatus.ConversionBucket.IGNORE
    bucket = getattr(status_obj, "conversion_bucket", None)
    if bucket in {
        LeadStatus.ConversionBucket.WON,
        LeadStatus.ConversionBucket.LOST,
        LeadStatus.ConversionBucket.IGNORE,
    }:
        return bucket
    if status_obj.is_terminal and status_obj.counts_for_conversion:
        return LeadStatus.ConversionBucket.WON
    if status_obj.is_terminal and not status_obj.counts_for_conversion:
        return LeadStatus.ConversionBucket.LOST
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


def _apply_sales_close_if_needed(lead: Lead, *, to_status: LeadStatus, update_fields: list[str]) -> None:
    bucket = _status_conversion_bucket(to_status)
    if bucket != LeadStatus.ConversionBucket.WON:
        return
    if not lead.sales_closed:
        lead.sales_closed = True
        update_fields.append("sales_closed")
    if lead.won_at is None:
        lead.won_at = timezone.now()
        update_fields.append("won_at")
    if lead.won_by_manager_id is None:
        lead.won_by_manager = getattr(lead, "manager", None)
        update_fields.append("won_by_manager")


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


def _transition_error_for_lead(*, lead: Lead, to_status: LeadStatus, reason: str, transition_map: dict | None = None) -> str | None:
    if not lead.pipeline_id or not lead.status_id:
        return "Lead has no current workflow status"
    if to_status.pipeline_id != lead.pipeline_id:
        return "to_status must belong to lead pipeline"
    if to_status.id == lead.status_id:
        return "Lead already has this status"

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
        "counts_for_conversion",
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
            "external_id",
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
        "won_by_manager",
        "source",
        "pipeline",
        "status",
    ).all().order_by("-received_at")
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    manager_ret_protected_fields = {
        "partner",
        "external_id",
        "manager",
        "received_at",
    }
    superuser_only_update_fields = {
        "partner",
        "external_id",
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

    @action(detail=False, methods=["get"], url_path="metrics")
    def metrics(self, request):
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
        manager = query.validated_data.get("manager")
        group_by = query.validated_data.get("group_by")
        stale_days = query.validated_data.get("stale_days", 7)
        requester_role = getattr(request.user, "role", None)
        if requester_role in {UserRole.MANAGER, UserRole.RET}:
            if manager and manager.id != request.user.id:
                raise PermissionDenied("Managers and RET can view metrics only for themselves")
            manager = request.user
        pipeline_id = str(pipeline.id) if pipeline else None

        leads_received_qs = Lead.objects.filter(received_at__gte=period_start, received_at__lt=period_end)
        leads_first_received_qs = Lead.objects.filter(
            first_assigned_at__gte=period_start,
            first_assigned_at__lt=period_end,
            first_manager_id__isnull=False,
        )
        leads_won_exec_qs = Lead.objects.filter(
            won_at__gte=period_start,
            won_at__lt=period_end,
            won_by_manager_id__isnull=False,
            sales_closed=True,
        )
        snapshot_qs = Lead.objects.all()
        transitions_qs = LeadStatusAuditLog.objects.filter(
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            created_at__gte=period_start,
            created_at__lt=period_end,
        )
        if pipeline:
            leads_received_qs = leads_received_qs.filter(pipeline=pipeline)
            leads_first_received_qs = leads_first_received_qs.filter(pipeline=pipeline)
            leads_won_exec_qs = leads_won_exec_qs.filter(pipeline=pipeline)
            snapshot_qs = snapshot_qs.filter(pipeline=pipeline)
            transitions_qs = transitions_qs.filter(to_status__pipeline=pipeline)
        if partner:
            leads_received_qs = leads_received_qs.filter(partner=partner)
            leads_first_received_qs = leads_first_received_qs.filter(partner=partner)
            leads_won_exec_qs = leads_won_exec_qs.filter(partner=partner)
            snapshot_qs = snapshot_qs.filter(partner=partner)
            transitions_qs = transitions_qs.filter(lead__partner=partner)
        if manager:
            leads_received_qs = leads_received_qs.filter(manager=manager)
            leads_first_received_qs = leads_first_received_qs.filter(first_manager=manager)
            leads_won_exec_qs = leads_won_exec_qs.filter(won_by_manager=manager)
            snapshot_qs = snapshot_qs.filter(manager=manager)
            transitions_qs = transitions_qs.filter(lead__manager=manager)

        won_transition_filter = Q(to_status__conversion_bucket=LeadStatus.ConversionBucket.WON) | (
            Q(to_status__conversion_bucket=LeadStatus.ConversionBucket.IGNORE)
            & Q(to_status__is_terminal=True, to_status__counts_for_conversion=True)
        )
        lost_transition_filter = Q(to_status__conversion_bucket=LeadStatus.ConversionBucket.LOST) | (
            Q(to_status__conversion_bucket=LeadStatus.ConversionBucket.IGNORE)
            & Q(to_status__is_terminal=True, to_status__counts_for_conversion=False)
        )

        def _median_seconds(values):
            if not values:
                return None
            return round(float(median(values)), 2)

        def _build_metrics_bundle(*, partner_filter: Partner | None, manager_filter=None):
            partner_leads_received_qs = leads_received_qs
            partner_leads_first_received_qs = leads_first_received_qs
            partner_leads_won_exec_qs = leads_won_exec_qs
            partner_snapshot_qs = snapshot_qs
            partner_transitions_qs = transitions_qs

            if partner_filter:
                partner_leads_received_qs = partner_leads_received_qs.filter(partner=partner_filter)
                partner_leads_first_received_qs = partner_leads_first_received_qs.filter(partner=partner_filter)
                partner_leads_won_exec_qs = partner_leads_won_exec_qs.filter(partner=partner_filter)
                partner_snapshot_qs = partner_snapshot_qs.filter(partner=partner_filter)
                partner_transitions_qs = partner_transitions_qs.filter(lead__partner=partner_filter)
            if manager_filter:
                partner_leads_received_qs = partner_leads_received_qs.filter(manager=manager_filter)
                partner_leads_first_received_qs = partner_leads_first_received_qs.filter(first_manager=manager_filter)
                partner_leads_won_exec_qs = partner_leads_won_exec_qs.filter(won_by_manager=manager_filter)
                partner_snapshot_qs = partner_snapshot_qs.filter(manager=manager_filter)
                partner_transitions_qs = partner_transitions_qs.filter(lead__manager=manager_filter)

            leads_received = partner_leads_received_qs.count()
            leads_in_status = list(
                partner_snapshot_qs.exclude(status__isnull=True)
                .values("status_id", "status__code", "status__name")
                .annotate(count=Count("id"))
                .order_by("status__code")
            )
            transitions_count = partner_transitions_qs.count()
            won_count = partner_transitions_qs.filter(won_transition_filter).count()
            lost_count = partner_transitions_qs.filter(lost_transition_filter).count()

            won_events_exists = LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id=OuterRef("pk"),
                created_at__lt=period_end,
            ).filter(won_transition_filter)
            if pipeline:
                won_events_exists = won_events_exists.filter(to_status__pipeline=pipeline)
            if partner_filter:
                won_events_exists = won_events_exists.filter(lead__partner=partner_filter)
            if manager_filter:
                won_events_exists = won_events_exists.filter(lead__manager=manager_filter)

            cohort_won = (
                partner_leads_received_qs.annotate(has_won_event=Exists(won_events_exists))
                .filter(
                    Q(has_won_event=True)
                    | Q(status__conversion_bucket=LeadStatus.ConversionBucket.WON)
                    | Q(
                        status__conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
                        status__is_terminal=True,
                        status__counts_for_conversion=True,
                    )
                )
                .count()
            )
            rate = round((cohort_won / leads_received), 4) if leads_received else 0.0

            cohort_leads = list(partner_leads_received_qs.values("id", "received_at"))
            cohort_ids = [row["id"] for row in cohort_leads]
            cohort_received_map = {row["id"]: row["received_at"] for row in cohort_leads}

            median_time_to_win_seconds = None
            median_time_to_lost_seconds = None
            median_time_in_status = []

            if cohort_ids:
                win_logs = list(
                    LeadStatusAuditLog.objects.filter(
                        event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                        lead_id__in=cohort_ids,
                        created_at__lt=period_end,
                    )
                    .filter(won_transition_filter)
                    .values("lead_id", "created_at")
                    .order_by("lead_id", "created_at")
                )
                lost_logs = list(
                    LeadStatusAuditLog.objects.filter(
                        event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                        lead_id__in=cohort_ids,
                        created_at__lt=period_end,
                    )
                    .filter(lost_transition_filter)
                    .values("lead_id", "created_at")
                    .order_by("lead_id", "created_at")
                )
                first_win_at = {}
                for row in win_logs:
                    first_win_at.setdefault(row["lead_id"], row["created_at"])
                first_lost_at = {}
                for row in lost_logs:
                    first_lost_at.setdefault(row["lead_id"], row["created_at"])

                win_durations = []
                for lead_id, finished_at in first_win_at.items():
                    started_at = cohort_received_map.get(lead_id)
                    if started_at and finished_at >= started_at:
                        win_durations.append((finished_at - started_at).total_seconds())
                lost_durations = []
                for lead_id, finished_at in first_lost_at.items():
                    started_at = cohort_received_map.get(lead_id)
                    if started_at and finished_at >= started_at:
                        lost_durations.append((finished_at - started_at).total_seconds())

                median_time_to_win_seconds = _median_seconds(win_durations)
                median_time_to_lost_seconds = _median_seconds(lost_durations)

                transition_logs = list(
                    LeadStatusAuditLog.objects.filter(
                        event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                        lead_id__in=cohort_ids,
                        from_status_id__isnull=False,
                        created_at__lt=period_end,
                    )
                    .values("lead_id", "from_status_id", "from_status__code", "from_status__name", "created_at")
                    .order_by("lead_id", "created_at")
                )
                prev_at_by_lead = dict(cohort_received_map)
                status_durations = {}
                for row in transition_logs:
                    lead_id = row["lead_id"]
                    prev_at = prev_at_by_lead.get(lead_id)
                    curr_at = row["created_at"]
                    if not prev_at or curr_at < prev_at:
                        prev_at_by_lead[lead_id] = curr_at
                        continue
                    duration_seconds = (curr_at - prev_at).total_seconds()
                    if duration_seconds < 0:
                        prev_at_by_lead[lead_id] = curr_at
                        continue
                    status_key = row["from_status_id"]
                    bucket = status_durations.setdefault(
                        status_key,
                        {
                            "status_id": str(status_key),
                            "status_code": row["from_status__code"],
                            "status_name": row["from_status__name"],
                            "durations": [],
                        },
                    )
                    bucket["durations"].append(duration_seconds)
                    prev_at_by_lead[lead_id] = curr_at

                median_time_in_status = sorted(
                    [
                        {
                            "status_id": data["status_id"],
                            "status_code": data["status_code"],
                            "status_name": data["status_name"],
                            "median_seconds": _median_seconds(data["durations"]),
                        }
                        for data in status_durations.values()
                        if data["durations"]
                    ],
                    key=lambda item: item["status_code"] or "",
                )

            stale_threshold = period_end - timedelta(days=stale_days)
            stale_qs = partner_snapshot_qs.exclude(status__isnull=True).filter(status__is_terminal=False)
            stale_total = stale_qs.count()
            stale_count = stale_qs.filter(updated_at__lt=stale_threshold).count()
            stale_rate = round((stale_count / stale_total), 4) if stale_total else 0.0

            owner_received = partner_leads_first_received_qs.count()
            owner_won_events_exists = LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id=OuterRef("pk"),
                created_at__lt=period_end,
            ).filter(won_transition_filter)
            if pipeline:
                owner_won_events_exists = owner_won_events_exists.filter(to_status__pipeline=pipeline)
            owner_won = (
                partner_leads_first_received_qs.annotate(has_won_event=Exists(owner_won_events_exists))
                .filter(
                    Q(sales_closed=True)
                    | Q(has_won_event=True)
                    | Q(status__conversion_bucket=LeadStatus.ConversionBucket.WON)
                    | Q(
                        status__conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
                        status__is_terminal=True,
                        status__counts_for_conversion=True,
                    )
                )
                .count()
            )
            owner_rate = round((owner_won / owner_received), 4) if owner_received else 0.0

            executor_won_total = partner_leads_won_exec_qs.count()
            executor_won_on_own = partner_leads_won_exec_qs.filter(first_manager_id=F("won_by_manager_id")).count()
            executor_won_on_foreign = executor_won_total - executor_won_on_own

            return {
                "leads_received": leads_received,
                "leads_in_status": [
                    {
                        "status_id": str(row["status_id"]),
                        "status_code": row["status__code"],
                        "status_name": row["status__name"],
                        "count": row["count"],
                    }
                    for row in leads_in_status
                ],
                "transitions_count": transitions_count,
                "won_count": won_count,
                "lost_count": lost_count,
                "overall_conversion": {
                    "cohort_received": owner_received,
                    "cohort_won": owner_won,
                    "rate": owner_rate,
                },
                "sales_conversion": {
                    "received_first": owner_received,
                    "won_from_owned": owner_won,
                    "rate": owner_rate,
                },
                "sales_executor": {
                    "won_total": executor_won_total,
                    "won_on_own": executor_won_on_own,
                    "won_on_foreign": executor_won_on_foreign,
                },
                "speed": {
                    "median_time_to_win_seconds": median_time_to_win_seconds,
                    "median_time_to_lost_seconds": median_time_to_lost_seconds,
                    "median_time_in_status": median_time_in_status,
                },
                "stale_leads": {
                    "stale_days": stale_days,
                    "count": stale_count,
                    "total_active_non_terminal": stale_total,
                    "rate": stale_rate,
                },
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
        if manager:
            payload["manager"] = {"id": str(manager.id), "username": manager.username}

        if group_by == "partner":
            partner_ids = set(snapshot_qs.values_list("partner_id", flat=True))
            partner_ids.update(leads_received_qs.values_list("partner_id", flat=True))
            partner_ids.update(leads_first_received_qs.values_list("partner_id", flat=True))
            partner_ids.update(leads_won_exec_qs.values_list("partner_id", flat=True))
            partner_ids.update(
                transitions_qs.exclude(lead__partner__isnull=True).values_list("lead__partner_id", flat=True)
            )
            partner_ids.discard(None)
            partners = list(Partner.objects.filter(id__in=partner_ids).order_by("code"))
            payload["group_by"] = "partner"
            payload["items"] = [
                {
                    "partner": {"id": str(item.id), "code": item.code, "name": item.name},
                    **_build_metrics_bundle(partner_filter=item, manager_filter=None),
                }
                for item in partners
            ]
        elif group_by == "manager":
            manager_ids = set(snapshot_qs.values_list("manager_id", flat=True))
            manager_ids.update(leads_received_qs.values_list("manager_id", flat=True))
            manager_ids.update(leads_first_received_qs.values_list("first_manager_id", flat=True))
            manager_ids.update(leads_won_exec_qs.values_list("won_by_manager_id", flat=True))
            manager_ids.update(
                transitions_qs.exclude(lead__manager__isnull=True).values_list("lead__manager_id", flat=True)
            )
            manager_ids.discard(None)
            managers = list(User.objects.filter(id__in=manager_ids, role__in=(UserRole.MANAGER, UserRole.RET)).order_by("username"))
            payload["group_by"] = "manager"
            payload["items"] = [
                {
                    "manager": {"id": str(item.id), "username": item.username},
                    **_build_metrics_bundle(partner_filter=None, manager_filter=item),
                }
                for item in managers
            ]
        else:
            payload.update(_build_metrics_bundle(partner_filter=None, manager_filter=None))

        return Response(payload, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="change-first-manager")
    def change_first_manager(self, request, pk=None):
        self._assert_can_change_first_manager()
        serializer = LeadChangeFirstManagerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        manager = serializer.validated_data["manager"]
        reason = serializer.validated_data.get("reason", "")

        lead = (
            Lead.objects.select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
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
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="assign")
            previous_manager = lead.manager
            update_fields = ["manager", "updated_at"]
            if getattr(previous_manager, "id", None) != manager.id:
                lead.assigned_at = timezone.now()
                update_fields.append("assigned_at")
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
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
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
                previous_manager = lead.manager
                update_fields = ["manager", "updated_at"]
                if getattr(previous_manager, "id", None) != manager.id:
                    lead.assigned_at = timezone.now()
                    update_fields.append("assigned_at")
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
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
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
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
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

            lead = (
                Lead.objects.select_for_update()
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
                .get(id=pk)
            )
            self._assert_can_edit(lead)
            error = _transition_error_for_lead(lead=lead, to_status=to_status, reason=reason)
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
                "sales_closed": lead.sales_closed,
                "won_by_manager_id": str(lead.won_by_manager_id) if lead.won_by_manager_id else None,
                "won_at": lead.won_at.isoformat() if lead.won_at else None,
            }

            lead.status = to_status
            lead.pipeline = to_status.pipeline
            update_fields = ["status", "pipeline", "updated_at"]
            _apply_sales_close_if_needed(lead, to_status=to_status, update_fields=update_fields)
            lead.save(update_fields=sorted(set(update_fields)))

            payload_after = {
                "lead_id": str(lead.id),
                "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                "status_id": str(lead.status_id) if lead.status_id else None,
                "status_code": to_status.code,
                "status_bucket": _status_conversion_bucket(to_status),
                "sales_closed": lead.sales_closed,
                "won_by_manager_id": str(lead.won_by_manager_id) if lead.won_by_manager_id else None,
                "won_at": lead.won_at.isoformat() if lead.won_at else None,
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
        payload_hash = _request_hash(
            {
                "lead_ids": [str(lead_id) for lead_id in lead_ids],
                "to_status": str(to_status.id),
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
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Unknown lead ids: {', '.join(missing)}"})
                failed.update({lead_id: "Unknown lead id" for lead_id in missing})

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
                    "sales_closed": lead.sales_closed,
                    "won_by_manager_id": str(lead.won_by_manager_id) if lead.won_by_manager_id else None,
                    "won_at": lead.won_at.isoformat() if lead.won_at else None,
                }

                lead.status = to_status
                lead.pipeline = to_status.pipeline
                update_fields = ["status", "pipeline", "updated_at"]
                _apply_sales_close_if_needed(lead, to_status=to_status, update_fields=update_fields)
                lead.save(update_fields=sorted(set(update_fields)))

                payload_after = {
                    "lead_id": str(lead.id),
                    "pipeline_id": str(lead.pipeline_id) if lead.pipeline_id else None,
                    "status_id": str(lead.status_id) if lead.status_id else None,
                    "status_code": to_status.code,
                    "status_bucket": _status_conversion_bucket(to_status),
                    "sales_closed": lead.sales_closed,
                    "won_by_manager_id": str(lead.won_by_manager_id) if lead.won_by_manager_id else None,
                    "won_at": lead.won_at.isoformat() if lead.won_at else None,
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
                .select_related("partner", "manager", "first_manager", "won_by_manager", "source", "pipeline", "status")
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
