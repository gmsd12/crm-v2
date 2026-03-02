from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, time, timedelta

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.db.models import Count, F, OuterRef, Q, Subquery, Sum
from django.db.models.functions import TruncDate, TruncMonth
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend
from django.utils import timezone
from rest_framework import filters as drf_filters, serializers, status, viewsets
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
    LeadComment,
    LeadStatus,
    LeadAuditEvent,
    LeadAuditLog,
    LeadAuditSource,
    LeadIdempotencyEndpoint,
    LeadIdempotencyKey,
)
from apps.partners.models import Partner

User = get_user_model()

from .serializers import (
    BulkLeadAssignManagerSerializer,
    LeadCloseWonTransferSerializer,
    LeadDepositCreateSerializer,
    LeadDepositSerializer,
    LeadDepositWriteSerializer,
    LeadRollbackRetTransferSerializer,
    BulkLeadUnassignManagerSerializer,
    BulkLeadStatusChangeSerializer,
    LeadAssignManagerSerializer,
    LeadChangeFirstManagerSerializer,
    LeadDepositStatsQuerySerializer,
    LeadFunnelMetricsQuerySerializer,
    LeadWriteSerializer,
    LeadCommentSerializer,
    LeadSerializer,
    LeadStatusChangeSerializer,
    LeadAuditLogSerializer,
    LeadStatusSerializer,
    LeadUnassignManagerSerializer,
)


def _status_payload(status_obj: LeadStatus | None) -> dict | None:
    if not status_obj:
        return None
    return {
        "id": str(status_obj.id),
        "code": status_obj.code,
        "name": status_obj.name,
        "order": status_obj.order,
        "color": status_obj.color,
        "is_default_for_new_leads": status_obj.is_default_for_new_leads,
        "is_active": status_obj.is_active,
        "is_valid": status_obj.is_valid,
        "work_bucket": status_obj.work_bucket,
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
        "first_name": (manager_obj.first_name or "").strip(),
        "last_name": (manager_obj.last_name or "").strip(),
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
        "status_id": str(lead.status_id) if lead.status_id else None,
        "geo": lead.geo,
        "age": lead.age,
        "full_name": lead.full_name,
        "phone": lead.phone,
        "email": lead.email,
        "priority": lead.priority,
        "next_contact_at": lead.next_contact_at.isoformat() if lead.next_contact_at else None,
        "last_contacted_at": lead.last_contacted_at.isoformat() if lead.last_contacted_at else None,
        "assigned_at": lead.assigned_at.isoformat() if lead.assigned_at else None,
        "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        "received_at": lead.received_at.isoformat() if lead.received_at else None,
        "custom_fields": lead.custom_fields,
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


def _deposit_unique_conflict_error(dep_type: int):
    if dep_type == LeadDeposit.Type.FTD:
        return serializers.ValidationError({"type": "FTD already exists for this lead"})
    if dep_type == LeadDeposit.Type.RELOAD:
        return serializers.ValidationError({"type": "Reload already exists for this lead"})
    return serializers.ValidationError({"type": "Deposit type conflict for this lead"})


def _lead_manager_role(lead: Lead) -> str | None:
    manager = getattr(lead, "manager", None)
    return getattr(manager, "role", None)


def _assert_deposit_create_allowed(*, actor_user, lead: Lead, manual_type: bool) -> None:
    role = getattr(actor_user, "role", None)
    lead_manager_role = _lead_manager_role(lead)

    if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
        return
    if role == UserRole.TEAMLEADER:
        if manual_type:
            raise PermissionDenied("Only admins and superusers can choose deposit type manually")
        if lead_manager_role not in {UserRole.MANAGER, UserRole.TEAMLEADER}:
            raise PermissionDenied("Teamleaders can create deposits only for manager/teamleader leads")
        return
    if role == UserRole.MANAGER:
        if manual_type:
            raise PermissionDenied("Only admins and superusers can choose deposit type manually")
        if lead.manager_id != actor_user.id:
            raise PermissionDenied("Managers can create deposits only for own leads")
        return
    if role == UserRole.RET:
        if manual_type:
            raise PermissionDenied("Only admins and superusers can choose deposit type manually")
        if lead.manager_id != actor_user.id:
            raise PermissionDenied("RET can create deposits only for own leads")
        return
    raise PermissionDenied("You cannot create deposits for this lead")


def _create_lead_deposit(
    *,
    actor_user,
    lead: Lead,
    amount,
    requested_type=None,
    reason: str = "",
) -> LeadDeposit:
    manual_type = requested_type is not None
    _assert_deposit_create_allowed(actor_user=actor_user, lead=lead, manual_type=manual_type)

    role = getattr(actor_user, "role", None)
    if requested_type is not None:
        dep_type = int(requested_type)
    else:
        dep_type = _next_deposit_type(lead, actor_role=role)

    if role == UserRole.TEAMLEADER and dep_type != LeadDeposit.Type.FTD:
        raise serializers.ValidationError({"type": "Teamleaders can create only FTD"})
    if role == UserRole.MANAGER and dep_type != LeadDeposit.Type.FTD:
        raise serializers.ValidationError({"type": "Managers can create only FTD"})
    if role == UserRole.RET and dep_type == LeadDeposit.Type.FTD:
        raise serializers.ValidationError({"type": "RET cannot create FTD"})

    if dep_type in {LeadDeposit.Type.FTD, LeadDeposit.Type.RELOAD}:
        if LeadDeposit.objects.filter(lead=lead, type=dep_type, is_deleted=False).exists():
            raise _deposit_unique_conflict_error(dep_type)

    try:
        dep = LeadDeposit.objects.create(
            lead=lead,
            creator=actor_user,
            amount=amount,
            type=dep_type,
        )
    except IntegrityError:
        raise _deposit_unique_conflict_error(dep_type)

    _touch_lead_last_contacted(lead, at=dep.created_at)
    _log_status_audit(
        event_type=LeadAuditEvent.DEPOSIT_CREATED,
        actor_user=actor_user,
        source=LeadAuditSource.API,
        entity_type=LeadAuditEntity.LEAD_DEPOSIT,
        entity_id=str(dep.id),
        lead=lead,
        reason=reason,
        payload_after=_deposit_payload(dep),
    )
    return dep


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


def _touch_lead_last_contacted(lead: Lead, *, at: datetime | None = None) -> None:
    contact_dt = at or timezone.now()
    lead.last_contacted_at = contact_dt
    lead.save(update_fields=["last_contacted_at", "updated_at"])


def _next_deposit_type(lead: Lead, *, actor_role: str | None = None) -> int:
    types = list(
        LeadDeposit.objects.filter(lead=lead)
        .order_by("created_at", "id")
        .values_list("type", flat=True)
    )
    if not types:
        if actor_role == UserRole.RET:
            return LeadDeposit.Type.RELOAD
        return LeadDeposit.Type.FTD

    if LeadDeposit.Type.FTD in types:
        if len(types) == 1:
            return LeadDeposit.Type.RELOAD
        return LeadDeposit.Type.DEPOSIT

    # If lead history started without FTD (RET-side manual work), continue as normal deposits.
    return LeadDeposit.Type.DEPOSIT


def _resolve_rollback_manager(lead: Lead):
    candidate_ids: list[int] = []
    transfer_log = (
        LeadAuditLog.objects.filter(
            lead=lead,
            event_type=LeadAuditEvent.RET_TRANSFERRED,
        )
        .order_by("-created_at", "-id")
        .first()
    )
    if transfer_log and isinstance(transfer_log.payload_after, dict):
        transfer_payload = transfer_log.payload_after.get("transfer")
        if isinstance(transfer_payload, dict):
            from_manager_id = transfer_payload.get("from_manager_id")
            transfer_author_id = transfer_payload.get("transfer_author_id")
            for raw_id in (from_manager_id, transfer_author_id):
                try:
                    parsed = int(raw_id)
                except (TypeError, ValueError):
                    continue
                candidate_ids.append(parsed)

    if lead.first_manager_id:
        candidate_ids.append(int(lead.first_manager_id))

    seen: set[int] = set()
    for candidate_id in candidate_ids:
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        candidate = User.objects.filter(id=candidate_id, is_active=True).first()
        if candidate and candidate.role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
            return candidate
    return None


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
    LeadAuditLog.objects.create(
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
        event_type = LeadAuditEvent.MANAGER_UNASSIGNED
    elif from_manager is None:
        event_type = LeadAuditEvent.MANAGER_ASSIGNED
    else:
        event_type = LeadAuditEvent.MANAGER_REASSIGNED

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


def _status_change_error_for_lead(
    *,
    lead: Lead,
    to_status: LeadStatus,
) -> str | None:
    if to_status.id == lead.status_id:
        return "Lead already has this status"
    return None


def _status_change_error_as_validation_error(error: str) -> serializers.ValidationError:
    if error == "Lead already has this status":
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

    record, created = LeadIdempotencyKey.objects.select_for_update().get_or_create(
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


class LeadStatusViewSet(BaseStatusCatalogViewSet):
    queryset = LeadStatus.objects.all().order_by("order", "code")
    serializer_class = LeadStatusSerializer
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]
    filterset_fields = [
        "is_active",
        "is_valid",
        "work_bucket",
        "conversion_bucket",
        "is_default_for_new_leads",
    ]
    ordering = ["order", "code"]
    ordering_fields = [
        "id",
        "order",
        "code",
        "name",
        "is_active",
        "is_valid",
        "work_bucket",
        "conversion_bucket",
        "is_default_for_new_leads",
        "created_at",
        "updated_at",
    ]

    def perform_create(self, serializer):
        status_obj = serializer.save()
        _log_status_audit(
            event_type=LeadAuditEvent.STATUS_CREATED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.STATUS_UPDATED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.STATUS_DELETED_HARD,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.STATUS_DELETED_SOFT,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.STATUS_UPDATED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_STATUS,
            entity_id=str(instance.id),
            to_status=instance,
            payload_before=before,
            payload_after=_status_payload(instance),
        )
        return Response(status=status.HTTP_200_OK)

class LeadAuditLogViewSet(RBACActionMixin, viewsets.ReadOnlyModelViewSet):
    queryset = LeadAuditLog.objects.select_related("lead", "from_status", "to_status", "actor_user").all().order_by(
        "-created_at"
    )
    serializer_class = LeadAuditLogSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.LEAD_STATUSES_READ,),
        "retrieve": (Perm.LEAD_STATUSES_READ,),
    }
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]
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
    ordering = ["-created_at"]
    ordering_fields = [
        "id",
        "created_at",
        "event_type",
        "entity_type",
        "entity_id",
        "source",
        "batch_id",
        "lead__id",
        "actor_user__username",
    ]


class NumberInFilter(django_filters.BaseInFilter, django_filters.NumberFilter):
    pass


class IdInFilter(django_filters.BaseInFilter, django_filters.NumberFilter):
    pass


class RoleInFilter(django_filters.BaseInFilter, django_filters.CharFilter):
    pass


class LeadRecordFilter(django_filters.FilterSet):
    id__in = IdInFilter(field_name="id", lookup_expr="in")
    partner__in = IdInFilter(field_name="partner_id", lookup_expr="in")
    manager__in = NumberInFilter(field_name="manager_id", lookup_expr="in")
    manager_role = django_filters.ChoiceFilter(field_name="manager__role", choices=UserRole.choices)
    manager_role__in = RoleInFilter(field_name="manager__role", lookup_expr="in")
    first_manager__in = NumberInFilter(field_name="first_manager_id", lookup_expr="in")
    first_manager_role = django_filters.ChoiceFilter(field_name="first_manager__role", choices=UserRole.choices)
    first_manager_role__in = RoleInFilter(field_name="first_manager__role", lookup_expr="in")
    source__in = IdInFilter(field_name="source_id", lookup_expr="in")
    status_code = django_filters.CharFilter(field_name="status__code", lookup_expr="iexact")
    status__in = IdInFilter(field_name="status_id", lookup_expr="in")
    priority__in = NumberInFilter(field_name="priority", lookup_expr="in")
    age__in = NumberInFilter(field_name="age", lookup_expr="in")
    age_from = django_filters.NumberFilter(field_name="age", lookup_expr="gte")
    age_to = django_filters.NumberFilter(field_name="age", lookup_expr="lte")
    full_name = django_filters.CharFilter(field_name="full_name", lookup_expr="icontains")
    phone__icontains = django_filters.CharFilter(field_name="phone", lookup_expr="icontains")
    email__icontains = django_filters.CharFilter(field_name="email", lookup_expr="icontains")
    received_from = django_filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="gte")
    received_to = django_filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="lte")
    assigned_from = django_filters.IsoDateTimeFilter(field_name="assigned_at", lookup_expr="gte")
    assigned_to = django_filters.IsoDateTimeFilter(field_name="assigned_at", lookup_expr="lte")
    first_assigned_from = django_filters.IsoDateTimeFilter(field_name="first_assigned_at", lookup_expr="gte")
    first_assigned_to = django_filters.IsoDateTimeFilter(field_name="first_assigned_at", lookup_expr="lte")
    next_contact_from = django_filters.IsoDateTimeFilter(field_name="next_contact_at", lookup_expr="gte")
    next_contact_to = django_filters.IsoDateTimeFilter(field_name="next_contact_at", lookup_expr="lte")
    is_unassigned = django_filters.BooleanFilter(field_name="manager_id", lookup_expr="isnull")
    has_next_contact = django_filters.BooleanFilter(method="filter_has_next_contact")
    has_email = django_filters.BooleanFilter(method="filter_has_email")
    has_phone = django_filters.BooleanFilter(method="filter_has_phone")

    class Meta:
        model = Lead
        fields = [
            "id",
            "id__in",
            "partner",
            "partner__in",
            "manager",
            "manager__in",
            "manager_role",
            "manager_role__in",
            "first_manager",
            "first_manager__in",
            "first_manager_role",
            "first_manager_role__in",
            "source",
            "source__in",
            "status",
            "status_code",
            "status__in",
            "geo",
            "phone",
            "phone__icontains",
            "email",
            "email__icontains",
            "full_name",
            "priority",
            "priority__in",
            "age",
            "age__in",
            "age_from",
            "age_to",
            "received_at",
            "received_from",
            "received_to",
            "assigned_at",
            "assigned_from",
            "assigned_to",
            "first_assigned_at",
            "first_assigned_from",
            "first_assigned_to",
            "next_contact_at",
            "next_contact_from",
            "next_contact_to",
            "is_unassigned",
            "has_next_contact",
            "has_email",
            "has_phone",
        ]

    def filter_has_next_contact(self, queryset, name, value):
        return queryset.filter(next_contact_at__isnull=not value)

    def filter_has_email(self, queryset, name, value):
        if value:
            return queryset.exclude(email="")
        return queryset.filter(email="")

    def filter_has_phone(self, queryset, name, value):
        if value:
            return queryset.exclude(phone="")
        return queryset.filter(phone="")


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
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]
    filterset_class = LeadCommentFilter
    ordering = ["-is_pinned", "-created_at"]
    ordering_fields = [
        "id",
        "lead__id",
        "author__username",
        "is_pinned",
        "created_at",
        "updated_at",
    ]

    def _assert_write_allowed(self, instance: LeadComment):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        if instance.author_id != self.request.user.id:
            raise PermissionDenied("You can modify only your own comments")

    def perform_create(self, serializer):
        comment = serializer.save(author=self.request.user)
        _touch_lead_last_contacted(comment.lead, at=comment.created_at)
        _log_status_audit(
            event_type=LeadAuditEvent.COMMENT_CREATED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(comment.id),
            lead=comment.lead,
            payload_after=_comment_payload(comment),
        )

    def perform_update(self, serializer):
        self._assert_write_allowed(serializer.instance)
        before = _comment_payload(serializer.instance)
        comment = serializer.save()
        event_type = LeadAuditEvent.COMMENT_UPDATED
        if "is_pinned" in serializer.validated_data and len(serializer.validated_data) == 1:
            event_type = (
                LeadAuditEvent.COMMENT_PINNED
                if comment.is_pinned
                else LeadAuditEvent.COMMENT_UNPINNED
            )
        _log_status_audit(
            event_type=event_type,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.COMMENT_SOFT_DELETED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.COMMENT_RESTORED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_COMMENT,
            entity_id=str(comment.id),
            lead=comment.lead,
            payload_before=before,
            payload_after=_comment_payload(comment),
        )
        return Response(status=status.HTTP_200_OK)


class LeadDepositFilter(django_filters.FilterSet):
    id__in = IdInFilter(field_name="id", lookup_expr="in")
    lead__in = IdInFilter(field_name="lead_id", lookup_expr="in")
    creator__in = NumberInFilter(field_name="creator_id", lookup_expr="in")
    creator_role = django_filters.ChoiceFilter(field_name="creator__role", choices=UserRole.choices)
    type__in = NumberInFilter(field_name="type", lookup_expr="in")
    created_from = django_filters.IsoDateTimeFilter(field_name="created_at", lookup_expr="gte")
    created_to = django_filters.IsoDateTimeFilter(field_name="created_at", lookup_expr="lte")

    class Meta:
        model = LeadDeposit
        fields = [
            "id",
            "id__in",
            "lead",
            "lead__in",
            "creator",
            "creator__in",
            "creator_role",
            "type",
            "type__in",
            "created_at",
            "created_from",
            "created_to",
        ]


class LeadDepositViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = LeadDeposit.objects.select_related("lead", "creator", "lead__manager").all().order_by("-created_at", "-id")
    permission_classes = [IsAuthenticated, RBACPermission]
    filter_backends = [DjangoFilterBackend, drf_filters.SearchFilter, drf_filters.OrderingFilter]
    filterset_class = LeadDepositFilter
    search_fields = [
        "lead__full_name",
        "lead__phone",
        "lead__email",
    ]
    ordering = ["-created_at", "-id"]
    ordering_fields = [
        "id",
        "lead__id",
        "creator__username",
        "creator__role",
        "type",
        "amount",
        "created_at",
        "updated_at",
    ]
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "stats_monthly": (Perm.LEADS_READ,),
        "stats_ftd_matrix": (Perm.LEADS_READ,),
        "create": (Perm.LEADS_WRITE,),
        "update": (Perm.LEADS_WRITE,),
        "partial_update": (Perm.LEADS_WRITE,),
        "soft_delete": (Perm.LEADS_WRITE,),
        "restore": (Perm.LEADS_WRITE,),
        "destroy": (Perm.LEADS_HARD_DELETE,),
    }

    def get_queryset(self):
        queryset = super().get_queryset()
        role = getattr(self.request.user, "role", None)
        if role == UserRole.MANAGER:
            return queryset.filter(creator_id=self.request.user.id)
        if role == UserRole.RET:
            return queryset.filter(Q(creator_id=self.request.user.id) | Q(creator__role=UserRole.MANAGER))
        if role == UserRole.TEAMLEADER:
            if self.action in {"list", "retrieve"}:
                return queryset.filter(creator__role__in=[UserRole.MANAGER, UserRole.TEAMLEADER])
            return queryset.filter(creator__role=UserRole.MANAGER)
        return queryset

    def get_serializer_class(self):
        if self.action in {"create", "update", "partial_update"}:
            return LeadDepositWriteSerializer
        return LeadDepositSerializer

    def _get_stats_queryset(self, query_params) -> tuple:
        query = LeadDepositStatsQuerySerializer(data=query_params)
        query.is_valid(raise_exception=True)
        validated = query.validated_data

        queryset = self.filter_queryset(self.get_queryset()).order_by()
        partner = validated.get("partner")
        creator = validated.get("creator")
        creator_role = validated.get("creator_role")
        if partner is not None:
            queryset = queryset.filter(lead__partner=partner)
        if creator is not None:
            queryset = queryset.filter(creator=creator)
        if creator_role:
            queryset = queryset.filter(creator__role=creator_role)

        date_from = validated.get("date_from")
        date_to = validated.get("date_to")
        if date_from is not None:
            start_at = timezone.make_aware(datetime.combine(date_from, time.min), timezone.get_current_timezone())
            queryset = queryset.filter(created_at__gte=start_at)
        if date_to is not None:
            end_exclusive = timezone.make_aware(
                datetime.combine(date_to + timedelta(days=1), time.min),
                timezone.get_current_timezone(),
            )
            queryset = queryset.filter(created_at__lt=end_exclusive)

        return queryset, validated

    @staticmethod
    def _month_range(date_from: date | None, date_to: date | None, month_values: list[datetime]) -> list[date]:
        if date_from and date_to:
            current = date_from.replace(day=1)
            final = date_to.replace(day=1)
            months: list[date] = []
            while current <= final:
                months.append(current)
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
            return months

        normalized = []
        seen: set[date] = set()
        for value in month_values:
            month_key = value.date().replace(day=1)
            if month_key in seen:
                continue
            seen.add(month_key)
            normalized.append(month_key)
        normalized.sort()
        return normalized

    @staticmethod
    def _month_payload(month_start: date) -> dict:
        month_key = month_start.strftime("%Y-%m")
        return {
            "year": month_start.year,
            "month": month_start.month,
            "month_key": month_key,
            "month_label": month_key,
        }

    @staticmethod
    def _amount_string(value) -> str:
        return f"{(value or 0):.2f}"

    @action(detail=False, methods=["get"], url_path="stats/monthly")
    def stats_monthly(self, request):
        queryset, params = self._get_stats_queryset(request.query_params)
        rows = list(
            queryset.annotate(month=TruncMonth("created_at"))
            .values("month")
            .annotate(
                ftd_count=Count("id", filter=Q(type=LeadDeposit.Type.FTD)),
                non_ftd_total_amount=Sum(
                    "amount",
                    filter=Q(type__in=[LeadDeposit.Type.RELOAD, LeadDeposit.Type.DEPOSIT]),
                ),
            )
            .order_by("month")
        )

        month_map = {
            row["month"].date().replace(day=1): {
                "ftd_count": row["ftd_count"],
                "non_ftd_total_amount": row["non_ftd_total_amount"],
            }
            for row in rows
            if row["month"] is not None
        }
        month_keys = self._month_range(
            params.get("date_from"),
            params.get("date_to"),
            [row["month"] for row in rows if row["month"] is not None],
        )

        items = []
        total_ftd_count = 0
        total_non_ftd_amount = 0
        for month_start in month_keys:
            data = month_map.get(month_start, {})
            ftd_count = int(data.get("ftd_count") or 0)
            non_ftd_total_amount = data.get("non_ftd_total_amount") or 0
            total_ftd_count += ftd_count
            total_non_ftd_amount += non_ftd_total_amount
            items.append(
                {
                    **self._month_payload(month_start),
                    "ftd_count": ftd_count,
                    "non_ftd_total_amount": self._amount_string(non_ftd_total_amount),
                }
            )

        return Response(
            {
                "period": {
                    "date_from": params.get("date_from").isoformat() if params.get("date_from") else None,
                    "date_to": params.get("date_to").isoformat() if params.get("date_to") else None,
                },
                "summary": {
                    "ftd_count": total_ftd_count,
                    "non_ftd_total_amount": self._amount_string(total_non_ftd_amount),
                },
                "items": items,
            }
        )

    @action(detail=False, methods=["get"], url_path="stats/ftd-matrix")
    def stats_ftd_matrix(self, request):
        queryset, params = self._get_stats_queryset(request.query_params)
        rows = list(
            queryset.filter(type=LeadDeposit.Type.FTD)
            .annotate(month=TruncMonth("created_at"))
            .values(
                "creator_id",
                "creator__username",
                "creator__first_name",
                "creator__last_name",
                "creator__role",
                "month",
            )
            .annotate(ftd_count=Count("id"))
            .order_by("creator__username", "creator_id", "month")
        )

        month_keys = self._month_range(
            params.get("date_from"),
            params.get("date_to"),
            [row["month"] for row in rows if row["month"] is not None],
        )
        columns = [self._month_payload(month_start) for month_start in month_keys]
        default_cells = {column["month_key"]: 0 for column in columns}

        row_map: dict[int, dict] = {}
        for row in rows:
            creator_id = row["creator_id"]
            if creator_id is None or row["month"] is None:
                continue
            if creator_id not in row_map:
                row_map[creator_id] = {
                    "user": {
                        "id": str(creator_id),
                        "username": row["creator__username"],
                        "first_name": (row["creator__first_name"] or "").strip(),
                        "last_name": (row["creator__last_name"] or "").strip(),
                        "role": row["creator__role"],
                    },
                    "total_ftd": 0,
                    "cells": dict(default_cells),
                }
            month_key = row["month"].date().replace(day=1).strftime("%Y-%m")
            ftd_count = int(row["ftd_count"] or 0)
            row_map[creator_id]["cells"][month_key] = ftd_count
            row_map[creator_id]["total_ftd"] += ftd_count

        matrix_rows = sorted(
            row_map.values(),
            key=lambda item: (-item["total_ftd"], item["user"]["username"] or "", item["user"]["id"]),
        )

        return Response(
            {
                "period": {
                    "date_from": params.get("date_from").isoformat() if params.get("date_from") else None,
                    "date_to": params.get("date_to").isoformat() if params.get("date_to") else None,
                },
                "columns": columns,
                "rows": matrix_rows,
            }
        )

    def _assert_update_allowed(self, deposit: LeadDeposit):
        role = getattr(self.request.user, "role", None)
        if "lead" in self.request.data:
            raise PermissionDenied("Lead cannot be changed for an existing deposit")
        if role in {UserRole.MANAGER, UserRole.RET, UserRole.TEAMLEADER} and "type" in self.request.data:
            raise PermissionDenied("Only admins and superusers can change deposit type")

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        lead = serializer.validated_data.get("lead")
        if lead is None:
            raise serializers.ValidationError({"lead": "This field is required"})
        dep = _create_lead_deposit(
            actor_user=request.user,
            lead=lead,
            amount=serializer.validated_data["amount"],
            requested_type=serializer.validated_data.get("type"),
            reason=serializer.validated_data.get("reason", ""),
        )
        return Response(LeadDepositSerializer(dep).data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        deposit = self.get_object()
        self._assert_update_allowed(deposit)

        serializer = self.get_serializer(deposit, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        validated = serializer.validated_data
        before = _deposit_payload(deposit)

        update_fields: list[str] = ["updated_at"]
        if "amount" in validated:
            deposit.amount = validated["amount"]
            update_fields.append("amount")
        if "type" in validated:
            new_type = int(validated["type"])
            if new_type != deposit.type and new_type in {LeadDeposit.Type.FTD, LeadDeposit.Type.RELOAD}:
                if LeadDeposit.objects.filter(lead=deposit.lead, type=new_type, is_deleted=False).exclude(id=deposit.id).exists():
                    raise _deposit_unique_conflict_error(new_type)
            deposit.type = new_type
            update_fields.append("type")

        if len(update_fields) == 1:
            raise serializers.ValidationError("No changes provided")

        try:
            deposit.save(update_fields=sorted(set(update_fields)))
        except IntegrityError:
            raise _deposit_unique_conflict_error(deposit.type)

        _log_status_audit(
            event_type=LeadAuditEvent.DEPOSIT_UPDATED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_DEPOSIT,
            entity_id=str(deposit.id),
            lead=deposit.lead,
            reason=validated.get("reason", ""),
            payload_before=before,
            payload_after=_deposit_payload(deposit),
        )
        return Response(LeadDepositSerializer(deposit).data, status=status.HTTP_200_OK)

    def partial_update(self, request, *args, **kwargs):
        kwargs["partial"] = True
        return self.update(request, *args, **kwargs)

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        deposit = self.get_object()
        before = _deposit_payload(deposit)
        deposit.delete()
        _log_status_audit(
            event_type=LeadAuditEvent.DEPOSIT_SOFT_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_DEPOSIT,
            entity_id=str(deposit.id),
            lead=deposit.lead,
            payload_before=before,
            payload_after=_deposit_payload(deposit),
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        role = getattr(request.user, "role", None)
        deposit = get_object_or_404(LeadDeposit.all_objects.select_related("lead", "creator", "lead__manager"), id=pk)
        allowed = False
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            allowed = True
        elif role == UserRole.MANAGER:
            allowed = deposit.creator_id == request.user.id
        elif role == UserRole.RET:
            allowed = deposit.creator_id == request.user.id or getattr(deposit.creator, "role", None) == UserRole.MANAGER
        elif role == UserRole.TEAMLEADER:
            allowed = getattr(deposit.creator, "role", None) == UserRole.MANAGER
        if not allowed:
            raise PermissionDenied("You do not have permission to restore this deposit")

        before = _deposit_payload(deposit)
        deposit.restore()
        _log_status_audit(
            event_type=LeadAuditEvent.DEPOSIT_RESTORED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_DEPOSIT,
            entity_id=str(deposit.id),
            lead=deposit.lead,
            payload_before=before,
            payload_after=_deposit_payload(deposit),
        )
        return Response(LeadDepositSerializer(deposit).data, status=status.HTTP_200_OK)

    def perform_destroy(self, instance):
        lead = instance.lead
        deposit_id = str(instance.id)
        before = _deposit_payload(instance)
        instance.hard_delete()
        _log_status_audit(
            event_type=LeadAuditEvent.DEPOSIT_HARD_DELETED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_DEPOSIT,
            entity_id=deposit_id,
            lead=lead,
            payload_before=before,
        )


class LeadViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = Lead.objects.select_related(
        "partner",
        "manager",
        "first_manager",
        "source",
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
        "custom_fields",
    }
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "timeline": (Perm.LEADS_READ,),
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
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]
    filterset_class = LeadRecordFilter
    ordering = ["-received_at"]
    ordering_fields = [
        "id",
        "received_at",
        "created_at",
        "updated_at",
        "assigned_at",
        "first_assigned_at",
        "next_contact_at",
        "last_contacted_at",
        "priority",
        "age",
        "full_name",
        "phone",
        "email",
        "geo",
        "partner__code",
        "manager__username",
        "first_manager__username",
        "status__order",
        "status__code",
        "source__code",
    ]

    def get_queryset(self):
        queryset = super().get_queryset()
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET}:
            queryset = queryset.filter(manager_id=self.request.user.id)
        if role == UserRole.TEAMLEADER:
            queryset = queryset.filter(
                Q(manager_id=self.request.user.id)
                | Q(manager__role=UserRole.MANAGER)
                | Q(manager__role=UserRole.TEAMLEADER)
                | Q(manager__isnull=True)
            )
        return queryset

    def get_serializer_class(self):
        if self.action in {"create", "update", "partial_update"}:
            return LeadWriteSerializer
        return LeadSerializer

    def _include_last_comment_requested(self) -> bool:
        raw = (self.request.query_params.get("include_last_comment") or "").strip().lower()
        return raw in {"1", "true", "yes", "y", "on"}

    def _build_last_comment_map_for_leads(self, lead_ids: list[int]) -> dict[str, dict]:
        if not lead_ids:
            return {}

        latest_comment_id = (
            LeadComment.objects.filter(lead_id=OuterRef("lead_id"), is_deleted=False)
            .order_by("-created_at", "-id")
            .values("id")[:1]
        )
        comments = (
            LeadComment.objects.filter(lead_id__in=lead_ids, is_deleted=False)
            .filter(id=Subquery(latest_comment_id))
            .select_related("author")
        )

        result: dict[str, dict] = {}
        for comment in comments:
            result[str(comment.lead_id)] = {
                "id": str(comment.id),
                "body": comment.body,
                "is_pinned": comment.is_pinned,
                "created_at": comment.created_at.isoformat() if comment.created_at else None,
                "author": _manager_payload(comment.author),
            }
        return result

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        include_last_comment = self._include_last_comment_requested()

        if page is not None:
            context = self.get_serializer_context()
            context["include_last_comment"] = include_last_comment
            if include_last_comment:
                context["last_comment_by_lead_id"] = self._build_last_comment_map_for_leads([lead.id for lead in page])
            serializer = self.get_serializer(page, many=True, context=context)
            return self.get_paginated_response(serializer.data)

        context = self.get_serializer_context()
        context["include_last_comment"] = include_last_comment
        if include_last_comment:
            context["last_comment_by_lead_id"] = self._build_last_comment_map_for_leads([lead.id for lead in queryset])
        serializer = self.get_serializer(queryset, many=True, context=context)
        return Response(serializer.data)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        include_last_comment = self._include_last_comment_requested()
        context = self.get_serializer_context()
        context["include_last_comment"] = include_last_comment
        if include_last_comment:
            context["last_comment_by_lead_id"] = self._build_last_comment_map_for_leads([instance.id])
        serializer = self.get_serializer(instance, context=context)
        return Response(serializer.data)

    @staticmethod
    def _timeline_status_payload(status_obj: LeadStatus | None) -> dict | None:
        if not status_obj:
            return None
        return {
            "id": str(status_obj.id),
            "code": status_obj.code,
            "name": status_obj.name,
            "color": status_obj.color,
        }

    @staticmethod
    def _timeline_actor_payload(actor_user) -> dict | None:
        if not actor_user:
            return None
        return {
            "id": str(actor_user.id),
            "username": actor_user.username,
            "first_name": (actor_user.first_name or "").strip(),
            "last_name": (actor_user.last_name or "").strip(),
            "role": actor_user.role,
        }

    @staticmethod
    def _timeline_deposit_type_label(dep_type: int | None) -> str | None:
        if dep_type is None:
            return None
        return dict(LeadDeposit.Type.choices).get(dep_type)

    def _timeline_details(self, audit: LeadAuditLog) -> str:
        payload_before = audit.payload_before if isinstance(audit.payload_before, dict) else {}
        payload_after = audit.payload_after if isinstance(audit.payload_after, dict) else {}

        if audit.event_type == LeadAuditEvent.STATUS_CHANGED:
            from_code = getattr(audit.from_status, "code", None) or payload_before.get("status", {}).get("code")
            to_code = getattr(audit.to_status, "code", None) or payload_after.get("status", {}).get("code")
            if from_code and to_code:
                return f"{from_code} -> {to_code}"

        if audit.event_type in {
            LeadAuditEvent.MANAGER_ASSIGNED,
            LeadAuditEvent.MANAGER_REASSIGNED,
            LeadAuditEvent.MANAGER_UNASSIGNED,
        }:
            before_manager = payload_before.get("manager") if isinstance(payload_before.get("manager"), dict) else {}
            after_manager = payload_after.get("manager") if isinstance(payload_after.get("manager"), dict) else {}
            before_name = before_manager.get("username") or "Unassigned"
            after_name = after_manager.get("username") or "Unassigned"
            return f"{before_name} -> {after_name}"

        if audit.event_type in {
            LeadAuditEvent.DEPOSIT_CREATED,
            LeadAuditEvent.DEPOSIT_UPDATED,
            LeadAuditEvent.DEPOSIT_REVERSED,
            LeadAuditEvent.DEPOSIT_SOFT_DELETED,
            LeadAuditEvent.DEPOSIT_RESTORED,
            LeadAuditEvent.DEPOSIT_HARD_DELETED,
        }:
            dep_payload = payload_after or payload_before
            dep_type = dep_payload.get("type")
            dep_amount = dep_payload.get("amount")
            dep_type_label = self._timeline_deposit_type_label(dep_type) or dep_type
            if dep_amount is not None and dep_type_label is not None:
                return f"{dep_type_label}: {dep_amount}"

        if audit.event_type in {
            LeadAuditEvent.COMMENT_CREATED,
            LeadAuditEvent.COMMENT_UPDATED,
            LeadAuditEvent.COMMENT_PINNED,
            LeadAuditEvent.COMMENT_UNPINNED,
        }:
            body = payload_after.get("body") if isinstance(payload_after, dict) else None
            if isinstance(body, str) and body:
                return body[:160]

        if audit.event_type == LeadAuditEvent.RET_TRANSFERRED:
            transfer = payload_after.get("transfer") if isinstance(payload_after.get("transfer"), dict) else {}
            from_manager_id = transfer.get("from_manager_id")
            target_manager_id = transfer.get("target_manager_id")
            if from_manager_id and target_manager_id:
                return f"manager {from_manager_id} -> {target_manager_id}"

        return (audit.reason or "").strip()

    def _timeline_item(self, audit: LeadAuditLog) -> dict:
        return {
            "id": str(audit.id),
            "at": audit.created_at.isoformat() if audit.created_at else None,
            "event_type": audit.event_type,
            "event_name": audit.get_event_type_display(),
            "entity_type": audit.entity_type,
            "entity_id": audit.entity_id,
            "source": audit.source,
            "reason": audit.reason,
            "batch_id": audit.batch_id,
            "actor": self._timeline_actor_payload(audit.actor_user),
            "from_status": self._timeline_status_payload(audit.from_status),
            "to_status": self._timeline_status_payload(audit.to_status),
            "details": self._timeline_details(audit),
            "payload_before": audit.payload_before,
            "payload_after": audit.payload_after,
        }

    @action(detail=True, methods=["get"], url_path="timeline")
    def timeline(self, request, pk=None):
        lead = self.get_object()
        queryset = (
            LeadAuditLog.objects.filter(lead=lead)
            .select_related("actor_user", "from_status", "to_status")
            .order_by("-created_at", "-id")
        )

        raw_events = (request.query_params.get("events") or "").strip()
        if raw_events:
            requested_events = [item.strip() for item in raw_events.split(",") if item.strip()]
            known_events = {value for value, _label in LeadAuditEvent.choices}
            unknown_events = sorted(set(requested_events) - known_events)
            if unknown_events:
                raise serializers.ValidationError(
                    {"events": f"Unknown event types: {', '.join(unknown_events)}"}
                )
            queryset = queryset.filter(event_type__in=requested_events)

        page = self.paginate_queryset(queryset)
        if page is not None:
            return self.get_paginated_response([self._timeline_item(item) for item in page])
        return Response([self._timeline_item(item) for item in queryset], status=status.HTTP_200_OK)

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
            if lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Teamleaders can edit only own leads and manager/teamleader leads")

    def _assert_can_manage_assignment(self, lead: Lead, *, operation: str):
        role = getattr(self.request.user, "role", None)
        if role != UserRole.TEAMLEADER:
            return

        lead_manager = getattr(lead, "manager", None)
        lead_manager_role = getattr(lead_manager, "role", None)

        if operation == "assign":
            if lead.manager_id is None or lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Teamleaders can assign only unassigned leads or manager/teamleader leads")

        if operation == "unassign":
            if lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Teamleaders can unassign only leads assigned to managers/teamleaders")

        if lead.manager_id is None:
            return
        raise PermissionDenied("Unsupported assignment operation")

    def _assert_can_change_first_manager(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        raise PermissionDenied("Only teamleaders, admins and superusers can change first_manager")

    def _assert_can_change_first_manager_for_lead(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER and self._teamleader_can_manage_manager_scope(lead):
            return
        raise PermissionDenied("Teamleaders can change first_manager only for manager/teamleader leads")

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

        if payload_fields & {"first_manager", "first_assigned_at"}:
            self._assert_can_change_first_manager()
            self._assert_can_change_first_manager_for_lead(lead)

        if "next_contact_at" in payload_fields:
            if role in {UserRole.MANAGER, UserRole.RET} and lead.manager_id != self.request.user.id:
                raise PermissionDenied("You can set next_contact_at only for your own leads")
            if role == UserRole.TEAMLEADER:
                if lead.manager_id == self.request.user.id:
                    return
                manager = getattr(lead, "manager", None)
                manager_role = getattr(manager, "role", None)
                if manager is None or manager_role not in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                    raise PermissionDenied(
                        "Teamleaders can set next_contact_at only for own and manager/teamleader leads"
                    )

    def _assert_create_payload_allowed(self):
        role = getattr(self.request.user, "role", None)
        payload_fields = set(self.request.data.keys())
        if role != UserRole.SUPERUSER and "geo" in payload_fields:
            raise PermissionDenied("Only superusers can set geo on create")
        if role != UserRole.SUPERUSER and "custom_fields" in payload_fields:
            raise PermissionDenied("Only superusers can set custom_fields on create")

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
            if lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                continue
            forbidden_ids.append(str(lead.id))
        if forbidden_ids:
            raise PermissionDenied("Teamleaders can change status only for own and manager/teamleader leads")

    def _teamleader_can_manage_manager_scope(self, lead: Lead) -> bool:
        if lead.manager_id == self.request.user.id:
            return True
        lead_manager_role = getattr(getattr(lead, "manager", None), "role", None)
        return lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}

    def _assert_can_force_status_change(self, *, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER and self._teamleader_can_manage_manager_scope(lead):
            return
        raise PermissionDenied(
            "Only teamleaders (own/manager/teamleader leads), admins, and superusers can use force status change"
        )

    def _assert_can_transfer_to_ret(self, *, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER:
            if self._teamleader_can_manage_manager_scope(lead):
                return
            raise PermissionDenied("Teamleaders can handoff only own and manager/teamleader leads")
        if role == UserRole.MANAGER and lead.manager_id == self.request.user.id:
            return
        raise PermissionDenied("You cannot handoff this lead")

    def _resolve_transfer_author(self, *, lead: Lead, transfer_author):
        role = getattr(self.request.user, "role", None)
        if role == UserRole.MANAGER:
            return self.request.user

        if role == UserRole.TEAMLEADER:
            if transfer_author is None:
                raise serializers.ValidationError({"transfer_author": "transfer_author is required for teamleaders"})
            if transfer_author.role == UserRole.TEAMLEADER:
                if transfer_author.id != self.request.user.id:
                    raise PermissionDenied("Teamleaders can set only themselves as TEAMLEADER transfer_author")
                return transfer_author
            if transfer_author.role == UserRole.MANAGER:
                if lead.manager_id != transfer_author.id:
                    raise serializers.ValidationError(
                        {"transfer_author": "For manager author, select current lead manager"}
                    )
                return transfer_author
            raise serializers.ValidationError({"transfer_author": "transfer_author must be MANAGER or TEAMLEADER"})

        if role in {UserRole.ADMIN, UserRole.SUPERUSER}:
            if transfer_author is not None:
                return transfer_author
            current_manager = getattr(lead, "manager", None)
            if current_manager and getattr(current_manager, "role", None) in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return current_manager
            return self.request.user

        return self.request.user

    def _assert_can_rollback_ret_transfer(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        raise PermissionDenied("Only teamleaders, admins and superusers can rollback RET transfer")

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
            event_type=LeadAuditEvent.LEAD_CREATED,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.LEAD_UPDATED,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.LEAD_HARD_DELETED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.LEAD_SOFT_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
            event_type=LeadAuditEvent.LEAD_RESTORED,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
            items = LeadDeposit.objects.filter(lead=lead).select_related("creator").order_by("-created_at", "-id")
            return Response(LeadDepositSerializer(items, many=True).data, status=status.HTTP_200_OK)

        serializer = LeadDepositCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        dep = _create_lead_deposit(
            actor_user=request.user,
            lead=lead,
            amount=serializer.validated_data["amount"],
            requested_type=serializer.validated_data.get("type"),
            reason=serializer.validated_data.get("reason", ""),
        )
        return Response(LeadDepositSerializer(dep).data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"], url_path="close-won-transfer")
    def close_won_transfer(self, request, pk=None):
        serializer = LeadCloseWonTransferSerializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        to_status = serializer.validated_data["to_status"]
        reason = serializer.validated_data.get("reason", "")
        comment_body = serializer.validated_data.get("comment")
        force_status = serializer.validated_data.get("force_status", False)
        requested_transfer_author = serializer.validated_data.get("transfer_author")

        with transaction.atomic():
            lead = (
                Lead.objects.select_for_update()
                .select_related("manager", "status")
                .get(id=pk)
            )
            self._assert_can_transfer_to_ret(lead=lead)

            existing_deposits = LeadDeposit.objects.filter(lead=lead).order_by("created_at", "id")
            non_ftd_exists = existing_deposits.exclude(type=LeadDeposit.Type.FTD).exists()
            if non_ftd_exists:
                raise serializers.ValidationError("Cannot transfer to RET: lead already has non-FTD deposits")

            from_manager = lead.manager
            if from_manager is None:
                raise serializers.ValidationError("Lead must have assigned manager before transfer to RET")
            if getattr(from_manager, "role", None) == UserRole.RET:
                raise serializers.ValidationError("Lead is already assigned to RET")
            transfer_author = self._resolve_transfer_author(lead=lead, transfer_author=requested_transfer_author)

            if to_status.id != lead.status_id:
                if force_status:
                    self._assert_can_force_status_change(lead=lead)
                status_change_error = _status_change_error_for_lead(
                    lead=lead,
                    to_status=to_status,
                )
                if status_change_error:
                    raise _status_change_error_as_validation_error(status_change_error)

            now = timezone.now()
            dep = existing_deposits.filter(type=LeadDeposit.Type.FTD).first()
            created_ftd = False
            if dep is None:
                dep = LeadDeposit.objects.create(
                    lead=lead,
                    creator=transfer_author,
                    amount=serializer.validated_data["amount"],
                    type=LeadDeposit.Type.FTD,
                )
                created_ftd = True
            elif existing_deposits.filter(type=LeadDeposit.Type.FTD).count() > 1:
                raise serializers.ValidationError("Cannot transfer to RET: multiple FTD records found")

            before = _lead_payload(lead)
            lead.manager = serializer.validated_data["ret_manager"]
            lead.assigned_at = now

            update_fields = [
                "manager",
                "assigned_at",
                "updated_at",
            ]
            if to_status.id != lead.status_id:
                lead.status = to_status
                update_fields.append("status")
            lead.save(update_fields=sorted(set(update_fields)))
            created_comment = None
            if comment_body:
                created_comment = LeadComment.objects.create(
                    lead=lead,
                    author=request.user,
                    body=comment_body,
                )
            contact_dt = None
            if created_ftd:
                contact_dt = dep.created_at
            if created_comment is not None:
                comment_created_at = created_comment.created_at
                if contact_dt is None or comment_created_at > contact_dt:
                    contact_dt = comment_created_at
            if contact_dt is not None:
                _touch_lead_last_contacted(lead, at=contact_dt)

            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadAuditSource.API,
                reason=reason,
                from_manager=from_manager,
                to_manager=lead.manager,
            )
            if created_ftd:
                _log_status_audit(
                    event_type=LeadAuditEvent.DEPOSIT_CREATED,
                    actor_user=transfer_author,
                    source=LeadAuditSource.API,
                    entity_type=LeadAuditEntity.LEAD,
                    entity_id=str(lead.id),
                    lead=lead,
                    reason=reason,
                    payload_after=_deposit_payload(dep),
                )
            if created_comment is not None:
                _log_status_audit(
                    event_type=LeadAuditEvent.COMMENT_CREATED,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    entity_type=LeadAuditEntity.LEAD_COMMENT,
                    entity_id=str(created_comment.id),
                    lead=lead,
                    reason=reason,
                    payload_after=_comment_payload(created_comment),
                )
            _log_status_audit(
                event_type=LeadAuditEvent.RET_TRANSFERRED,
                actor_user=request.user,
                source=LeadAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=reason,
                payload_before=before,
                payload_after={
                    "lead": _lead_payload(lead),
                    "transfer": {
                        "from_manager_id": str(from_manager.id) if from_manager else None,
                        "target_manager_id": str(lead.manager_id) if lead.manager_id else None,
                        "to_ret_id": (
                            str(lead.manager_id)
                            if lead.manager_id and getattr(lead.manager, "role", None) == UserRole.RET
                            else None
                        ),
                        "transfer_author_id": str(transfer_author.id) if transfer_author else None,
                        "performed_by_user_id": str(request.user.id),
                        "transferred_at": now.isoformat(),
                    },
                    "ftd": _deposit_payload(dep),
                    "comment": _comment_payload(created_comment),
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
                .select_related("manager", "first_manager")
                .get(id=pk)
            )
            if getattr(getattr(lead, "manager", None), "role", None) not in {
                UserRole.RET,
                UserRole.ADMIN,
                UserRole.SUPERUSER,
            }:
                raise serializers.ValidationError("Active RET transfer not found")
            rollback_manager = _resolve_rollback_manager(lead)
            if rollback_manager is None:
                raise serializers.ValidationError("Cannot rollback transfer: source manager not found")

            if LeadDeposit.objects.filter(lead=lead).exclude(type=LeadDeposit.Type.FTD).exists():
                raise serializers.ValidationError("Cannot rollback transfer: lead already has non-FTD deposits")

            ftd = LeadDeposit.objects.filter(lead=lead, type=LeadDeposit.Type.FTD).order_by("-created_at").first()
            if not ftd:
                raise serializers.ValidationError("Cannot rollback transfer: FTD not found")

            before_lead = _lead_payload(lead)
            before_ftd = _deposit_payload(ftd)
            ftd.delete()

            previous_manager = lead.manager
            rollback_ts = timezone.now()
            lead.manager = rollback_manager
            lead.assigned_at = rollback_ts
            lead.save(
                update_fields=[
                    "manager",
                    "assigned_at",
                    "updated_at",
                ]
            )

            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadAuditSource.API,
                reason=serializer.validated_data.get("reason", ""),
                from_manager=previous_manager,
                to_manager=lead.manager,
            )
            _log_status_audit(
                event_type=LeadAuditEvent.DEPOSIT_REVERSED,
                actor_user=request.user,
                source=LeadAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=serializer.validated_data.get("reason", ""),
                payload_before=before_ftd,
                payload_after=_deposit_payload(ftd),
            )
            _log_status_audit(
                event_type=LeadAuditEvent.RET_TRANSFER_ROLLBACK,
                actor_user=request.user,
                source=LeadAuditSource.API,
                entity_type=LeadAuditEntity.LEAD,
                entity_id=str(lead.id),
                lead=lead,
                reason=serializer.validated_data.get("reason", ""),
                payload_before=before_lead,
                payload_after={
                    "lead": _lead_payload(lead),
                    "rollback_to_manager_id": str(rollback_manager.id),
                    "rolled_back_at": rollback_ts.isoformat(),
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

        partner = query.validated_data.get("partner")
        group_by = query.validated_data.get("group_by")
        requester_role = getattr(request.user, "role", None)
        manager_scope = request.user if requester_role in {UserRole.MANAGER, UserRole.RET} else None

        leads_received_qs = Lead.objects.filter(received_at__gte=period_start, received_at__lt=period_end)
        if partner:
            leads_received_qs = leads_received_qs.filter(partner=partner)
        if manager_scope:
            leads_received_qs = leads_received_qs.filter(first_manager=manager_scope)

        valid_status_filter = Q(status__is_valid=True)
        lost_status_filter = Q(status__conversion_bucket=LeadStatus.ConversionBucket.LOST)
        working_status_filter = Q(status__work_bucket=LeadStatus.WorkBucket.WORKING)
        return_status_filter = Q(status__work_bucket=LeadStatus.WorkBucket.RETURN)
        non_working_status_filter = Q(status__work_bucket=LeadStatus.WorkBucket.NON_WORKING)

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

            partner_ftd_qs = LeadDeposit.objects.filter(
                type=LeadDeposit.Type.FTD,
                is_deleted=False,
                created_at__gte=period_start,
                created_at__lt=period_end,
            )
            if partner:
                partner_ftd_qs = partner_ftd_qs.filter(lead__partner=partner)
            if partner_filter:
                partner_ftd_qs = partner_ftd_qs.filter(lead__partner=partner_filter)
            if manager_scope:
                partner_ftd_qs = partner_ftd_qs.filter(lead__first_manager=manager_scope)

            total = partner_leads_received_qs.count()
            leads_in_status = list(
                partner_leads_received_qs.exclude(status__isnull=True)
                .values("status_id", "status__code", "status__name", "status__work_bucket")
                .annotate(count=Count("id"))
                .order_by("status__code")
            )
            valid_count = partner_leads_received_qs.filter(valid_status_filter).count()
            invalid_count = max(total - valid_count, 0)
            won_count = partner_ftd_qs.values("lead_id").distinct().count()
            lost_count = partner_leads_received_qs.filter(lost_status_filter).count()
            working_count = partner_leads_received_qs.filter(working_status_filter).count()
            return_count = partner_leads_received_qs.filter(return_status_filter).count()
            non_working_count = partner_leads_received_qs.filter(non_working_status_filter).count()
            same_day_won_count = (
                partner_ftd_qs.filter(
                    lead__received_at__gte=period_start,
                    lead__received_at__lt=period_end,
                )
                .annotate(
                    received_date=TruncDate("lead__received_at"),
                    ftd_date=TruncDate("created_at"),
                )
                .filter(received_date=F("ftd_date"))
                .values("lead_id")
                .distinct()
                .count()
            )
            won_by_manager_rows = list(
                partner_ftd_qs.values(
                    "creator_id",
                    "creator__username",
                    "creator__first_name",
                    "creator__last_name",
                    "creator__role",
                )
                .annotate(won_total=Count("id"))
                .order_by("-won_total", "creator__username", "creator_id")
            )

            valid_rate = _rate(valid_count, total)
            invalid_rate = _rate(invalid_count, total)
            won_rate = _rate(won_count, total)
            lost_rate = _rate(lost_count, total)
            working_rate = _rate(working_count, total)
            return_rate = _rate(return_count, total)
            non_working_rate = _rate(non_working_count, total)
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
                        "work_bucket": row["status__work_bucket"],
                        "count": row["count"],
                        "rate": status_rate,
                        "percent": _percent(status_rate),
                    }
                )
            won_by_manager = []
            for row in won_by_manager_rows:
                creator_id = row["creator_id"]
                if creator_id is None:
                    manager_payload = None
                else:
                    manager_payload = {
                        "id": str(creator_id),
                        "username": row["creator__username"],
                        "first_name": (row["creator__first_name"] or "").strip(),
                        "last_name": (row["creator__last_name"] or "").strip(),
                        "role": row["creator__role"],
                    }
                manager_won_rate = _rate(row["won_total"], won_count)
                won_by_manager.append(
                    {
                        "manager": manager_payload,
                        "won_total": row["won_total"],
                        "won_rate": manager_won_rate,
                        "won_percent": _percent(manager_won_rate),
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
                    "working_total": working_count,
                    "working_rate": working_rate,
                    "working_percent": _percent(working_rate),
                    "return_total": return_count,
                    "return_rate": return_rate,
                    "return_percent": _percent(return_rate),
                    "non_working_total": non_working_count,
                    "non_working_rate": non_working_rate,
                    "non_working_percent": _percent(non_working_rate),
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
                "won_by_manager": won_by_manager,
                "status_breakdown": status_breakdown,
            }

        payload = {
            "period": {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
        }
        if partner:
            payload["partner"] = {"id": str(partner.id), "code": partner.code, "name": partner.name}
        if manager_scope:
            payload["manager"] = _manager_payload(manager_scope)

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
        first_assigned_at = serializer.validated_data.get("first_assigned_at")
        reason = serializer.validated_data.get("reason", "")

        lead = self.get_object()
        self._assert_can_change_first_manager_for_lead(lead)
        before = {
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(getattr(lead, "first_manager", None)),
            "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        }
        lead.first_manager = manager
        update_fields = ["first_manager", "updated_at"]
        if first_assigned_at is not None:
            lead.first_assigned_at = first_assigned_at
            update_fields.append("first_assigned_at")
        elif lead.first_assigned_at is None:
            lead.first_assigned_at = timezone.now()
            update_fields.append("first_assigned_at")
        lead.save(update_fields=update_fields)
        after = {
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(getattr(lead, "first_manager", None)),
            "first_assigned_at": lead.first_assigned_at.isoformat() if lead.first_assigned_at else None,
        }
        _log_status_audit(
            event_type=LeadAuditEvent.LEAD_UPDATED,
            actor_user=request.user,
            source=LeadAuditSource.API,
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
                endpoint=LeadIdempotencyEndpoint.ASSIGN_MANAGER,
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
                .select_related("partner", "manager", "first_manager", "source", "status")
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
                source=LeadAuditSource.API,
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
                endpoint=LeadIdempotencyEndpoint.UNASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            serializer = LeadUnassignManagerSerializer(data=request.data or {})
            serializer.is_valid(raise_exception=True)
            reason = serializer.validated_data.get("reason", "")

            lead = (
                Lead.objects.select_for_update()
                .select_related("partner", "manager", "first_manager", "source", "status")
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="unassign")
            previous_manager = lead.manager
            lead.manager = None
            lead.save(update_fields=["manager", "updated_at"])
            _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadAuditSource.API,
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
                endpoint=LeadIdempotencyEndpoint.BULK_ASSIGN_MANAGER,
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
                    source=LeadAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=manager,
                    batch_id=batch_id,
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "source", "status")
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
                endpoint=LeadIdempotencyEndpoint.BULK_UNASSIGN_MANAGER,
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
                    source=LeadAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=None,
                    batch_id=batch_id,
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "source", "status")
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
                endpoint=LeadIdempotencyEndpoint.CHANGE_STATUS,
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
                .select_related("partner", "manager", "first_manager", "source", "status")
                .get(id=pk)
            )
            self._assert_can_edit(lead)
            if force:
                self._assert_can_force_status_change(lead=lead)
            error = _status_change_error_for_lead(lead=lead, to_status=to_status)
            if error:
                raise _status_change_error_as_validation_error(error)

            from_status = lead.status
            from_bucket = _status_conversion_bucket(from_status)
            payload_before = {
                "lead_id": str(lead.id),
                "status_id": str(from_status.id) if from_status else None,
                "status_code": from_status.code if from_status else None,
                "status_bucket": from_bucket,
            }

            lead.status = to_status
            update_fields = ["status", "updated_at"]
            lead.save(update_fields=sorted(set(update_fields)))

            payload_after = {
                "lead_id": str(lead.id),
                "status_id": str(lead.status_id) if lead.status_id else None,
                "status_code": to_status.code,
                "status_bucket": _status_conversion_bucket(to_status),
            }

            _log_status_audit(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                actor_user=request.user,
                source=LeadAuditSource.API,
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
                endpoint=LeadIdempotencyEndpoint.BULK_CHANGE_STATUS,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            locked_leads = list(
                Lead.objects.select_for_update()
                .select_related("status")
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

            errors = {}
            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                error = _status_change_error_for_lead(
                    lead=lead,
                    to_status=to_status,
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
                    "status_id": str(from_status.id) if from_status else None,
                    "status_code": from_status.code if from_status else None,
                    "status_bucket": from_bucket,
                }

                lead.status = to_status
                update_fields = ["status", "updated_at"]
                lead.save(update_fields=sorted(set(update_fields)))

                payload_after = {
                    "lead_id": str(lead.id),
                    "status_id": str(lead.status_id) if lead.status_id else None,
                    "status_code": to_status.code,
                    "status_bucket": _status_conversion_bucket(to_status),
                }

                _log_status_audit(
                    event_type=LeadAuditEvent.STATUS_CHANGED,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
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
                .select_related("partner", "manager", "first_manager", "source", "status")
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
