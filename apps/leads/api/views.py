from __future__ import annotations

import hashlib
import json
import uuid
import mimetypes
from datetime import date, datetime, time, timedelta

from django.contrib.auth import get_user_model
from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import Count, F, OuterRef, Q, Subquery, Sum
from django.db.models.functions import TruncDate, TruncMonth
from django.http import FileResponse, Http404
from django.shortcuts import get_object_or_404
from django.utils._os import safe_join
from django_filters import rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend
from django.utils import timezone
from rest_framework import filters as drf_filters, serializers, status, viewsets
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.authentication import SessionAuthentication
from rest_framework_simplejwt.authentication import JWTAuthentication

from apps.iam.api.rbac_mixins import RBACActionMixin
from apps.iam.api.rbac_permissions import RBACPermission
from apps.iam.models import UserRole
from apps.iam.rbac import Perm
from apps.notifications.publishers import (
    publish_bulk_lead_assigned,
    publish_bulk_lead_status_changed,
    publish_bulk_lead_unassigned,
    publish_comment_added,
    publish_deposit_created,
    publish_lead_assigned,
    publish_lead_status_changed,
    publish_lead_unassigned,
    publish_next_contact_planned_resync,
)
from apps.leads.attachment_validation import AttachmentValidationError, validate_uploaded_attachment
from apps.leads.models import (
    Lead,
    LeadAuditEntity,
    LeadAttachment,
    LeadDeposit,
    LeadComment,
    LeadTag,
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
    BulkLeadAddTagsSerializer,
    BulkLeadAssignManagerSerializer,
    BulkLeadClearTagsSerializer,
    BulkLeadRemoveTagsSerializer,
    LeadAttachmentSerializer,
    LeadAttachmentWriteSerializer,
    LeadDepositCreateSerializer,
    LeadDepositSerializer,
    LeadTagSerializer,
    LeadDepositWriteSerializer,
    BulkLeadUnassignManagerSerializer,
    BulkLeadStatusChangeSerializer,
    LeadAssignManagerSerializer,
    LeadChangeFirstManagerSerializer,
    LeadDepositStatsQuerySerializer,
    LeadFunnelMetricsQuerySerializer,
    LeadMetricsDrilldownQuerySerializer,
    LeadMetricsDrilldownSerializer,
    LeadSetTagsSerializer,
    LeadWriteSerializer,
    LeadCommentSerializer,
    LeadSerializer,
    LeadStatusChangeSerializer,
    LeadAuditLogSerializer,
    LeadStatusSerializer,
    LeadUnassignManagerSerializer,
)


def _protected_media_file_response(*, file_path: str, allow_deleted: bool = False):
    queryset = LeadAttachment.all_objects if allow_deleted else LeadAttachment.objects
    attachment = queryset.filter(file=file_path).only("id", "file", "mime_type").first()
    if attachment is None or not attachment.file:
        raise Http404("Файл не найден")

    try:
        safe_join(str(settings.MEDIA_ROOT), file_path)
    except Exception as exc:
        raise Http404("Некорректный путь к файлу") from exc

    content_type, _encoding = mimetypes.guess_type(attachment.file.name)
    content_type = attachment.mime_type or content_type or "application/octet-stream"
    return FileResponse(attachment.file.open("rb"), content_type=content_type)


@api_view(["GET"])
@authentication_classes([SessionAuthentication, JWTAuthentication])
@permission_classes([IsAuthenticated])
def protected_media(request, file_path: str):
    return _protected_media_file_response(file_path=file_path, allow_deleted=False)


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


def _tag_payload(tag_obj: LeadTag | None) -> dict | None:
    if not tag_obj:
        return None
    return {
        "id": str(tag_obj.id),
        "name": tag_obj.name,
        "color": tag_obj.color,
        "icon": tag_obj.icon,
        "is_deleted": bool(getattr(tag_obj, "is_deleted", False)),
        "deleted_at": tag_obj.deleted_at.isoformat() if getattr(tag_obj, "deleted_at", None) else None,
        "created_at": tag_obj.created_at.isoformat() if tag_obj.created_at else None,
        "updated_at": tag_obj.updated_at.isoformat() if tag_obj.updated_at else None,
    }


def _lead_tags_payload(lead: Lead | None) -> list[dict]:
    if not lead:
        return []
    tags = (
        lead.tags.filter(is_deleted=False)
        .order_by("name", "id")
    )
    return [_tag_payload(tag) for tag in tags]


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
        "source": lead.source,
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


def _attachment_payload(attachment: LeadAttachment | None) -> dict | None:
    if not attachment:
        return None
    file_url = ""
    if attachment.file:
        try:
            file_url = attachment.file.url
        except Exception:
            file_url = ""
    return {
        "id": str(attachment.id),
        "lead_id": str(attachment.lead_id) if attachment.lead_id else None,
        "uploaded_by_id": str(attachment.uploaded_by_id) if attachment.uploaded_by_id else None,
        "file": attachment.file.name if attachment.file else "",
        "file_url": file_url,
        "kind": attachment.kind,
        "original_name": attachment.original_name,
        "mime_type": attachment.mime_type,
        "size_bytes": attachment.size_bytes,
        "is_deleted": bool(getattr(attachment, "is_deleted", False)),
        "deleted_at": attachment.deleted_at.isoformat() if getattr(attachment, "deleted_at", None) else None,
        "created_at": attachment.created_at.isoformat() if attachment.created_at else None,
    }


def _create_lead_attachment(
    *,
    actor_user,
    lead: Lead,
    uploaded_file,
    requested_kind: str | None = None,
    reason: str = "",
) -> LeadAttachment:
    try:
        resolved_kind, mime_type = validate_uploaded_attachment(
            uploaded_file,
            requested_kind=requested_kind,
        )
    except AttachmentValidationError as exc:
        raise serializers.ValidationError({exc.field: exc.message})

    attachment = LeadAttachment.objects.create(
        lead=lead,
        uploaded_by=actor_user,
        file=uploaded_file,
        kind=resolved_kind,
        original_name=getattr(uploaded_file, "name", "") or "",
        mime_type=mime_type,
        size_bytes=getattr(uploaded_file, "size", 0) or 0,
    )
    _touch_lead_last_contacted(lead, at=attachment.created_at)
    _log_status_audit(
        event_type=LeadAuditEvent.ATTACHMENT_CREATED,
        actor_user=actor_user,
        source=LeadAuditSource.API,
        entity_type=LeadAuditEntity.LEAD_ATTACHMENT,
        entity_id=str(attachment.id),
        lead=lead,
        reason=reason,
        payload_after=_attachment_payload(attachment),
    )
    return attachment


def _deposit_unique_conflict_error(dep_type: int):
    if dep_type == LeadDeposit.Type.FTD:
        return serializers.ValidationError({"type": "FTD уже существует для этого лида"})
    if dep_type == LeadDeposit.Type.RELOAD:
        return serializers.ValidationError({"type": "Reload уже существует для этого лида"})
    return serializers.ValidationError({"type": "Конфликт типа депозита для этого лида"})


def _lead_manager_role(lead: Lead) -> str | None:
    manager = getattr(lead, "manager", None)
    return getattr(manager, "role", None)


def _user_can_view_lead(*, actor_user, lead: Lead) -> bool:
    role = getattr(actor_user, "role", None)
    if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
        return True
    if role in {UserRole.MANAGER, UserRole.RET}:
        return lead.manager_id == actor_user.id
    if role == UserRole.TEAMLEADER:
        lead_manager_role = _lead_manager_role(lead)
        return (
            lead.manager_id == actor_user.id
            or lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}
            or lead.manager_id is None
        )
    return False


def _user_can_edit_lead(*, actor_user, lead: Lead) -> bool:
    role = getattr(actor_user, "role", None)
    if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
        return True
    if role in {UserRole.MANAGER, UserRole.RET}:
        return lead.manager_id == actor_user.id
    if role == UserRole.TEAMLEADER:
        lead_manager_role = _lead_manager_role(lead)
        return lead.manager_id == actor_user.id or lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}
    return False


def _assert_deposit_create_allowed(*, actor_user, lead: Lead, manual_type: bool) -> None:
    role = getattr(actor_user, "role", None)
    lead_manager_role = _lead_manager_role(lead)

    if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
        return
    if role == UserRole.TEAMLEADER:
        if manual_type:
            raise PermissionDenied("Только админы и суперпользователи могут вручную выбирать тип депозита")
        if lead_manager_role not in {UserRole.MANAGER, UserRole.TEAMLEADER}:
            raise PermissionDenied("Тимлиды могут создавать депозиты только для лидов менеджеров/тимлидов")
        return
    if role == UserRole.MANAGER:
        if manual_type:
            raise PermissionDenied("Только админы и суперпользователи могут вручную выбирать тип депозита")
        if lead.manager_id != actor_user.id:
            raise PermissionDenied("Менеджеры могут создавать депозиты только для своих лидов")
        return
    if role == UserRole.RET:
        if manual_type:
            raise PermissionDenied("Только админы и суперпользователи могут вручную выбирать тип депозита")
        if lead.manager_id != actor_user.id:
            raise PermissionDenied("RET может создавать депозиты только для своих лидов")
        return
    raise PermissionDenied("Вы не можете создавать депозиты для этого лида")


def _filter_deposits_visible_for_user(*, actor_user, queryset, include_teamleader_self: bool = True):
    role = getattr(actor_user, "role", None)
    if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
        return queryset
    if role == UserRole.MANAGER:
        return queryset.filter(creator_id=actor_user.id)
    if role == UserRole.RET:
        return queryset.filter(creator_id=actor_user.id)
    if role == UserRole.TEAMLEADER:
        if include_teamleader_self:
            return queryset.filter(
                Q(creator_id=actor_user.id)
                | Q(creator__role=UserRole.MANAGER)
                | Q(creator__role=UserRole.TEAMLEADER)
            )
        return queryset.filter(creator__role=UserRole.MANAGER)
    return queryset.none()


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
        raise serializers.ValidationError({"type": "Тимлиды могут создавать только FTD"})
    if role == UserRole.MANAGER and dep_type != LeadDeposit.Type.FTD:
        raise serializers.ValidationError({"type": "Менеджеры могут создавать только FTD"})
    if role == UserRole.RET and dep_type == LeadDeposit.Type.FTD:
        raise serializers.ValidationError({"type": "RET не может создавать FTD"})

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
    if int(dep.type) == int(LeadDeposit.Type.FTD):
        publish_deposit_created(
            deposit_id=dep.id,
            actor_user_id=getattr(actor_user, "id", None),
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
) -> LeadAuditLog:
    return LeadAuditLog.objects.create(
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
) -> LeadAuditLog | None:
    from_id = getattr(from_manager, "id", None)
    to_id = getattr(to_manager, "id", None)
    if from_id == to_id:
        return None
    if to_manager is None:
        event_type = LeadAuditEvent.MANAGER_UNASSIGNED
    elif from_manager is None:
        event_type = LeadAuditEvent.MANAGER_ASSIGNED
    else:
        event_type = LeadAuditEvent.MANAGER_REASSIGNED

    return _log_status_audit(
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


def _log_first_manager_audit(
    *,
    lead: Lead,
    actor_user,
    source: str,
    reason: str,
    from_manager,
    to_manager,
    before_first_assigned_at,
    after_first_assigned_at,
    batch_id: str = "",
) -> None:
    _log_status_audit(
        event_type=LeadAuditEvent.FIRST_MANAGER_CHANGED,
        actor_user=actor_user,
        source=source,
        entity_type=LeadAuditEntity.LEAD,
        entity_id=str(lead.id),
        batch_id=batch_id,
        reason=reason,
        lead=lead,
        payload_before={
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(from_manager),
            "first_assigned_at": before_first_assigned_at.isoformat() if before_first_assigned_at else None,
        },
        payload_after={
            "lead_id": str(lead.id),
            "first_manager": _manager_payload(to_manager),
            "first_assigned_at": after_first_assigned_at.isoformat() if after_first_assigned_at else None,
        },
    )


def _status_change_error_for_lead(
    *,
    lead: Lead,
    to_status: LeadStatus,
) -> str | None:
    if to_status.id == lead.status_id:
        return "У лида уже этот статус"
    return None


def _status_change_error_as_validation_error(error: str) -> serializers.ValidationError:
    if error == "У лида уже этот статус":
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
        raise serializers.ValidationError({"idempotency_key": "Idempotency-Key должен быть длиной не более 128 символов"})
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
                {"idempotency_key": "Этот Idempotency-Key уже использован с другим payload"}
            )
        if record.response_status == 0:
            raise serializers.ValidationError({"idempotency_key": "Запрос с этим Idempotency-Key уже выполняется"})
        return record, Response(record.response_body, status=record.response_status)
    return record, None


def _save_idempotency_response(record, *, response_status: int, response_body):
    if not record:
        return
    record.response_status = response_status
    record.response_body = response_body
    record.save(update_fields=["response_status", "response_body", "updated_at"])


def _locked_leads_queryset():
    # PostgreSQL rejects SELECT ... FOR UPDATE when the query joins nullable FK targets.
    # Lock the Lead rows first and let related objects load separately when needed.
    return Lead.objects.select_for_update()


def _assert_status_not_used(status_obj: LeadStatus, *, action: str):
    if Lead.all_objects.filter(status_id=status_obj.id).exists():
        raise serializers.ValidationError({"status": f"Нельзя {action} статус, который уже назначен лидам"})


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


class LeadTagViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = LeadTag.objects.all().order_by("name", "id")
    serializer_class = LeadTagSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "create": (Perm.LEADS_WRITE,),
        "update": (Perm.LEADS_WRITE,),
        "partial_update": (Perm.LEADS_WRITE,),
        "soft_delete": (Perm.LEADS_WRITE,),
        "restore": (Perm.LEADS_WRITE,),
        "destroy": (Perm.LEADS_HARD_DELETE,),
    }
    filter_backends = [DjangoFilterBackend, drf_filters.SearchFilter, drf_filters.OrderingFilter]
    filterset_fields = ["is_deleted"]
    search_fields = ["name"]
    ordering = ["name", "id"]
    ordering_fields = [
        "id",
        "name",
        "created_at",
        "updated_at",
    ]

    def _assert_write_allowed(self, *, hard_delete: bool = False):
        role = getattr(self.request.user, "role", None)
        if hard_delete:
            if role != UserRole.SUPERUSER:
                raise PermissionDenied("Только суперпользователь может удалять теги навсегда")
            return
        if role not in {UserRole.TEAMLEADER, UserRole.ADMIN, UserRole.SUPERUSER}:
            raise PermissionDenied("Только тимлиды, админы и суперпользователи могут управлять тегами")

    def perform_create(self, serializer):
        self._assert_write_allowed()
        tag = serializer.save()
        _log_status_audit(
            event_type=LeadAuditEvent.TAG_CREATED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_TAG,
            entity_id=str(tag.id),
            payload_after=_tag_payload(tag),
        )

    def perform_update(self, serializer):
        self._assert_write_allowed()
        before = _tag_payload(serializer.instance)
        tag = serializer.save()
        _log_status_audit(
            event_type=LeadAuditEvent.TAG_UPDATED,
            actor_user=self.request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_TAG,
            entity_id=str(tag.id),
            payload_before=before,
            payload_after=_tag_payload(tag),
        )

    def destroy(self, request, *args, **kwargs):
        self._assert_write_allowed(hard_delete=True)
        instance = get_object_or_404(LeadTag.all_objects.all(), id=kwargs.get("pk"))
        before = _tag_payload(instance)
        instance.hard_delete()
        _log_status_audit(
            event_type=LeadAuditEvent.TAG_HARD_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_TAG,
            entity_id=str(instance.id),
            payload_before=before,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        self._assert_write_allowed()
        instance = self.get_object()
        before = _tag_payload(instance)
        instance.delete()
        _log_status_audit(
            event_type=LeadAuditEvent.TAG_SOFT_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_TAG,
            entity_id=str(instance.id),
            payload_before=before,
            payload_after=_tag_payload(instance),
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        self._assert_write_allowed()
        instance = get_object_or_404(LeadTag.all_objects.all(), id=pk)
        before = _tag_payload(instance)
        instance.restore()
        _log_status_audit(
            event_type=LeadAuditEvent.TAG_RESTORED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_TAG,
            entity_id=str(instance.id),
            payload_before=before,
            payload_after=_tag_payload(instance),
        )
        return Response(LeadTagSerializer(instance).data, status=status.HTTP_200_OK)


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


class CharInFilter(django_filters.BaseInFilter, django_filters.CharFilter):
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
    source__in = CharInFilter(field_name="source", lookup_expr="in")
    status_code = django_filters.CharFilter(field_name="status__code", lookup_expr="iexact")
    status__in = IdInFilter(field_name="status_id", lookup_expr="in")
    tag = django_filters.NumberFilter(field_name="tags__id", distinct=True)
    tags__in = IdInFilter(field_name="tags__id", lookup_expr="in", distinct=True)
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
            "tag",
            "tags__in",
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

    def _metrics_drilldown_requested(self) -> bool:
        raw = (self.request.query_params.get("metrics_drilldown") or "").strip().lower()
        return raw in {"1", "true", "yes", "y", "on"}

    def get_queryset(self):
        queryset = super().get_queryset()
        role = getattr(self.request.user, "role", None)
        if self._metrics_drilldown_requested() and role == UserRole.TEAMLEADER:
            return queryset.filter(
                Q(lead__manager__role__in=[UserRole.MANAGER, UserRole.TEAMLEADER])
                | Q(author__role__in=[UserRole.MANAGER, UserRole.TEAMLEADER])
            )
        return queryset

    def _assert_write_allowed(self, instance: LeadComment):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        if instance.author_id != self.request.user.id:
            raise PermissionDenied("Вы можете изменять только свои комментарии")

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
        comment_id = comment.id
        publish_comment_added(comment_id=comment_id)

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


class LeadAttachmentFilter(django_filters.FilterSet):
    id__in = IdInFilter(field_name="id", lookup_expr="in")
    lead__in = IdInFilter(field_name="lead_id", lookup_expr="in")
    uploaded_by__in = NumberInFilter(field_name="uploaded_by_id", lookup_expr="in")
    kind__in = CharInFilter(field_name="kind", lookup_expr="in")

    class Meta:
        model = LeadAttachment
        fields = [
            "id",
            "id__in",
            "lead",
            "lead__in",
            "uploaded_by",
            "uploaded_by__in",
            "kind",
            "kind__in",
            "created_at",
        ]


class LeadAttachmentViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = LeadAttachment.objects.select_related("lead", "uploaded_by", "lead__manager").all().order_by("-created_at", "-id")
    permission_classes = [IsAuthenticated, RBACPermission]
    http_method_names = ["get", "post", "delete", "head", "options"]
    filter_backends = [DjangoFilterBackend, drf_filters.SearchFilter, drf_filters.OrderingFilter]
    filterset_class = LeadAttachmentFilter
    search_fields = [
        "original_name",
        "mime_type",
        "lead__full_name",
        "lead__phone",
        "lead__email",
    ]
    ordering = ["-created_at", "-id"]
    ordering_fields = [
        "id",
        "lead__id",
        "lead__full_name",
        "uploaded_by__username",
        "kind",
        "original_name",
        "size_bytes",
        "created_at",
        "updated_at",
    ]
    action_perms = {
        "list": (Perm.LEADS_READ,),
        "retrieve": (Perm.LEADS_READ,),
        "create": (Perm.LEADS_WRITE,),
        "soft_delete": (Perm.LEADS_WRITE,),
        "restore": (Perm.LEADS_WRITE,),
        "destroy": (Perm.LEADS_HARD_DELETE,),
    }
    parser_classes = [MultiPartParser, FormParser]

    def get_queryset(self):
        queryset = super().get_queryset()
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET}:
            return queryset.filter(lead__manager_id=self.request.user.id)
        if role == UserRole.TEAMLEADER:
            return queryset.filter(
                Q(lead__manager_id=self.request.user.id)
                | Q(lead__manager__role=UserRole.MANAGER)
                | Q(lead__manager__role=UserRole.TEAMLEADER)
                | Q(lead__manager__isnull=True)
            )
        return queryset

    def get_serializer_class(self):
        if self.action == "create":
            return LeadAttachmentWriteSerializer
        return LeadAttachmentSerializer

    def _assert_write_allowed(self, lead: Lead):
        if not _user_can_edit_lead(actor_user=self.request.user, lead=lead):
            raise PermissionDenied("У вас нет прав на изменение вложений этого лида")

    def _extract_uploaded_files(self, request):
        files = request.FILES.getlist("files")
        if not files:
            single = request.FILES.get("file")
            if single is not None:
                files = [single]
        if not files:
            raise serializers.ValidationError({"files": "Передайте хотя бы один файл"})
        return files

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        lead = serializer.validated_data.get("lead")
        if lead is None:
            raise serializers.ValidationError({"lead": "Это поле обязательно"})
        self._assert_write_allowed(lead)
        files = self._extract_uploaded_files(request)
        reason = serializer.validated_data.get("reason", "")
        requested_kind = serializer.validated_data.get("kind")
        items = [
            _create_lead_attachment(
                actor_user=request.user,
                lead=lead,
                uploaded_file=uploaded_file,
                requested_kind=requested_kind,
                reason=reason,
            )
            for uploaded_file in files
        ]
        return Response(
            {
                "count": len(items),
                "items": LeadAttachmentSerializer(items, many=True, context=self.get_serializer_context()).data,
            },
            status=status.HTTP_201_CREATED,
        )

    @action(detail=True, methods=["post"])
    def soft_delete(self, request, pk=None):
        attachment = self.get_object()
        self._assert_write_allowed(attachment.lead)
        before = _attachment_payload(attachment)
        attachment.delete()
        _log_status_audit(
            event_type=LeadAuditEvent.ATTACHMENT_SOFT_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_ATTACHMENT,
            entity_id=str(attachment.id),
            lead=attachment.lead,
            payload_before=before,
            payload_after=_attachment_payload(attachment),
        )
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def restore(self, request, pk=None):
        attachment = get_object_or_404(
            LeadAttachment.all_objects.select_related("lead", "uploaded_by", "lead__manager"),
            id=pk,
        )
        self._assert_write_allowed(attachment.lead)
        before = _attachment_payload(attachment)
        attachment.restore()
        _log_status_audit(
            event_type=LeadAuditEvent.ATTACHMENT_RESTORED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_ATTACHMENT,
            entity_id=str(attachment.id),
            lead=attachment.lead,
            payload_before=before,
            payload_after=_attachment_payload(attachment),
        )
        return Response(LeadAttachmentSerializer(attachment, context=self.get_serializer_context()).data, status=status.HTTP_200_OK)

    def destroy(self, request, *args, **kwargs):
        attachment = self.get_object()
        self._assert_write_allowed(attachment.lead)
        before = _attachment_payload(attachment)
        attachment.hard_delete()
        _log_status_audit(
            event_type=LeadAuditEvent.ATTACHMENT_HARD_DELETED,
            actor_user=request.user,
            source=LeadAuditSource.API,
            entity_type=LeadAuditEntity.LEAD_ATTACHMENT,
            entity_id=str(attachment.id),
            lead=attachment.lead,
            payload_before=before,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)


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
        "stats_non_ftd_matrix": (Perm.LEADS_READ,),
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
        if self.action in {"list", "stats_monthly", "stats_ftd_matrix", "stats_non_ftd_matrix"} and role == UserRole.RET:
            return queryset.filter(creator_id=self.request.user.id)
        include_teamleader_self = self.action in {
            "list",
            "retrieve",
            "stats_monthly",
            "stats_ftd_matrix",
            "stats_non_ftd_matrix",
        }
        return _filter_deposits_visible_for_user(
            actor_user=self.request.user,
            queryset=queryset,
            include_teamleader_self=include_teamleader_self,
        )

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
            months.reverse()
            return months

        normalized = []
        seen: set[date] = set()
        for value in month_values:
            month_key = value.date().replace(day=1)
            if month_key in seen:
                continue
            seen.add(month_key)
            normalized.append(month_key)
        normalized.sort(reverse=True)
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

    def _assert_metrics_access_allowed(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET}:
            raise PermissionDenied("У вас нет доступа к метрикам")

    @action(detail=False, methods=["get"], url_path="stats/monthly")
    def stats_monthly(self, request):
        self._assert_metrics_access_allowed()
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
        self._assert_metrics_access_allowed()
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

    @action(detail=False, methods=["get"], url_path="stats/non-ftd-matrix")
    def stats_non_ftd_matrix(self, request):
        self._assert_metrics_access_allowed()
        queryset, params = self._get_stats_queryset(request.query_params)
        rows = list(
            queryset.filter(type__in=[LeadDeposit.Type.RELOAD, LeadDeposit.Type.DEPOSIT])
            .annotate(month=TruncMonth("created_at"))
            .values(
                "creator_id",
                "creator__username",
                "creator__first_name",
                "creator__last_name",
                "creator__role",
                "month",
            )
            .annotate(
                reload_count=Count("id", filter=Q(type=LeadDeposit.Type.RELOAD)),
                deposit_count=Count("id", filter=Q(type=LeadDeposit.Type.DEPOSIT)),
                total_amount=Sum("amount"),
            )
            .order_by("creator__username", "creator_id", "month")
        )

        month_keys = self._month_range(
            params.get("date_from"),
            params.get("date_to"),
            [row["month"] for row in rows if row["month"] is not None],
        )
        columns = [self._month_payload(month_start) for month_start in month_keys]
        default_cells = {
            column["month_key"]: {"reload_count": 0, "deposit_count": 0, "total_amount": self._amount_string(0)}
            for column in columns
        }

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
                    "total": {
                        "reload_count": 0,
                        "deposit_count": 0,
                        "total_amount": self._amount_string(0),
                    },
                    "cells": {month_key: cell.copy() for month_key, cell in default_cells.items()},
                    "_total_amount_value": 0,
                }
            month_key = row["month"].date().replace(day=1).strftime("%Y-%m")
            reload_count = int(row["reload_count"] or 0)
            deposit_count = int(row["deposit_count"] or 0)
            total_amount = row["total_amount"] or 0
            row_map[creator_id]["cells"][month_key] = {
                "reload_count": reload_count,
                "deposit_count": deposit_count,
                "total_amount": self._amount_string(total_amount),
            }
            row_map[creator_id]["total"]["reload_count"] += reload_count
            row_map[creator_id]["total"]["deposit_count"] += deposit_count
            row_map[creator_id]["_total_amount_value"] += total_amount
            row_map[creator_id]["total"]["total_amount"] = self._amount_string(row_map[creator_id]["_total_amount_value"])

        matrix_rows = sorted(
            row_map.values(),
            key=lambda item: (
                -item["_total_amount_value"],
                -(item["total"]["reload_count"] + item["total"]["deposit_count"]),
                item["user"]["username"] or "",
                item["user"]["id"],
            ),
        )
        for row in matrix_rows:
            row.pop("_total_amount_value", None)

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
            raise PermissionDenied("Нельзя изменить лид у существующего депозита")
        if role in {UserRole.MANAGER, UserRole.RET, UserRole.TEAMLEADER} and "type" in self.request.data:
            raise PermissionDenied("Только админы и суперпользователи могут менять тип депозита")

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        lead = serializer.validated_data.get("lead")
        if lead is None:
            raise serializers.ValidationError({"lead": "Это поле обязательно"})
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
            raise serializers.ValidationError("Нет изменений для сохранения")

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
            raise PermissionDenied("У вас нет прав на восстановление этого депозита")

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
        "status",
    ).prefetch_related("tags").all().order_by("-received_at")
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
        "deposits": (Perm.LEADS_WRITE,),
        "attachments": (Perm.LEADS_WRITE,),
        "set_tags": (Perm.LEADS_WRITE,),
        "bulk_add_tags": (Perm.LEADS_WRITE,),
        "bulk_remove_tags": (Perm.LEADS_WRITE,),
        "bulk_clear_tags": (Perm.LEADS_WRITE,),
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
        "source",
    ]

    def get_required_perms(self) -> tuple[str, ...]:
        if self.action in {"deposits", "attachments"}:
            if self.request.method.upper() == "GET":
                return (Perm.LEADS_READ,)
            return (Perm.LEADS_WRITE,)
        return super().get_required_perms()

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
            before_name = before_manager.get("username") or "Не назначен"
            after_name = after_manager.get("username") or "Не назначен"
            return f"{before_name} -> {after_name}"

        if audit.event_type == LeadAuditEvent.FIRST_MANAGER_CHANGED:
            before_manager = (
                payload_before.get("first_manager") if isinstance(payload_before.get("first_manager"), dict) else {}
            )
            after_manager = (
                payload_after.get("first_manager") if isinstance(payload_after.get("first_manager"), dict) else {}
            )
            before_name = before_manager.get("username") or "Не назначен"
            after_name = after_manager.get("username") or "Не назначен"
            return f"{before_name} -> {after_name}"

        if audit.event_type in {
            LeadAuditEvent.DEPOSIT_CREATED,
            LeadAuditEvent.DEPOSIT_UPDATED,
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
            LeadAuditEvent.ATTACHMENT_CREATED,
            LeadAuditEvent.ATTACHMENT_SOFT_DELETED,
            LeadAuditEvent.ATTACHMENT_RESTORED,
            LeadAuditEvent.ATTACHMENT_HARD_DELETED,
        }:
            attachment_payload = payload_after or payload_before
            original_name = attachment_payload.get("original_name")
            if isinstance(original_name, str) and original_name:
                return original_name

        if audit.event_type in {
            LeadAuditEvent.COMMENT_CREATED,
            LeadAuditEvent.COMMENT_UPDATED,
            LeadAuditEvent.COMMENT_PINNED,
            LeadAuditEvent.COMMENT_UNPINNED,
        }:
            body = payload_after.get("body") if isinstance(payload_after, dict) else None
            if isinstance(body, str) and body:
                return body[:160]

        if audit.event_type in {
            LeadAuditEvent.TAG_CREATED,
            LeadAuditEvent.TAG_UPDATED,
            LeadAuditEvent.TAG_SOFT_DELETED,
            LeadAuditEvent.TAG_RESTORED,
            LeadAuditEvent.TAG_HARD_DELETED,
        }:
            tag_payload = payload_after or payload_before
            tag_name = tag_payload.get("name")
            if isinstance(tag_name, str) and tag_name:
                return tag_name

        if audit.event_type == LeadAuditEvent.LEAD_TAGS_CHANGED:
            before_tags = payload_before.get("tags") if isinstance(payload_before.get("tags"), list) else []
            after_tags = payload_after.get("tags") if isinstance(payload_after.get("tags"), list) else []
            before_names = ", ".join(tag.get("name") for tag in before_tags if isinstance(tag, dict) and tag.get("name")) or "Нет тегов"
            after_names = ", ".join(tag.get("name") for tag in after_tags if isinstance(tag, dict) and tag.get("name")) or "Нет тегов"
            return f"{before_names} -> {after_names}"

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
                    {"events": f"Неизвестные типы событий: {', '.join(unknown_events)}"}
                )
            queryset = queryset.filter(event_type__in=requested_events)

        page = self.paginate_queryset(queryset)
        if page is not None:
            return self.get_paginated_response([self._timeline_item(item) for item in page])
        return Response([self._timeline_item(item) for item in queryset], status=status.HTTP_200_OK)

    def _assert_can_create(self):
        role = getattr(self.request.user, "role", None)
        if role not in {UserRole.SUPERUSER, UserRole.ADMIN}:
            raise PermissionDenied("Только админы и суперпользователи могут создавать лидов")

    def _assert_can_edit(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.MANAGER, UserRole.RET} and lead.manager_id != self.request.user.id:
            raise PermissionDenied("Вы можете редактировать только своих лидов")
        if role == UserRole.TEAMLEADER:
            if lead.manager_id == self.request.user.id:
                return
            lead_manager_role = getattr(getattr(lead, "manager", None), "role", None)
            if lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Тимлиды могут редактировать только свои лиды и лиды менеджеров/тимлидов")

    def _assert_can_manage_assignment(self, lead: Lead, *, operation: str):
        role = getattr(self.request.user, "role", None)
        if role != UserRole.TEAMLEADER:
            return

        lead_manager = getattr(lead, "manager", None)
        lead_manager_role = getattr(lead_manager, "role", None)

        if operation == "assign":
            if lead.manager_id is None or lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Тимлиды могут назначать только неназначенные лиды или лиды менеджеров/тимлидов")

        if operation == "unassign":
            if lead_manager_role in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                return
            raise PermissionDenied("Тимлиды могут снимать назначение только с лидов менеджеров/тимлидов")

        if lead.manager_id is None:
            return
        raise PermissionDenied("Неподдерживаемая операция назначения")

    def _assert_can_change_first_manager(self):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN, UserRole.TEAMLEADER}:
            return
        raise PermissionDenied("Только тимлиды, админы и суперпользователи могут менять first_manager")

    def _assert_can_change_first_manager_for_lead(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER and self._teamleader_can_manage_manager_scope(lead):
            return
        raise PermissionDenied("Тимлиды могут менять first_manager только у лидов менеджеров/тимлидов")

    def _assert_assign_first_manager_override_allowed(self, *, first_assigned_at_provided: bool):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER:
            if first_assigned_at_provided:
                raise PermissionDenied("Тимлиды не могут вручную задавать first_assigned_at при назначении")
            return
        raise PermissionDenied("Только тимлиды, админы и суперпользователи могут переопределять first_manager при назначении")

    def _resolve_assign_first_assigned_at(self, *, requested_first_assigned_at):
        role = getattr(self.request.user, "role", None)
        if requested_first_assigned_at is not None and role in {UserRole.ADMIN, UserRole.SUPERUSER}:
            return requested_first_assigned_at
        return timezone.now()

    def _assert_update_payload_allowed(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        payload_fields = set(self.request.data.keys())

        if role != UserRole.SUPERUSER:
            forbidden_sensitive = sorted(payload_fields & self.superuser_only_update_fields)
            if forbidden_sensitive:
                raise PermissionDenied(
                    f"Только суперпользователь может менять чувствительные поля: {', '.join(forbidden_sensitive)}"
                )

        if role in {UserRole.MANAGER, UserRole.RET}:
            forbidden = sorted(payload_fields & self.manager_ret_protected_fields)
            if forbidden:
                raise PermissionDenied(
                    f"Менеджеры и RET не могут менять защищенные поля: {', '.join(forbidden)}"
                )

        if payload_fields & {"first_manager", "first_assigned_at"}:
            self._assert_can_change_first_manager()
            self._assert_can_change_first_manager_for_lead(lead)

        if "next_contact_at" in payload_fields:
            if role in {UserRole.MANAGER, UserRole.RET} and lead.manager_id != self.request.user.id:
                raise PermissionDenied("Вы можете ставить next_contact_at только своим лидам")
            if role == UserRole.TEAMLEADER:
                if lead.manager_id == self.request.user.id:
                    return
                manager = getattr(lead, "manager", None)
                manager_role = getattr(manager, "role", None)
                if manager is None or manager_role not in {UserRole.MANAGER, UserRole.TEAMLEADER}:
                    raise PermissionDenied(
                        "Тимлиды могут задавать next_contact_at только у своих лидов и лидов менеджеров/тимлидов"
                    )

    def _assert_create_payload_allowed(self):
        role = getattr(self.request.user, "role", None)
        payload_fields = set(self.request.data.keys())
        if role != UserRole.SUPERUSER and "geo" in payload_fields:
            raise PermissionDenied("Только суперпользователь может задавать geo при создании")
        if role != UserRole.SUPERUSER and "custom_fields" in payload_fields:
            raise PermissionDenied("Только суперпользователь может задавать custom_fields при создании")

    def _assert_bulk_status_change_allowed(self, leads: list[Lead]):
        role = getattr(self.request.user, "role", None)
        if role not in {UserRole.MANAGER, UserRole.RET, UserRole.TEAMLEADER}:
            return
        if role in {UserRole.MANAGER, UserRole.RET}:
            foreign_ids = [str(lead.id) for lead in leads if lead.manager_id != self.request.user.id]
            if foreign_ids:
                raise PermissionDenied("Вы можете менять статус только у своих лидов")
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
            raise PermissionDenied("Тимлиды могут менять статус только у своих лидов и лидов менеджеров/тимлидов")

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
            "Только тимлиды (по своим/менеджерским/тимлидерским лидам), админы и суперпользователи могут использовать принудительную смену статуса"
        )

    def _assert_can_set_tags(self, lead: Lead):
        role = getattr(self.request.user, "role", None)
        if role in {UserRole.SUPERUSER, UserRole.ADMIN}:
            return
        if role == UserRole.TEAMLEADER and self._teamleader_can_manage_manager_scope(lead):
            return
        raise PermissionDenied("Только тимлиды, админы и суперпользователи могут менять теги лида")

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
        if lead.next_contact_at is not None:
            publish_next_contact_planned_resync(lead_id=lead.id, remind_before_minutes=15)
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
        before_next_contact_at = lead.next_contact_at
        serializer = self.get_serializer(lead, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        lead = serializer.save()
        post_save_updates: list[str] = []
        _set_first_manager_if_needed(lead, update_fields=post_save_updates)
        if post_save_updates:
            lead.save(update_fields=sorted(set(post_save_updates + ["updated_at"])))
        if before_next_contact_at != lead.next_contact_at:
            publish_next_contact_planned_resync(lead_id=lead.id, remind_before_minutes=15)
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

    @action(
        detail=True,
        methods=["get", "post"],
        url_path="attachments",
        parser_classes=[MultiPartParser, FormParser],
    )
    def attachments(self, request, pk=None):
        lead = self.get_object()
        if request.method.upper() == "GET":
            items = LeadAttachment.objects.filter(lead=lead).select_related("uploaded_by").order_by("-created_at", "-id")
            return Response(
                LeadAttachmentSerializer(items, many=True, context=self.get_serializer_context()).data,
                status=status.HTTP_200_OK,
            )

        self._assert_can_edit(lead)
        serializer = LeadAttachmentWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        files = request.FILES.getlist("files")
        if not files:
            single = request.FILES.get("file")
            if single is not None:
                files = [single]
        if not files:
            raise serializers.ValidationError({"files": "Передайте хотя бы один файл"})

        reason = serializer.validated_data.get("reason", "")
        requested_kind = serializer.validated_data.get("kind")
        items = [
            _create_lead_attachment(
                actor_user=request.user,
                lead=lead,
                uploaded_file=uploaded_file,
                requested_kind=requested_kind,
                reason=reason,
            )
            for uploaded_file in files
        ]
        return Response(
            {
                "count": len(items),
                "items": LeadAttachmentSerializer(items, many=True, context=self.get_serializer_context()).data,
            },
            status=status.HTTP_201_CREATED,
        )

    @action(detail=True, methods=["post"], url_path="set-tags")
    def set_tags(self, request, pk=None):
        serializer = LeadSetTagsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        reason = serializer.validated_data.get("reason", "")
        tags = serializer.validated_data["tag_ids"]

        with transaction.atomic():
            lead = (
                _locked_leads_queryset()
                .prefetch_related("tags")
                .get(id=pk)
            )
            self._assert_can_set_tags(lead)
            before_tags = _lead_tags_payload(lead)
            before_tag_ids = {tag["id"] for tag in before_tags}
            lead.tags.set(tags)
            lead.refresh_from_db()
            lead = (
                Lead.objects.select_related("partner", "manager", "first_manager", "status")
                .prefetch_related("tags")
                .get(id=lead.id)
            )
            after_tags = _lead_tags_payload(lead)
            after_tag_ids = {tag["id"] for tag in after_tags}
            if before_tag_ids != after_tag_ids:
                _log_status_audit(
                    event_type=LeadAuditEvent.LEAD_TAGS_CHANGED,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    entity_type=LeadAuditEntity.LEAD,
                    entity_id=str(lead.id),
                    lead=lead,
                    reason=reason,
                    payload_before={"lead_id": str(lead.id), "tags": before_tags},
                    payload_after={"lead_id": str(lead.id), "tags": after_tags},
                )

        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    def _bulk_update_tags(self, request, *, lead_ids: list[int], tags: list[LeadTag], reason: str, allow_partial: bool, mode: str):
        batch_id = uuid.uuid4().hex
        changed_ids: list[str] = []
        unchanged_ids: list[str] = []
        failed: dict[str, str] = {}

        with transaction.atomic():
            locked_leads = list(
                _locked_leads_queryset()
                .prefetch_related("tags")
                .filter(id__in=lead_ids)
            )
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Неизвестные ID лидов: {', '.join(missing)}"})
                failed.update({lead_id: "Неизвестный ID лида" for lead_id in missing})

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                self._assert_can_set_tags(lead)
                before_tags = _lead_tags_payload(lead)
                before_tag_ids = {tag["id"] for tag in before_tags}
                if mode == "add":
                    lead.tags.add(*tags)
                elif mode == "remove":
                    lead.tags.remove(*tags)
                elif mode == "clear":
                    lead.tags.clear()
                else:
                    raise ValueError(f"Неподдерживаемый режим bulk-тегов: {mode}")
                after_tags = _lead_tags_payload(lead)
                after_tag_ids = {tag["id"] for tag in after_tags}
                if before_tag_ids == after_tag_ids:
                    unchanged_ids.append(str(lead.id))
                    continue
                _log_status_audit(
                    event_type=LeadAuditEvent.LEAD_TAGS_CHANGED,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    entity_type=LeadAuditEntity.LEAD,
                    entity_id=str(lead.id),
                    batch_id=batch_id,
                    lead=lead,
                    reason=reason,
                    payload_before={"lead_id": str(lead.id), "tags": before_tags},
                    payload_after={"lead_id": str(lead.id), "tags": after_tags},
                )
                changed_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=[*changed_ids, *unchanged_ids])
                .select_related("partner", "manager", "first_manager", "status")
                .prefetch_related("tags")
                .order_by("-received_at", "-id")
            )

        return Response(
            {
                "batch_id": batch_id,
                "processed_count": len(refreshed_leads),
                "changed_count": len(changed_ids),
                "changed_ids": changed_ids,
                "unchanged_count": len(unchanged_ids),
                "unchanged_ids": unchanged_ids,
                "failed_count": len(failed),
                "failed": failed,
                "results": LeadSerializer(refreshed_leads, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    @action(detail=False, methods=["post"], url_path="bulk-add-tags")
    def bulk_add_tags(self, request):
        serializer = BulkLeadAddTagsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return self._bulk_update_tags(
            request,
            lead_ids=serializer.validated_data["_lead_ids"],
            tags=serializer.validated_data["tag_ids"],
            reason=serializer.validated_data.get("reason", ""),
            allow_partial=serializer.validated_data.get("allow_partial", False),
            mode="add",
        )

    @action(detail=False, methods=["post"], url_path="bulk-remove-tags")
    def bulk_remove_tags(self, request):
        serializer = BulkLeadRemoveTagsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return self._bulk_update_tags(
            request,
            lead_ids=serializer.validated_data["_lead_ids"],
            tags=serializer.validated_data["tag_ids"],
            reason=serializer.validated_data.get("reason", ""),
            allow_partial=serializer.validated_data.get("allow_partial", False),
            mode="remove",
        )

    @action(detail=False, methods=["post"], url_path="bulk-clear-tags")
    def bulk_clear_tags(self, request):
        serializer = BulkLeadClearTagsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return self._bulk_update_tags(
            request,
            lead_ids=serializer.validated_data["_lead_ids"],
            tags=[],
            reason=serializer.validated_data.get("reason", ""),
            allow_partial=serializer.validated_data.get("allow_partial", False),
            mode="clear",
        )

    @action(detail=False, methods=["get"], url_path="metrics")
    def metrics(self, request):
        requester_role = self._assert_lead_funnel_metrics_access_allowed(request)
        query = LeadFunnelMetricsQuerySerializer(data=request.query_params)
        query.is_valid(raise_exception=True)

        date_to = query.validated_data.get("date_to") or timezone.localdate()
        date_from = query.validated_data.get("date_from") or (date_to - timedelta(days=30))
        if date_from > date_to:
            raise serializers.ValidationError({"date_from": "date_from должен быть меньше или равен date_to"})

        partner = query.validated_data.get("partner")
        group_by = query.validated_data.get("group_by")
        metrics_scope = self._resolve_metrics_scope(
            requester_role=requester_role,
            request_user=request.user,
            date_from=date_from,
            date_to=date_to,
            partner=partner,
        )
        period_start = metrics_scope["period_start"]
        period_end = metrics_scope["period_end"]
        manager_scope = metrics_scope["manager_scope"]
        leads_received_qs = metrics_scope["leads_received_qs"]

        valid_status_filter = Q(status__is_valid=True)
        won_status_filter = Q(status__conversion_bucket=LeadStatus.ConversionBucket.WON)
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
            won_status_count = partner_leads_received_qs.filter(won_status_filter).count()
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
            won_rate = _rate(won_status_count, total)
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
                    "won_total": won_status_count,
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

    def _assert_lead_funnel_metrics_access_allowed(self, request):
        requester_role = getattr(request.user, "role", None)
        if requester_role in {UserRole.MANAGER, UserRole.RET}:
            raise PermissionDenied("У вас нет доступа к метрикам")
        if "manager" in request.query_params:
            raise serializers.ValidationError({"manager": "Фильтр manager не поддерживается в этом эндпоинте"})
        return requester_role

    def _resolve_metrics_scope(self, *, requester_role, request_user, date_from, date_to, partner):
        tz = timezone.get_current_timezone()
        period_start = timezone.make_aware(datetime.combine(date_from, time.min), tz)
        period_end = timezone.make_aware(datetime.combine(date_to + timedelta(days=1), time.min), tz)
        manager_scope = request_user if requester_role in {UserRole.MANAGER, UserRole.RET} else None

        leads_received_qs = Lead.objects.filter(received_at__gte=period_start, received_at__lt=period_end)
        if partner:
            leads_received_qs = leads_received_qs.filter(partner=partner)
        if manager_scope:
            leads_received_qs = leads_received_qs.filter(first_manager=manager_scope)

        return {
            "period_start": period_start,
            "period_end": period_end,
            "manager_scope": manager_scope,
            "leads_received_qs": leads_received_qs,
        }

    @action(detail=False, methods=["get"], url_path="metrics-drilldown")
    def metrics_drilldown(self, request):
        requester_role = self._assert_lead_funnel_metrics_access_allowed(request)
        query = LeadMetricsDrilldownQuerySerializer(data=request.query_params)
        query.is_valid(raise_exception=True)

        date_to = query.validated_data.get("date_to") or timezone.localdate()
        date_from = query.validated_data.get("date_from") or (date_to - timedelta(days=30))
        if date_from > date_to:
            raise serializers.ValidationError({"date_from": "date_from должен быть меньше или равен date_to"})

        partner = query.validated_data.get("partner")
        status_obj = query.validated_data["status"]
        metrics_scope = self._resolve_metrics_scope(
            requester_role=requester_role,
            request_user=request.user,
            date_from=date_from,
            date_to=date_to,
            partner=partner,
        )

        queryset = (
            metrics_scope["leads_received_qs"]
            .filter(status=status_obj)
            .select_related("partner", "manager", "status")
            .prefetch_related("tags")
            .order_by("-received_at", "-id")
        )

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = LeadMetricsDrilldownSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = LeadMetricsDrilldownSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

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
        previous_first_manager = getattr(lead, "first_manager", None)
        previous_first_assigned_at = lead.first_assigned_at
        lead.first_manager = manager
        update_fields = ["first_manager", "updated_at"]
        if first_assigned_at is not None:
            lead.first_assigned_at = first_assigned_at
            update_fields.append("first_assigned_at")
        elif lead.first_assigned_at is None:
            lead.first_assigned_at = timezone.now()
            update_fields.append("first_assigned_at")
        lead.save(update_fields=update_fields)
        _log_first_manager_audit(
            lead=lead,
            actor_user=request.user,
            source=LeadAuditSource.API,
            reason=reason,
            from_manager=previous_first_manager,
            to_manager=getattr(lead, "first_manager", None),
            before_first_assigned_at=previous_first_assigned_at,
            after_first_assigned_at=lead.first_assigned_at,
        )
        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="assign-manager")
    def assign_manager(self, request, pk=None):
        payload_hash = _request_hash(
            {
                "lead_id": str(pk),
                "manager": str(request.data.get("manager") or ""),
                "set_as_first_manager": bool(request.data.get("set_as_first_manager", False)),
                "first_assigned_at": request.data.get("first_assigned_at"),
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
            set_as_first_manager = serializer.validated_data.get("set_as_first_manager", False)
            requested_first_assigned_at = serializer.validated_data.get("first_assigned_at")
            reason = serializer.validated_data.get("reason", "")

            lead = (
                _locked_leads_queryset()
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="assign")
            if set_as_first_manager:
                self._assert_assign_first_manager_override_allowed(
                    first_assigned_at_provided=requested_first_assigned_at is not None
                )
            previous_manager = lead.manager
            previous_first_manager = getattr(lead, "first_manager", None)
            previous_first_assigned_at = lead.first_assigned_at
            update_fields = ["manager", "updated_at"]
            if getattr(previous_manager, "id", None) != manager.id:
                lead.assigned_at = timezone.now()
                update_fields.append("assigned_at")
            lead.manager = manager
            if set_as_first_manager:
                lead.first_manager = manager
                update_fields.append("first_manager")
                lead.first_assigned_at = self._resolve_assign_first_assigned_at(
                    requested_first_assigned_at=requested_first_assigned_at
                )
                update_fields.append("first_assigned_at")
            else:
                _set_first_manager_if_needed(lead, update_fields=update_fields)
            lead.save(update_fields=update_fields)
            manager_audit = _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadAuditSource.API,
                reason=reason,
                from_manager=previous_manager,
                to_manager=manager,
            )
            if getattr(previous_manager, "id", None) != manager.id:
                publish_lead_assigned(
                    lead_id=lead.id,
                    to_manager_id=manager.id,
                    actor_user_id=request.user.id,
                    from_manager_id=getattr(previous_manager, "id", None),
                    audit_log_id=getattr(manager_audit, "id", None),
                    suppress_actor_watcher=True,
                )
                if getattr(previous_manager, "id", None):
                    publish_lead_unassigned(
                        lead_id=lead.id,
                        from_manager_id=previous_manager.id,
                        actor_user_id=request.user.id,
                        audit_log_id=getattr(manager_audit, "id", None),
                        suppress_actor_watcher=True,
                    )
            if set_as_first_manager:
                _log_first_manager_audit(
                    lead=lead,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    reason=reason,
                    from_manager=previous_first_manager,
                    to_manager=getattr(lead, "first_manager", None),
                    before_first_assigned_at=previous_first_assigned_at,
                    after_first_assigned_at=lead.first_assigned_at,
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
                _locked_leads_queryset()
                .get(id=pk)
            )
            self._assert_can_manage_assignment(lead, operation="unassign")
            previous_manager = lead.manager
            lead.manager = None
            lead.save(update_fields=["manager", "updated_at"])
            manager_audit = _log_manager_audit(
                lead=lead,
                actor_user=request.user,
                source=LeadAuditSource.API,
                reason=reason,
                from_manager=previous_manager,
                to_manager=None,
            )
            if getattr(previous_manager, "id", None):
                publish_lead_unassigned(
                    lead_id=lead.id,
                    from_manager_id=previous_manager.id,
                    actor_user_id=request.user.id,
                    audit_log_id=getattr(manager_audit, "id", None),
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
        set_as_first_manager = serializer.validated_data.get("set_as_first_manager", False)
        requested_first_assigned_at = serializer.validated_data.get("first_assigned_at")
        reason = serializer.validated_data.get("reason", "")
        allow_partial = serializer.validated_data.get("allow_partial", False)
        payload_hash = _request_hash(
            {
                "lead_ids": [str(lead_id) for lead_id in lead_ids],
                "manager": str(manager.id),
                "set_as_first_manager": set_as_first_manager,
                "first_assigned_at": requested_first_assigned_at.isoformat() if requested_first_assigned_at else None,
                "reason": reason,
                "allow_partial": allow_partial,
            }
        )
        batch_id = uuid.uuid4().hex

        updated_ids = []
        failed: dict[str, str] = {}
        assignment_notification_items: list[tuple[int, int, int, int | None, int | None]] = []
        response_payload = None
        with transaction.atomic():
            idempotency_record, cached_response = _acquire_idempotency_record(
                request=request,
                endpoint=LeadIdempotencyEndpoint.BULK_ASSIGN_MANAGER,
                payload_hash=payload_hash,
            )
            if cached_response is not None:
                return cached_response

            if set_as_first_manager:
                self._assert_assign_first_manager_override_allowed(
                    first_assigned_at_provided=requested_first_assigned_at is not None
                )

            locked_leads = list(
                _locked_leads_queryset()
                .filter(id__in=lead_ids)
            )
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Неизвестные ID лидов: {', '.join(missing)}"})
                failed.update({lead_id: "Неизвестный ID лида" for lead_id in missing})

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                self._assert_can_manage_assignment(lead, operation="assign")
                previous_manager = lead.manager
                previous_first_manager = getattr(lead, "first_manager", None)
                previous_first_assigned_at = lead.first_assigned_at
                update_fields = ["manager", "updated_at"]
                if getattr(previous_manager, "id", None) != manager.id:
                    lead.assigned_at = timezone.now()
                    update_fields.append("assigned_at")
                lead.manager = manager
                if set_as_first_manager:
                    lead.first_manager = manager
                    update_fields.append("first_manager")
                    lead.first_assigned_at = self._resolve_assign_first_assigned_at(
                        requested_first_assigned_at=requested_first_assigned_at
                    )
                    update_fields.append("first_assigned_at")
                else:
                    _set_first_manager_if_needed(lead, update_fields=update_fields)
                lead.save(update_fields=update_fields)
                manager_audit = _log_manager_audit(
                    lead=lead,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=manager,
                    batch_id=batch_id,
                )
                if getattr(previous_manager, "id", None) != manager.id:
                    assignment_notification_items.append(
                        (lead.id, manager.id, request.user.id, getattr(previous_manager, "id", None), getattr(manager_audit, "id", None))
                    )
                if set_as_first_manager:
                    _log_first_manager_audit(
                        lead=lead,
                        actor_user=request.user,
                        source=LeadAuditSource.API,
                        reason=reason,
                        from_manager=previous_first_manager,
                        to_manager=getattr(lead, "first_manager", None),
                        before_first_assigned_at=previous_first_assigned_at,
                        after_first_assigned_at=lead.first_assigned_at,
                        batch_id=batch_id,
                    )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "status")
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
            if assignment_notification_items:
                publish_bulk_lead_assigned(
                    lead_ids=[
                        lead_id
                        for lead_id, _to_manager_id, _actor_user_id, _from_manager_id, _manager_audit_id in assignment_notification_items
                    ],
                    to_manager_id=manager.id,
                    actor_user_id=request.user.id,
                    from_manager_ids=[
                        from_manager_id
                        for _lead_id, _to_manager_id, _actor_user_id, from_manager_id, _manager_audit_id in assignment_notification_items
                        if from_manager_id
                    ],
                    batch_id=batch_id,
                    suppress_actor_watcher=True,
                )
                publish_bulk_lead_unassigned(
                    lead_to_from_manager=[
                        (lead_id, from_manager_id)
                        for lead_id, _to_manager_id, _actor_user_id, from_manager_id, _manager_audit_id in assignment_notification_items
                        if from_manager_id
                    ],
                    actor_user_id=request.user.id,
                    batch_id=batch_id,
                    suppress_actor_watcher=True,
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
        unassign_notification_items: list[tuple[int, int, int, int | None]] = []
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
                _locked_leads_queryset()
                .filter(id__in=lead_ids)
            )
            leads_map = {lead.id: lead for lead in locked_leads}
            missing = [str(lead_id) for lead_id in lead_ids if lead_id not in leads_map]
            if missing:
                if not allow_partial:
                    raise serializers.ValidationError({"lead_ids": f"Неизвестные ID лидов: {', '.join(missing)}"})
                failed.update({lead_id: "Неизвестный ID лида" for lead_id in missing})

            for lead_id in lead_ids:
                lead = leads_map.get(lead_id)
                if lead is None:
                    continue
                self._assert_can_manage_assignment(lead, operation="unassign")
                previous_manager = lead.manager
                lead.manager = None
                lead.save(update_fields=["manager", "updated_at"])
                manager_audit = _log_manager_audit(
                    lead=lead,
                    actor_user=request.user,
                    source=LeadAuditSource.API,
                    reason=reason,
                    from_manager=previous_manager,
                    to_manager=None,
                    batch_id=batch_id,
                )
                if getattr(previous_manager, "id", None):
                    unassign_notification_items.append(
                        (lead.id, previous_manager.id, request.user.id, getattr(manager_audit, "id", None))
                    )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "status")
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
            if unassign_notification_items:
                publish_bulk_lead_unassigned(
                    lead_to_from_manager=[
                        (lead_id, from_manager_id)
                        for lead_id, from_manager_id, _actor_user_id, _manager_audit_id in unassign_notification_items
                    ],
                    actor_user_id=request.user.id,
                    batch_id=batch_id,
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
                _locked_leads_queryset()
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

            status_audit = _log_status_audit(
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
            publish_lead_status_changed(
                lead_id=lead.id,
                from_status_id=getattr(from_status, "id", None),
                to_status_id=to_status.id,
                actor_user_id=request.user.id,
                audit_log_id=getattr(status_audit, "id", None),
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
        status_notification_items: list[tuple[int, int | None, int, int, int | None]] = []
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
                _locked_leads_queryset()
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
                    raise serializers.ValidationError({"lead_ids": f"Неизвестные ID лидов: {', '.join(missing)}"})
                failed.update({lead_id: "Неизвестный ID лида" for lead_id in missing})

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

                status_audit = _log_status_audit(
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
                status_notification_items.append(
                    (lead.id, getattr(from_status, "id", None), to_status.id, request.user.id, getattr(status_audit, "id", None))
                )
                updated_ids.append(str(lead.id))

            refreshed_leads = list(
                Lead.objects.filter(id__in=updated_ids)
                .select_related("partner", "manager", "first_manager", "status")
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
            if status_notification_items:
                publish_bulk_lead_status_changed(
                    lead_status_items=[
                        (lead_id, from_status_id, to_status_id)
                        for lead_id, from_status_id, to_status_id, _actor_user_id, _status_audit_id in status_notification_items
                    ],
                    actor_user_id=request.user.id,
                    batch_id=batch_id,
                )

        return Response(response_payload, status=status.HTTP_200_OK)
