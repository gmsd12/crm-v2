from __future__ import annotations

import re
from decimal import Decimal

from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework import serializers

from apps.iam.models import UserRole
from apps.leads.models import (
    LeadAuditLog,
    Lead,
    LeadComment,
    LeadDeposit,
    LeadStatus,
    LeadStatusTransition,
    Pipeline,
)
from apps.partners.models import Partner

User = get_user_model()
GEO_CODE_RE = re.compile(r"^[A-Z]{2}$")


class PipelineSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pipeline
        fields = ["id", "code", "name", "is_default", "is_active", "created_at", "updated_at"]
        read_only_fields = ["id", "created_at", "updated_at"]


class LeadStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = LeadStatus
        fields = [
            "id",
            "pipeline",
            "code",
            "name",
            "order",
            "color",
            "is_default_for_new_leads",
            "is_active",
            "is_terminal",
            "is_valid",
            "conversion_bucket",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class LeadStatusTransitionSerializer(serializers.ModelSerializer):
    class Meta:
        model = LeadStatusTransition
        fields = [
            "id",
            "pipeline",
            "from_status",
            "to_status",
            "is_active",
            "requires_comment",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]

    def validate(self, attrs):
        pipeline = attrs.get("pipeline") or getattr(self.instance, "pipeline", None)
        from_status = attrs.get("from_status") or getattr(self.instance, "from_status", None)
        to_status = attrs.get("to_status") or getattr(self.instance, "to_status", None)

        if from_status and to_status and from_status.id == to_status.id:
            raise serializers.ValidationError({"to_status": "from_status and to_status must be different"})
        if pipeline and from_status and from_status.pipeline_id != pipeline.id:
            raise serializers.ValidationError({"from_status": "from_status must belong to selected pipeline"})
        if pipeline and to_status and to_status.pipeline_id != pipeline.id:
            raise serializers.ValidationError({"to_status": "to_status must belong to selected pipeline"})
        return attrs


class LeadStatusAuditLogSerializer(serializers.ModelSerializer):
    actor_username = serializers.CharField(source="actor_user.username", read_only=True)
    from_status_code = serializers.CharField(source="from_status.code", read_only=True)
    to_status_code = serializers.CharField(source="to_status.code", read_only=True)

    class Meta:
        model = LeadAuditLog
        fields = [
            "id",
            "lead",
            "entity_type",
            "entity_id",
            "event_type",
            "from_status",
            "from_status_code",
            "to_status",
            "to_status_code",
            "actor_user",
            "actor_username",
            "source",
            "reason",
            "batch_id",
            "payload_before",
            "payload_after",
            "created_at",
        ]
        read_only_fields = fields


class LeadSerializer(serializers.ModelSerializer):
    pipeline = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    partner = serializers.SerializerMethodField()
    manager = serializers.SerializerMethodField()
    first_manager = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "geo",
            "partner",
            "manager",
            "first_manager",
            "source",
            "pipeline",
            "status",
            "full_name",
            "phone",
            "email",
            "priority",
            "next_contact_at",
            "last_contacted_at",
            "assigned_at",
            "first_assigned_at",
            "manager_outcome",
            "manager_outcome_at",
            "manager_outcome_by",
            "transferred_to_ret_at",
            "custom_fields",
            "received_at",
        ]

    def get_pipeline(self, obj):
        if not obj.pipeline_id:
            return None
        return {"id": str(obj.pipeline_id), "code": obj.pipeline.code, "name": obj.pipeline.name}

    def get_status(self, obj):
        if not obj.status_id:
            return None
        return {"id": str(obj.status_id), "code": obj.status.code, "name": obj.status.name}

    def get_partner(self, obj):
        return {"id": str(obj.partner_id), "code": obj.partner.code, "name": obj.partner.name}

    def get_manager(self, obj):
        if not obj.manager_id:
            return None
        return {"id": str(obj.manager_id), "username": obj.manager.username}

    def get_first_manager(self, obj):
        if not obj.first_manager_id:
            return None
        return {"id": str(obj.first_manager_id), "username": obj.first_manager.username}

    def get_source(self, obj):
        if not obj.source_id:
            return None
        return {"id": str(obj.source_id), "code": obj.source.code, "name": obj.source.name}


class LeadWriteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Lead
        fields = [
            "id",
            "partner",
            "source",
            "geo",
            "full_name",
            "phone",
            "email",
            "priority",
            "next_contact_at",
            "last_contacted_at",
            "custom_fields",
        ]
        read_only_fields = ["id"]
        validators = []
        extra_kwargs = {
            "partner": {"required": True},
            "source": {"required": False, "allow_null": True},
            "geo": {"required": False, "allow_blank": True},
            "full_name": {"required": False, "allow_blank": True},
            "phone": {"required": False, "allow_blank": True, "validators": []},
            "email": {"required": False, "allow_blank": True},
        }

    def validate(self, attrs):
        instance = getattr(self, "instance", None)

        partner = attrs.get("partner") or getattr(instance, "partner", None)
        if not partner:
            raise serializers.ValidationError({"partner": "This field is required"})

        phone = (attrs.get("phone") if "phone" in attrs else getattr(instance, "phone", "")) or ""
        email = (attrs.get("email") if "email" in attrs else getattr(instance, "email", "")) or ""
        phone = phone.strip()
        email = email.strip().lower()
        if not phone:
            raise serializers.ValidationError({"phone": "phone is required"})

        attrs["phone"] = phone
        attrs["email"] = email
        geo = (attrs.get("geo") if "geo" in attrs else getattr(instance, "geo", "")) or ""
        geo = geo.strip().upper()
        if geo and not GEO_CODE_RE.fullmatch(geo):
            raise serializers.ValidationError({"geo": "geo must be a 2-letter uppercase country code"})
        attrs["geo"] = geo
        attrs["full_name"] = ((attrs.get("full_name") if "full_name" in attrs else getattr(instance, "full_name", "")) or "").strip()
        if "next_contact_at" in attrs and attrs["next_contact_at"] is not None:
            next_contact_at = attrs["next_contact_at"]
            if timezone.is_naive(next_contact_at):
                attrs["next_contact_at"] = timezone.make_aware(next_contact_at, timezone.get_current_timezone())

        duplicate_qs = Lead.objects.all()
        if instance:
            duplicate_qs = duplicate_qs.exclude(id=instance.id)
        if phone and duplicate_qs.filter(phone=phone).exists():
            raise serializers.ValidationError({"phone": "Duplicate phone"})

        return attrs


class LeadStatusChangeSerializer(serializers.Serializer):
    to_status = serializers.PrimaryKeyRelatedField(queryset=LeadStatus.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    force = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        lead = self.context["lead"]
        to_status = attrs["to_status"]
        reason = (attrs.get("reason") or "").strip()
        force = attrs.get("force", False)

        if to_status.id == lead.status_id:
            raise serializers.ValidationError({"to_status": "Lead already has this status"})
        if force:
            attrs["reason"] = reason
            attrs["_transition"] = None
            return attrs

        if not lead.pipeline_id or not lead.status_id:
            raise serializers.ValidationError("Lead has no current workflow status")
        if to_status.pipeline_id != lead.pipeline_id:
            raise serializers.ValidationError({"to_status": "to_status must belong to lead pipeline"})

        transition = (
            LeadStatusTransition.objects.filter(
                pipeline_id=lead.pipeline_id,
                from_status_id=lead.status_id,
                to_status_id=to_status.id,
                is_active=True,
            )
            .first()
        )
        if not transition:
            raise serializers.ValidationError({"to_status": "Transition is not allowed"})
        if transition.requires_comment and not reason:
            raise serializers.ValidationError({"reason": "Comment is required for this transition"})

        attrs["reason"] = reason
        attrs["_transition"] = transition
        return attrs


class BulkLeadStatusChangeSerializer(serializers.Serializer):
    lead_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    to_status = serializers.PrimaryKeyRelatedField(queryset=LeadStatus.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    allow_partial = serializers.BooleanField(required=False, default=False)
    force = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        lead_ids = attrs["lead_ids"]
        reason = (attrs.get("reason") or "").strip()

        unique_ids = list(dict.fromkeys(lead_ids))
        max_ids = int(getattr(settings, "LEADS_BULK_STATUS_CHANGE_MAX_IDS", 500))
        if len(unique_ids) > max_ids:
            raise serializers.ValidationError({"lead_ids": f"Maximum {max_ids} lead ids allowed per request"})

        attrs["reason"] = reason
        attrs["_lead_ids"] = unique_ids
        return attrs


class LeadAssignManagerSerializer(serializers.Serializer):
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class LeadChangeFirstManagerSerializer(serializers.Serializer):
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class BulkLeadAssignManagerSerializer(serializers.Serializer):
    lead_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    allow_partial = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        unique_ids = list(dict.fromkeys(attrs["lead_ids"]))
        max_ids = int(getattr(settings, "LEADS_BULK_STATUS_CHANGE_MAX_IDS", 500))
        if len(unique_ids) > max_ids:
            raise serializers.ValidationError({"lead_ids": f"Maximum {max_ids} lead ids allowed per request"})
        attrs["reason"] = (attrs.get("reason") or "").strip()
        attrs["_lead_ids"] = unique_ids
        return attrs


class LeadUnassignManagerSerializer(serializers.Serializer):
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class BulkLeadUnassignManagerSerializer(serializers.Serializer):
    lead_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    allow_partial = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        unique_ids = list(dict.fromkeys(attrs["lead_ids"]))
        max_ids = int(getattr(settings, "LEADS_BULK_STATUS_CHANGE_MAX_IDS", 500))
        if len(unique_ids) > max_ids:
            raise serializers.ValidationError({"lead_ids": f"Maximum {max_ids} lead ids allowed per request"})
        attrs["reason"] = (attrs.get("reason") or "").strip()
        attrs["_lead_ids"] = unique_ids
        return attrs


class LeadFunnelMetricsQuerySerializer(serializers.Serializer):
    date_from = serializers.DateField(required=False)
    date_to = serializers.DateField(required=False)
    pipeline = serializers.PrimaryKeyRelatedField(queryset=Pipeline.objects.filter(is_active=True), required=False)
    partner = serializers.PrimaryKeyRelatedField(queryset=Partner.objects.all(), required=False)
    group_by = serializers.ChoiceField(choices=("partner",), required=False)

    def validate(self, attrs):
        date_from = attrs.get("date_from")
        date_to = attrs.get("date_to")
        if date_from and date_to and date_from > date_to:
            raise serializers.ValidationError({"date_from": "date_from must be less than or equal to date_to"})
        return attrs


class LeadCommentSerializer(serializers.ModelSerializer):
    author_username = serializers.CharField(source="author.username", read_only=True)

    class Meta:
        model = LeadComment
        fields = [
            "id",
            "lead",
            "author",
            "author_username",
            "body",
            "is_pinned",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "author", "author_username", "created_at", "updated_at"]


class LeadCloseWonTransferSerializer(serializers.Serializer):
    ret_manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    amount = serializers.DecimalField(max_digits=12, decimal_places=2, min_value=Decimal("0.01"))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        ret_manager = attrs["ret_manager"]
        if ret_manager.role != UserRole.RET:
            raise serializers.ValidationError({"ret_manager": "ret_manager must have RET role"})
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class LeadRollbackRetTransferSerializer(serializers.Serializer):
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class LeadDepositSerializer(serializers.ModelSerializer):
    creator_username = serializers.CharField(source="creator.username", read_only=True)

    class Meta:
        model = LeadDeposit
        fields = [
            "id",
            "lead",
            "creator",
            "creator_username",
            "amount",
            "type",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "lead", "creator", "creator_username", "created_at", "updated_at"]


class LeadDepositCreateSerializer(serializers.Serializer):
    amount = serializers.DecimalField(max_digits=12, decimal_places=2, min_value=Decimal("0.01"))
    type = serializers.ChoiceField(choices=LeadDeposit.Type.choices, required=False)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs
