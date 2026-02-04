from __future__ import annotations

from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework import serializers

from apps.iam.models import UserRole
from apps.leads.models import (
    Lead,
    LeadComment,
    LeadStatus,
    LeadStatusAuditLog,
    LeadStatusTransition,
    Pipeline,
)
from apps.partners.models import Partner

User = get_user_model()
ASSIGNEE_ROLES = (UserRole.MANAGER, UserRole.RET)


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
            "counts_for_conversion",
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
        model = LeadStatusAuditLog
        fields = [
            "id",
            "lead",
            "event_type",
            "from_status",
            "from_status_code",
            "to_status",
            "to_status_code",
            "actor_user",
            "actor_username",
            "source",
            "reason",
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
    source = serializers.SerializerMethodField()
    duplicate_of = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "external_id",
            "partner",
            "manager",
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
            "expected_revenue",
            "currency",
            "product",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
            "is_duplicate",
            "duplicate_of",
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

    def get_source(self, obj):
        if not obj.source_id:
            return None
        return {"id": str(obj.source_id), "code": obj.source.code, "name": obj.source.name}

    def get_duplicate_of(self, obj):
        if not obj.duplicate_of_id:
            return None
        return str(obj.duplicate_of_id)


class LeadWriteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Lead
        fields = [
            "id",
            "partner",
            "source",
            "external_id",
            "full_name",
            "phone",
            "email",
            "priority",
            "next_contact_at",
            "last_contacted_at",
            "expected_revenue",
            "currency",
            "product",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
            "custom_fields",
        ]
        read_only_fields = ["id"]
        extra_kwargs = {
            "partner": {"required": True},
            "source": {"required": False, "allow_null": True},
            "external_id": {"required": False, "allow_null": True, "allow_blank": True},
            "full_name": {"required": False, "allow_blank": True},
            "phone": {"required": False, "allow_blank": True},
            "email": {"required": False, "allow_blank": True},
            "currency": {"required": False, "allow_blank": False},
            "product": {"required": False, "allow_blank": True},
            "utm_source": {"required": False, "allow_blank": True},
            "utm_medium": {"required": False, "allow_blank": True},
            "utm_campaign": {"required": False, "allow_blank": True},
            "utm_content": {"required": False, "allow_blank": True},
            "utm_term": {"required": False, "allow_blank": True},
        }

    def validate(self, attrs):
        instance = getattr(self, "instance", None)
        is_create = instance is None

        partner = attrs.get("partner") or getattr(instance, "partner", None)
        if not partner:
            raise serializers.ValidationError({"partner": "This field is required"})

        phone = (attrs.get("phone") if "phone" in attrs else getattr(instance, "phone", "")) or ""
        email = (attrs.get("email") if "email" in attrs else getattr(instance, "email", "")) or ""
        phone = phone.strip()
        email = email.strip().lower()
        if not phone and not email:
            raise serializers.ValidationError({"phone": "Either phone or email is required"})

        attrs["phone"] = phone
        attrs["email"] = email
        attrs["full_name"] = ((attrs.get("full_name") if "full_name" in attrs else getattr(instance, "full_name", "")) or "").strip()
        attrs["product"] = ((attrs.get("product") if "product" in attrs else getattr(instance, "product", "")) or "").strip()
        if "currency" in attrs:
            attrs["currency"] = (attrs.get("currency") or "USD").upper()

        duplicate_qs = Lead.objects.filter(partner=partner)
        if instance:
            duplicate_qs = duplicate_qs.exclude(id=instance.id)
        if phone and duplicate_qs.filter(phone=phone).exists():
            raise serializers.ValidationError({"phone": "Duplicate phone for this partner"})
        if is_create and "is_duplicate" not in attrs:
            attrs["is_duplicate"] = False
            attrs["duplicate_of"] = None

        return attrs


class LeadStatusChangeSerializer(serializers.Serializer):
    to_status = serializers.PrimaryKeyRelatedField(queryset=LeadStatus.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        lead = self.context["lead"]
        to_status = attrs["to_status"]
        reason = (attrs.get("reason") or "").strip()

        if not lead.pipeline_id or not lead.status_id:
            raise serializers.ValidationError("Lead has no current workflow status")
        if to_status.pipeline_id != lead.pipeline_id:
            raise serializers.ValidationError({"to_status": "to_status must belong to lead pipeline"})
        if to_status.id == lead.status_id:
            raise serializers.ValidationError({"to_status": "Lead already has this status"})

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
        child=serializers.UUIDField(),
        allow_empty=False,
    )
    to_status = serializers.PrimaryKeyRelatedField(queryset=LeadStatus.objects.filter(is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    allow_partial = serializers.BooleanField(required=False, default=False)

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
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(role__in=ASSIGNEE_ROLES, is_active=True))
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class BulkLeadAssignManagerSerializer(serializers.Serializer):
    lead_ids = serializers.ListField(
        child=serializers.UUIDField(),
        allow_empty=False,
    )
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(role__in=ASSIGNEE_ROLES, is_active=True))
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
        child=serializers.UUIDField(),
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
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(role__in=ASSIGNEE_ROLES), required=False)
    group_by = serializers.ChoiceField(choices=("partner", "manager"), required=False)
    stale_days = serializers.IntegerField(required=False, min_value=1, max_value=365, default=7)

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
