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
)
from apps.partners.models import Partner

User = get_user_model()
GEO_CODE_RE = re.compile(r"^[A-Z]{2}$")


class LeadStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = LeadStatus
        fields = [
            "id",
            "code",
            "name",
            "order",
            "color",
            "is_default_for_new_leads",
            "is_active",
            "is_valid",
            "work_bucket",
            "conversion_bucket",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class LeadAuditLogSerializer(serializers.ModelSerializer):
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
    status = serializers.SerializerMethodField()
    partner = serializers.SerializerMethodField()
    manager = serializers.SerializerMethodField()
    first_manager = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()
    last_comment = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "geo",
            "age",
            "partner",
            "manager",
            "first_manager",
            "source",
            "status",
            "full_name",
            "phone",
            "email",
            "priority",
            "next_contact_at",
            "last_contacted_at",
            "assigned_at",
            "first_assigned_at",
            "custom_fields",
            "received_at",
            "last_comment",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.context.get("include_last_comment", False):
            self.fields.pop("last_comment", None)

    @staticmethod
    def _manager_like_payload(user):
        return {
            "id": str(user.id),
            "username": user.username,
            "first_name": (user.first_name or "").strip(),
            "last_name": (user.last_name or "").strip(),
            "role": user.role,
        }

    def get_status(self, obj):
        if not obj.status_id:
            return None
        return {
            "id": str(obj.status_id),
            "code": obj.status.code,
            "name": obj.status.name,
            "color": obj.status.color,
            "work_bucket": obj.status.work_bucket,
        }

    def get_partner(self, obj):
        return {"id": str(obj.partner_id), "code": obj.partner.code, "name": obj.partner.name}

    def get_manager(self, obj):
        if not obj.manager_id:
            return None
        return self._manager_like_payload(obj.manager)

    def get_first_manager(self, obj):
        if not obj.first_manager_id:
            return None
        return self._manager_like_payload(obj.first_manager)

    def get_source(self, obj):
        if not obj.source_id:
            return None
        return {"id": str(obj.source_id), "code": obj.source.code, "name": obj.source.name}

    def get_last_comment(self, obj):
        comments_by_lead = self.context.get("last_comment_by_lead_id", {})
        return comments_by_lead.get(str(obj.id))


class LeadWriteSerializer(serializers.ModelSerializer):
    first_manager = serializers.PrimaryKeyRelatedField(
        queryset=User.objects.filter(is_active=True),
        required=False,
        allow_null=True,
    )
    first_assigned_at = serializers.DateTimeField(required=False, allow_null=True)

    class Meta:
        model = Lead
        fields = [
            "id",
            "partner",
            "source",
            "first_manager",
            "first_assigned_at",
            "geo",
            "full_name",
            "phone",
            "email",
            "age",
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
            "age": {"required": False, "allow_null": True},
            "priority": {"required": False, "allow_null": True},
            "custom_fields": {"required": False, "allow_null": True},
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
        if "first_assigned_at" in attrs and attrs["first_assigned_at"] is not None:
            first_assigned_at = attrs["first_assigned_at"]
            if timezone.is_naive(first_assigned_at):
                attrs["first_assigned_at"] = timezone.make_aware(first_assigned_at, timezone.get_current_timezone())

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
        if to_status.id == lead.status_id:
            raise serializers.ValidationError({"to_status": "Lead already has this status"})

        attrs["reason"] = reason
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
    set_as_first_manager = serializers.BooleanField(required=False, default=False)
    first_assigned_at = serializers.DateTimeField(required=False, allow_null=True)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        if "first_assigned_at" in attrs and attrs["first_assigned_at"] is not None:
            dt = attrs["first_assigned_at"]
            if timezone.is_naive(dt):
                attrs["first_assigned_at"] = timezone.make_aware(dt, timezone.get_current_timezone())
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class LeadChangeFirstManagerSerializer(serializers.Serializer):
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    first_assigned_at = serializers.DateTimeField(required=False)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        if "first_assigned_at" in attrs and attrs["first_assigned_at"] is not None:
            dt = attrs["first_assigned_at"]
            if timezone.is_naive(dt):
                attrs["first_assigned_at"] = timezone.make_aware(dt, timezone.get_current_timezone())
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class BulkLeadAssignManagerSerializer(serializers.Serializer):
    lead_ids = serializers.ListField(
        child=serializers.IntegerField(min_value=1),
        allow_empty=False,
    )
    manager = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True))
    set_as_first_manager = serializers.BooleanField(required=False, default=False)
    first_assigned_at = serializers.DateTimeField(required=False, allow_null=True)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    allow_partial = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        unique_ids = list(dict.fromkeys(attrs["lead_ids"]))
        max_ids = int(getattr(settings, "LEADS_BULK_STATUS_CHANGE_MAX_IDS", 500))
        if len(unique_ids) > max_ids:
            raise serializers.ValidationError({"lead_ids": f"Maximum {max_ids} lead ids allowed per request"})
        if "first_assigned_at" in attrs and attrs["first_assigned_at"] is not None:
            dt = attrs["first_assigned_at"]
            if timezone.is_naive(dt):
                attrs["first_assigned_at"] = timezone.make_aware(dt, timezone.get_current_timezone())
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
    partner = serializers.PrimaryKeyRelatedField(queryset=Partner.objects.all(), required=False)
    group_by = serializers.ChoiceField(choices=("partner",), required=False)

    def validate(self, attrs):
        date_from = attrs.get("date_from")
        date_to = attrs.get("date_to")
        if date_from and date_to and date_from > date_to:
            raise serializers.ValidationError({"date_from": "date_from must be less than or equal to date_to"})
        return attrs


class LeadDepositStatsQuerySerializer(serializers.Serializer):
    date_from = serializers.DateField(required=False)
    date_to = serializers.DateField(required=False)
    partner = serializers.PrimaryKeyRelatedField(queryset=Partner.objects.all(), required=False)
    creator = serializers.PrimaryKeyRelatedField(queryset=User.objects.filter(is_active=True), required=False)
    creator_role = serializers.ChoiceField(choices=UserRole.choices, required=False)

    def validate(self, attrs):
        today = timezone.localdate()
        date_from = attrs.get("date_from") or today.replace(month=1, day=1)
        date_to = attrs.get("date_to") or today
        attrs["date_from"] = date_from
        attrs["date_to"] = date_to
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


class LeadDepositSerializer(serializers.ModelSerializer):
    lead_full_name = serializers.CharField(source="lead.full_name", read_only=True)
    creator_username = serializers.CharField(source="creator.username", read_only=True)
    creator_first_name = serializers.CharField(source="creator.first_name", read_only=True)
    creator_last_name = serializers.CharField(source="creator.last_name", read_only=True)
    creator_role = serializers.CharField(source="creator.role", read_only=True)

    class Meta:
        model = LeadDeposit
        fields = [
            "id",
            "lead",
            "lead_full_name",
            "creator",
            "creator_username",
            "creator_first_name",
            "creator_last_name",
            "creator_role",
            "amount",
            "type",
            "is_deleted",
            "deleted_at",
            "created_at",
            "updated_at",
        ]
        read_only_fields = fields


class LeadDepositCreateSerializer(serializers.Serializer):
    amount = serializers.DecimalField(max_digits=12, decimal_places=2, min_value=Decimal("0.01"))
    type = serializers.ChoiceField(choices=LeadDeposit.Type.choices, required=False)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs


class LeadDepositWriteSerializer(serializers.Serializer):
    lead = serializers.PrimaryKeyRelatedField(queryset=Lead.objects.all(), required=False)
    amount = serializers.DecimalField(max_digits=12, decimal_places=2, min_value=Decimal("0.01"), required=False)
    type = serializers.ChoiceField(choices=LeadDeposit.Type.choices, required=False)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=1000)

    def validate(self, attrs):
        attrs["reason"] = (attrs.get("reason") or "").strip()
        return attrs
