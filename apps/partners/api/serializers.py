from __future__ import annotations

import re

from django.utils import timezone
from rest_framework import serializers
from apps.partners.models import Partner, PartnerSource, PartnerToken
from apps.leads.models import (
    Lead,
    LeadAuditEntity,
    LeadAuditEvent,
    LeadAuditLog,
    LeadAuditSource,
    LeadDuplicateAttempt,
    LeadStatus,
)
from django.db import transaction, IntegrityError

GEO_CODE_RE = re.compile(r"^[A-Z]{2}$")


class PartnerAdminSerializer(serializers.ModelSerializer):
    class Meta:
        model = Partner
        fields = [
            "id",
            "name",
            "code",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class PartnerSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PartnerSource
        fields = ["id", "name", "code", "is_active"]


class PartnerSourceAdminSerializer(serializers.ModelSerializer):
    partner_code = serializers.CharField(source="partner.code", read_only=True)
    partner_name = serializers.CharField(source="partner.name", read_only=True)

    class Meta:
        model = PartnerSource
        fields = [
            "id",
            "partner",
            "partner_code",
            "partner_name",
            "name",
            "code",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "partner_code", "partner_name", "created_at", "updated_at"]
        extra_kwargs = {
            "partner": {"queryset": Partner.objects.all()},
            "name": {"required": True},
            "code": {"required": True},
        }


class PartnerTokenAdminSerializer(serializers.ModelSerializer):
    partner_code = serializers.CharField(source="partner.code", read_only=True)
    source_code = serializers.CharField(source="source.code", read_only=True)
    issued_token = serializers.SerializerMethodField(read_only=True)
    raw_token = serializers.CharField(write_only=True, required=False, min_length=20)

    class Meta:
        model = PartnerToken
        fields = [
            "id",
            "partner",
            "partner_code",
            "name",
            "source",
            "source_code",
            "is_active",
            "expires_at",
            "revoked_at",
            "last_used_at",
            "prefix",
            "issued_token",
            "raw_token",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "partner_code",
            "source_code",
            "revoked_at",
            "last_used_at",
            "prefix",
            "issued_token",
            "created_at",
            "updated_at",
        ]
        extra_kwargs = {
            "partner": {"queryset": Partner.objects.all()},
            "source": {"queryset": PartnerSource.objects.all(), "required": False, "allow_null": True},
        }

    def get_issued_token(self, obj):
        return getattr(obj, "_issued_token", None)

    def validate(self, attrs):
        instance = getattr(self, "instance", None)
        partner = attrs.get("partner") or getattr(instance, "partner", None)
        source = attrs.get("source") if "source" in attrs else getattr(instance, "source", None)
        if source is not None and partner is not None and source.partner_id != partner.id:
            raise serializers.ValidationError({"source": "source must belong to selected partner"})
        return attrs

    def create(self, validated_data):
        raw_token = validated_data.pop("raw_token", None) or PartnerToken.generate_raw_token()
        token = PartnerToken.build(
            partner=validated_data["partner"],
            raw_token=raw_token,
            name=validated_data.get("name", ""),
            source=validated_data.get("source"),
        )
        token.is_active = validated_data.get("is_active", True)
        token.expires_at = validated_data.get("expires_at")
        if not token.is_active:
            token.revoked_at = timezone.now()
        token.save()
        token._issued_token = raw_token
        return token

    def update(self, instance, validated_data):
        for field in ("partner", "name", "source", "expires_at"):
            if field in validated_data:
                setattr(instance, field, validated_data[field])
        if "is_active" in validated_data:
            instance.is_active = validated_data["is_active"]
            if instance.is_active:
                instance.revoked_at = None
            elif instance.revoked_at is None:
                instance.revoked_at = timezone.now()
        instance.save()
        return instance


class LeadCreateSerializer(serializers.ModelSerializer):
    # partner определяем из токена, не из тела запроса
    source_code = serializers.SlugField(required=False, allow_blank=True)

    class Meta:
        model = Lead
        fields = [
            "id",
            "source_code",
            "geo",
            "full_name",
            "phone",
            "email",
            "priority",
            "custom_fields",
            "received_at",
        ]
        read_only_fields = ["id", "received_at"]
        validators = []
        extra_kwargs = {
            "phone": {"validators": []},
        }

    def validate(self, attrs):
        request = self.context["request"]
        partner_auth = request.partner_auth

        token_bound_source = partner_auth.source
        source_code = (attrs.get("source_code") or "").strip()

        if token_bound_source:
            # токен привязан к source => игнорируем source_code от клиента
            attrs["_source"] = token_bound_source
        else:
            if source_code:
                source = PartnerSource.objects.filter(
                    partner=partner_auth.partner,
                    code=source_code,
                    is_active=True,
                ).first()
                if not source:
                    raise serializers.ValidationError({"source_code": "Unknown source_code for this partner"})
                attrs["_source"] = source
            else:
                attrs["_source"] = None

        phone = (attrs.get("phone") or "").strip()
        email = (attrs.get("email") or "").strip()
        if not phone:
            raise serializers.ValidationError({"phone": "phone is required"})

        attrs["phone"] = phone
        attrs["email"] = email
        geo = (attrs.get("geo") or "").strip().upper()
        if geo and not GEO_CODE_RE.fullmatch(geo):
            raise serializers.ValidationError({"geo": "geo must be a 2-letter uppercase country code"})
        attrs["geo"] = geo
        attrs["full_name"] = (attrs.get("full_name") or "").strip()
        return attrs

    def create(self, validated_data):
        request = self.context["request"]
        partner_auth = request.partner_auth

        source = validated_data.pop("_source", None)
        validated_data.pop("source_code", None)

        partner = partner_auth.partner
        default_status = (
            LeadStatus.objects.select_related("pipeline")
            .filter(is_default_for_new_leads=True, is_active=True, pipeline__is_active=True)
            .order_by("-pipeline__is_default", "pipeline__code", "order", "created_at")
            .first()
        )
        pipeline = default_status.pipeline if default_status else None
        phone = (validated_data.get("phone") or "").strip()

        # дубли учитываем только по телефону: не создаём лид, а записываем попытку
        if phone:
            duplicate_lead = Lead.objects.filter(phone=phone).order_by("received_at").first()
            if duplicate_lead:
                attempt = LeadDuplicateAttempt.objects.create(
                    partner=partner,
                    source=source,
                    existing_lead=duplicate_lead,
                    phone=phone,
                    full_name=validated_data.get("full_name") or "",
                    email=validated_data.get("email") or "",
                )
                LeadAuditLog.objects.create(
                    lead=duplicate_lead,
                    event_type=LeadAuditEvent.DUPLICATE_REJECTED,
                    entity_type=LeadAuditEntity.DUPLICATE_ATTEMPT,
                    entity_id=str(attempt.id),
                    source=LeadAuditSource.IMPORT,
                    reason="Duplicate phone rejected",
                    payload_before={"lead_id": str(duplicate_lead.id), "phone": duplicate_lead.phone},
                    payload_after={
                        "attempt_id": str(attempt.id),
                        "partner_id": str(partner.id),
                        "source_id": str(source.id) if source else None,
                        "existing_lead_id": str(duplicate_lead.id),
                        "phone": attempt.phone,
                        "full_name": attempt.full_name,
                        "email": attempt.email,
                    },
                )
                duplicate_lead._was_created = False
                duplicate_lead._duplicate_rejected = True
                return duplicate_lead

        try:
            with transaction.atomic():
                lead = Lead.objects.create(
                    partner=partner,
                    source=source,
                    pipeline=pipeline,
                    status=default_status,
                    **validated_data,
                )
                lead._was_created = True
                lead._duplicate_rejected = False
                return lead
        except IntegrityError:
            # параллельный create того же phone: возвращаем как duplicate_rejected
            duplicate_lead = Lead.objects.filter(phone=phone).order_by("received_at").first()
            if not duplicate_lead:
                raise
            attempt = LeadDuplicateAttempt.objects.create(
                partner=partner,
                source=source,
                existing_lead=duplicate_lead,
                phone=phone,
                full_name=validated_data.get("full_name") or "",
                email=validated_data.get("email") or "",
            )
            LeadAuditLog.objects.create(
                lead=duplicate_lead,
                event_type=LeadAuditEvent.DUPLICATE_REJECTED,
                entity_type=LeadAuditEntity.DUPLICATE_ATTEMPT,
                entity_id=str(attempt.id),
                source=LeadAuditSource.IMPORT,
                reason="Duplicate phone rejected",
                payload_before={"lead_id": str(duplicate_lead.id), "phone": duplicate_lead.phone},
                payload_after={
                    "attempt_id": str(attempt.id),
                    "partner_id": str(partner.id),
                    "source_id": str(source.id) if source else None,
                    "existing_lead_id": str(duplicate_lead.id),
                    "phone": attempt.phone,
                    "full_name": attempt.full_name,
                    "email": attempt.email,
                },
            )
            duplicate_lead._was_created = False
            duplicate_lead._duplicate_rejected = True
            return duplicate_lead


class LeadListSerializer(serializers.ModelSerializer):
    source = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    pipeline = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "geo",
            "pipeline",
            "status",
            "source",
            "full_name",
            "phone",
            "email",
            "priority",
            "custom_fields",
            "received_at",
        ]

    def get_source(self, obj):
        if not obj.source_id:
            return None
        return {"id": str(obj.source_id), "code": obj.source.code, "name": obj.source.name}

    def get_status(self, obj):
        if not obj.status_id:
            return None
        return {"id": str(obj.status_id), "code": obj.status.code, "name": obj.status.name}

    def get_pipeline(self, obj):
        if not obj.pipeline_id:
            return None
        return {"id": str(obj.pipeline_id), "code": obj.pipeline.code, "name": obj.pipeline.name}
