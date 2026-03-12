from __future__ import annotations

import re

from django.utils import timezone
from rest_framework import serializers
from apps.notifications.publishers import publish_partner_duplicate_attempt
from apps.partners.models import Partner, PartnerToken
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


class PartnerTokenAdminSerializer(serializers.ModelSerializer):
    partner_code = serializers.CharField(source="partner.code", read_only=True)
    issued_token = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = PartnerToken
        fields = [
            "id",
            "partner",
            "partner_code",
            "name",
            "source",
            "is_active",
            "expires_at",
            "revoked_at",
            "last_used_at",
            "prefix",
            "issued_token",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "partner_code",
            "revoked_at",
            "last_used_at",
            "prefix",
            "issued_token",
            "created_at",
            "updated_at",
        ]
        extra_kwargs = {
            "partner": {"queryset": Partner.objects.all()},
            "source": {"required": False, "allow_blank": True},
        }

    def get_issued_token(self, obj):
        return getattr(obj, "_issued_token", None)

    def create(self, validated_data):
        raw_token = PartnerToken.generate_raw_token()
        token = PartnerToken.build(
            partner=validated_data["partner"],
            raw_token=raw_token,
            name=validated_data.get("name", ""),
            source=(validated_data.get("source") or "").strip(),
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
                value = validated_data[field]
                if field == "source":
                    value = (value or "").strip()
                setattr(instance, field, value)
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
    source = serializers.CharField(required=False, allow_blank=True)

    class Meta:
        model = Lead
        fields = [
            "id",
            "source",
            "geo",
            "age",
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
            "custom_fields": {"required": False, "allow_null": True},
        }

    def validate(self, attrs):
        request = self.context["request"]
        partner_auth = request.partner_auth

        token_bound_source = partner_auth.source
        requested_source = (attrs.get("source") or "").strip()

        if token_bound_source:
            # токен привязан к source-строке => игнорируем source из тела
            attrs["_source"] = token_bound_source
        else:
            attrs["_source"] = requested_source
        attrs["_requested_source"] = requested_source

        phone = (attrs.get("phone") or "").strip()
        email = (attrs.get("email") or "").strip()
        if not phone:
            raise serializers.ValidationError({"phone": "Телефон обязателен"})

        attrs["phone"] = phone
        attrs["email"] = email
        geo = (attrs.get("geo") or "").strip().upper()
        if geo and not GEO_CODE_RE.fullmatch(geo):
            raise serializers.ValidationError({"geo": "geo должен быть кодом страны из 2 заглавных букв"})
        attrs["geo"] = geo
        attrs["full_name"] = (attrs.get("full_name") or "").strip()
        return attrs

    @staticmethod
    def _duplicate_echo_payload(*, requested_source: str, validated_data: dict) -> dict:
        return {
            "source": requested_source,
            "geo": validated_data.get("geo") or "",
            "age": validated_data.get("age"),
            "full_name": validated_data.get("full_name") or "",
            "phone": validated_data.get("phone") or "",
            "email": validated_data.get("email") or "",
            "custom_fields": validated_data.get("custom_fields"),
        }

    def create(self, validated_data):
        request = self.context["request"]
        partner_auth = request.partner_auth

        source = validated_data.pop("_source", None)
        requested_source = validated_data.pop("_requested_source", "")
        validated_data.pop("source", None)

        partner = partner_auth.partner
        default_status = LeadStatus.objects.filter(is_default_for_new_leads=True, is_active=True).order_by("order", "created_at").first()
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
                    reason="Отклонен дубликат телефона",
                    payload_before={"lead_id": str(duplicate_lead.id), "phone": duplicate_lead.phone},
                    payload_after={
                        "attempt_id": str(attempt.id),
                        "partner_id": str(partner.id),
                        "source": source or "",
                        "existing_lead_id": str(duplicate_lead.id),
                        "phone": attempt.phone,
                        "full_name": attempt.full_name,
                        "email": attempt.email,
                    },
                )
                publish_partner_duplicate_attempt(attempt_id=attempt.id)
                duplicate_lead._was_created = False
                duplicate_lead._duplicate_rejected = True
                duplicate_lead._partner_response_payload = self._duplicate_echo_payload(
                    requested_source=requested_source,
                    validated_data=validated_data,
                )
                return duplicate_lead

        try:
            with transaction.atomic():
                lead = Lead.objects.create(
                    partner=partner,
                    source=source,
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
                reason="Отклонен дубликат телефона",
                payload_before={"lead_id": str(duplicate_lead.id), "phone": duplicate_lead.phone},
                payload_after={
                    "attempt_id": str(attempt.id),
                    "partner_id": str(partner.id),
                    "source": source or "",
                    "existing_lead_id": str(duplicate_lead.id),
                    "phone": attempt.phone,
                    "full_name": attempt.full_name,
                    "email": attempt.email,
                },
            )
            publish_partner_duplicate_attempt(attempt_id=attempt.id)
            duplicate_lead._was_created = False
            duplicate_lead._duplicate_rejected = True
            duplicate_lead._partner_response_payload = self._duplicate_echo_payload(
                requested_source=requested_source,
                validated_data=validated_data,
            )
            return duplicate_lead


class LeadListSerializer(serializers.ModelSerializer):
    source = serializers.CharField(read_only=True)
    status = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "source",
            "received_at",
            "geo",
            "age",
            "status",
            "full_name",
            "phone",
            "email",
            "custom_fields",
        ]

    def get_status(self, obj):
        if not obj.status_id:
            return None
        return {
            "id": str(obj.status_id),
            "code": obj.status.code,
            "name": obj.status.name,
            "work_bucket": obj.status.work_bucket,
        }
