from __future__ import annotations

from rest_framework import serializers
from apps.partners.models import PartnerSource
from apps.leads.models import Lead, LeadDuplicateAttempt, LeadStatus
from django.db import transaction, IntegrityError


class PartnerSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PartnerSource
        fields = ["id", "name", "code", "is_active"]


class LeadCreateSerializer(serializers.ModelSerializer):
    # partner определяем из токена, не из тела запроса
    source_code = serializers.SlugField(required=False, allow_blank=True)

    class Meta:
        model = Lead
        fields = [
            "id",
            "external_id",
            "source_code",
            "full_name",
            "phone",
            "email",
            "priority",
            "expected_revenue",
            "currency",
            "product",
            "custom_fields",
            "received_at",
        ]
        read_only_fields = ["id", "received_at"]

    def validate(self, attrs):
        request = self.context["request"]
        partner_auth = request.partner_auth

        token_bound_source = partner_auth.source
        source_code = (attrs.get("source_code") or "").strip()

        if token_bound_source:
            # токен привязан к source => игнорируем source_code от клиента
            attrs["_source"] = token_bound_source
            return attrs

        if source_code:
            source = PartnerSource.objects.filter(partner=partner_auth.partner, code=source_code,
                                                  is_active=True).first()
            if not source:
                raise serializers.ValidationError({"source_code": "Unknown source_code for this partner"})
            attrs["_source"] = source
        else:
            attrs["_source"] = None

        phone = (attrs.get("phone") or "").strip()
        email = (attrs.get("email") or "").strip()
        if not phone and not email:
            raise serializers.ValidationError({"phone": "Either phone or email is required"})

        attrs["phone"] = phone
        attrs["email"] = email
        attrs["full_name"] = (attrs.get("full_name") or "").strip()
        attrs["product"] = (attrs.get("product") or "").strip()
        attrs["currency"] = (attrs.get("currency") or "USD").upper()
        return attrs

    def create(self, validated_data):
        request = self.context["request"]
        partner_auth = request.partner_auth

        source = validated_data.pop("_source", None)
        validated_data.pop("source_code", None)

        partner = partner_auth.partner
        external_id = validated_data.get("external_id")
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
            duplicate_lead = Lead.objects.filter(partner=partner, phone=phone).order_by("received_at").first()
            if duplicate_lead:
                LeadDuplicateAttempt.objects.create(
                    partner=partner,
                    source=source,
                    existing_lead=duplicate_lead,
                    phone=phone,
                    full_name=validated_data.get("full_name") or "",
                    email=validated_data.get("email") or "",
                )
                duplicate_lead._was_created = False
                duplicate_lead._duplicate_rejected = True
                return duplicate_lead

        # Нет external_id => всегда новый лид
        if not external_id:
            lead = Lead.objects.create(
                partner=partner,
                source=source,
                pipeline=pipeline,
                status=default_status,
                **validated_data,
            )
            lead._was_created = True
            return lead

        # Есть external_id => идемпотентно: либо создаём, либо возвращаем существующий БЕЗ обновления
        existing = Lead.objects.filter(partner=partner, external_id=external_id).first()
        if existing:
            existing._was_created = False
            existing._duplicate_rejected = False
            return existing

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
            # параллельный create — просто читаем
            lead = Lead.objects.get(partner=partner, external_id=external_id)
            lead._was_created = False
            lead._duplicate_rejected = False
            return lead


class LeadListSerializer(serializers.ModelSerializer):
    source = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    pipeline = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = [
            "id",
            "external_id",
            "pipeline",
            "status",
            "source",
            "full_name",
            "phone",
            "email",
            "priority",
            "expected_revenue",
            "currency",
            "product",
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
