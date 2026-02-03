from __future__ import annotations

from rest_framework import mixins, viewsets, status
from django_filters.rest_framework import FilterSet, filters
from rest_framework.response import Response

from apps.partners.auth import PartnerTokenAuthentication
from apps.partners.pagination import PartnerLeadPagination
from apps.partners.permissions import IsPartnerAuthenticated
from apps.partners.throttling import PartnerTokenRateThrottle
from apps.partners.models import PartnerSource
from apps.leads.models import Lead
from .serializers import (
    PartnerSourceSerializer,
    LeadCreateSerializer,
    LeadListSerializer,
)


class PartnerSourceViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    authentication_classes = [PartnerTokenAuthentication]
    permission_classes = [IsPartnerAuthenticated]
    serializer_class = PartnerSourceSerializer

    def get_queryset(self):
        partner = self.request.partner_auth.partner
        return PartnerSource.objects.filter(partner=partner).order_by("code")


class LeadFilter(FilterSet):
    source = filters.CharFilter(method="filter_source")
    external_id = filters.CharFilter(field_name="external_id", lookup_expr="exact")
    received_from = filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="gte")
    received_to = filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="lte")

    def filter_source(self, qs, name, value):
        value = (value or "").strip()
        if not value:
            return qs
        # фильтр по source.code
        return qs.filter(source__code=value)

    class Meta:
        model = Lead
        fields = ["external_id"]


class PartnerLeadViewSet(
    mixins.CreateModelMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    authentication_classes = [PartnerTokenAuthentication]
    permission_classes = [IsPartnerAuthenticated]
    throttle_classes = [PartnerTokenRateThrottle]
    pagination_class = PartnerLeadPagination
    filterset_class = LeadFilter
    ordering_fields = ["received_at"]
    ordering = ["-received_at"]

    def get_queryset(self):
        # ЖЕСТКАЯ изоляция: только лиды партнёра
        partner = self.request.partner_auth.partner
        return Lead.objects.filter(partner=partner).select_related("source").order_by("-received_at")

    def get_serializer_class(self):
        if self.action == "create":
            return LeadCreateSerializer
        return LeadListSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        lead = serializer.save()

        created = getattr(lead, "_was_created", True)

        out = LeadListSerializer(lead).data
        out["created"] = created

        return Response(out, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)
