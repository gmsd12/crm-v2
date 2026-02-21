from __future__ import annotations

from rest_framework import mixins, viewsets, status
from django_filters.rest_framework import FilterSet, filters
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.iam.api.rbac_mixins import RBACActionMixin
from apps.iam.api.rbac_permissions import RBACPermission
from apps.iam.rbac import Perm
from apps.partners.auth import PartnerTokenAuthentication
from apps.partners.models import Partner, PartnerSource, PartnerToken
from apps.partners.pagination import PartnerLeadPagination
from apps.partners.permissions import IsPartnerAuthenticated
from apps.partners.throttling import PartnerTokenRateThrottle
from apps.leads.models import Lead
from .serializers import (
    PartnerAdminSerializer,
    PartnerSourceAdminSerializer,
    PartnerTokenAdminSerializer,
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


class _BasePartnerCatalogAdminViewSet(RBACActionMixin, viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.BRANDS_READ,),
        "retrieve": (Perm.BRANDS_READ,),
        "create": (Perm.BRANDS_WRITE,),
        "update": (Perm.BRANDS_WRITE,),
        "partial_update": (Perm.BRANDS_WRITE,),
        "soft_delete": (Perm.BRANDS_WRITE,),
        "restore": (Perm.BRANDS_WRITE,),
        "destroy": (Perm.BRANDS_HARD_DELETE,),
    }
    filter_backends = [DjangoFilterBackend]

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


class PartnerAdminViewSet(_BasePartnerCatalogAdminViewSet):
    serializer_class = PartnerAdminSerializer
    filterset_fields = ["is_active", "code", "name"]

    def get_queryset(self):
        if self.action in {"restore"}:
            return Partner.all_objects.all().order_by("code")
        return Partner.objects.all().order_by("code")


class PartnerSourceAdminViewSet(_BasePartnerCatalogAdminViewSet):
    serializer_class = PartnerSourceAdminSerializer
    filterset_fields = ["partner", "is_active", "code"]

    def get_queryset(self):
        if self.action in {"restore"}:
            return PartnerSource.all_objects.select_related("partner").all().order_by("partner__code", "code")
        return PartnerSource.objects.select_related("partner").all().order_by("partner__code", "code")


class PartnerTokenAdminViewSet(_BasePartnerCatalogAdminViewSet):
    serializer_class = PartnerTokenAdminSerializer
    filterset_fields = ["partner", "source", "is_active", "name"]

    def get_queryset(self):
        if self.action in {"restore"}:
            return PartnerToken.all_objects.select_related("partner", "source").all().order_by("-created_at")
        return PartnerToken.objects.select_related("partner", "source").all().order_by("-created_at")


class LeadFilter(FilterSet):
    source = filters.CharFilter(method="filter_source")
    phone = filters.CharFilter(field_name="phone", lookup_expr="exact")
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
        fields = ["phone"]


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
        return (
            Lead.objects.filter(partner=partner)
            .select_related("source", "status")
            .order_by("-received_at")
        )

    def get_serializer_class(self):
        if self.action == "create":
            return LeadCreateSerializer
        return LeadListSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        lead = serializer.save()

        created = getattr(lead, "_was_created", True)
        duplicate_rejected = getattr(lead, "_duplicate_rejected", False)

        out = LeadListSerializer(lead).data
        out["created"] = created
        out["duplicate_rejected"] = duplicate_rejected

        return Response(out, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)
