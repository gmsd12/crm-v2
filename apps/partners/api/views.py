from __future__ import annotations

from rest_framework import filters as drf_filters, mixins, viewsets, status
from django_filters.rest_framework import FilterSet, filters
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.iam.api.rbac_mixins import RBACActionMixin
from apps.iam.api.rbac_permissions import RBACPermission
from apps.iam.rbac import Perm
from apps.partners.auth import PartnerTokenAuthentication
from apps.partners.models import Partner, PartnerToken
from apps.partners.pagination import PartnerLeadPagination
from apps.partners.permissions import IsPartnerAuthenticated
from apps.partners.throttling import PartnerTokenRateThrottle
from apps.leads.models import Lead
from .serializers import (
    PartnerAdminSerializer,
    PartnerTokenAdminSerializer,
    LeadCreateSerializer,
    LeadListSerializer,
)


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
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]

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
    ordering = ["code"]
    ordering_fields = ["id", "code", "name", "is_active", "created_at", "updated_at"]

    def get_queryset(self):
        if self.action in {"restore"}:
            return Partner.all_objects.all().order_by("code")
        return Partner.objects.all().order_by("code")


class PartnerTokenAdminViewSet(_BasePartnerCatalogAdminViewSet):
    serializer_class = PartnerTokenAdminSerializer
    filterset_fields = ["partner", "source", "is_active", "name"]
    ordering = ["-created_at"]
    ordering_fields = [
        "id",
        "created_at",
        "updated_at",
        "partner__code",
        "name",
        "source",
        "is_active",
        "expires_at",
        "last_used_at",
    ]

    def get_queryset(self):
        if self.action in {"restore"}:
            return PartnerToken.all_objects.select_related("partner").all().order_by("-created_at")
        return PartnerToken.objects.select_related("partner").all().order_by("-created_at")


class LeadFilter(FilterSet):
    class CharInFilter(filters.BaseInFilter, filters.CharFilter):
        pass

    source = filters.CharFilter(field_name="source", lookup_expr="iexact")
    phone = filters.CharFilter(field_name="phone", lookup_expr="exact")
    age = filters.NumberFilter(field_name="age", lookup_expr="exact")
    age_from = filters.NumberFilter(field_name="age", lookup_expr="gte")
    age_to = filters.NumberFilter(field_name="age", lookup_expr="lte")
    status__in = CharInFilter(field_name="status__code", lookup_expr="in")
    received_from = filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="gte")
    received_to = filters.IsoDateTimeFilter(field_name="received_at", lookup_expr="lte")

    class Meta:
        model = Lead
        fields = ["phone", "age", "age_from", "age_to", "source", "status__in"]


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
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter]
    filterset_class = LeadFilter
    ordering_fields = [
        "id",
        "received_at",
        "age",
        "phone",
        "full_name",
        "email",
        "priority",
        "source",
        "status__code",
    ]
    ordering = ["-received_at"]

    def get_queryset(self):
        # ЖЕСТКАЯ изоляция: только лиды партнёра
        partner = self.request.partner_auth.partner
        return (
            Lead.objects.filter(partner=partner)
            .select_related("status")
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

        out = getattr(lead, "_partner_response_payload", None) or LeadListSerializer(lead).data
        out["created"] = created
        out["duplicate_rejected"] = duplicate_rejected

        return Response(out, status=status.HTTP_201_CREATED if created else status.HTTP_409_CONFLICT)
