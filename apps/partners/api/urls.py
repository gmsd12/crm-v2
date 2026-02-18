from rest_framework.routers import DefaultRouter
from django.urls import path, include
from .views import (
    PartnerAdminViewSet,
    PartnerSourceAdminViewSet,
    PartnerSourceViewSet,
    PartnerTokenAdminViewSet,
    PartnerLeadViewSet,
)

router = DefaultRouter()
router.register(r"partners/sources", PartnerSourceAdminViewSet, basename="partners-sources-admin")
router.register(r"partners/tokens", PartnerTokenAdminViewSet, basename="partners-tokens-admin")
router.register(r"partners", PartnerAdminViewSet, basename="partners-admin")
router.register(r"partner/sources", PartnerSourceViewSet, basename="partner-sources")
router.register(r"partner/leads", PartnerLeadViewSet, basename="partner-leads")

urlpatterns = [
    path("", include(router.urls)),
]
