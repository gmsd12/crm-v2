from rest_framework.routers import DefaultRouter
from django.urls import path, include
from .views import PartnerSourceViewSet, PartnerLeadViewSet

router = DefaultRouter()
router.register(r"partner/sources", PartnerSourceViewSet, basename="partner-sources")
router.register(r"partner/leads", PartnerLeadViewSet, basename="partner-leads")

urlpatterns = [
    path("", include(router.urls)),
]
