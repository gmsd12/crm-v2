from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
    LeadViewSet,
    LeadStatusAuditLogViewSet,
    LeadStatusTransitionViewSet,
    LeadStatusViewSet,
    PipelineViewSet,
)

router = DefaultRouter()
router.register(r"leads/pipelines", PipelineViewSet, basename="lead-pipelines")
router.register(r"leads/statuses", LeadStatusViewSet, basename="lead-statuses")
router.register(r"leads/status-transitions", LeadStatusTransitionViewSet, basename="lead-status-transitions")
router.register(r"leads/status-audit-logs", LeadStatusAuditLogViewSet, basename="lead-status-audit-logs")
router.register(r"leads/records", LeadViewSet, basename="leads-records")

urlpatterns = [
    path("", include(router.urls)),
]
