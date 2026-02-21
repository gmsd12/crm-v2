from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
    LeadCommentViewSet,
    LeadViewSet,
    LeadAuditLogViewSet,
    LeadStatusViewSet,
)

router = DefaultRouter()
router.register(r"leads/statuses", LeadStatusViewSet, basename="lead-statuses")
router.register(r"leads/audit-logs", LeadAuditLogViewSet, basename="lead-audit-logs")
router.register(r"leads/comments", LeadCommentViewSet, basename="lead-comments")
router.register(r"leads/records", LeadViewSet, basename="leads-records")

urlpatterns = [
    path("", include(router.urls)),
]
