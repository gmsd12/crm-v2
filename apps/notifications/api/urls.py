from django.urls import include, path
from rest_framework.routers import DefaultRouter

from apps.notifications.api.views import NotificationPolicyViewSet, NotificationViewSet

router = DefaultRouter()
router.register(r"v1/notifications", NotificationViewSet, basename="notifications")
router.register(r"v1/notification-policies", NotificationPolicyViewSet, basename="notification-policies")

urlpatterns = [
    path("", include(router.urls)),
]
