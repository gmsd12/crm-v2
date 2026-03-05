from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import NotificationPolicyViewSet, NotificationViewSet, health

router = DefaultRouter()
router.register(r"v1/notifications", NotificationViewSet, basename="notifications")
router.register(r"v1/notification-policies", NotificationPolicyViewSet, basename="notification-policies")

urlpatterns = [
    path("health/", health, name="health"),
    path("", include(router.urls)),
]
