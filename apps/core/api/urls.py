from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import health

router = DefaultRouter()

urlpatterns = [
    path("health/", health, name="health"),
    path("", include(router.urls)),
]
