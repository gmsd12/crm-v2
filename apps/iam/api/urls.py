from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import UserAdminViewSet, login_view, refresh_view, logout_view, me_view

router = DefaultRouter()
router.register(r"iam/users", UserAdminViewSet, basename="iam-users")

urlpatterns = [
    path("auth/login/", login_view, name="auth-login"),
    path("auth/refresh/", refresh_view, name="auth-refresh"),
    path("auth/logout/", logout_view, name="auth-logout"),
    path("auth/me/", me_view, name="auth-me"),
    path("", include(router.urls)),
]
