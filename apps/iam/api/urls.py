from django.urls import path
from .views import login_view, refresh_view, logout_view, me_view

urlpatterns = [
    path("auth/login/", login_view, name="auth-login"),
    path("auth/refresh/", refresh_view, name="auth-refresh"),
    path("auth/logout/", logout_view, name="auth-logout"),
    path("auth/me/", me_view, name="auth-me"),
]
