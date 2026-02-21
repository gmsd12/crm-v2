from __future__ import annotations

from django.contrib.auth import get_user_model
from django.conf import settings
from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed, NotAuthenticated, PermissionDenied

from rest_framework_simplejwt.tokens import RefreshToken

from apps.iam.rbac import Perm
from .rbac_mixins import RBACActionMixin
from .rbac_permissions import RBACPermission
from .serializers import LoginSerializer, UserAdminSerializer

User = get_user_model()


def _origin_allowed(request) -> bool:
    origin = request.headers.get("Origin")
    # Если Origin отсутствует (например curl) — разрешаем.
    if not origin:
        return True
    return origin in getattr(settings, "CORS_ALLOWED_ORIGINS", [])


def _ensure_origin_allowed(request) -> None:
    if not _origin_allowed(request):
        raise PermissionDenied("Origin not allowed")


def _set_refresh_cookie(response: Response, refresh: str) -> None:
    response.set_cookie(
        key=settings.JWT_REFRESH_COOKIE_NAME,
        value=refresh,
        httponly=settings.JWT_REFRESH_COOKIE_HTTPONLY,
        secure=settings.JWT_REFRESH_COOKIE_SECURE,
        samesite=settings.JWT_REFRESH_COOKIE_SAMESITE,
        domain=settings.JWT_REFRESH_COOKIE_DOMAIN,
        path="/api/v1/auth/",
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.JWT_REFRESH_COOKIE_NAME,
        domain=settings.JWT_REFRESH_COOKIE_DOMAIN,
        path="/api/v1/auth/",
    )


def _blacklist_refresh_token(raw_refresh: str | None) -> None:
    if not raw_refresh:
        return
    try:
        RefreshToken(raw_refresh).blacklist()
    except Exception:
        # Не валим запрос, даже если токен битый/уже в blacklist.
        return


@api_view(["POST"])
@permission_classes([AllowAny])
def login_view(request):
    _ensure_origin_allowed(request)

    ser = LoginSerializer(data=request.data)
    ser.is_valid(raise_exception=True)
    user = ser.validated_data["user"]

    refresh = RefreshToken.for_user(user)
    access = str(refresh.access_token)

    resp = Response(
        {
            "access": access,
            "user": {
                "id": user.id,
                "username": user.username,
                "first_name": (user.first_name or "").strip(),
                "last_name": (user.last_name or "").strip(),
                "role": getattr(user, "role", None),
            },
        },
        status=status.HTTP_200_OK,
    )
    _set_refresh_cookie(resp, str(refresh))
    return resp


@api_view(["POST"])
@permission_classes([AllowAny])
def refresh_view(request):
    _ensure_origin_allowed(request)

    cookie_name = settings.JWT_REFRESH_COOKIE_NAME
    raw_refresh = request.COOKIES.get(cookie_name)
    if not raw_refresh:
        raise NotAuthenticated("Missing refresh cookie")

    try:
        refresh = RefreshToken(raw_refresh)
        access = str(refresh.access_token)

        if settings.SIMPLE_JWT.get("ROTATE_REFRESH_TOKENS", False):
            if settings.SIMPLE_JWT.get("BLACKLIST_AFTER_ROTATION", False):
                _blacklist_refresh_token(raw_refresh)
            refresh.set_jti()
            refresh.set_exp()

        resp = Response({"access": access}, status=status.HTTP_200_OK)
        _set_refresh_cookie(resp, str(refresh))
        return resp
    except Exception:
        raise AuthenticationFailed("Invalid refresh token")


@api_view(["POST"])
@permission_classes([AllowAny])
def logout_view(request):
    _ensure_origin_allowed(request)

    cookie_name = settings.JWT_REFRESH_COOKIE_NAME
    _blacklist_refresh_token(request.COOKIES.get(cookie_name))

    resp = Response({"ok": True}, status=status.HTTP_200_OK)
    _clear_refresh_cookie(resp)
    return resp


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def me_view(request):
    u = request.user
    return Response(
        {
            "id": u.id,
            "username": u.username,
            "first_name": (u.first_name or "").strip(),
            "last_name": (u.last_name or "").strip(),
            "role": getattr(u, "role", None),
        },
        status=status.HTTP_200_OK,
    )


class UserAdminViewSet(RBACActionMixin, viewsets.ModelViewSet):
    queryset = User.objects.all().order_by("id")
    serializer_class = UserAdminSerializer
    permission_classes = [IsAuthenticated, RBACPermission]
    action_perms = {
        "list": (Perm.IAM_USERS_READ,),
        "retrieve": (Perm.IAM_USERS_READ,),
        "create": (Perm.IAM_USERS_WRITE,),
        "update": (Perm.IAM_USERS_WRITE,),
        "partial_update": (Perm.IAM_USERS_WRITE,),
        "destroy": (Perm.IAM_USERS_HARD_DELETE,),
    }
