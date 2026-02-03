from __future__ import annotations

from django.conf import settings
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from rest_framework_simplejwt.tokens import RefreshToken

from .serializers import LoginSerializer


def _origin_allowed(request) -> bool:
    origin = request.headers.get("Origin")
    # Если Origin отсутствует (например curl) — разрешаем.
    if not origin:
        return True
    return origin in getattr(settings, "CORS_ALLOWED_ORIGINS", [])


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
    if not _origin_allowed(request):
        return Response({"detail": "Origin not allowed"}, status=status.HTTP_403_FORBIDDEN)

    ser = LoginSerializer(data=request.data)
    ser.is_valid(raise_exception=True)
    user = ser.validated_data["user"]

    refresh = RefreshToken.for_user(user)
    access = str(refresh.access_token)

    resp = Response(
        {
            "access": access,
            "user": {"id": user.id, "username": user.username, "role": getattr(user, "role", None)},
        },
        status=status.HTTP_200_OK,
    )
    _set_refresh_cookie(resp, str(refresh))
    return resp


@api_view(["POST"])
@permission_classes([AllowAny])
def refresh_view(request):
    if not _origin_allowed(request):
        return Response({"detail": "Origin not allowed"}, status=status.HTTP_403_FORBIDDEN)

    cookie_name = settings.JWT_REFRESH_COOKIE_NAME
    raw_refresh = request.COOKIES.get(cookie_name)
    if not raw_refresh:
        return Response({"detail": "Missing refresh cookie"}, status=status.HTTP_401_UNAUTHORIZED)

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
        return Response({"detail": "Invalid refresh token"}, status=status.HTTP_401_UNAUTHORIZED)


@api_view(["POST"])
@permission_classes([AllowAny])
def logout_view(request):
    if not _origin_allowed(request):
        return Response({"detail": "Origin not allowed"}, status=status.HTTP_403_FORBIDDEN)

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
        {"id": u.id, "username": u.username, "role": getattr(u, "role", None)},
        status=status.HTTP_200_OK,
    )
