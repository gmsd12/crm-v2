from __future__ import annotations

from functools import wraps
from rest_framework.response import Response
from rest_framework import status

from apps.iam.rbac import has_all_perms, has_any_perm


def require_perms(*perms: str, require_all: bool = True):
    def decorator(fn):
        @wraps(fn)
        def wrapper(request, *args, **kwargs):
            if not request.user or not request.user.is_authenticated:
                return Response({"detail": "Authentication credentials were not provided."},
                                status=status.HTTP_401_UNAUTHORIZED)

            ok = has_all_perms(request.user, perms) if require_all else has_any_perm(request.user, perms)
            if not ok:
                return Response({"detail": "You do not have permission to perform this action."},
                                status=status.HTTP_403_FORBIDDEN)

            return fn(request, *args, **kwargs)
        return wrapper
    return decorator
