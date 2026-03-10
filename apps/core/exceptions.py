from __future__ import annotations

from typing import Any, Dict, Optional

from apps.core.logging import get_request_logger
from rest_framework.views import exception_handler as drf_exception_handler
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework import status


def _get_request_id(request) -> Optional[str]:
    return getattr(request, "request_id", None)


def _log_unhandled_api_exception(exc, context, request, request_id: str | None) -> None:
    logger = get_request_logger(request) if request is not None else None
    view = context.get("view")
    view_name = view.__class__.__name__ if view is not None else "-"
    action = getattr(view, "action", "-") if view is not None else "-"
    method = getattr(request, "method", "-") if request is not None else "-"
    path = getattr(request, "path", "-") if request is not None else "-"
    user = getattr(request, "user", None) if request is not None else None
    user_id = getattr(user, "pk", None) if user is not None and getattr(user, "is_authenticated", False) else None
    message = (
        "Unhandled API exception "
        f"method={method} path={path} view={view_name} action={action} "
        f"user_id={user_id or '-'} rid={request_id or '-'}"
    )
    if logger is not None:
        logger.exception(message)


def custom_exception_handler(exc, context):
    """
    Unified error format for all DRF exceptions.

    Response shape:
    {
      "error": {
        "code": "...",
        "message": "...",
        "details": ...,
        "request_id": "..."
      }
    }
    """
    request = context.get("request")
    request_id = _get_request_id(request) if request else None

    # DRF стандартная обработка (NotFound, PermissionDenied, APIException, etc.)
    response = drf_exception_handler(exc, context)

    if response is None:
        _log_unhandled_api_exception(exc, context, request, request_id)
        # Это не DRF исключение (или упало где-то глубже). Не палим внутренности.
        payload = {
            "error": {
                "code": "internal_error",
                "message": "Внутренняя ошибка сервера",
                "details": None,
                "request_id": request_id,
            }
        }
        return Response(payload, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Нормализуем код/сообщение/детали
    code = "error"
    message = "Запрос не выполнен"
    details: Any = response.data

    if isinstance(exc, ValidationError):
        code = "validation_error"
        message = "Ошибка валидации"
    else:
        # DRF APIException часто имеет атрибут default_code
        code = getattr(exc, "default_code", "error")
        # Иногда DRF кладет {"detail": "..."}
        if isinstance(response.data, dict) and "detail" in response.data:
            message = str(response.data.get("detail"))
            details = None

    response.data = {
        "error": {
            "code": str(code),
            "message": message,
            "details": details,
            "request_id": request_id,
        }
    }
    # Ensure request id header present even on exceptions
    if request_id:
        response["X-Request-ID"] = request_id
    return response
