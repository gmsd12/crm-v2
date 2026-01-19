from __future__ import annotations

from typing import Any, Dict, Optional

from rest_framework.views import exception_handler as drf_exception_handler
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework import status


def _get_request_id(request) -> Optional[str]:
    return getattr(request, "request_id", None)


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
        # Это не DRF исключение (или упало где-то глубже). Не палим внутренности.
        payload = {
            "error": {
                "code": "internal_error",
                "message": "Internal server error",
                "details": None,
                "request_id": request_id,
            }
        }
        return Response(payload, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Нормализуем код/сообщение/детали
    code = "error"
    message = "Request failed"
    details: Any = response.data

    if isinstance(exc, ValidationError):
        code = "validation_error"
        message = "Validation failed"
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
