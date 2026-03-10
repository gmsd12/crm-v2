import uuid
import logging
from time import monotonic
from django.utils.deprecation import MiddlewareMixin
from apps.core.logging import get_request_logger
from apps.core.request_id import normalize_request_id

REQUEST_ID_HEADER = "HTTP_X_REQUEST_ID"
RESPONSE_HEADER = "X-Request-ID"

logger = logging.getLogger("crm.request")


class RequestIdMiddleware(MiddlewareMixin):
    def process_request(self, request):
        incoming = request.META.get(REQUEST_ID_HEADER)
        request_id = normalize_request_id(incoming) or str(uuid.uuid4())

        request.request_id = request_id
        request.logger = logging.LoggerAdapter(logger, {"request_id": request_id})
        request._request_started_at = monotonic()

    def process_exception(self, request, exception):
        request_logger = get_request_logger(request)
        user_id = self._get_user_id(request)
        request_logger.exception(
            "Unhandled Django exception method=%s path=%s user_id=%s rid=%s",
            request.method,
            request.path,
            user_id or "-",
            getattr(request, "request_id", "-"),
        )
        return None

    def process_response(self, request, response):
        request_id = getattr(request, "request_id", None)
        if request_id:
            response[RESPONSE_HEADER] = request_id

        request_logger = get_request_logger(request)
        duration_ms = self._get_duration_ms(request)
        full_path = self._get_full_path(request)
        user_id = self._get_user_id(request)

        if self._should_log_api_response(request):
            request_logger.info(
                "HTTP response method=%s path=%s status=%s duration_ms=%s user_id=%s rid=%s",
                getattr(request, "method", "-"),
                full_path,
                response.status_code,
                duration_ms if duration_ms is not None else "-",
                user_id or "-",
                request_id or "-",
            )

        if response.status_code >= 500:
            request_logger.error(
                "HTTP 5xx response method=%s path=%s status=%s duration_ms=%s rid=%s",
                getattr(request, "method", "-"),
                full_path,
                response.status_code,
                duration_ms if duration_ms is not None else "-",
                request_id or "-",
            )
        return response

    def _get_user_id(self, request):
        user = getattr(request, "user", None)
        return getattr(user, "pk", None) if user is not None and getattr(user, "is_authenticated", False) else None

    def _get_duration_ms(self, request):
        started_at = getattr(request, "_request_started_at", None)
        if started_at is None:
            return None
        return int((monotonic() - started_at) * 1000)

    def _get_full_path(self, request):
        if hasattr(request, "get_full_path"):
            return request.get_full_path()
        return getattr(request, "path", "-")

    def _should_log_api_response(self, request):
        return getattr(request, "path", "").startswith("/api/")
