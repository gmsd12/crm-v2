import logging
import uuid
from time import monotonic

from asgiref.sync import iscoroutinefunction, markcoroutinefunction

from apps.core.logging import get_request_logger
from apps.core.request_id import normalize_request_id

REQUEST_ID_HEADER = "HTTP_X_REQUEST_ID"
RESPONSE_HEADER = "X-Request-ID"

logger = logging.getLogger("crm.request")


class RequestIdMiddleware:
    """
    Async-capable request middleware.
    Keeps request-id propagation + response/exception logging for both sync and async views.
    """

    sync_capable = True
    async_capable = True

    def __init__(self, get_response):
        self.get_response = get_response
        self._is_async = iscoroutinefunction(get_response)
        if self._is_async:
            markcoroutinefunction(self)

    def __call__(self, request):
        if self._is_async:
            return self._acall(request)
        return self._scall(request)

    def _prepare_request(self, request):
        incoming = request.META.get(REQUEST_ID_HEADER)
        request_id = normalize_request_id(incoming) or str(uuid.uuid4())

        request.request_id = request_id
        request.logger = logging.LoggerAdapter(logger, {"request_id": request_id})
        request._request_started_at = monotonic()

    def _log_exception(self, request):
        request_logger = get_request_logger(request)
        user_id = self._get_user_id(request)
        request_logger.exception(
            "Unhandled Django exception method=%s path=%s user_id=%s rid=%s",
            getattr(request, "method", "-"),
            getattr(request, "path", "-"),
            user_id or "-",
            getattr(request, "request_id", "-"),
        )

    def _finalize_response(self, request, response):
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

    def _scall(self, request):
        self._prepare_request(request)
        try:
            response = self.get_response(request)
        except Exception:
            self._log_exception(request)
            raise
        return self._finalize_response(request, response)

    async def _acall(self, request):
        self._prepare_request(request)
        try:
            response = await self.get_response(request)
        except Exception:
            self._log_exception(request)
            raise
        return self._finalize_response(request, response)

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
