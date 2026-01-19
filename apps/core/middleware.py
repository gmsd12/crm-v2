import uuid
import logging
from django.utils.deprecation import MiddlewareMixin
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

    def process_response(self, request, response):
        request_id = getattr(request, "request_id", None)
        if request_id:
            response[RESPONSE_HEADER] = request_id
        return response
