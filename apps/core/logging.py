import logging


class RequestIdFilter(logging.Filter):
    """
    Injects request_id into log records.
    If request_id is absent, sets it to "-".
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            request = getattr(record, "request", None)
            record.request_id = getattr(request, "request_id", "-") if request is not None else "-"
        return True


def get_request_logger(request) -> logging.LoggerAdapter:
    request_id = getattr(request, "request_id", "-")
    existing = getattr(request, "logger", None)
    if existing is not None:
        return existing
    return logging.LoggerAdapter(logging.getLogger("crm.request"), {"request_id": request_id})
