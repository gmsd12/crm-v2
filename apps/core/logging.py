import logging


class RequestIdFilter(logging.Filter):
    """
    Injects request_id into log records.
    If request_id is absent, sets it to "-".
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = "-"
        return True
