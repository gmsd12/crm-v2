import re
import uuid

_MAX_LEN = 128
_ALLOWED = re.compile(r"^[a-zA-Z0-9\-_:]+$")


def normalize_request_id(raw: str | None) -> str | None:
    if not raw:
        return None

    value = raw.strip()
    if not value:
        return None

    # Частый кейс: request-id реально UUID
    try:
        return str(uuid.UUID(value))
    except Exception:
        pass

    # Если не UUID — режем длину и проверяем символы
    if len(value) > _MAX_LEN:
        return None

    if not _ALLOWED.match(value):
        return None

    return value
