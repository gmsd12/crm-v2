from __future__ import annotations

import re

GEO_CODE_RE = re.compile(r"^[A-Z]{2}$")

try:
    import phonenumbers
    from phonenumbers import NumberParseException
except ImportError:  # pragma: no cover - graceful fallback if dependency is not installed yet
    phonenumbers = None
    NumberParseException = Exception


def normalize_geo_code(value: str | None) -> str:
    return (value or "").strip().upper()


def infer_geo_from_phone(phone: str | None) -> str:
    if not phonenumbers:
        return ""

    raw_phone = str(phone or "").strip()
    if not raw_phone:
        return ""

    digits = "".join(char for char in raw_phone if char.isdigit())
    if not digits:
        return ""

    if len(digits) == 11 and digits.startswith("8"):
        digits = f"7{digits[1:]}"
    elif len(digits) == 10:
        digits = f"7{digits}"

    try:
        parsed_number = phonenumbers.parse(f"+{digits}", None)
    except NumberParseException:
        return ""

    region = phonenumbers.region_code_for_number(parsed_number) or ""
    return normalize_geo_code(region)


def resolve_geo(*, phone: str | None, provided_geo: str | None) -> str:
    normalized_geo = normalize_geo_code(provided_geo)
    inferred_geo = infer_geo_from_phone(phone)
    return inferred_geo or normalized_geo
