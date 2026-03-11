from __future__ import annotations

from datetime import datetime

from django.utils import timezone


def format_human_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return timezone.localtime(value).strftime("%d-%m-%Y %H:%M")


def format_user_short(user) -> str:
    if not user:
        return ""
    first_name = (getattr(user, "first_name", "") or "").strip()
    last_name = (getattr(user, "last_name", "") or "").strip()
    username = (getattr(user, "username", "") or "").strip()
    if last_name and first_name:
        return f"{first_name[0].upper()}. {last_name}"
    if last_name:
        return last_name
    return username or f"пользователь {getattr(user, 'id', '')}"
