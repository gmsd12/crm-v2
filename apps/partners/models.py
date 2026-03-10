from __future__ import annotations

import secrets
import hashlib
from django.db import models
from apps.core.models import BaseModel


class Partner(BaseModel):
    name = models.CharField(max_length=255)
    code = models.SlugField(max_length=64, unique=True)  # удобный человеко-код
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "partners"

    def __str__(self) -> str:
        return f"{self.name}"


class PartnerToken(BaseModel):
    """
    Токен доступа к Partner API.
    raw токен показываем только при создании; в БД храним hash.
    """
    partner = models.ForeignKey(Partner, on_delete=models.CASCADE, related_name="tokens")
    name = models.CharField(max_length=255, blank=True)  # "prod", "tiktok", etc

    prefix = models.CharField(max_length=12, db_index=True)  # для поиска/аудита
    token_hash = models.CharField(max_length=64, db_index=True)  # sha256 hex

    # optional: если токен "привязан" к source-строке, можно автоставить её при создании лида
    source = models.CharField(max_length=128, blank=True, default="")

    is_active = models.BooleanField(default=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "partner_tokens"
        indexes = [
            models.Index(fields=["partner", "is_active"]),
        ]

    @staticmethod
    def generate_raw_token() -> str:
        # длинный, url-safe
        return secrets.token_urlsafe(48)

    @staticmethod
    def hash_token(raw: str) -> str:
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @classmethod
    def build(cls, *, partner: Partner, raw_token: str, name: str = "", source: str | None = ""):
        token_hash = cls.hash_token(raw_token)
        prefix = raw_token[:12]
        return cls(partner=partner, name=name, prefix=prefix, token_hash=token_hash, source=(source or ""))
