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
        return f"{self.name} ({self.code})"


class PartnerSource(BaseModel):
    """
    Гибкий source: это может быть GEO (ru), либо ads source (google/fb),
    либо комбинация (ru_google) — решает партнёр/админ как удобно.
    """
    partner = models.ForeignKey(Partner, on_delete=models.CASCADE, related_name="sources")
    name = models.CharField(max_length=255)               # "RU", "RU Google", "FB", "TikTok RU"
    code = models.SlugField(max_length=64)                # "ru", "ru_google", "fb" ...
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "partner_sources"
        constraints = [
            models.UniqueConstraint(fields=["partner", "code"], name="uniq_partner_source_code"),
        ]
        indexes = [
            models.Index(fields=["partner", "code"]),
        ]

    def __str__(self) -> str:
        return f"{self.partner.code}:{self.code}"


class PartnerToken(BaseModel):
    """
    Токен доступа к Partner API.
    raw токен показываем только при создании; в БД храним hash.
    """
    partner = models.ForeignKey(Partner, on_delete=models.CASCADE, related_name="tokens")
    name = models.CharField(max_length=255, blank=True)  # "prod", "tiktok", etc

    prefix = models.CharField(max_length=12, db_index=True)  # для поиска/аудита
    token_hash = models.CharField(max_length=64, db_index=True)  # sha256 hex

    # optional: если токен "привязан" к одному source, можно автоставить source при создании лида
    source = models.ForeignKey(PartnerSource, null=True, blank=True, on_delete=models.SET_NULL)

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
    def build(cls, *, partner: Partner, raw_token: str, name: str = "", source: PartnerSource | None = None):
        token_hash = cls.hash_token(raw_token)
        prefix = raw_token[:12]
        return cls(partner=partner, name=name, prefix=prefix, token_hash=token_hash, source=source)
