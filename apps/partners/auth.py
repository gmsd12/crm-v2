from __future__ import annotations

from django.utils import timezone
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed

from apps.partners.models import PartnerToken


class PartnerAuthResult:
    def __init__(self, token: PartnerToken):
        self.token = token
        self.partner = token.partner
        self.source = token.source


class PartnerTokenAuthentication(BaseAuthentication):
    """
    Auth for Partner API.
    Reads token from:
      - X-Partner-Token
      - Authorization: Bearer <token>
    Sets request.partner_auth (PartnerAuthResult).
    """

    def authenticate(self, request):
        raw = request.headers.get("X-Partner-Token")

        if not raw:
            auth = request.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                raw = auth.split(" ", 1)[1].strip()

        if not raw:
            return None  # DRF will treat as unauthenticated

        raw = raw.strip()
        if len(raw) < 20:
            raise AuthenticationFailed("Invalid partner token")

        from apps.partners.models import PartnerToken as PT  # avoid circular
        token_hash = PT.hash_token(raw)
        prefix = raw[:12]

        token = (
            PartnerToken.objects
            .select_related("partner", "source")
            .filter(prefix=prefix, token_hash=token_hash, is_active=True, revoked_at__isnull=True)
            .first()
        )
        if not token:
            raise AuthenticationFailed("Invalid partner token")

        if not token.partner.is_active:
            raise AuthenticationFailed("Partner is inactive")

        if token.expires_at and token.expires_at <= timezone.now():
            raise AuthenticationFailed("Partner token expired")

        # touch last_used_at (не каждую миллисекунду, но пока просто)
        PartnerToken.objects.filter(pk=token.pk).update(last_used_at=timezone.now())

        request.partner_auth = PartnerAuthResult(token)
        # Возвращаем (user, auth) как требует DRF; user нам не нужен => None
        return (None, token)

    def authenticate_header(self, request):
        return "Bearer"
