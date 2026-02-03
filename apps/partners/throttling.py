from rest_framework.throttling import SimpleRateThrottle


class PartnerTokenRateThrottle(SimpleRateThrottle):
    scope = "partner_token"

    def get_cache_key(self, request, view):
        partner_auth = getattr(request, "partner_auth", None)
        if partner_auth and getattr(partner_auth, "token", None):
            ident = str(partner_auth.token.id)
        else:
            ident = self.get_ident(request)
        return self.cache_format % {"scope": self.scope, "ident": ident}
