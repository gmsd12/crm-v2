from rest_framework.permissions import BasePermission


class IsPartnerAuthenticated(BasePermission):
    def has_permission(self, request, view) -> bool:
        return hasattr(request, "partner_auth") and request.partner_auth is not None
