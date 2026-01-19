from rest_framework.permissions import BasePermission
from apps.iam.models import UserRole


class HasRole(BasePermission):
    allowed_roles: set[str] = set()

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        return getattr(request.user, "role", None) in self.allowed_roles


class IsAdminOrSuperuser(HasRole):
    allowed_roles = {UserRole.ADMIN, UserRole.SUPERUSER}
