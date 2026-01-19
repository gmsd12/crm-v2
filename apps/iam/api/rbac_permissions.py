from __future__ import annotations

from rest_framework.permissions import BasePermission
from apps.iam.rbac import has_perm, has_any_perm, has_all_perms


class RBACPermission(BasePermission):
    """
    Базовый permission: задаёшь required_perms в классе или во view.
    """
    required_perms: tuple[str, ...] = ()
    require_all: bool = True

    def has_permission(self, request, view) -> bool:
        perms = getattr(view, "required_perms", self.required_perms) or ()
        require_all = getattr(view, "require_all", self.require_all)

        if not perms:
            return True  # если не указано — не блокируем

        if require_all:
            return has_all_perms(request.user, perms)
        return has_any_perm(request.user, perms)
