from __future__ import annotations

from typing import Iterable

from apps.iam.models import UserRole


# 1) Набор “прав” (не Django permissions, а твой доменный RBAC)
# Это будет расширяться по мере роста CRM.
class Perm:
    # IAM
    IAM_USERS_READ = "iam.users.read"
    IAM_USERS_WRITE = "iam.users.write"
    IAM_USERS_HARD_DELETE = "iam.users.hard_delete"

    # Brands
    BRANDS_READ = "brands.read"
    BRANDS_WRITE = "brands.write"
    BRANDS_HARD_DELETE = "brands.hard_delete"

    # Leads (пример на будущее)
    LEADS_READ = "leads.read"
    LEADS_WRITE = "leads.write"
    LEADS_HARD_DELETE = "leads.hard_delete"
    LEADS_STATUS_WRITE = "leads.status.write"
    LEADS_ASSIGN_MANAGER = "leads.assign_manager"

    # Lead status catalog
    LEAD_STATUSES_READ = "lead_statuses.read"
    LEAD_STATUSES_WRITE = "lead_statuses.write"
    LEAD_STATUSES_HARD_DELETE = "lead_statuses.hard_delete"


ROLE_PERMS: dict[str, set[str]] = {
    UserRole.SUPERUSER: {
        Perm.IAM_USERS_READ, Perm.IAM_USERS_WRITE, Perm.IAM_USERS_HARD_DELETE,
        Perm.BRANDS_READ, Perm.BRANDS_WRITE, Perm.BRANDS_HARD_DELETE,
        Perm.LEADS_READ, Perm.LEADS_WRITE, Perm.LEADS_HARD_DELETE, Perm.LEADS_STATUS_WRITE, Perm.LEADS_ASSIGN_MANAGER,
        Perm.LEAD_STATUSES_READ, Perm.LEAD_STATUSES_WRITE, Perm.LEAD_STATUSES_HARD_DELETE,
    },
    UserRole.ADMIN: {
        Perm.IAM_USERS_READ, Perm.IAM_USERS_WRITE,
        Perm.BRANDS_READ, Perm.BRANDS_WRITE,
        Perm.LEADS_READ, Perm.LEADS_WRITE, Perm.LEADS_STATUS_WRITE, Perm.LEADS_ASSIGN_MANAGER,
        Perm.LEAD_STATUSES_READ, Perm.LEAD_STATUSES_WRITE,
    },
    UserRole.TEAMLEADER: {
        Perm.IAM_USERS_READ,
        Perm.BRANDS_READ,
        Perm.LEADS_READ, Perm.LEADS_WRITE, Perm.LEADS_STATUS_WRITE, Perm.LEADS_ASSIGN_MANAGER,
        Perm.LEAD_STATUSES_READ,
    },
    UserRole.MANAGER: {
        Perm.IAM_USERS_READ,
        Perm.BRANDS_READ,
        Perm.LEADS_READ, Perm.LEADS_WRITE, Perm.LEADS_STATUS_WRITE,
        Perm.LEAD_STATUSES_READ,
    },
    UserRole.RET: {
        Perm.IAM_USERS_READ,
        Perm.BRANDS_READ,
        Perm.LEADS_READ, Perm.LEADS_WRITE, Perm.LEADS_STATUS_WRITE,
        Perm.LEAD_STATUSES_READ,
    },
}


def user_role(user) -> str | None:
    return getattr(user, "role", None)


def has_perm(user, perm: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    role = user_role(user)
    if not role:
        return False
    return perm in ROLE_PERMS.get(role, set())


def has_any_perm(user, perms: Iterable[str]) -> bool:
    return any(has_perm(user, p) for p in perms)


def has_all_perms(user, perms: Iterable[str]) -> bool:
    return all(has_perm(user, p) for p in perms)
