from __future__ import annotations

from django.contrib.auth import get_user_model
from django.db.models import Q

from apps.notifications.models import NotificationPolicy, NotificationPreference
from apps.iam.models import UserRole
from apps.notifications.events import NotificationEvent

User = get_user_model()


DEFAULT_POLICY_CONFIG = {
    NotificationEvent.NEXT_CONTACT_PLANNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_ASSIGNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.COMMENT_ADDED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_UNASSIGNED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.LEAD_STATUS_CHANGED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.DEPOSIT_CREATED: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.MANAGER_NO_ACTIVITY: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.PARTNER_DUPLICATE_ATTEMPT: {
        "enabled_by_default": True,
        "default_repeat_minutes": 60,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
    NotificationEvent.NEXT_CONTACT_OVERDUE: {
        "enabled_by_default": True,
        "default_repeat_minutes": 15,
        "default_watch_scope": NotificationPolicy.WatchScope.OWN,
        "apply_to_teamleaders": True,
        "apply_to_admins": True,
        "apply_to_superusers": True,
    },
}


def event_types_for_user(*, user) -> list[str]:
    role = getattr(user, "role", None)
    if role in {UserRole.TEAMLEADER, UserRole.ADMIN, UserRole.SUPERUSER}:
        return list(DEFAULT_POLICY_CONFIG.keys())
    allowed = {
        NotificationEvent.NEXT_CONTACT_PLANNED,
        NotificationEvent.LEAD_ASSIGNED,
        NotificationEvent.COMMENT_ADDED,
        NotificationEvent.NEXT_CONTACT_OVERDUE,
        NotificationEvent.LEAD_UNASSIGNED,
    }
    return [event_type for event_type in DEFAULT_POLICY_CONFIG.keys() if event_type in allowed]


def get_or_create_policy(event_type: str) -> NotificationPolicy:
    defaults = DEFAULT_POLICY_CONFIG.get(
        event_type,
        {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": NotificationPolicy.WatchScope.OWN,
            "apply_to_teamleaders": False,
            "apply_to_admins": False,
            "apply_to_superusers": False,
        },
    )
    policy, _created = NotificationPolicy.objects.get_or_create(event_type=event_type, defaults=defaults)
    return policy


def resolve_user_notification_settings(*, user, event_type: str) -> dict:
    policy = get_or_create_policy(event_type)
    pref = NotificationPreference.objects.filter(user_id=user.id, event_type=event_type).first()
    enabled = policy.enabled_by_default if pref is None or pref.enabled is None else pref.enabled
    repeat_minutes = (
        policy.default_repeat_minutes if pref is None or pref.repeat_minutes is None else pref.repeat_minutes
    )
    watch_scope = policy.default_watch_scope if pref is None or not pref.watch_scope else pref.watch_scope
    return {
        "policy": policy,
        "enabled": bool(enabled),
        "repeat_minutes": max(1, int(repeat_minutes or 1)),
        "watch_scope": watch_scope,
    }


def resolve_user_settings_cached(*, user, event_type: str, cache: dict[tuple[int, str], dict]) -> dict:
    key = (user.id, event_type)
    settings = cache.get(key)
    if settings is None:
        settings = resolve_user_notification_settings(user=user, event_type=event_type)
        cache[key] = settings
    return settings


def policy_candidate_watchers(*, policy: NotificationPolicy):
    query = User.objects.filter(is_active=True)
    role_q = Q()
    if policy.apply_to_teamleaders:
        role_q |= Q(role=UserRole.TEAMLEADER)
    if policy.apply_to_admins:
        role_q |= Q(role=UserRole.ADMIN)
    if policy.apply_to_superusers:
        role_q |= Q(role=UserRole.SUPERUSER)
    if not role_q:
        return query.none()
    return query.filter(role_q)
