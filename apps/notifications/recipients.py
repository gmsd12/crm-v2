from __future__ import annotations

from django.contrib.auth import get_user_model
from django.db import transaction

from apps.notifications.policies import (
    get_or_create_policy,
    policy_candidate_watchers,
    resolve_user_settings_cached,
)
from apps.notifications.models import NotificationPolicy, NotificationWatchTarget

User = get_user_model()


def _user_payload(user) -> dict:
    return {
        "id": str(user.id),
        "username": user.username,
        "first_name": (user.first_name or "").strip(),
        "last_name": (user.last_name or "").strip(),
        "role": getattr(user, "role", None),
    }


def _watch_targets_cache_get(*, watcher, event_type: str, cache: dict[tuple[int, str], dict] | None = None) -> dict:
    key = (watcher.id, event_type)
    if cache is not None and key in cache:
        return cache[key]

    rows = list(
        NotificationWatchTarget.objects.select_related("target_user")
        .filter(watcher_id=watcher.id, event_type=event_type)
        .order_by("target_role", "target_user_id", "id")
    )
    target_user_ids = []
    users = []
    roles = []
    for row in rows:
        if row.target_user_id and row.target_user and getattr(row.target_user, "is_active", False):
            target_user_ids.append(row.target_user_id)
            users.append(_user_payload(row.target_user))
        elif row.target_role:
            roles.append(row.target_role)

    value = {
        "user_ids": sorted(dict.fromkeys(target_user_ids)),
        "users": users,
        "roles": sorted(dict.fromkeys(roles)),
    }
    if cache is not None:
        cache[key] = value
    return value


def get_notification_watch_targets(*, watcher, event_type: str) -> dict:
    return _watch_targets_cache_get(watcher=watcher, event_type=event_type, cache=None)


def replace_notification_watch_targets(
    *,
    watcher,
    event_type: str,
    watched_user_ids: list[int] | None = None,
    watched_roles: list[str] | None = None,
) -> dict:
    current = get_notification_watch_targets(watcher=watcher, event_type=event_type)
    resolved_roles = current["roles"] if watched_roles is None else sorted(dict.fromkeys([str(role) for role in watched_roles if role]))
    resolved_user_ids = current["user_ids"] if watched_user_ids is None else sorted(
        dict.fromkeys([int(user_id) for user_id in watched_user_ids if user_id])
    )

    if resolved_user_ids:
        active_user_ids = set(
            User.objects.filter(id__in=resolved_user_ids, is_active=True).values_list("id", flat=True)
        )
        missing = [user_id for user_id in resolved_user_ids if user_id not in active_user_ids]
        if missing:
            raise ValueError(f"Unknown or inactive users: {', '.join(str(item) for item in missing)}")

    with transaction.atomic():
        NotificationWatchTarget.objects.filter(watcher_id=watcher.id, event_type=event_type).delete()
        NotificationWatchTarget.objects.bulk_create(
            [
                NotificationWatchTarget(
                    watcher_id=watcher.id,
                    event_type=event_type,
                    target_user_id=user_id,
                )
                for user_id in resolved_user_ids
            ]
            + [
                NotificationWatchTarget(
                    watcher_id=watcher.id,
                    event_type=event_type,
                    target_role=role,
                )
                for role in resolved_roles
            ]
        )

    return get_notification_watch_targets(watcher=watcher, event_type=event_type)


def lead_matches_scope(
    *,
    event_type: str,
    lead,
    watcher,
    watch_scope: str,
    manager_id: int | None = None,
    manager_role: str | None = None,
    watch_targets_cache: dict[tuple[int, str], dict] | None = None,
) -> bool:
    if watch_scope == NotificationPolicy.WatchScope.ALL:
        return True
    resolved_manager_id = manager_id if manager_id is not None else lead.manager_id
    if watch_scope == NotificationPolicy.WatchScope.OWN:
        return resolved_manager_id == watcher.id
    if resolved_manager_id is None:
        return False
    if resolved_manager_id == watcher.id:
        return True

    targets = _watch_targets_cache_get(
        watcher=watcher,
        event_type=event_type,
        cache=watch_targets_cache,
    )
    if resolved_manager_id in targets["user_ids"]:
        return True

    resolved_manager_role = manager_role if manager_role is not None else getattr(lead.manager, "role", None)
    if resolved_manager_role and resolved_manager_role in targets["roles"]:
        return True
    return False


def manager_matches_scope(
    *,
    event_type: str,
    manager,
    watcher,
    watch_scope: str,
    watch_targets_cache: dict[tuple[int, str], dict] | None = None,
) -> bool:
    if watch_scope == NotificationPolicy.WatchScope.ALL:
        return True
    if watch_scope == NotificationPolicy.WatchScope.OWN:
        return manager.id == watcher.id
    if manager.id == watcher.id:
        return True

    targets = _watch_targets_cache_get(
        watcher=watcher,
        event_type=event_type,
        cache=watch_targets_cache,
    )
    if manager.id in targets["user_ids"]:
        return True
    return getattr(manager, "role", None) in set(targets["roles"])


def resolve_lead_event_recipients(
    *,
    event_type: str,
    lead,
    primary_recipient_ids: list[int] | None = None,
    manager_id_for_scope: int | None = None,
    manager_role_for_scope: str | None = None,
    excluded_recipient_ids: list[int] | None = None,
) -> list:
    policy = get_or_create_policy(event_type)
    excluded_ids = {int(recipient_id) for recipient_id in (excluded_recipient_ids or []) if recipient_id}
    recipients_map = {}

    for user in User.objects.filter(id__in=(primary_recipient_ids or []), is_active=True):
        if user.id in excluded_ids:
            continue
        recipients_map[user.id] = user

    for watcher in policy_candidate_watchers(policy=policy):
        recipients_map[watcher.id] = watcher

    settings_cache: dict[tuple[int, str], dict] = {}
    watch_targets_cache: dict[tuple[int, str], dict] = {}
    recipients = []
    for recipient in recipients_map.values():
        settings = resolve_user_settings_cached(user=recipient, event_type=event_type, cache=settings_cache)
        if not settings["enabled"]:
            continue
        if not lead_matches_scope(
            event_type=event_type,
            lead=lead,
            watcher=recipient,
            watch_scope=settings["watch_scope"],
            manager_id=manager_id_for_scope,
            manager_role=manager_role_for_scope,
            watch_targets_cache=watch_targets_cache,
        ):
            continue
        recipients.append(recipient)
    return recipients


def resolve_manager_event_recipients(*, event_type: str, manager) -> list:
    policy = get_or_create_policy(event_type)
    settings_cache: dict[tuple[int, str], dict] = {}
    watch_targets_cache: dict[tuple[int, str], dict] = {}
    recipients = []
    for watcher in policy_candidate_watchers(policy=policy):
        settings = resolve_user_settings_cached(user=watcher, event_type=event_type, cache=settings_cache)
        if not settings["enabled"]:
            continue
        if not manager_matches_scope(
            event_type=event_type,
            manager=manager,
            watcher=watcher,
            watch_scope=settings["watch_scope"],
            watch_targets_cache=watch_targets_cache,
        ):
            continue
        recipients.append(watcher)
    return recipients
