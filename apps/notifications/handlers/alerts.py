from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

from apps.leads.models import LeadDuplicateAttempt
from apps.notifications.events import NotificationEvent
from apps.notifications.models import NotificationPolicy
from apps.notifications.policies import get_or_create_policy, resolve_user_settings_cached, policy_candidate_watchers
from apps.notifications.recipients import lead_matches_scope
from apps.notifications.runtime import NotificationEmitPayload, emit

User = get_user_model()


def emit_partner_duplicate_attempt_notification(*, attempt_id: int, now=None) -> int:
    now = now or timezone.now()
    attempt = (
        LeadDuplicateAttempt.objects.select_related("partner", "existing_lead", "existing_lead__manager")
        .filter(id=attempt_id)
        .only(
            "id",
            "partner_id",
            "source",
            "existing_lead_id",
            "created_at",
            "phone",
            "full_name",
            "email",
            "partner__id",
            "partner__code",
            "partner__name",
        )
        .first()
    )
    if attempt is None:
        return 0

    window_minutes = max(1, int(getattr(settings, "NOTIFICATIONS_PARTNER_DUPLICATE_WINDOW_MINUTES", 60)))
    threshold = max(1, int(getattr(settings, "NOTIFICATIONS_PARTNER_DUPLICATE_THRESHOLD", 10)))
    window_start = now - timedelta(minutes=window_minutes)
    attempts_count = LeadDuplicateAttempt.objects.filter(
        partner_id=attempt.partner_id,
        created_at__gte=window_start,
        created_at__lte=now,
    ).count()
    if attempts_count < threshold:
        return 0

    threshold_block = attempts_count // threshold
    if threshold_block <= 0:
        return 0

    policy = get_or_create_policy(NotificationEvent.PARTNER_DUPLICATE_ATTEMPT)
    recipients = []
    settings_cache: dict[tuple[int, str], dict] = {}
    for watcher in policy_candidate_watchers(policy=policy):
        user_settings = resolve_user_settings_cached(
            user=watcher,
            event_type=NotificationEvent.PARTNER_DUPLICATE_ATTEMPT,
            cache=settings_cache,
        )
        if not user_settings["enabled"]:
            continue
        watch_scope = user_settings["watch_scope"]
        if attempt.existing_lead_id is not None and attempt.existing_lead is not None:
            if not lead_matches_scope(
                event_type=NotificationEvent.PARTNER_DUPLICATE_ATTEMPT,
                lead=attempt.existing_lead,
                watcher=watcher,
                watch_scope=watch_scope,
            ):
                continue
        elif watch_scope != NotificationPolicy.WatchScope.ALL:
            continue
        recipients.append(watcher)

    if not recipients:
        return 0

    window_slot = int(now.timestamp() // (window_minutes * 60))
    created = 0
    partner_label = getattr(attempt.partner, "name", "") or getattr(attempt.partner, "code", "") or f"Партнер #{attempt.partner_id}"
    for recipient in recipients:
        notification = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.PARTNER_DUPLICATE_ATTEMPT,
                recipient_id=recipient.id,
                actor_user_id=None,
                lead_id=attempt.existing_lead_id,
                title=f"Алерт по дублям от партнера: {partner_label}",
                body=f"{attempts_count} дублей загрузки за последние {window_minutes} минут",
                payload={
                    "attempt_id": str(attempt.id),
                    "partner_id": str(attempt.partner_id),
                    "source": attempt.source or "",
                    "existing_lead_id": str(attempt.existing_lead_id) if attempt.existing_lead_id else None,
                    "phone": attempt.phone,
                    "attempts_count": attempts_count,
                    "threshold": threshold,
                    "window_minutes": window_minutes,
                    "threshold_block": threshold_block,
                },
                dedupe_key=f"partner_duplicate_attempt:{attempt.partner_id}:{recipient.id}:{window_slot}:{threshold_block}",
            )
        )
        if notification is not None:
            created += 1
    return created
