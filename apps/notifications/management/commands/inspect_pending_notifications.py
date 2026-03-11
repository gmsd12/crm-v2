from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from apps.notifications.models import NotificationDelivery


class Command(BaseCommand):
    help = "Inspect in-app notification deliveries stuck in pending state"

    def add_arguments(self, parser):
        parser.add_argument(
            "--older-than-minutes",
            type=int,
            default=15,
            help="Only show pending deliveries scheduled at or before now - N minutes",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Maximum number of stuck deliveries to print",
        )

    def handle(self, *args, **options):
        older_than_minutes = max(1, int(options.get("older_than_minutes") or 15))
        limit = max(1, int(options.get("limit") or 50))
        cutoff = timezone.now() - timedelta(minutes=older_than_minutes)

        deliveries = list(
            NotificationDelivery.objects.select_related(
                "notification",
                "notification__recipient",
                "notification__actor_user",
                "notification__lead",
            )
            .filter(
                status=NotificationDelivery.Status.PENDING,
                scheduled_for__lte=cutoff,
            )
            .order_by("scheduled_for", "id")[:limit]
        )

        if not deliveries:
            self.stdout.write(
                self.style.SUCCESS(
                    f"No stuck pending deliveries older than {older_than_minutes} minutes."
                )
            )
            return

        self.stdout.write(
            self.style.WARNING(
                f"Found {len(deliveries)} pending deliveries older than {older_than_minutes} minutes:"
            )
        )
        for delivery in deliveries:
            notification = delivery.notification
            recipient = notification.recipient.username if notification.recipient_id else "-"
            lead_id = notification.lead_id or "-"
            self.stdout.write(
                (
                    f"[delivery={delivery.id}] event={notification.event_type} "
                    f"recipient={recipient} lead={lead_id} "
                    f"scheduled_for={delivery.scheduled_for.isoformat()} "
                    f"attempts={delivery.attempts} dedupe_key={delivery.dedupe_key or '-'} "
                    f"last_error={(delivery.last_error or '-').strip()}"
                )
            )
