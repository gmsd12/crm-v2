from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.notifications.handlers.followups import emit_next_contact_overdue_notifications


class Command(BaseCommand):
    help = "Emit in-app notifications for leads with overdue next_contact_at"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Optional limit for processed overdue leads")

    def handle(self, *args, **options):
        limit = options.get("limit") or None
        created = emit_next_contact_overdue_notifications(limit=limit)
        self.stdout.write(self.style.SUCCESS(f"Created overdue notifications: {created}"))

