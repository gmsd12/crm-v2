from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.notifications.handlers.followups import emit_manager_no_activity_notifications


class Command(BaseCommand):
    help = "Emit in-app notifications for managers with overdue leads above threshold"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Optional limit for processed managers")

    def handle(self, *args, **options):
        limit = options.get("limit") or None
        created = emit_manager_no_activity_notifications(limit=limit)
        self.stdout.write(self.style.SUCCESS(f"Created manager_no_activity notifications: {created}"))
