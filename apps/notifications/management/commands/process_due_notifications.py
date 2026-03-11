from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.notifications.runtime import process_due_notifications


class Command(BaseCommand):
    help = "Deliver due pending in-app notifications"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=500, help="Maximum pending notifications to process")

    def handle(self, *args, **options):
        limit = max(1, int(options.get("limit") or 500))
        delivered = process_due_notifications(limit=limit)
        self.stdout.write(self.style.SUCCESS(f"Delivered notifications: {delivered}"))

