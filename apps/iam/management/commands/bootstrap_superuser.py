from __future__ import annotations

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand

from apps.iam.models import UserRole


DEFAULT_USERNAME = "chtnr"
DEFAULT_PASSWORD = "QwKl79$3H"


class Command(BaseCommand):
    help = "Creates or updates the default bootstrap superuser."

    def handle(self, *args, **options):
        user_model = get_user_model()
        user, created = user_model.objects.get_or_create(
            username=DEFAULT_USERNAME,
            defaults={
                "role": UserRole.SUPERUSER,
                "is_active": True,
                "is_staff": True,
                "is_superuser": True,
            },
        )

        user.role = UserRole.SUPERUSER
        user.is_active = True
        user.is_staff = True
        user.is_superuser = True
        user.set_password(DEFAULT_PASSWORD)
        user.save(update_fields=["role", "is_active", "is_staff", "is_superuser", "password"])

        action = "created" if created else "updated"
        self.stdout.write(
            self.style.SUCCESS(
                f"Bootstrap superuser {DEFAULT_USERNAME!r} {action} successfully."
            )
        )
