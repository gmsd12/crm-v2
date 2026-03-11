from __future__ import annotations

from django.db import models
from django.contrib.auth.models import AbstractUser


class UserRole(models.TextChoices):
    SUPERUSER = "SUPERUSER", "Superuser"
    ADMIN = "ADMIN", "Admin"
    TEAMLEADER = "TEAMLEADER", "Teamleader"
    RET = "RET", "Ret"
    MANAGER = "MANAGER", "Manager"


class User(AbstractUser):
    """
    Username-first user.
    - username: required + unique (наследуется от AbstractUser)
    - email: убрали (чтобы не лез в админку/валидации как обязательный)
    - phone: не нужен
    """

    # Убираем поля, которые тебе не нужны
    email = models.EmailField(blank=True, null=True)
    first_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)

    role = models.CharField(max_length=32, choices=UserRole.choices, default=UserRole.MANAGER, db_index=True)

    # У AbstractUser уже есть is_active/is_staff/is_superuser/last_login/date_joined/password и т.д.

    class Meta:
        db_table = "iam_users"
        indexes = [
            models.Index(fields=["role", "is_active"]),
        ]

    def __str__(self) -> str:
        return '{}'.format(self.last_name or self.username)
