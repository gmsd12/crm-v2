from __future__ import annotations

from django.db import models
from django.contrib.auth.models import BaseUserManager, AbstractUser


class UserRole(models.TextChoices):
    SUPERUSER = "SUPERUSER", "Superuser"
    ADMIN = "ADMIN", "Admin"
    TEAMLEADER = "TEAMLEADER", "Teamleader"
    RET = "RET", "Ret"
    MANAGER = "MANAGER", "Manager"


class UserManager(BaseUserManager):
    use_in_migrations = True

    def _create_user(self, email: str, password: str | None, **extra_fields):
        if not email:
            raise ValueError("Email is required")

        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)

        if password:
            user.set_password(password)
        else:
            user.set_unusable_password()

        user.save(using=self._db)
        return user

    def create_user(self, email: str, password: str | None = None, **extra_fields):
        extra_fields.setdefault("is_staff", False)
        extra_fields.setdefault("is_superuser", False)
        extra_fields.setdefault("is_active", True)
        extra_fields.setdefault("role", UserRole.MANAGER)
        return self._create_user(email, password, **extra_fields)

    def create_superuser(self, email: str, password: str, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        extra_fields.setdefault("is_active", True)
        extra_fields.setdefault("role", UserRole.SUPERUSER)

        if extra_fields.get("is_staff") is not True:
            raise ValueError("Superuser must have is_staff=True")
        if extra_fields.get("is_superuser") is not True:
            raise ValueError("Superuser must have is_superuser=True")

        return self._create_user(email, password, **extra_fields)


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
        return self.username
