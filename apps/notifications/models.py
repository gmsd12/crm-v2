from __future__ import annotations

from django.conf import settings
from django.db import models
from django.db.models import Q
from django.utils import timezone

from apps.core.models import TimeStampedModel
from apps.iam.models import UserRole


class Notification(TimeStampedModel):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    id = models.BigAutoField(primary_key=True)
    event_type = models.CharField(max_length=64, db_index=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING, db_index=True)
    scheduled_for = models.DateTimeField(default=timezone.now, db_index=True)
    sent_at = models.DateTimeField(null=True, blank=True, db_index=True)
    recipient = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="notifications")
    actor_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="triggered_notifications",
    )
    lead = models.ForeignKey(
        "leads.Lead",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="notifications",
    )
    title = models.CharField(max_length=255)
    body = models.TextField(blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)
    is_read = models.BooleanField(default=False, db_index=True)
    read_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "notifications"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["recipient", "is_read", "created_at"]),
            models.Index(fields=["recipient", "status", "scheduled_for"]),
            models.Index(fields=["recipient", "event_type", "created_at"]),
        ]

    def mark_read(self, *, at=None) -> bool:
        if self.is_read:
            return False
        self.is_read = True
        self.read_at = at or timezone.now()
        self.save(update_fields=["is_read", "read_at", "updated_at"])
        return True


class NotificationPolicy(TimeStampedModel):
    class WatchScope(models.TextChoices):
        OWN = "own", "Own"
        TEAM = "team", "Team"
        ALL = "all", "All"

    event_type = models.CharField(max_length=64, unique=True)
    enabled_by_default = models.BooleanField(default=True)
    default_repeat_minutes = models.PositiveIntegerField(default=15)
    default_watch_scope = models.CharField(
        max_length=16,
        choices=WatchScope.choices,
        default=WatchScope.OWN,
    )
    apply_to_teamleaders = models.BooleanField(default=False)
    apply_to_admins = models.BooleanField(default=False)
    apply_to_superusers = models.BooleanField(default=False)

    class Meta:
        db_table = "notification_policies"
        ordering = ["event_type"]

    def __str__(self):
        return self.event_type


class NotificationPreference(TimeStampedModel):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="notification_preferences")
    event_type = models.CharField(max_length=64, db_index=True)
    enabled = models.BooleanField(null=True, blank=True)
    repeat_minutes = models.PositiveIntegerField(null=True, blank=True)
    watch_scope = models.CharField(max_length=16, choices=NotificationPolicy.WatchScope.choices, null=True, blank=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="updated_notification_preferences",
    )

    class Meta:
        db_table = "notification_preferences"
        ordering = ["event_type", "user_id"]
        constraints = [
            models.UniqueConstraint(fields=["user", "event_type"], name="uniq_notification_preference_user_event"),
        ]

    def __str__(self):
        return f"{self.user_id}:{self.event_type}"


class NotificationOutbox(TimeStampedModel):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PROCESSING = "processing", "Processing"
        PROCESSED = "processed", "Processed"
        FAILED = "failed", "Failed"

    id = models.BigAutoField(primary_key=True)
    event_type = models.CharField(max_length=64, db_index=True)
    aggregate_type = models.CharField(max_length=64, blank=True, default="")
    aggregate_id = models.CharField(max_length=64, blank=True, default="", db_index=True)
    actor_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="notification_outbox_events",
    )
    payload = models.JSONField(default=dict, blank=True)
    dedupe_key = models.CharField(max_length=255, blank=True, default="", db_index=True)
    available_at = models.DateTimeField(default=timezone.now, db_index=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    processed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_error = models.TextField(blank=True, default="")

    class Meta:
        db_table = "notification_outbox"
        ordering = ["available_at", "id"]
        indexes = [
            models.Index(fields=["status", "available_at"]),
            models.Index(fields=["event_type", "status", "available_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["dedupe_key"],
                condition=~Q(dedupe_key=""),
                name="uniq_notification_outbox_dedupe_key_nonempty",
            ),
        ]

    def mark_processing(self, *, at=None) -> None:
        self.status = self.Status.PROCESSING
        self.attempts = (self.attempts or 0) + 1
        self.last_error = ""
        self.save(update_fields=["status", "attempts", "last_error", "updated_at"])

    def mark_processed(self, *, at=None) -> None:
        now = at or timezone.now()
        self.status = self.Status.PROCESSED
        self.processed_at = now
        self.last_error = ""
        self.save(update_fields=["status", "processed_at", "last_error", "updated_at"])

    def mark_failed(self, *, error_message: str) -> None:
        self.status = self.Status.FAILED
        self.last_error = (error_message or "").strip()
        self.save(update_fields=["status", "last_error", "updated_at"])


class NotificationWatchTarget(TimeStampedModel):
    watcher = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="notification_watch_targets",
    )
    event_type = models.CharField(max_length=64, db_index=True)
    target_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="watched_by_notification_targets",
    )
    target_role = models.CharField(max_length=32, choices=UserRole.choices, blank=True, default="")

    class Meta:
        db_table = "notification_watch_targets"
        ordering = ["event_type", "watcher_id", "id"]
        indexes = [
            models.Index(fields=["watcher", "event_type"]),
            models.Index(fields=["event_type", "target_role"]),
            models.Index(fields=["event_type", "target_user"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=(
                    (Q(target_user__isnull=False) & Q(target_role=""))
                    | (Q(target_user__isnull=True) & ~Q(target_role=""))
                ),
                name="notification_watch_target_exactly_one_target",
            ),
            models.UniqueConstraint(
                fields=["watcher", "event_type", "target_user"],
                condition=Q(target_user__isnull=False),
                name="uniq_notification_watch_target_user",
            ),
            models.UniqueConstraint(
                fields=["watcher", "event_type", "target_role"],
                condition=~Q(target_role=""),
                name="uniq_notification_watch_target_role",
            ),
        ]

    def __str__(self) -> str:
        target = f"user:{self.target_user_id}" if self.target_user_id else f"role:{self.target_role}"
        return f"{self.watcher_id}:{self.event_type}:{target}"


class NotificationDelivery(TimeStampedModel):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    id = models.BigAutoField(primary_key=True)
    notification = models.OneToOneField(
        "notifications.Notification",
        on_delete=models.CASCADE,
        related_name="delivery",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING, db_index=True)
    scheduled_for = models.DateTimeField(default=timezone.now, db_index=True)
    sent_at = models.DateTimeField(null=True, blank=True, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    dedupe_key = models.CharField(max_length=255, blank=True, default="", db_index=True)

    class Meta:
        db_table = "notification_deliveries"
        ordering = ["scheduled_for", "id"]
        indexes = [
            models.Index(fields=["status", "scheduled_for"]),
            models.Index(fields=["notification", "status"]),
            models.Index(fields=["dedupe_key", "status"]),
        ]

    def mark_sent(self, *, at=None) -> bool:
        if self.status == self.Status.SENT:
            return False
        now = at or timezone.now()
        self.status = self.Status.SENT
        self.sent_at = now
        self.last_error = ""
        self.attempts = (self.attempts or 0) + 1
        self.save(update_fields=["status", "sent_at", "last_error", "attempts", "updated_at"])
        return True

    def mark_failed(self, *, error_message: str) -> None:
        self.status = self.Status.FAILED
        self.last_error = (error_message or "").strip()
        self.attempts = (self.attempts or 0) + 1
        self.save(update_fields=["status", "last_error", "attempts", "updated_at"])

    def mark_cancelled(self) -> None:
        if self.status == self.Status.CANCELLED:
            return
        self.status = self.Status.CANCELLED
        self.save(update_fields=["status", "updated_at"])


class NotificationDeliveryAttempt(TimeStampedModel):
    class Status(models.TextChoices):
        STARTED = "started", "Started"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    id = models.BigAutoField(primary_key=True)
    delivery = models.ForeignKey(
        "notifications.NotificationDelivery",
        on_delete=models.CASCADE,
        related_name="attempt_logs",
    )
    sequence = models.PositiveIntegerField()
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.STARTED, db_index=True)
    started_at = models.DateTimeField(default=timezone.now, db_index=True)
    finished_at = models.DateTimeField(null=True, blank=True, db_index=True)
    error_message = models.TextField(blank=True, default="")

    class Meta:
        db_table = "notification_delivery_attempts"
        ordering = ["delivery_id", "sequence", "id"]
        indexes = [
            models.Index(fields=["delivery", "sequence"]),
            models.Index(fields=["status", "started_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["delivery", "sequence"],
                name="uniq_notification_delivery_attempt_sequence",
            ),
        ]

    def mark_sent(self, *, at=None) -> None:
        self.status = self.Status.SENT
        self.finished_at = at or timezone.now()
        self.error_message = ""
        self.save(update_fields=["status", "finished_at", "error_message", "updated_at"])

    def mark_failed(self, *, error_message: str, at=None) -> None:
        self.status = self.Status.FAILED
        self.finished_at = at or timezone.now()
        self.error_message = (error_message or "").strip()
        self.save(update_fields=["status", "finished_at", "error_message", "updated_at"])

    def mark_cancelled(self, *, at=None, error_message: str = "") -> None:
        self.status = self.Status.CANCELLED
        self.finished_at = at or timezone.now()
        self.error_message = (error_message or "").strip()
        self.save(update_fields=["status", "finished_at", "error_message", "updated_at"])
