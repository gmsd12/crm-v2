from django.db import models
from django.utils import timezone


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True


class SoftDeleteQuerySet(models.QuerySet):
    def alive(self):
        return self.filter(is_deleted=False)

    def dead(self):
        return self.filter(is_deleted=True)

    def delete(self):
        # массовый soft-delete
        return super().update(is_deleted=True, deleted_at=timezone.now())

    def hard_delete(self):
        return super().delete()


class SoftDeleteManager(models.Manager):
    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db).alive()


class AllObjectsManager(models.Manager):
    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db)


class SoftDeleteModel(models.Model):
    is_deleted = models.BooleanField(default=False, db_index=True)
    deleted_at = models.DateTimeField(null=True, blank=True, db_index=True)

    objects = SoftDeleteManager()
    all_objects = AllObjectsManager()

    class Meta:
        abstract = True

    def delete(self, using=None, keep_parents=False):
        # одиночный soft-delete
        if not self.is_deleted:
            self.is_deleted = True
            self.deleted_at = timezone.now()
            self.save(update_fields=["is_deleted", "deleted_at"])

    def restore(self):
        if self.is_deleted:
            self.is_deleted = False
            self.deleted_at = None
            self.save(update_fields=["is_deleted", "deleted_at"])

    def hard_delete(self, using=None, keep_parents=False):
        return super().delete(using=using, keep_parents=keep_parents)


class BaseModel(TimeStampedModel, SoftDeleteModel):
    id = models.BigAutoField(primary_key=True)

    class Meta:
        abstract = True


class Notification(TimeStampedModel):
    class Channel(models.TextChoices):
        IN_APP = "in_app", "In App"

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    id = models.BigAutoField(primary_key=True)
    event_type = models.CharField(max_length=64, db_index=True)
    channel = models.CharField(max_length=32, choices=Channel.choices, default=Channel.IN_APP, db_index=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING, db_index=True)
    scheduled_for = models.DateTimeField(default=timezone.now, db_index=True)
    sent_at = models.DateTimeField(null=True, blank=True, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    recipient = models.ForeignKey("iam.User", on_delete=models.CASCADE, related_name="notifications")
    actor_user = models.ForeignKey(
        "iam.User",
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
    dedupe_key = models.CharField(max_length=255, blank=True, default="", db_index=True)
    is_read = models.BooleanField(default=False, db_index=True)
    read_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "notifications"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["recipient", "is_read", "created_at"]),
            models.Index(fields=["recipient", "status", "scheduled_for"]),
            models.Index(fields=["recipient", "event_type", "created_at"]),
            models.Index(fields=["recipient", "dedupe_key"]),
        ]

    def mark_read(self, *, at=None) -> bool:
        if self.is_read:
            return False
        self.is_read = True
        self.read_at = at or timezone.now()
        self.save(update_fields=["is_read", "read_at", "updated_at"])
        return True

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
    user = models.ForeignKey("iam.User", on_delete=models.CASCADE, related_name="notification_preferences")
    event_type = models.CharField(max_length=64, db_index=True)
    enabled = models.BooleanField(null=True, blank=True)
    repeat_minutes = models.PositiveIntegerField(null=True, blank=True)
    watch_scope = models.CharField(max_length=16, choices=NotificationPolicy.WatchScope.choices, null=True, blank=True)
    updated_by = models.ForeignKey(
        "iam.User",
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
