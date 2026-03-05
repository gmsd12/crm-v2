from django.contrib import admin
from apps.core.models import Notification, NotificationPolicy, NotificationPreference


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "created_at",
        "event_type",
        "channel",
        "status",
        "scheduled_for",
        "sent_at",
        "recipient",
        "actor_user",
        "lead",
        "is_read",
        "read_at",
    )
    list_filter = ("event_type", "channel", "status", "is_read", "created_at")
    search_fields = ("title", "body", "recipient__username", "actor_user__username", "lead__phone", "lead__full_name")
    ordering = ("-created_at", "-id")


@admin.register(NotificationPolicy)
class NotificationPolicyAdmin(admin.ModelAdmin):
    list_display = (
        "event_type",
        "enabled_by_default",
        "default_repeat_minutes",
        "default_watch_scope",
        "apply_to_teamleaders",
        "apply_to_admins",
        "apply_to_superusers",
    )
    search_fields = ("event_type",)
    ordering = ("event_type",)


@admin.register(NotificationPreference)
class NotificationPreferenceAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "event_type", "enabled", "repeat_minutes", "watch_scope", "updated_by", "updated_at")
    list_filter = ("event_type", "enabled", "watch_scope")
    search_fields = ("user__username", "event_type")
    ordering = ("-updated_at", "-id")
