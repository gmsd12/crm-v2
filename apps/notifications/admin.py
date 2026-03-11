from django.contrib import admin

from apps.notifications.models import (
    Notification,
    NotificationDelivery,
    NotificationDeliveryAttempt,
    NotificationOutbox,
    NotificationPolicy,
    NotificationPreference,
    NotificationWatchTarget,
)


class NotificationDeliveryAttemptInline(admin.TabularInline):
    model = NotificationDeliveryAttempt
    extra = 0
    can_delete = False
    fields = ("sequence", "status", "started_at", "finished_at", "error_message", "created_at", "updated_at")
    readonly_fields = fields
    ordering = ("sequence", "id")


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "created_at",
        "event_type",
        "status",
        "scheduled_for",
        "sent_at",
        "recipient",
        "actor_user",
        "lead",
        "delivery_status",
        "delivery_attempts",
        "is_read",
        "read_at",
    )
    list_filter = ("event_type", "status", "is_read", "created_at")
    search_fields = ("title", "body", "recipient__username", "actor_user__username", "lead__phone", "lead__full_name")
    ordering = ("-created_at", "-id")
    list_select_related = ("recipient", "actor_user", "lead")

    @admin.display(description="Delivery")
    def delivery_status(self, obj):
        try:
            delivery = obj.delivery
        except Notification.delivery.RelatedObjectDoesNotExist:
            delivery = None
        if delivery is None:
            return "-"
        return delivery.status

    @admin.display(description="Attempts")
    def delivery_attempts(self, obj):
        try:
            delivery = obj.delivery
        except Notification.delivery.RelatedObjectDoesNotExist:
            delivery = None
        if delivery is None:
            return 0
        return delivery.attempts


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


@admin.register(NotificationOutbox)
class NotificationOutboxAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "event_type",
        "aggregate_type",
        "aggregate_id",
        "status",
        "attempts",
        "available_at",
        "processed_at",
        "actor_user",
    )
    list_filter = ("event_type", "status", "aggregate_type", "created_at")
    search_fields = ("event_type", "aggregate_id", "dedupe_key", "last_error", "actor_user__username")
    readonly_fields = ("created_at", "updated_at")
    list_select_related = ("actor_user",)
    ordering = ("status", "available_at", "id")


@admin.register(NotificationDelivery)
class NotificationDeliveryAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "notification_id",
        "notification_event_type",
        "recipient",
        "status",
        "scheduled_for",
        "sent_at",
        "attempts",
        "last_error_short",
        "dedupe_key",
    )
    list_filter = ("status", "scheduled_for", "created_at")
    search_fields = (
        "notification__event_type",
        "notification__recipient__username",
        "notification__title",
        "dedupe_key",
        "last_error",
    )
    readonly_fields = ("created_at", "updated_at")
    list_select_related = ("notification", "notification__recipient")
    ordering = ("status", "scheduled_for", "id")
    inlines = (NotificationDeliveryAttemptInline,)

    @admin.display(description="Event")
    def notification_event_type(self, obj):
        return obj.notification.event_type

    @admin.display(description="Recipient")
    def recipient(self, obj):
        return obj.notification.recipient

    @admin.display(description="Last error")
    def last_error_short(self, obj):
        if not obj.last_error:
            return "-"
        return obj.last_error[:80]


@admin.register(NotificationDeliveryAttempt)
class NotificationDeliveryAttemptAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "delivery_id",
        "notification_id",
        "sequence",
        "status",
        "started_at",
        "finished_at",
        "error_message_short",
    )
    list_filter = ("status", "started_at", "finished_at")
    search_fields = (
        "delivery__notification__event_type",
        "delivery__notification__recipient__username",
        "error_message",
    )
    readonly_fields = ("created_at", "updated_at")
    list_select_related = ("delivery", "delivery__notification", "delivery__notification__recipient")
    ordering = ("-started_at", "-id")

    @admin.display(description="Notification")
    def notification_id(self, obj):
        return obj.delivery.notification_id

    @admin.display(description="Error")
    def error_message_short(self, obj):
        if not obj.error_message:
            return "-"
        return obj.error_message[:80]


@admin.register(NotificationWatchTarget)
class NotificationWatchTargetAdmin(admin.ModelAdmin):
    list_display = ("id", "watcher", "event_type", "target_user", "target_role", "created_at")
    list_filter = ("event_type", "target_role", "created_at")
    search_fields = ("watcher__username", "target_user__username", "event_type", "target_role")
    readonly_fields = ("created_at", "updated_at")
    list_select_related = ("watcher", "target_user")
    ordering = ("event_type", "watcher_id", "id")
