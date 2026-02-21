from django.contrib import admin
from .models import (
    LeadAuditLog,
    Lead,
    LeadDeposit,
    LeadComment,
    LeadDuplicateAttempt,
    LeadStatus,
    LeadIdempotencyKey,
)


class SoftDeleteAdminMixin:
    @admin.action(description="Restore selected")
    def restore_selected(self, request, queryset):
        queryset.update(is_deleted=False, deleted_at=None)

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        if hasattr(self.model, "all_objects"):
            return self.model.all_objects.get_queryset()
        return queryset


@admin.register(LeadStatus)
class LeadStatusAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "code",
        "name",
        "order",
        "is_default_for_new_leads",
        "is_active",
        "is_terminal",
        "is_valid",
        "conversion_bucket",
        "is_deleted",
    )
    list_filter = ("is_active", "is_terminal", "is_valid", "is_deleted")
    search_fields = ("code", "name")
    ordering = ("order", "code")
    actions = ("restore_selected",)


@admin.register(Lead)
class LeadAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "partner",
        "full_name",
        "phone",
        "manager",
        "first_manager",
        "manager_outcome",
        "priority",
        "status",
        "source",
        "received_at",
        "is_deleted",
    )
    list_filter = (
        "partner",
        "manager",
        "first_manager",
        "manager_outcome",
        "priority",
        "status",
        "source",
        "is_deleted",
    )
    search_fields = (
        "full_name",
        "phone",
        "email",
        "partner__code",
        "partner__name",
        "status__code",
        "source__code",
        "source__name",
    )
    ordering = ("-received_at",)
    readonly_fields = ("received_at", "created_at", "updated_at", "deleted_at")
    actions = ("restore_selected",)


@admin.register(LeadComment)
class LeadCommentAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "author", "is_pinned", "created_at", "is_deleted",)
    list_filter = ("is_pinned", "created_at", "is_deleted",)
    search_fields = ("lead__id", "lead__phone", "author__username", "body")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    actions = ("restore_selected",)


@admin.register(LeadDeposit)
class LeadDepositAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "type", "amount", "creator", "created_at", "is_deleted")
    list_filter = ("type", "creator", "is_deleted")
    search_fields = ("lead__id", "lead__phone", "creator__username")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    actions = ("restore_selected",)


@admin.register(LeadDuplicateAttempt)
class LeadDuplicateAttemptAdmin(admin.ModelAdmin):
    list_display = ("id", "partner", "source", "existing_lead", "phone", "email", "created_at")
    list_filter = ("partner", "source", "created_at")
    search_fields = ("phone", "email", "full_name", "partner__code", "partner__name")
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)


@admin.register(LeadAuditLog)
class LeadAuditLogAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "entity_type",
        "entity_id",
        "event_type",
        "lead",
        "from_status",
        "to_status",
        "actor_user",
        "source",
        "batch_id",
        "created_at",
    )
    list_filter = ("entity_type", "event_type", "source", "created_at")
    search_fields = ("entity_id", "lead__id", "lead__phone", "from_status__code", "to_status__code", "actor_user__username")
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)


@admin.register(LeadIdempotencyKey)
class LeadIdempotencyKeyAdmin(admin.ModelAdmin):
    list_display = ("id", "actor_user", "endpoint", "key", "response_status", "created_at", "updated_at")
    list_filter = ("endpoint", "response_status", "created_at")
    search_fields = ("actor_user__username", "key")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at")
