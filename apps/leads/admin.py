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
    @admin.action(description="Soft delete selected")
    def soft_delete_selected(self, request, queryset):
        queryset.delete()

    @admin.action(description="Restore selected")
    def restore_selected(self, request, queryset):
        queryset.update(is_deleted=False, deleted_at=None)

    def get_actions(self, request):
        actions = super().get_actions(request)
        if request.user.is_superuser:
            # Keep default Django bulk delete, but with explicit label.
            if "delete_selected" in actions:
                func, name, _ = actions["delete_selected"]
                actions["delete_selected"] = (func, name, "Hard delete selected")
        else:
            # Non-superusers should not see hard delete action.
            actions.pop("delete_selected", None)
        return actions

    def has_delete_permission(self, request, obj=None):
        # For non-superusers we keep soft-delete only via explicit action.
        if obj is not None and not request.user.is_superuser:
            return False
        return super().has_delete_permission(request, obj=obj)

    def delete_model(self, request, obj):
        if request.user.is_superuser:
            obj.hard_delete()
            return
        obj.delete()

    def delete_queryset(self, request, queryset):
        if request.user.is_superuser:
            queryset.hard_delete()
            return
        queryset.delete()

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
        "is_valid",
        "conversion_bucket",
        "is_deleted",
    )
    list_filter = ("is_active", "is_valid", "is_deleted")
    search_fields = ("code", "name")
    ordering = ("order", "code")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(Lead)
class LeadAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "partner",
        "full_name",
        "phone",
        "manager",
        "first_manager",
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
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(LeadComment)
class LeadCommentAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "author", "is_pinned", "created_at", "is_deleted",)
    list_filter = ("is_pinned", "created_at", "is_deleted",)
    search_fields = ("lead__id", "lead__phone", "author__username", "body")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(LeadDeposit)
class LeadDepositAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "type", "amount", "creator", "created_at", "is_deleted")
    list_filter = ("type", "creator", "is_deleted")
    search_fields = ("lead__id", "lead__phone", "creator__username")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    actions = ("soft_delete_selected", "restore_selected")


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
