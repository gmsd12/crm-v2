from django.contrib import admin
from .models import (
    LeadAuditLog,
    Lead,
    LeadComment,
    LeadDuplicateAttempt,
    LeadStatus,
    LeadStatusIdempotencyKey,
    LeadStatusTransition,
    Pipeline,
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


@admin.register(Pipeline)
class PipelineAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("code", "name", "is_default", "is_active", "is_deleted", "created_at")
    list_filter = ("is_default", "is_active", "is_deleted")
    search_fields = ("code", "name")
    ordering = ("code",)
    actions = ("restore_selected",)


@admin.register(LeadStatus)
class LeadStatusAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "pipeline",
        "code",
        "name",
        "order",
        "is_default_for_new_leads",
        "is_active",
        "is_terminal",
        "counts_for_conversion",
        "conversion_bucket",
        "is_deleted",
    )
    list_filter = ("pipeline", "is_active", "is_terminal", "counts_for_conversion", "is_deleted")
    search_fields = ("code", "name", "pipeline__code", "pipeline__name")
    ordering = ("pipeline__code", "order", "code")
    actions = ("restore_selected",)


@admin.register(LeadStatusTransition)
class LeadStatusTransitionAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("pipeline", "from_status", "to_status", "is_active", "requires_comment", "is_deleted")
    list_filter = ("pipeline", "is_active", "requires_comment", "is_deleted")
    search_fields = ("pipeline__code", "from_status__code", "to_status__code")
    ordering = ("pipeline__code", "from_status__order", "to_status__order")
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
        "won_by_manager",
        "sales_closed",
        "priority",
        "pipeline",
        "status",
        "source",
        "external_id",
        "received_at",
        "is_deleted",
    )
    list_filter = ("partner", "manager", "first_manager", "won_by_manager", "sales_closed", "priority", "pipeline", "status", "source", "is_deleted")
    search_fields = (
        "external_id",
        "full_name",
        "phone",
        "email",
        "partner__code",
        "partner__name",
        "pipeline__code",
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
    search_fields = ("lead__external_id", "author__username", "body")
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
    search_fields = ("entity_id", "lead__external_id", "from_status__code", "to_status__code", "actor_user__username")
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)


@admin.register(LeadStatusIdempotencyKey)
class LeadStatusIdempotencyKeyAdmin(admin.ModelAdmin):
    list_display = ("id", "actor_user", "endpoint", "key", "response_status", "created_at", "updated_at")
    list_filter = ("endpoint", "response_status", "created_at")
    search_fields = ("actor_user__username", "key")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at")
