from django.contrib import admin
from .models import Partner, PartnerSource, PartnerToken


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


@admin.register(Partner)
class PartnerAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("name", "code", "is_active", "is_deleted", "created_at", "updated_at")
    list_filter = ("is_active", "is_deleted")
    search_fields = ("name", "code")
    ordering = ("code",)
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(PartnerSource)
class PartnerSourceAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("partner", "code", "name", "is_active", "is_deleted", "created_at")
    list_filter = ("is_active", "partner", "is_deleted")
    search_fields = ("code", "name", "partner__code", "partner__name")
    ordering = ("partner__code", "code")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(PartnerToken)
class PartnerTokenAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "partner",
        "name",
        "prefix",
        "is_active",
        "is_deleted",
        "source",
        "last_used_at",
        "expires_at",
        "revoked_at",
        "created_at",
    )
    list_filter = ("is_active", "partner", "revoked_at", "is_deleted")
    search_fields = ("prefix", "name", "partner__code", "partner__name")
    ordering = ("-created_at",)
    readonly_fields = ("prefix", "token_hash", "last_used_at", "created_at", "updated_at")
    actions = ("soft_delete_selected", "restore_selected")
