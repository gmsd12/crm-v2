from django.contrib import admin
from .models import Partner, PartnerSource, PartnerToken


@admin.register(Partner)
class PartnerAdmin(admin.ModelAdmin):
    list_display = ("name", "code", "is_active", "created_at", "updated_at")
    list_filter = ("is_active",)
    search_fields = ("name", "code")
    ordering = ("code",)


@admin.register(PartnerSource)
class PartnerSourceAdmin(admin.ModelAdmin):
    list_display = ("partner", "code", "name", "is_active", "created_at")
    list_filter = ("is_active", "partner")
    search_fields = ("code", "name", "partner__code", "partner__name")
    ordering = ("partner__code", "code")


@admin.register(PartnerToken)
class PartnerTokenAdmin(admin.ModelAdmin):
    list_display = (
        "partner",
        "name",
        "prefix",
        "is_active",
        "source",
        "last_used_at",
        "expires_at",
        "revoked_at",
        "created_at",
    )
    list_filter = ("is_active", "partner", "revoked_at")
    search_fields = ("prefix", "name", "partner__code", "partner__name")
    ordering = ("-created_at",)
    readonly_fields = ("prefix", "token_hash", "last_used_at", "created_at", "updated_at")
