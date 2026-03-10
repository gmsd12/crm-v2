from django_filters import DateRangeFilter

from apps.leads.attachment_validation import AttachmentValidationError, validate_uploaded_attachment
from django import forms
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html

try:
    from rangefilter.filters import DateTimeRangeFilter
except ImportError:  # pragma: no cover
    DateTimeRangeFilter = None

try:
    from import_export.admin import ExportActionMixin, ImportExportModelAdmin
    from import_export.forms import ConfirmImportForm, ImportForm, SelectableFieldsExportForm
    from import_export.formats import base_formats
except ImportError:
    class ExportActionMixin:
        pass

    class ImportExportModelAdmin(admin.ModelAdmin):
        pass

    base_formats = None
    IMPORT_EXPORT_AVAILABLE = False
else:
    from .resources import LeadResource

    IMPORT_EXPORT_AVAILABLE = True

from .models import (
    LeadAuditLog,
    Lead,
    LeadAttachment,
    LeadDeposit,
    LeadComment,
    LeadDuplicateAttempt,
    LeadTag,
    LeadStatus,
    LeadIdempotencyKey,
)
from apps.iam.models import User
from apps.partners.models import Partner

if IMPORT_EXPORT_AVAILABLE:
    class LeadExportForm(SelectableFieldsExportForm):
        DEFAULT_EXPORT_FIELDS = [
            "partner_name",
            "source",
            "status_name",
            "geo",
            "full_name",
            "phone",
            "email",
            "received_at",
            "comments",
        ]

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if self.is_bound:
                return

            for resource in self.resources:
                resource_name = resource.__name__.lower()
                resource_fields = resource().get_export_order()
                for field_name in resource_fields:
                    boolean_name = f"{resource_name}_{field_name}"
                    field = self.fields.get(boolean_name)
                    if field is not None:
                        field.initial = field_name in self.DEFAULT_EXPORT_FIELDS

    class LeadImportForm(ImportForm):
        partner = forms.ModelChoiceField(
            queryset=Partner.objects.filter(is_deleted=False).order_by("name"),
            required=False,
            help_text="Optional fallback when partner_code is omitted in CSV.",
        )
        source = forms.CharField(
            required=False,
            help_text="Optional fallback when source is omitted in CSV.",
        )
        status = forms.ModelChoiceField(
            queryset=LeadStatus.objects.filter(is_deleted=False).order_by("order", "code"),
            required=False,
            help_text="Optional fallback when status_code is omitted in CSV.",
        )
        manager = forms.ModelChoiceField(
            queryset=User.objects.filter(is_active=True).order_by("username"),
            required=False,
            help_text="Optional fallback when manager_username is omitted in CSV.",
        )
        first_manager = forms.ModelChoiceField(
            queryset=User.objects.filter(is_active=True).order_by("username"),
            required=False,
            help_text="Optional fallback when first_manager_username is omitted in CSV.",
        )

    class LeadConfirmImportForm(ConfirmImportForm):
        partner = forms.ModelChoiceField(
            queryset=Partner.objects.filter(is_deleted=False).order_by("name"),
            required=False,
            widget=forms.HiddenInput(),
        )
        source = forms.CharField(
            required=False,
            widget=forms.HiddenInput(),
        )
        status = forms.ModelChoiceField(
            queryset=LeadStatus.objects.filter(is_deleted=False),
            required=False,
            widget=forms.HiddenInput(),
        )
        manager = forms.ModelChoiceField(
            queryset=User.objects.filter(is_active=True),
            required=False,
            widget=forms.HiddenInput(),
        )
        first_manager = forms.ModelChoiceField(
            queryset=User.objects.filter(is_active=True),
            required=False,
            widget=forms.HiddenInput(),
        )


class LeadAttachmentAdminForm(forms.ModelForm):
    class Meta:
        model = LeadAttachment
        fields = "__all__"

    def clean(self):
        cleaned_data = super().clean()
        uploaded_file = cleaned_data.get("file")
        requested_kind = cleaned_data.get("kind")

        if uploaded_file:
            try:
                resolved_kind, detected_mime = validate_uploaded_attachment(
                    uploaded_file,
                    requested_kind=requested_kind,
                )
            except AttachmentValidationError as exc:
                self.add_error(exc.field, exc.message)
                return cleaned_data
            cleaned_data["kind"] = resolved_kind
            self._detected_mime_type = detected_mime
            self._detected_original_name = getattr(uploaded_file, "name", "") or ""
            self._detected_size_bytes = getattr(uploaded_file, "size", 0) or 0
        elif self.instance.pk and requested_kind and requested_kind != self.instance.kind:
            self.add_error("kind", "Kind is derived from file and cannot be changed manually.")

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=False)
        if hasattr(self, "_detected_mime_type"):
            instance.mime_type = self._detected_mime_type
            instance.original_name = getattr(self, "_detected_original_name", instance.original_name)
            instance.size_bytes = getattr(self, "_detected_size_bytes", instance.size_bytes)
        if commit:
            instance.save()
            self.save_m2m()
        return instance


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
        "work_bucket",
        "conversion_bucket",
        "is_deleted",
    )
    list_filter = ("is_active", "is_valid", "work_bucket", "is_deleted")
    search_fields = ("code", "name")
    ordering = ("order", "code")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(LeadTag)
class LeadTagAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("name", "color", "icon", "is_deleted")
    list_filter = ("is_deleted",)
    search_fields = ("name", "icon")
    ordering = ("name", "id")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(Lead)
class LeadAdmin(SoftDeleteAdminMixin, ExportActionMixin, ImportExportModelAdmin):
    list_display = (
        "id",
        "full_name",
        "phone",
        "email",
        "partner",
        "manager",
        "first_manager",
        "status",
        "received_at",
        "last_contacted_at",
        "is_deleted",
    )
    if DateTimeRangeFilter is not None:
        list_filter = (
            "partner",
            "manager",
            "first_manager",
            "priority",
            "status",
            "source",
            ("created_at", DateTimeRangeFilter),
            "is_deleted",
        )
    else:
        list_filter = (
            "partner",
            "manager",
            "first_manager",
            "priority",
            "status__name",
            "source",
            "received_at",
            "is_deleted",
        )
    search_fields = (
        "full_name",
        "phone",
        "email",
        "partner__code",
        "partner__name",
        "status__code",
        "source",
    )
    ordering = ("-received_at",)
    readonly_fields = ("received_at", "created_at", "updated_at", "deleted_at")
    actions = ("soft_delete_selected", "restore_selected")
    resource_classes = [LeadResource] if IMPORT_EXPORT_AVAILABLE else []
    export_form_class = LeadExportForm if IMPORT_EXPORT_AVAILABLE else None
    import_form_class = LeadImportForm if IMPORT_EXPORT_AVAILABLE else None
    confirm_form_class = LeadConfirmImportForm if IMPORT_EXPORT_AVAILABLE else None

    def get_import_formats(self):
        if not IMPORT_EXPORT_AVAILABLE:
            return []
        return [base_formats.CSV]

    def get_export_formats(self):
        if not IMPORT_EXPORT_AVAILABLE:
            return []
        formats = [base_formats.CSV, base_formats.JSON]
        xlsx_format = getattr(base_formats, "XLSX", None)
        if xlsx_format is not None:
            formats.append(xlsx_format)
        return formats

    def get_confirm_form_initial(self, request, import_form):
        initial = super().get_confirm_form_initial(request, import_form)
        if not IMPORT_EXPORT_AVAILABLE or import_form is None:
            return initial
        for field_name in ("partner", "source", "status", "manager", "first_manager"):
            value = import_form.cleaned_data.get(field_name)
            if value is not None:
                initial[field_name] = value.pk if hasattr(value, "pk") else value
        return initial

    def get_import_resource_kwargs(self, request, **kwargs):
        resource_kwargs = super().get_import_resource_kwargs(request, **kwargs)
        form = kwargs.get("form")
        if form and hasattr(form, "cleaned_data"):
            resource_kwargs.update(
                {
                    "default_partner": form.cleaned_data.get("partner"),
                    "default_source": form.cleaned_data.get("source"),
                    "default_status": form.cleaned_data.get("status"),
                    "default_manager": form.cleaned_data.get("manager"),
                    "default_first_manager": form.cleaned_data.get("first_manager"),
                }
            )
        return resource_kwargs


@admin.register(LeadComment)
class LeadCommentAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "author", "is_pinned", "created_at", "is_deleted",)
    list_filter = ("is_pinned", "created_at", "is_deleted",)
    search_fields = ("lead__id", "lead__phone", "author__username", "body")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    actions = ("soft_delete_selected", "restore_selected")


@admin.register(LeadAttachment)
class LeadAttachmentAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    form = LeadAttachmentAdminForm
    list_display = ("id", "lead", "kind", "original_name", "download_link", "uploaded_by", "created_at", "is_deleted")
    list_filter = ("kind", "uploaded_by", "is_deleted")
    search_fields = ("lead__id", "lead__phone", "lead__full_name", "original_name", "mime_type", "uploaded_by__username")
    ordering = ("-created_at", "-id")
    readonly_fields = ("download_link", "original_name", "mime_type", "size_bytes", "created_at", "updated_at", "deleted_at")
    actions = ("soft_delete_selected", "restore_selected")

    @admin.display(description="Download")
    def download_link(self, obj):
        if not obj or not obj.file:
            return "-"
        url = reverse("protected-media", kwargs={"file_path": obj.file.name})
        label = obj.original_name or "download"
        return format_html('<a href="{}">{}</a>', url, label)


@admin.register(LeadDeposit)
class LeadDepositAdmin(SoftDeleteAdminMixin, admin.ModelAdmin):
    list_display = ("id", "lead", "type", "amount", "creator", "created_at", "is_deleted")
    list_filter = ("type", "creator", "is_deleted")
    search_fields = ("lead__id", "lead__phone", "creator__username")
    ordering = ("-created_at", "-id")
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
