from __future__ import annotations

import json

from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from import_export import fields, resources, widgets

from apps.iam.models import User
from apps.leads.models import Lead, LeadStatus, LeadTag
from apps.partners.models import Partner


COMMENT_EXPORT_SEPARATOR = "\n---\n"


class PartnerCodeWidget(widgets.Widget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return None
        try:
            return Partner.objects.get(code=raw_value, is_deleted=False)
        except Partner.DoesNotExist as exc:
            raise ValidationError(f"Unknown partner_code: {raw_value}") from exc

    def render(self, value, obj=None, **kwargs):
        return value.code if value else ""


class LeadStatusCodeWidget(widgets.Widget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return None
        try:
            return LeadStatus.objects.get(code=raw_value, is_deleted=False)
        except LeadStatus.DoesNotExist as exc:
            raise ValidationError(f"Unknown status_code: {raw_value}") from exc

    def render(self, value, obj=None, **kwargs):
        return value.code if value else ""


class ActiveUserByUsernameWidget(widgets.Widget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return None
        try:
            return User.objects.get(username=raw_value, is_active=True)
        except User.DoesNotExist as exc:
            raise ValidationError(f"Unknown username: {raw_value}") from exc

    def render(self, value, obj=None, **kwargs):
        return value.username if value else ""


class ISODateTimeWidget(widgets.Widget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return None
        parsed = parse_datetime(raw_value)
        if parsed is None:
            raise ValidationError(f"Invalid datetime: {raw_value}")
        if timezone.is_naive(parsed):
            parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed

    def render(self, value, obj=None, **kwargs):
        return value.isoformat() if value else ""


class PriorityWidget(widgets.Widget):
    VALUE_MAP = {
        "LOW": Lead.Priority.LOW,
        "NORMAL": Lead.Priority.NORMAL,
        "HIGH": Lead.Priority.HIGH,
        "URGENT": Lead.Priority.URGENT,
    }

    def clean(self, value, row=None, **kwargs):
        raw_value = (str(value).strip() if value is not None else "")
        if not raw_value:
            return None
        if raw_value.isdigit():
            parsed = int(raw_value)
            if parsed in {choice.value for choice in Lead.Priority}:
                return parsed
        normalized = raw_value.upper()
        if normalized in self.VALUE_MAP:
            return self.VALUE_MAP[normalized]
        raise ValidationError(f"Invalid priority: {raw_value}")

    def render(self, value, obj=None, **kwargs):
        return "" if value is None else value


class JSONStringWidget(widgets.Widget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"Invalid custom_fields JSON: {exc.msg}") from exc
        if parsed is None:
            return None
        if not isinstance(parsed, (dict, list, str, int, float, bool)):
            raise ValidationError("custom_fields must be valid JSON")
        return parsed

    def render(self, value, obj=None, **kwargs):
        if value is None:
            return ""
        return json.dumps(value, ensure_ascii=False)


class StrictLeadTagWidget(widgets.ManyToManyWidget):
    def clean(self, value, row=None, **kwargs):
        raw_value = (value or "").strip()
        if not raw_value:
            return LeadTag.objects.none()
        names = [item.strip() for item in raw_value.split(self.separator) if item.strip()]
        queryset = LeadTag.objects.filter(name__in=names, is_deleted=False)
        found = set(queryset.values_list("name", flat=True))
        missing = [name for name in names if name not in found]
        if missing:
            raise ValidationError(f"Unknown tag(s): {', '.join(missing)}")
        ordered_ids = [queryset.get(name=name).id for name in names]
        return LeadTag.objects.filter(id__in=ordered_ids)

    def render(self, value, obj=None, **kwargs):
        if value is None:
            return ""
        return ", ".join(value.filter(is_deleted=False).order_by("name", "id").values_list("name", flat=True))


class LeadResource(resources.ModelResource):
    id = fields.Field(attribute="id", column_name="id", readonly=True)
    partner_code = fields.Field(attribute="partner", column_name="partner_code", widget=PartnerCodeWidget())
    partner_name = fields.Field(column_name="partner_name", readonly=True)
    source = fields.Field(attribute="source", column_name="source")
    status_code = fields.Field(attribute="status", column_name="status_code", widget=LeadStatusCodeWidget())
    status_name = fields.Field(column_name="status_name", readonly=True)
    manager_username = fields.Field(
        attribute="manager",
        column_name="manager_username",
        widget=ActiveUserByUsernameWidget(),
    )
    first_manager_username = fields.Field(
        attribute="first_manager",
        column_name="first_manager_username",
        widget=ActiveUserByUsernameWidget(),
    )
    priority = fields.Field(attribute="priority", column_name="priority", widget=PriorityWidget())
    next_contact_at = fields.Field(attribute="next_contact_at", column_name="next_contact_at", widget=ISODateTimeWidget())
    last_contacted_at = fields.Field(
        attribute="last_contacted_at",
        column_name="last_contacted_at",
        widget=ISODateTimeWidget(),
    )
    assigned_at = fields.Field(attribute="assigned_at", column_name="assigned_at", widget=ISODateTimeWidget())
    first_assigned_at = fields.Field(
        attribute="first_assigned_at",
        column_name="first_assigned_at",
        widget=ISODateTimeWidget(),
    )
    received_at = fields.Field(attribute="received_at", column_name="received_at", widget=ISODateTimeWidget())
    tags = fields.Field(attribute="tags", column_name="tags", widget=StrictLeadTagWidget(LeadTag, field="name", separator=","))
    comments = fields.Field(column_name="comments", readonly=True)
    custom_fields = fields.Field(attribute="custom_fields", column_name="custom_fields", widget=JSONStringWidget())

    class Meta:
        model = Lead
        import_id_fields = ("phone",)
        fields = (
            "id",
            "partner_code",
            "partner_name",
            "source",
            "status_code",
            "status_name",
            "manager_username",
            "first_manager_username",
            "geo",
            "full_name",
            "phone",
            "email",
            "age",
            "priority",
            "next_contact_at",
            "last_contacted_at",
            "assigned_at",
            "first_assigned_at",
            "received_at",
            "tags",
            "comments",
            "custom_fields",
        )
        export_order = fields
        report_skipped = True
        clean_model_instances = True

    def __init__(
        self,
        default_partner=None,
        default_source=None,
        default_status=None,
        default_manager=None,
        default_first_manager=None,
        **kwargs,
    ):
        self.default_partner = default_partner
        self.default_source = default_source
        self.default_status = default_status
        self.default_manager = default_manager
        self.default_first_manager = default_first_manager
        super().__init__(**kwargs)

    def filter_export(self, queryset, **kwargs):
        return (
            queryset.select_related("partner", "status", "manager", "first_manager")
            .prefetch_related("tags", "comments__author")
            .order_by("-received_at", "-id")
        )

    def before_import_row(self, row, **kwargs):
        if not (row.get("phone") or "").strip():
            raise ValidationError("phone is required")

        if self.default_partner and not (row.get("partner_code") or "").strip():
            row["partner_code"] = self.default_partner.code
        if self.default_source and not (row.get("source") or "").strip():
            row["source"] = self.default_source
        if self.default_status and not (row.get("status_code") or "").strip():
            row["status_code"] = self.default_status.code
        if self.default_manager and not (row.get("manager_username") or "").strip():
            row["manager_username"] = self.default_manager.username
        if self.default_first_manager and not (row.get("first_manager_username") or "").strip():
            row["first_manager_username"] = self.default_first_manager.username

        super().before_import_row(row, **kwargs)

    def dehydrate_partner_name(self, obj):
        return obj.partner.name if obj.partner_id else ""

    def dehydrate_status_name(self, obj):
        return obj.status.name if obj.status_id else ""

    def dehydrate_comments(self, obj):
        comments = []
        queryset = obj.comments.filter(is_deleted=False).select_related("author").order_by("created_at", "id")
        for comment in queryset:
            if comment.created_at:
                timestamp = timezone.localtime(comment.created_at).strftime("%d.%m.%Y %H:%M")
            else:
                timestamp = ""
            comments.append(f"{timestamp} | {comment.body}")
        return COMMENT_EXPORT_SEPARATOR.join(comments)

    def skip_row(self, instance, original, row, import_validation_errors=None):
        if original is not None and getattr(original, "pk", None):
            return True
        return super().skip_row(instance, original, row, import_validation_errors=import_validation_errors)

    def after_init_instance(self, instance, new, row, **kwargs):
        if new:
            if instance.partner_id is None and self.default_partner is not None:
                instance.partner = self.default_partner
            if not (instance.source or "").strip() and self.default_source is not None:
                instance.source = self.default_source
            if instance.status_id is None and self.default_status is not None:
                instance.status = self.default_status
            if instance.manager_id is None and self.default_manager is not None:
                instance.manager = self.default_manager
            if instance.first_manager_id is None and self.default_first_manager is not None:
                instance.first_manager = self.default_first_manager
        super().after_init_instance(instance, new, row, **kwargs)

    def before_save_instance(self, instance, row, **kwargs):
        if instance.partner_id is None:
            raise ValidationError("partner_code is required or choose partner in the import form")
        if instance.status_id is None:
            instance.status = (
                LeadStatus.objects.filter(is_deleted=False, is_default_for_new_leads=True)
                .order_by("id")
                .first()
            )
        if instance.geo:
            instance.geo = instance.geo.upper()
        instance.source = (instance.source or "").strip()
        if instance.email:
            instance.email = instance.email.lower()
        super().before_save_instance(instance, row, **kwargs)
