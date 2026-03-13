from __future__ import annotations

import csv
import mimetypes
import secrets
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction
from django.utils import timezone
from django.utils.text import slugify

from apps.iam.models import User, UserRole
from apps.leads.models import Lead, LeadAttachment, LeadComment, LeadDeposit, LeadStatus
from apps.partners.models import Partner


SUPPORTED_STATUS_COLORS = {
    "primary",
    "secondary",
    "success",
    "info",
    "warning",
    "error",
    "neutral",
}


@dataclass(frozen=True)
class LegacyTables:
    user: str
    database: str
    status: str
    lead: str
    comment: str
    deposit: str
    record: str


class Command(BaseCommand):
    help = "Imports data from a legacy CRM database into the current schema."

    def add_arguments(self, parser):
        parser.add_argument("--legacy-alias", default="legacy", help="Database alias for the legacy source.")
        parser.add_argument("--dry-run", action="store_true", help="Do not write data into the current database.")
        parser.add_argument(
            "--report-path",
            default=str(settings.BASE_DIR / "legacy" / "reports" / "duplicate_phones.csv"),
            help="CSV path for skipped duplicate-phone leads.",
        )
        parser.add_argument("--user-table", default="main_userprofile")
        parser.add_argument("--database-table", default="main_database")
        parser.add_argument("--status-table", default="main_status")
        parser.add_argument("--lead-table", default="main_lead")
        parser.add_argument("--comment-table", default="main_comment")
        parser.add_argument("--deposit-table", default="main_deposit")
        parser.add_argument("--record-table", default="main_record")
        parser.add_argument(
            "--only",
            nargs="+",
            choices=["users", "partners", "statuses", "leads", "comments", "deposits", "attachments"],
            help="Import only selected sections. Default is all.",
        )
        parser.add_argument(
            "--progress-every",
            type=int,
            default=1000,
            help="Print progress every N rows while processing large tables.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=2000,
            help="Batch size for bulk inserts during import.",
        )
        parser.add_argument(
            "--use-orphan-deposit-lead",
            action="store_true",
            default=True,
            help="Attach legacy deposits without lead_id to a technical fallback lead.",
        )
        parser.add_argument(
            "--no-orphan-deposit-lead",
            action="store_false",
            dest="use_orphan_deposit_lead",
            help="Skip legacy deposits without lead_id instead of attaching them to a technical fallback lead.",
        )
        parser.add_argument(
            "--orphan-partner-code",
            default="legacy-orphans",
            help="Partner code for the technical fallback partner used by orphan deposits.",
        )
        parser.add_argument(
            "--orphan-lead-name",
            default="Legacy orphan deposits",
            help="Full name for the technical fallback lead used by orphan deposits.",
        )

    def handle(self, *args, **options):
        legacy_alias = options["legacy_alias"]
        if legacy_alias not in settings.DATABASES:
            raise CommandError(
                f"Legacy database alias '{legacy_alias}' is not configured. "
                f"Set LEGACY_DATABASE_URL in .env or pass another --legacy-alias."
            )

        self.legacy_connection = connections[legacy_alias]
        self.dry_run = options["dry_run"]
        self.report_path = Path(options["report_path"])
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.progress_every = max(1, int(options["progress_every"]))
        self.batch_size = max(100, int(options["batch_size"]))
        self.use_orphan_deposit_lead = bool(options["use_orphan_deposit_lead"])
        self.orphan_partner_code = str(options["orphan_partner_code"]).strip() or "legacy-orphans"
        self.orphan_lead_name = str(options["orphan_lead_name"]).strip() or "Legacy orphan deposits"
        self.tables = LegacyTables(
            user=options["user_table"],
            database=options["database_table"],
            status=options["status_table"],
            lead=options["lead_table"],
            comment=options["comment_table"],
            deposit=options["deposit_table"],
            record=options["record_table"],
        )
        selected = set(options["only"] or ["users", "partners", "statuses", "leads", "comments", "deposits", "attachments"])

        self.user_map: dict[int, int] = {}
        self.partner_map: dict[int, int] = {}
        self.status_map: dict[int, int] = {}
        self.lead_map: dict[int, int] = {}
        self.duplicate_phone_rows: list[dict[str, object]] = []
        self.skip_stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.skip_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
        self.warning_stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.warning_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
        self.orphan_partner_pk: int | None = None
        self.orphan_lead_pk: int | None = None

        self.stdout.write(self.style.NOTICE(f"Using legacy tables: {self.tables}"))

        if self.dry_run:
            self.stdout.write(self.style.WARNING("Dry-run mode: nothing will be written."))

        if "users" in selected:
            self.import_users()
        else:
            self.build_user_map_only()

        if "partners" in selected:
            self.import_partners()
        else:
            self.build_partner_map_only()

        if "statuses" in selected:
            self.import_statuses()
        else:
            self.build_status_map_only()

        if "leads" in selected:
            self.import_leads()
        else:
            self.build_lead_map_only()

        if "comments" in selected:
            self.import_comments()
        if "deposits" in selected:
            self.import_deposits()
        if "attachments" in selected:
            self.import_attachments()

        self.write_duplicate_report()
        self.write_skip_reports()
        self.write_warning_reports()
        self.print_skip_summary()
        self.print_warning_summary()
        self.stdout.write(self.style.SUCCESS("Legacy import completed."))

    def fetch_rows(self, table_name: str, columns: list[str]) -> list[dict[str, object]]:
        quoted_table = self.legacy_connection.ops.quote_name(table_name)
        quoted_columns = ", ".join(self.legacy_connection.ops.quote_name(column) for column in columns)
        sql = f"SELECT {quoted_columns} FROM {quoted_table}"
        with self.legacy_connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
        return [dict(zip(column_names, row, strict=False)) for row in rows]

    def count_rows(self, table_name: str) -> int:
        quoted_table = self.legacy_connection.ops.quote_name(table_name)
        sql = f"SELECT COUNT(*) FROM {quoted_table}"
        with self.legacy_connection.cursor() as cursor:
            cursor.execute(sql)
            return int(cursor.fetchone()[0])

    def iter_rows(self, table_name: str, columns: list[str]):
        quoted_table = self.legacy_connection.ops.quote_name(table_name)
        quoted_columns = ", ".join(self.legacy_connection.ops.quote_name(column) for column in columns)
        sql = f"SELECT {quoted_columns} FROM {quoted_table}"
        with self.legacy_connection.cursor() as cursor:
            cursor.execute(sql)
            column_names = [description[0] for description in cursor.description]
            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(column_names, row, strict=False))

    def normalized_slug(self, value: str, *, fallback_prefix: str) -> str:
        base = slugify((value or "").strip())[:40]
        if not base:
            base = f"{fallback_prefix}-{secrets.token_hex(3)}"
        return base

    def normalize_phone(self, value: object) -> str:
        raw_phone = str(value or "").strip()
        if not raw_phone:
            return ""
        raw_phone = raw_phone.replace(".0", "")
        digits = "".join(char for char in raw_phone if char.isdigit())
        if not digits:
            return ""
        if len(digits) == 11 and digits.startswith("8"):
            digits = f"7{digits[1:]}"
        elif len(digits) == 10:
            digits = f"7{digits}"
        return digits

    def mapped_pk(self, persisted_pk: int | None, legacy_id: int) -> int:
        if persisted_pk:
            return persisted_pk
        if self.dry_run:
            return -legacy_id
        return 0

    def format_duration(self, seconds: float) -> str:
        seconds = max(0, int(seconds))
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {seconds}s"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def log_progress(self, label: str, index: int, total: int, started_at: float):
        if index == 1 or index == total or index % self.progress_every == 0:
            elapsed = max(time.monotonic() - started_at, 0.001)
            rate = index / elapsed
            remaining = max(total - index, 0)
            eta = self.format_duration(remaining / rate) if rate > 0 else "unknown"
            self.stdout.write(
                f"{label}: {index}/{total} | elapsed={self.format_duration(elapsed)} | eta={eta}"
            )

    def log_stage_finished(self, label: str, started_at: float):
        elapsed = time.monotonic() - started_at
        self.stdout.write(f"{label} finished in {elapsed:.1f}s")

    def record_skip(self, section: str, reason: str, row: dict[str, object]):
        self.skip_stats[section][reason] += 1
        payload = {"reason": reason, **row}
        self.skip_rows[section].append(payload)

    def record_warning(self, section: str, reason: str, row: dict[str, object]):
        self.warning_stats[section][reason] += 1
        payload = {"reason": reason, **row}
        self.warning_rows[section].append(payload)

    def unique_username(self, last_name: str, legacy_id: int) -> str:
        base = self.normalized_slug(last_name, fallback_prefix=f"user-{legacy_id}")
        candidate = base
        while User.objects.filter(username=candidate).exists():
            candidate = f"{base}-{secrets.token_hex(2)}"
        return candidate[:150]

    def unique_partner_code(self, name: str, legacy_id: int) -> str:
        base = self.normalized_slug(name, fallback_prefix=f"partner-{legacy_id}")
        candidate = f"{base}-{legacy_id}"
        while Partner.all_objects.filter(code=candidate).exists():
            candidate = f"{base}-{legacy_id}-{secrets.token_hex(2)}"
        return candidate[:64]

    def unique_status_code(self, tid: str, legacy_id: int) -> str:
        base = self.normalized_slug((tid or "").lower().strip(), fallback_prefix=f"status-{legacy_id}")
        candidate = base
        while LeadStatus.all_objects.filter(code=candidate).exists():
            candidate = f"{base}-{secrets.token_hex(2)}"
        return candidate[:64]

    def map_role(self, row: dict[str, object]) -> str:
        if row.get("is_superuser"):
            return UserRole.ADMIN
        if row.get("is_staff") and row.get("is_retention"):
            return UserRole.RET
        if row.get("is_staff") and row.get("is_teamlead"):
            return UserRole.TEAMLEADER
        if row.get("is_staff"):
            return UserRole.MANAGER
        return UserRole.MANAGER

    def normalize_status_color(self, value: str) -> str:
        if value in SUPPORTED_STATUS_COLORS:
            return value
        return "neutral"

    def sync_timestamps(self, model, object_id: int, created_at, updated_at):
        if self.dry_run:
            return
        update_fields = {}
        if created_at:
            update_fields["created_at"] = created_at
        if updated_at:
            update_fields["updated_at"] = updated_at
        if update_fields:
            model.all_objects.filter(pk=object_id).update(**update_fields)

    def ensure_orphan_deposit_target(self) -> int:
        if self.orphan_lead_pk:
            return self.orphan_lead_pk

        partner = Partner.all_objects.filter(code=self.orphan_partner_code).first()
        if partner is None:
            partner = Partner(
                name="Legacy Orphans",
                code=self.orphan_partner_code,
                is_active=True,
            )
            if not self.dry_run:
                partner.save()
        elif not self.dry_run and (partner.is_deleted or not partner.is_active or partner.name != "Legacy Orphans"):
            partner.name = "Legacy Orphans"
            partner.is_active = True
            partner.is_deleted = False
            partner.deleted_at = None
            partner.save(update_fields=["name", "is_active", "is_deleted", "deleted_at", "updated_at"])

        self.orphan_partner_pk = self.mapped_pk(partner.pk, 1)

        lead = Lead.all_objects.filter(
            partner_id=partner.pk if partner.pk else None,
            custom_fields__legacy_orphan_deposit_sink=True,
        ).first()
        if lead is None:
            lead = Lead(
                partner_id=partner.pk if partner.pk else self.orphan_partner_pk,
                manager_id=None,
                first_manager_id=None,
                status_id=None,
                source="legacy",
                full_name=self.orphan_lead_name,
                phone="",
                email="",
                geo="",
                priority=None,
                next_contact_at=None,
                last_contacted_at=None,
                assigned_at=None,
                first_assigned_at=None,
                received_at=timezone.now(),
                custom_fields={"legacy_orphan_deposit_sink": True},
            )
            if not self.dry_run:
                lead.save()
        elif not self.dry_run:
            lead.full_name = self.orphan_lead_name
            lead.source = "legacy"
            custom_fields = dict(lead.custom_fields or {})
            custom_fields["legacy_orphan_deposit_sink"] = True
            lead.custom_fields = custom_fields
            lead.is_deleted = False
            lead.deleted_at = None
            lead.save(update_fields=["full_name", "source", "custom_fields", "is_deleted", "deleted_at", "updated_at"])

        self.orphan_lead_pk = self.mapped_pk(lead.pk, 1)
        return self.orphan_lead_pk

    def import_users(self):
        stage_started_at = time.monotonic()
        rows = self.fetch_rows(
            self.tables.user,
            [
                "id",
                "name",
                "email",
                "password",
                "is_staff",
                "is_superuser",
                "is_active",
                "is_teamlead",
                "is_retention",
                "last_login",
                "date_joined",
            ],
        )
        created = 0
        updated = 0
        total = len(rows)
        for index, row in enumerate(rows, start=1):
            self.log_progress("Users", index, total, stage_started_at)
            email = (row.get("email") or "").strip()
            if email:
                user = User.objects.filter(email=email).first()
            else:
                user = None
            if user is None:
                user = User(
                    username=self.unique_username(str(row.get("name") or ""), int(row["id"])),
                    email=email or None,
                )
                created += 1
            else:
                updated += 1

            user.last_name = (row.get("name") or "").strip()
            user.first_name = ""
            user.role = self.map_role(row)
            user.is_active = bool(row.get("is_active", True))
            # Legacy import preserves business role only; Django admin flags are assigned manually later.
            user.is_staff = False
            user.is_superuser = False
            if row.get("password"):
                user.password = row["password"]
            if not self.dry_run:
                user.save()
                User.objects.filter(pk=user.pk).update(
                    last_login=row.get("last_login"),
                    date_joined=row.get("date_joined") or timezone.now(),
                )
            self.user_map[int(row["id"])] = self.mapped_pk(user.pk, int(row["id"]))

        self.stdout.write(self.style.SUCCESS(f"Users: created={created}, updated={updated}, mapped={len(self.user_map)}"))
        self.log_stage_finished("Users", stage_started_at)

    def build_user_map_only(self):
        rows = self.fetch_rows(self.tables.user, ["id", "email"])
        for row in rows:
            email = (row.get("email") or "").strip()
            if not email:
                continue
            user = User.objects.filter(email=email).first()
            if user:
                self.user_map[int(row["id"])] = user.pk

    def import_partners(self):
        stage_started_at = time.monotonic()
        rows = self.fetch_rows(self.tables.database, ["id", "name"])
        created = 0
        total = len(rows)
        for index, row in enumerate(rows, start=1):
            self.log_progress("Partners", index, total, stage_started_at)
            legacy_id = int(row["id"])
            name = (row.get("name") or "").strip() or f"Partner {legacy_id}"
            code = self.unique_partner_code(name, legacy_id)
            partner = Partner.all_objects.filter(code=code).first()
            if partner is None:
                partner = Partner(name=name, code=code, is_active=True)
                if not self.dry_run:
                    partner.save()
                created += 1
            else:
                if not self.dry_run:
                    partner.name = name
                    partner.is_active = True
                    partner.is_deleted = False
                    partner.deleted_at = None
                    partner.save(update_fields=["name", "is_active", "is_deleted", "deleted_at", "updated_at"])
            self.partner_map[legacy_id] = self.mapped_pk(partner.pk, legacy_id)

        self.stdout.write(self.style.SUCCESS(f"Partners: created={created}, mapped={len(self.partner_map)}"))
        self.log_stage_finished("Partners", stage_started_at)

    def build_partner_map_only(self):
        rows = self.fetch_rows(self.tables.database, ["id", "name"])
        for row in rows:
            legacy_id = int(row["id"])
            name = (row.get("name") or "").strip() or f"Partner {legacy_id}"
            exact = Partner.all_objects.filter(name=name).order_by("id").first()
            if exact:
                self.partner_map[legacy_id] = exact.pk

    def import_statuses(self):
        stage_started_at = time.monotonic()
        rows = self.fetch_rows(self.tables.status, ["id", "name", "tid", "color", "is_valid"])
        created = 0
        updated = 0
        total = len(rows)
        for index, row in enumerate(rows, start=1):
            self.log_progress("Statuses", index, total, stage_started_at)
            legacy_id = int(row["id"])
            incoming_code = self.normalized_slug(str(row.get("tid") or "").lower().strip(), fallback_prefix=f"status-{legacy_id}")
            status = LeadStatus.all_objects.filter(code=incoming_code).first()
            if status is None:
                incoming_code = self.unique_status_code(str(row.get("tid") or ""), legacy_id)
                status = LeadStatus(
                    code=incoming_code,
                    name=(row.get("name") or "").strip() or incoming_code,
                    color=self.normalize_status_color(str(row.get("color") or "")),
                    is_valid=bool(row.get("is_valid", True)),
                    is_active=True,
                    is_default_for_new_leads=False,
                    work_bucket=LeadStatus.WorkBucket.WORKING if row.get("is_valid", True) else LeadStatus.WorkBucket.NON_WORKING,
                    conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
                )
                if not self.dry_run:
                    status.save()
                created += 1
            else:
                status.name = (row.get("name") or "").strip() or status.name
                status.color = self.normalize_status_color(str(row.get("color") or "")) or status.color
                status.is_valid = bool(row.get("is_valid", status.is_valid))
                status.is_deleted = False
                status.deleted_at = None
                if not self.dry_run:
                    status.save(
                        update_fields=["name", "color", "is_valid", "is_deleted", "deleted_at", "updated_at"]
                    )
                updated += 1
            self.status_map[legacy_id] = self.mapped_pk(status.pk, legacy_id)

        self.stdout.write(self.style.SUCCESS(f"Statuses: created={created}, updated={updated}, mapped={len(self.status_map)}"))
        self.log_stage_finished("Statuses", stage_started_at)

    def build_status_map_only(self):
        rows = self.fetch_rows(self.tables.status, ["id", "tid"])
        for row in rows:
            legacy_id = int(row["id"])
            incoming_code = self.normalized_slug(str(row.get("tid") or "").lower().strip(), fallback_prefix=f"status-{legacy_id}")
            status = LeadStatus.all_objects.filter(code=incoming_code).first()
            if status:
                self.status_map[legacy_id] = status.pk

    def import_leads(self):
        stage_started_at = time.monotonic()
        rows = self.fetch_rows(
            self.tables.lead,
            [
                "id",
                "user_id",
                "database_id",
                "status_id",
                "name",
                "phone",
                "email",
                "geo",
                "created_at",
                "updated_at",
            ],
        )
        created = 0
        updated = 0
        phone_max_length = Lead._meta.get_field("phone").max_length
        existing_legacy_map: dict[int, int] = {}
        existing_alive_phone_map: dict[str, int] = {}
        for lead_id, custom_fields, phone, is_deleted in Lead.all_objects.values_list("id", "custom_fields", "phone", "is_deleted"):
            legacy_lead_id = (custom_fields or {}).get("legacy_lead_id")
            if legacy_lead_id is not None:
                existing_legacy_map[int(legacy_lead_id)] = lead_id
            if phone and not is_deleted:
                existing_alive_phone_map[phone] = lead_id

        phone_master_legacy_id: dict[str, int] = {}
        phone_master_sort_key: dict[str, tuple[object, int]] = {}
        for row in rows:
            phone = self.normalize_phone(row.get("phone"))
            if not phone:
                continue
            legacy_id = int(row["id"])
            sort_key = (row.get("created_at") or timezone.now(), legacy_id)
            current_key = phone_master_sort_key.get(phone)
            if current_key is None or sort_key < current_key:
                phone_master_sort_key[phone] = sort_key
                phone_master_legacy_id[phone] = legacy_id

        pending_creates: list[tuple[Lead, int]] = []

        def flush_pending_creates():
            nonlocal created
            if not pending_creates:
                return

            objects = [item[0] for item in pending_creates]
            legacy_ids = [item[1] for item in pending_creates]
            if self.dry_run:
                for lead, legacy_id in pending_creates:
                    self.lead_map[legacy_id] = self.mapped_pk(None, legacy_id)
                    if lead.phone and not lead.is_deleted:
                        existing_alive_phone_map[lead.phone] = self.lead_map[legacy_id]
                created += len(pending_creates)
                pending_creates.clear()
                return

            created_objects = Lead.all_objects.bulk_create(objects, batch_size=self.batch_size)
            if created_objects:
                Lead.all_objects.bulk_update(created_objects, ["created_at", "updated_at"], batch_size=self.batch_size)
            for lead, legacy_id in zip(created_objects, legacy_ids, strict=False):
                self.lead_map[legacy_id] = lead.pk
                existing_legacy_map[legacy_id] = lead.pk
                if lead.phone and not lead.is_deleted:
                    existing_alive_phone_map[lead.phone] = lead.pk
            created += len(created_objects)
            pending_creates.clear()

        total = len(rows)
        for index, row in enumerate(rows, start=1):
            self.log_progress("Leads", index, total, stage_started_at)
            legacy_id = int(row["id"])
            phone = self.normalize_phone(row.get("phone"))
            original_phone = phone
            if phone and len(phone) > phone_max_length:
                phone = phone[:phone_max_length]
                self.record_warning(
                    "leads",
                    "phone_truncated",
                    {
                        "legacy_lead_id": legacy_id,
                        "original_phone": original_phone,
                        "stored_phone": phone,
                        "original_length": len(original_phone),
                    },
                )
            email = (row.get("email") or "").strip()
            partner_id = self.partner_map.get(int(row["database_id"])) if row.get("database_id") else None
            if not partner_id:
                self.record_skip(
                    "leads",
                    "missing_partner_mapping",
                    {
                        "legacy_lead_id": legacy_id,
                        "database_id": row.get("database_id"),
                        "phone": phone,
                        "email": email,
                        "full_name": (row.get("name") or "").strip(),
                    },
                )
                continue

            existing_lead_id = existing_legacy_map.get(legacy_id)
            duplicate_master_legacy_id = None
            duplicate_of_existing_lead_id = None
            is_soft_duplicate = False
            if phone:
                duplicate_master_legacy_id = phone_master_legacy_id.get(phone)
                if duplicate_master_legacy_id is not None and duplicate_master_legacy_id != legacy_id:
                    is_soft_duplicate = True
                elif phone in existing_alive_phone_map and existing_lead_id is None:
                    is_soft_duplicate = True
                    duplicate_of_existing_lead_id = existing_alive_phone_map[phone]
            if existing_lead_id:
                lead = Lead.all_objects.get(pk=existing_lead_id)
                updated += 1
            else:
                lead = Lead(partner_id=partner_id)

            manager_id = self.user_map.get(int(row["user_id"])) if row.get("user_id") else None
            legacy_created_at = row.get("created_at") or timezone.now()
            lead.manager_id = manager_id
            lead.first_manager_id = manager_id
            lead.status_id = self.status_map.get(int(row["status_id"])) if row.get("status_id") else None
            lead.full_name = (row.get("name") or "").strip()
            lead.phone = phone
            lead.email = email
            lead.geo = (str(row.get("geo") or "").strip().upper())[:2]
            lead.source = ""
            lead.priority = None
            lead.next_contact_at = None
            lead.last_contacted_at = None
            lead.assigned_at = legacy_created_at if manager_id else None
            lead.first_assigned_at = legacy_created_at if manager_id else None
            lead.received_at = legacy_created_at
            lead.custom_fields = {
                "legacy_lead_id": legacy_id,
                "legacy_duplicate_import": is_soft_duplicate,
            }
            if original_phone and original_phone != phone:
                lead.custom_fields["legacy_original_phone"] = original_phone
            if duplicate_master_legacy_id and duplicate_master_legacy_id != legacy_id:
                lead.custom_fields["legacy_duplicate_master_legacy_id"] = duplicate_master_legacy_id
            if duplicate_of_existing_lead_id:
                lead.custom_fields["legacy_duplicate_of_existing_lead_id"] = duplicate_of_existing_lead_id
            lead.created_at = legacy_created_at
            lead.updated_at = row.get("updated_at") or legacy_created_at
            lead.is_deleted = is_soft_duplicate
            lead.deleted_at = timezone.now() if is_soft_duplicate else None

            if is_soft_duplicate:
                payload = {
                    "legacy_lead_id": legacy_id,
                    "phone": phone,
                    "master_legacy_lead_id": duplicate_master_legacy_id,
                    "existing_lead_id": duplicate_of_existing_lead_id,
                }
                self.record_warning("leads", "duplicate_phone_soft_deleted", payload)
                self.duplicate_phone_rows.append(
                    {
                        "legacy_lead_id": legacy_id,
                        "phone": phone,
                        "reason": "duplicate_phone_soft_deleted",
                        "master_legacy_lead_id": duplicate_master_legacy_id,
                        "existing_lead_id": duplicate_of_existing_lead_id,
                    }
                )

            if existing_lead_id:
                if not self.dry_run:
                    lead.save()
                self.lead_map[legacy_id] = self.mapped_pk(lead.pk, legacy_id)
                if lead.phone and not lead.is_deleted:
                    existing_alive_phone_map[lead.phone] = self.lead_map[legacy_id]
            else:
                pending_creates.append((lead, legacy_id))
                if len(pending_creates) >= self.batch_size:
                    flush_pending_creates()

        flush_pending_creates()

        self.stdout.write(
            self.style.SUCCESS(
                f"Leads: created={created}, updated={updated}, mapped={len(self.lead_map)}, duplicates={len(self.duplicate_phone_rows)}"
            )
        )
        self.log_stage_finished("Leads", stage_started_at)

    def build_lead_map_only(self):
        for lead in Lead.all_objects.exclude(custom_fields__legacy_lead_id=None).values("id", "custom_fields"):
            legacy_id = lead["custom_fields"].get("legacy_lead_id")
            if legacy_id is not None:
                self.lead_map[int(legacy_id)] = lead["id"]

    def import_comments(self):
        stage_started_at = time.monotonic()
        created = 0
        total = self.count_rows(self.tables.comment)
        rows = self.iter_rows(
            self.tables.comment,
            ["id", "user_id", "lead_id", "comment", "created_at", "is_pinned"],
        )
        pending_comments: list[LeadComment] = []
        latest_contact_by_lead: dict[int, object] = {}

        def flush_pending_comments():
            nonlocal created
            if not pending_comments:
                return
            if self.dry_run:
                created += len(pending_comments)
                pending_comments.clear()
                return
            created_comments = LeadComment.all_objects.bulk_create(pending_comments, batch_size=self.batch_size)
            if created_comments:
                LeadComment.all_objects.bulk_update(
                    created_comments,
                    ["created_at", "updated_at"],
                    batch_size=self.batch_size,
                )
            created += len(created_comments)
            pending_comments.clear()

        for index, row in enumerate(rows, start=1):
            self.log_progress("Comments", index, total, stage_started_at)
            if not row.get("lead_id"):
                self.record_skip(
                    "comments",
                    "missing_legacy_lead_id",
                    {"legacy_comment_id": row.get("id")},
                )
                continue
            lead_id = self.lead_map.get(int(row["lead_id"]))
            if not lead_id:
                self.record_skip(
                    "comments",
                    "lead_not_imported",
                    {"legacy_comment_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
                continue
            author_id = self.user_map.get(int(row["user_id"])) if row.get("user_id") else None
            body = str(row.get("comment") or "").strip()
            if not body:
                self.record_skip(
                    "comments",
                    "empty_body",
                    {"legacy_comment_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
                continue
            created_at = row.get("created_at") or timezone.now()
            comment = LeadComment(
                lead_id=lead_id,
                author_id=author_id,
                body=body,
                is_pinned=bool(row.get("is_pinned", False)),
                created_at=created_at,
                updated_at=created_at,
            )
            pending_comments.append(comment)
            current_latest = latest_contact_by_lead.get(lead_id)
            if current_latest is None or created_at > current_latest:
                latest_contact_by_lead[lead_id] = created_at
            if len(pending_comments) >= self.batch_size:
                flush_pending_comments()

        flush_pending_comments()
        if not self.dry_run and latest_contact_by_lead:
            leads_by_id = Lead.all_objects.in_bulk(latest_contact_by_lead.keys())
            leads_to_update: list[Lead] = []
            for lead_id, latest_contact_at in latest_contact_by_lead.items():
                lead = leads_by_id.get(lead_id)
                if lead is None:
                    continue
                if lead.last_contacted_at is None or latest_contact_at > lead.last_contacted_at:
                    lead.last_contacted_at = latest_contact_at
                    leads_to_update.append(lead)
            if leads_to_update:
                Lead.all_objects.bulk_update(leads_to_update, ["last_contacted_at"], batch_size=self.batch_size)

        self.stdout.write(self.style.SUCCESS(f"Comments: created={created}"))
        self.log_stage_finished("Comments", stage_started_at)

    def import_deposits(self):
        stage_started_at = time.monotonic()
        created = 0
        skipped = 0
        total = self.count_rows(self.tables.deposit)
        rows = self.iter_rows(
            self.tables.deposit,
            ["id", "creator_id", "lead_id", "amount", "created_at", "type"],
        )
        pending_deposits: list[LeadDeposit] = []
        latest_contact_by_lead: dict[int, object] = {}
        existing_ftd_leads = set(
            LeadDeposit.all_objects.filter(type=LeadDeposit.Type.FTD, is_deleted=False).values_list("lead_id", flat=True)
        )
        existing_reload_leads = set(
            LeadDeposit.all_objects.filter(type=LeadDeposit.Type.RELOAD, is_deleted=False).values_list("lead_id", flat=True)
        )
        pending_ftd_leads: set[int] = set()
        pending_reload_leads: set[int] = set()

        def flush_pending_deposits():
            nonlocal created
            if not pending_deposits:
                return
            if self.dry_run:
                created += len(pending_deposits)
                pending_deposits.clear()
                return
            created_deposits = LeadDeposit.all_objects.bulk_create(pending_deposits, batch_size=self.batch_size)
            if created_deposits:
                LeadDeposit.all_objects.bulk_update(
                    created_deposits,
                    ["created_at", "updated_at"],
                    batch_size=self.batch_size,
                )
            created += len(created_deposits)
            pending_deposits.clear()

        for index, row in enumerate(rows, start=1):
            self.log_progress("Deposits", index, total, stage_started_at)
            if not row.get("lead_id"):
                if self.use_orphan_deposit_lead:
                    lead_id = self.ensure_orphan_deposit_target()
                    self.record_warning(
                        "deposits",
                        "assigned_to_orphan_lead",
                        {"legacy_deposit_id": row.get("id"), "orphan_lead_id": lead_id},
                    )
                else:
                    skipped += 1
                    self.record_skip(
                        "deposits",
                        "missing_legacy_lead_id",
                        {"legacy_deposit_id": row.get("id")},
                    )
                    continue
            else:
                lead_id = self.lead_map.get(int(row["lead_id"]))
            if not lead_id:
                skipped += 1
                self.record_skip(
                    "deposits",
                    "lead_not_imported",
                    {"legacy_deposit_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
                continue
            creator_id = None
            if not row.get("creator_id"):
                self.record_warning(
                    "deposits",
                    "missing_creator",
                    {"legacy_deposit_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
            else:
                creator_id = self.user_map.get(int(row["creator_id"]))
                if not creator_id:
                    self.record_warning(
                        "deposits",
                        "unmapped_creator",
                        {
                            "legacy_deposit_id": row.get("id"),
                            "legacy_lead_id": row.get("lead_id"),
                            "legacy_creator_id": row.get("creator_id"),
                        },
                    )
            deposit_type = int(row.get("type") or LeadDeposit.Type.DEPOSIT)
            amount = row.get("amount") or Decimal("0.00")
            if deposit_type == LeadDeposit.Type.FTD and (lead_id in existing_ftd_leads or lead_id in pending_ftd_leads):
                skipped += 1
                self.record_skip(
                    "deposits",
                    "duplicate_ftd",
                    {"legacy_deposit_id": row.get("id"), "legacy_lead_id": row.get("lead_id"), "type": deposit_type},
                )
                continue
            if deposit_type == LeadDeposit.Type.RELOAD and (lead_id in existing_reload_leads or lead_id in pending_reload_leads):
                skipped += 1
                self.record_skip(
                    "deposits",
                    "duplicate_reload",
                    {"legacy_deposit_id": row.get("id"), "legacy_lead_id": row.get("lead_id"), "type": deposit_type},
                )
                continue

            created_at = row.get("created_at") or timezone.now()
            deposit = LeadDeposit(
                lead_id=lead_id,
                creator_id=creator_id,
                amount=amount,
                type=deposit_type,
                created_at=created_at,
                updated_at=created_at,
            )
            pending_deposits.append(deposit)
            if deposit_type == LeadDeposit.Type.FTD:
                pending_ftd_leads.add(lead_id)
            elif deposit_type == LeadDeposit.Type.RELOAD:
                pending_reload_leads.add(lead_id)
            current_latest = latest_contact_by_lead.get(lead_id)
            if current_latest is None or created_at > current_latest:
                latest_contact_by_lead[lead_id] = created_at
            if len(pending_deposits) >= self.batch_size:
                flush_pending_deposits()

        flush_pending_deposits()
        if not self.dry_run and latest_contact_by_lead:
            leads_by_id = Lead.all_objects.in_bulk(latest_contact_by_lead.keys())
            leads_to_update: list[Lead] = []
            for lead_id, latest_contact_at in latest_contact_by_lead.items():
                lead = leads_by_id.get(lead_id)
                if lead is None:
                    continue
                if lead.last_contacted_at is None or latest_contact_at > lead.last_contacted_at:
                    lead.last_contacted_at = latest_contact_at
                    leads_to_update.append(lead)
            if leads_to_update:
                Lead.all_objects.bulk_update(leads_to_update, ["last_contacted_at"], batch_size=self.batch_size)
        self.stdout.write(self.style.SUCCESS(f"Deposits: created={created}, skipped={skipped}"))
        self.log_stage_finished("Deposits", stage_started_at)

    def import_attachments(self):
        stage_started_at = time.monotonic()
        created = 0
        total = self.count_rows(self.tables.record)
        rows = self.iter_rows(
            self.tables.record,
            ["id", "author_id", "comment_id", "lead_id", "record", "created_at"],
        )
        pending_attachments: list[LeadAttachment] = []

        def flush_pending_attachments():
            nonlocal created
            if not pending_attachments:
                return
            if self.dry_run:
                created += len(pending_attachments)
                pending_attachments.clear()
                return
            created_attachments = LeadAttachment.all_objects.bulk_create(pending_attachments, batch_size=self.batch_size)
            if created_attachments:
                LeadAttachment.all_objects.bulk_update(
                    created_attachments,
                    ["created_at", "updated_at"],
                    batch_size=self.batch_size,
                )
            created += len(created_attachments)
            pending_attachments.clear()

        for index, row in enumerate(rows, start=1):
            self.log_progress("Attachments", index, total, stage_started_at)
            if not row.get("lead_id"):
                self.record_skip(
                    "attachments",
                    "missing_legacy_lead_id",
                    {"legacy_record_id": row.get("id")},
                )
                continue
            lead_id = self.lead_map.get(int(row["lead_id"]))
            if not lead_id:
                self.record_skip(
                    "attachments",
                    "lead_not_imported",
                    {"legacy_record_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
                continue
            uploaded_by_id = self.user_map.get(int(row["author_id"])) if row.get("author_id") else None
            file_name = str(row.get("record") or "").strip()
            if not file_name:
                self.record_skip(
                    "attachments",
                    "missing_file_name",
                    {"legacy_record_id": row.get("id"), "legacy_lead_id": row.get("lead_id")},
                )
                continue
            mime_type, _ = mimetypes.guess_type(file_name)
            kind = LeadAttachment.Kind.AUDIO if (mime_type or "").startswith("audio/") else LeadAttachment.Kind.IMAGE
            absolute_path = Path(settings.MEDIA_ROOT) / file_name
            size_bytes = absolute_path.stat().st_size if absolute_path.exists() else 0
            created_at = row.get("created_at") or timezone.now()
            attachment = LeadAttachment(
                lead_id=lead_id,
                uploaded_by_id=uploaded_by_id,
                file=file_name,
                kind=kind,
                original_name=Path(file_name).name,
                mime_type=mime_type or "",
                size_bytes=size_bytes,
                created_at=created_at,
                updated_at=created_at,
            )
            pending_attachments.append(attachment)
            if len(pending_attachments) >= self.batch_size:
                flush_pending_attachments()

        flush_pending_attachments()
        self.stdout.write(self.style.SUCCESS(f"Attachments: created={created}"))
        self.log_stage_finished("Attachments", stage_started_at)

    def write_duplicate_report(self):
        if not self.duplicate_phone_rows:
            self.stdout.write("Duplicate phone report: no rows.")
            return
        fieldnames = sorted({key for row in self.duplicate_phone_rows for key in row.keys()})
        with self.report_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.duplicate_phone_rows)
        self.stdout.write(
            self.style.WARNING(
                f"Duplicate phone report written to {self.report_path} ({len(self.duplicate_phone_rows)} rows)."
            )
        )

    def write_skip_reports(self):
        for section, rows in self.skip_rows.items():
            if not rows:
                continue
            report_file = self.report_path.parent / f"{section}_skipped.csv"
            fieldnames = sorted({key for row in rows for key in row.keys()})
            with report_file.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            self.stdout.write(
                self.style.WARNING(f"{section.capitalize()} skip report written to {report_file} ({len(rows)} rows).")
            )

    def write_warning_reports(self):
        for section, rows in self.warning_rows.items():
            if not rows:
                continue
            report_file = self.report_path.parent / f"{section}_warnings.csv"
            fieldnames = sorted({key for row in rows for key in row.keys()})
            with report_file.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            self.stdout.write(
                self.style.WARNING(f"{section.capitalize()} warning report written to {report_file} ({len(rows)} rows).")
            )

    def print_skip_summary(self):
        if not self.skip_stats:
            self.stdout.write("Skip summary: no skipped rows.")
            return
        self.stdout.write("Skip summary:")
        for section in sorted(self.skip_stats.keys()):
            reasons = ", ".join(
                f"{reason}={count}"
                for reason, count in sorted(self.skip_stats[section].items(), key=lambda item: (-item[1], item[0]))
            )
            self.stdout.write(f"  - {section}: {reasons}")

    def print_warning_summary(self):
        if not self.warning_stats:
            self.stdout.write("Warning summary: no warnings.")
            return
        self.stdout.write("Warning summary:")
        for section in sorted(self.warning_stats.keys()):
            reasons = ", ".join(
                f"{reason}={count}"
                for reason, count in sorted(self.warning_stats[section].items(), key=lambda item: (-item[1], item[0]))
            )
            self.stdout.write(f"  - {section}: {reasons}")
