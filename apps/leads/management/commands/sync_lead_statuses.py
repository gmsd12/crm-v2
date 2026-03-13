from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import BaseCommand
from django.db import transaction

from apps.leads.models import LeadStatus


@dataclass(frozen=True)
class StatusSeed:
    code: str
    name: str
    order: int
    color: str
    is_valid: bool
    work_bucket: str
    conversion_bucket: str
    is_default: bool = False


STATUS_SEEDS: list[StatusSeed] = [
    StatusSeed(
        code="new",
        name="Новый",
        order=10,
        color="info",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
        is_default=True,
    ),
    StatusSeed(
        code="potential",
        name="Потенциальный",
        order=20,
        color="success",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="in_process",
        name="В обработке",
        order=30,
        color="warning",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="in_work",
        name="В работе",
        order=40,
        color="primary",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="recall",
        name="Рек",
        order=50,
        color="warning",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="rehab",
        name="Реабилитация",
        order=70,
        color="secondary",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.RETURN,
        conversion_bucket=LeadStatus.ConversionBucket.WON,
    ),
    StatusSeed(
        code="cold",
        name="Холодка",
        order=80,
        color="neutral",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.RETURN,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="deposit",
        name="Депозит",
        order=90,
        color="success",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.WON,
    ),
        StatusSeed(
        code="lost",
        name="Срез",
        order=90,
        color="error",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.RETURN,
        conversion_bucket=LeadStatus.ConversionBucket.LOST,
    ),
    StatusSeed(
        code="never_answer",
        name="Never Answer",
        order=100,
        color="error",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="high_age",
        name="Высокий возраст",
        order=110,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="other_person",
        name="Другой человек",
        order=120,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="duplicate",
        name="Дубль",
        order=130,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="nd",
        name="НД",
        order=140,
        color="warning",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="inadequate",
        name="Не адекватный",
        order=160,
        color="error",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="not_potential",
        name="Не потенциальный",
        order=170,
        color="error",
        is_valid=True,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="not_registered",
        name="Не регистрировался",
        order=180,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="wrong_number",
        name="Неверный номер",
        order=190,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="invalid_language",
        name="Недопустимый язык",
        order=200,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="under_18",
        name="Нет 18",
        order=210,
        color="error",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
    StatusSeed(
        code="test",
        name="Тест",
        order=220,
        color="neutral",
        is_valid=False,
        work_bucket=LeadStatus.WorkBucket.NON_WORKING,
        conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
    ),
]


class Command(BaseCommand):
    help = "Creates or updates the predefined lead statuses set."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show changes without writing them to database.",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        created_count = 0
        updated_count = 0
        restored_count = 0
        default_codes = [seed.code for seed in STATUS_SEEDS if seed.is_default]

        existing_defaults = list(
            LeadStatus.all_objects.filter(is_default_for_new_leads=True).exclude(code__in=default_codes)
        )

        if existing_defaults:
            self.stdout.write(
                self.style.WARNING(
                    "Default status will be cleared for: "
                    + ", ".join(sorted(status.code for status in existing_defaults))
                )
            )
            if not dry_run:
                LeadStatus.all_objects.filter(id__in=[status.id for status in existing_defaults]).update(
                    is_default_for_new_leads=False
                )

        for seed in STATUS_SEEDS:
            status_obj = LeadStatus.all_objects.filter(code=seed.code).first()
            created = status_obj is None
            if created:
                status_obj = LeadStatus(code=seed.code)
                created_count += 1

            changed_fields: list[str] = []
            field_values = {
                "name": seed.name,
                "order": seed.order,
                "color": seed.color,
                "is_default_for_new_leads": seed.is_default,
                "is_active": True,
                "is_valid": seed.is_valid,
                "work_bucket": seed.work_bucket,
                "conversion_bucket": seed.conversion_bucket,
            }
            for field_name, value in field_values.items():
                if getattr(status_obj, field_name) != value:
                    setattr(status_obj, field_name, value)
                    changed_fields.append(field_name)

            if status_obj.is_deleted:
                status_obj.is_deleted = False
                status_obj.deleted_at = None
                changed_fields.extend(["is_deleted", "deleted_at"])
                restored_count += 1

            if created:
                self.stdout.write(self.style.SUCCESS(f"[create] {seed.code} -> {seed.name}"))
                if not dry_run:
                    status_obj.save()
                continue

            if changed_fields:
                updated_count += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"[update] {seed.code} ({', '.join(changed_fields)})"
                    )
                )
                if not dry_run:
                    status_obj.save(update_fields=sorted(set(changed_fields + ["updated_at"])))
            else:
                self.stdout.write(f"[skip] {seed.code}")

        if dry_run:
            transaction.set_rollback(True)
            self.stdout.write(self.style.WARNING("Dry-run completed, changes rolled back."))
            return

        self.stdout.write(
            self.style.SUCCESS(
                f"Done: created={created_count}, updated={updated_count}, restored={restored_count}"
            )
        )
