from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import BaseCommand, CommandError
from rest_framework.test import APIClient

from apps.leads.models import Lead, LeadStatus
from apps.partners.models import Partner, PartnerToken


PARTNER_NAME_POOL = [
    "North Peak Media",
    "Blue Harbor Ads",
    "Evergreen Growth",
    "Solaris Traffic",
    "Cedar Lead Lab",
]

SOURCE_POOL = ["google_ads", "facebook_ads", "organic", "referral", "tiktok_ads"]

LEAD_NAME_POOL = [
    "Emma Carter",
    "Liam Walker",
    "Olivia Harris",
    "Noah Turner",
    "Sophia King",
    "Lucas White",
    "Mia Hall",
    "Ethan Young",
    "Ava Scott",
    "James Green",
]

GEO_POOL = ["US", "CA", "GB", "DE", "CH"]


@dataclass
class PartnerRunResult:
    code: str
    token: str
    created: int
    duplicates: int
    first_phone: str
    last_phone: str


class Command(BaseCommand):
    help = "Simulate partner lead uploads via Partner API with real token auth"

    def add_arguments(self, parser):
        parser.add_argument("--partners", type=int, default=5, help="How many partners to simulate")
        parser.add_argument("--leads-per-partner", type=int, default=5, help="How many leads to upload per partner")
        parser.add_argument("--base-code", default="demo-partner", help="Partner code prefix")

    def handle(self, *args, **options):
        partners_count = options["partners"]
        leads_per_partner = options["leads_per_partner"]
        base_code = options["base_code"]

        if partners_count <= 0:
            raise CommandError("--partners must be > 0")
        if leads_per_partner <= 0:
            raise CommandError("--leads-per-partner must be > 0")

        self._ensure_default_status()
        client = APIClient(HTTP_HOST="localhost")

        results: list[PartnerRunResult] = []
        for idx in range(1, partners_count + 1):
            partner = self._ensure_partner(idx=idx, base_code=base_code)
            raw_token = self._ensure_token(partner=partner)
            result = self._upload_leads(
                client=client,
                partner=partner,
                raw_token=raw_token,
                partner_idx=idx,
                leads_per_partner=leads_per_partner,
            )
            results.append(result)

        total_created = sum(item.created for item in results)
        total_duplicates = sum(item.duplicates for item in results)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Partner upload simulation completed"))
        self.stdout.write(f"Created leads: {total_created}")
        self.stdout.write(f"Duplicate rejects: {total_duplicates}")
        self.stdout.write("")
        self.stdout.write("Per partner:")
        for item in results:
            self.stdout.write(
                f"- {item.code}: created={item.created}, duplicates={item.duplicates}, "
                f"phones={item.first_phone}..{item.last_phone}, token={item.token}"
            )

        self.stdout.write("")
        self.stdout.write("Example curl (replace token/source/phone):")
        self.stdout.write(
            "curl -X POST http://localhost:8000/api/v1/partner/leads/ "
            "-H 'Content-Type: application/json' "
            "-H 'X-Partner-Token: <TOKEN>' "
            "-d '{\"source\":\"google_ads\",\"geo\":\"US\",\"full_name\":\"John Doe\","
            "\"phone\":\"+15550000001\",\"email\":\"john@example.com\","
            "\"custom_fields\":{\"campaign\":\"spring_sale\"}}'"
        )

    def _ensure_default_status(self) -> None:
        existing_default = (
            LeadStatus.objects
            .filter(is_default_for_new_leads=True, is_active=True)
            .order_by("order", "created_at")
            .first()
        )
        if existing_default:
            return

        status = LeadStatus.all_objects.filter(code="NEW").first()
        if status is None:
            LeadStatus.objects.create(
                code="NEW",
                name="New",
                order=10,
                color="#3B82F6",
                is_default_for_new_leads=True,
                is_active=True,
                is_valid=False,
                conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
            )
            return

        update_fields: list[str] = []
        if status.is_deleted:
            status.is_deleted = False
            status.deleted_at = None
            update_fields.extend(["is_deleted", "deleted_at"])
        if not status.is_active:
            status.is_active = True
            update_fields.append("is_active")
        if not status.is_default_for_new_leads:
            status.is_default_for_new_leads = True
            update_fields.append("is_default_for_new_leads")
        if update_fields:
            update_fields.append("updated_at")
            status.save(update_fields=sorted(set(update_fields)))

    def _ensure_partner(self, *, idx: int, base_code: str) -> Partner:
        code = f"{base_code}-{idx:02d}"
        name = PARTNER_NAME_POOL[(idx - 1) % len(PARTNER_NAME_POOL)]

        partner = Partner.all_objects.filter(code=code).first()
        if partner is None:
            return Partner.objects.create(code=code, name=name, is_active=True)

        update_fields: list[str] = []
        if partner.name != name:
            partner.name = name
            update_fields.append("name")
        if partner.is_deleted:
            partner.is_deleted = False
            partner.deleted_at = None
            update_fields.extend(["is_deleted", "deleted_at"])
        if not partner.is_active:
            partner.is_active = True
            update_fields.append("is_active")
        if update_fields:
            update_fields.append("updated_at")
            partner.save(update_fields=sorted(set(update_fields)))
        return partner

    def _ensure_token(self, *, partner: Partner) -> str:
        raw_token = f"tok_live_{partner.code.replace('-', '_')}_demo_2026_v1"
        token_hash = PartnerToken.hash_token(raw_token)

        token = PartnerToken.all_objects.filter(partner=partner, token_hash=token_hash).first()
        if token is None:
            token = PartnerToken.build(
                partner=partner,
                raw_token=raw_token,
                name="demo-ingest",
                source="",
            )

        token.name = "demo-ingest"
        token.source = ""
        token.is_active = True
        token.revoked_at = None
        token.is_deleted = False
        token.deleted_at = None
        token.save()
        return raw_token

    def _upload_leads(
        self,
        *,
        client: APIClient,
        partner: Partner,
        raw_token: str,
        partner_idx: int,
        leads_per_partner: int,
    ) -> PartnerRunResult:
        existing_count = Lead.objects.filter(partner=partner).count()
        created = 0
        duplicates = 0
        first_phone = ""
        last_phone = ""

        for i in range(leads_per_partner):
            seq = existing_count + i + 1
            full_name = LEAD_NAME_POOL[(partner_idx * 7 + i) % len(LEAD_NAME_POOL)]
            source = SOURCE_POOL[(partner_idx * 3 + i) % len(SOURCE_POOL)]
            geo = GEO_POOL[(partner_idx + i) % len(GEO_POOL)]
            phone = f"+1555{partner_idx:02d}{seq:04d}"
            email = f"{partner.code.replace('-', '.')}.{seq}@demo.local"

            payload = {
                "source": source,
                "geo": geo,
                "full_name": full_name,
                "phone": phone,
                "email": email,
                "priority": 20,
                "custom_fields": {
                    "campaign": f"campaign_{partner_idx:02d}",
                    "landing": f"https://{partner.code}.example.com/lp-{(i % 3) + 1}",
                },
            }

            response = client.post(
                "/api/v1/partner/leads/",
                payload,
                format="json",
                HTTP_X_PARTNER_TOKEN=raw_token,
                HTTP_HOST="localhost",
            )
            if response.status_code not in {201, 409}:
                body = getattr(response, "data", None)
                if body is None:
                    body = response.content.decode("utf-8", errors="ignore")
                raise CommandError(
                    f"Upload failed for {partner.code} (phone={phone}), "
                    f"status={response.status_code}, body={body}"
                )

            if not first_phone:
                first_phone = phone
            last_phone = phone

            if response.data.get("duplicate_rejected"):
                duplicates += 1
            elif response.data.get("created"):
                created += 1

        return PartnerRunResult(
            code=partner.code,
            token=raw_token,
            created=created,
            duplicates=duplicates,
            first_phone=first_phone,
            last_phone=last_phone,
        )
