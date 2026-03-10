from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from apps.partners.models import Partner, PartnerToken


class Command(BaseCommand):
    help = "Create partner token and print raw token once"

    def add_arguments(self, parser):
        parser.add_argument("--partner", required=True, help="Partner code (slug)")
        parser.add_argument("--name", default="", help="Token name (optional)")
        parser.add_argument("--source", default="", help="Optional source string to bind token to")

    def handle(self, *args, **options):
        partner_code = options["partner"]
        name = options["name"]
        source = (options["source"] or "").strip()

        partner = Partner.objects.filter(code=partner_code).first()
        if not partner:
            raise CommandError(f"Partner not found: {partner_code}")

        raw = PartnerToken.generate_raw_token()
        token = PartnerToken.build(partner=partner, raw_token=raw, name=name, source=source)
        token.save()

        self.stdout.write(self.style.SUCCESS("Partner token created. SAVE THIS TOKEN NOW — it won't be shown again."))
        self.stdout.write(raw)
