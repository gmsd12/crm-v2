from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from apps.partners.models import Partner, PartnerSource, PartnerToken


class Command(BaseCommand):
    help = "Create partner token and print raw token once"

    def add_arguments(self, parser):
        parser.add_argument("--partner", required=True, help="Partner code (slug)")
        parser.add_argument("--name", default="", help="Token name (optional)")
        parser.add_argument("--source", default="", help="Optional partner source code to bind token to")

    def handle(self, *args, **options):
        partner_code = options["partner"]
        name = options["name"]
        source_code = options["source"]

        partner = Partner.objects.filter(code=partner_code).first()
        if not partner:
            raise CommandError(f"Partner not found: {partner_code}")

        source = None
        if source_code:
            source = PartnerSource.objects.filter(partner=partner, code=source_code).first()
            if not source:
                raise CommandError(f"Source not found for partner {partner_code}: {source_code}")

        raw = PartnerToken.generate_raw_token()
        token = PartnerToken.build(partner=partner, raw_token=raw, name=name, source=source)
        token.save()

        self.stdout.write(self.style.SUCCESS("Partner token created. SAVE THIS TOKEN NOW — it won't be shown again."))
        self.stdout.write(raw)
