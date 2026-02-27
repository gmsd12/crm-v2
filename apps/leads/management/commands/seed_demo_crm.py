from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import models
from django.utils import timezone

from apps.iam.models import UserRole
from apps.leads.models import (
    Lead,
    LeadComment,
    LeadStatus,
    LeadAuditEvent,
    LeadAuditLog,
    LeadAuditSource,
)
from apps.partners.models import Partner, PartnerSource

User = get_user_model()


class Command(BaseCommand):
    help = "Seed readable demo CRM data and top up leads/comments without random gibberish."

    def add_arguments(self, parser):
        parser.add_argument("--partner-code", default="demo", help="Partner code for demo data")
        parser.add_argument("--partner-name", default="Northwind Media", help="Partner display name")
        parser.add_argument("--leads", type=int, default=40, help="How many leads to add now")
        parser.add_argument("--comments", type=int, default=80, help="How many comments to add now")
        parser.add_argument("--password", default="demo12345", help="Password for demo users (created/updated)")
        parser.add_argument("--without-users", action="store_true", help="Do not create/update demo users")

    def handle(self, *args, **options):
        leads_count = options["leads"]
        comments_count = options["comments"]
        if leads_count < 0 or comments_count < 0:
            raise CommandError("--leads and --comments must be >= 0")

        partner = self._ensure_partner(options["partner_code"], options["partner_name"])
        sources = self._ensure_sources(partner)
        statuses = self._ensure_statuses()

        users = []
        if not options["without_users"]:
            users = self._ensure_demo_users(password=options["password"])

        assignable = [u for u in users if u.role in {UserRole.MANAGER, UserRole.RET}]
        if not assignable:
            assignable = list(User.objects.filter(role__in=[UserRole.MANAGER, UserRole.RET], is_active=True).order_by("id"))

        created_leads = self._create_leads(
            partner=partner,
            sources=sources,
            statuses=statuses,
            assignees=assignable,
            users=users,
            leads_count=leads_count,
        )
        created_comments = self._create_comments(partner=partner, authors=users, comments_count=comments_count)

        self.stdout.write(self.style.SUCCESS("Demo data ready."))
        self.stdout.write(f"- Partner: {partner.code} ({partner.name})")
        self.stdout.write(f"- Leads added: {created_leads}")
        self.stdout.write(f"- Comments added: {created_comments}")
        if users:
            self.stdout.write("- Demo users (username / role):")
            for user in users:
                self.stdout.write(f"  - {user.username} / {user.role}")
            self.stdout.write(f"- Password for demo users: {options['password']}")
        self.stdout.write(
            "- To top up later: ./.venv/bin/python manage.py seed_demo_crm --leads 20 --comments 40"
        )

    def _ensure_partner(self, code: str, name: str) -> Partner:
        partner, created = Partner.objects.get_or_create(code=code, defaults={"name": name, "is_active": True})
        if not created and partner.name != name:
            partner.name = name
            partner.save(update_fields=["name", "updated_at"])
        return partner

    def _ensure_sources(self, partner: Partner) -> list[PartnerSource]:
        source_defs = [
            ("website", "Website"),
            ("referral", "Referral"),
            ("ads", "Paid Ads"),
            ("events", "Events"),
        ]
        result = []
        for code, name in source_defs:
            source, created = PartnerSource.objects.get_or_create(
                partner=partner,
                code=code,
                defaults={"name": name, "is_active": True},
            )
            if not created and source.name != name:
                source.name = name
                source.save(update_fields=["name", "updated_at"])
            result.append(source)
        return result

    def _ensure_statuses(self) -> dict[str, LeadStatus]:
        status_defs = [
            ("NEW", "New", 10, "#3B82F6", False, LeadStatus.ConversionBucket.IGNORE),
            ("CONTACTED", "Contacted", 20, "#06B6D4", False, LeadStatus.ConversionBucket.IGNORE),
            ("QUALIFIED", "Qualified", 30, "#10B981", False, LeadStatus.ConversionBucket.IGNORE),
            ("PROPOSAL", "Proposal", 40, "#F59E0B", False, LeadStatus.ConversionBucket.IGNORE),
            ("WON", "Won", 90, "#22C55E", True, LeadStatus.ConversionBucket.WON),
            ("LOST", "Lost", 100, "#EF4444", True, LeadStatus.ConversionBucket.LOST),
        ]

        statuses: dict[str, LeadStatus] = {}
        for code, name, order, color, is_valid, conversion_bucket in status_defs:
            status_obj, created = LeadStatus.objects.get_or_create(
                code=code,
                defaults={
                    "name": name,
                    "order": order,
                    "color": color,
                    "is_default_for_new_leads": False,
                    "is_active": True,
                    "is_valid": is_valid,
                    "conversion_bucket": conversion_bucket,
                },
            )
            if not created:
                fields_to_update = []
                if status_obj.name != name:
                    status_obj.name = name
                    fields_to_update.append("name")
                if status_obj.order != order:
                    status_obj.order = order
                    fields_to_update.append("order")
                if status_obj.color != color:
                    status_obj.color = color
                    fields_to_update.append("color")
                if status_obj.is_valid != is_valid:
                    status_obj.is_valid = is_valid
                    fields_to_update.append("is_valid")
                if status_obj.conversion_bucket != conversion_bucket:
                    status_obj.conversion_bucket = conversion_bucket
                    fields_to_update.append("conversion_bucket")
                if not status_obj.is_active:
                    status_obj.is_active = True
                    fields_to_update.append("is_active")
                if fields_to_update:
                    fields_to_update.append("updated_at")
                    status_obj.save(update_fields=fields_to_update)
            statuses[code] = status_obj

        if not statuses["NEW"].is_default_for_new_leads:
            LeadStatus.objects.filter(is_default_for_new_leads=True).update(is_default_for_new_leads=False)
            statuses["NEW"].is_default_for_new_leads = True
            statuses["NEW"].save(update_fields=["is_default_for_new_leads", "updated_at"])

        return statuses

    def _ensure_demo_users(self, password: str) -> list[User]:
        user_defs = [
            ("super_demo", UserRole.SUPERUSER, True, True, "Super", "Demo"),
            ("admin_demo", UserRole.ADMIN, True, False, "Alex", "Admin"),
            ("teamlead_demo", UserRole.TEAMLEADER, True, False, "Taylor", "Lead"),
            ("manager_anna", UserRole.MANAGER, False, False, "Anna", "Mills"),
            ("manager_ivan", UserRole.MANAGER, False, False, "Ivan", "Stone"),
            ("ret_olga", UserRole.RET, False, False, "Olga", "Reed"),
        ]
        result = []
        for username, role, is_staff, is_superuser, first_name, last_name in user_defs:
            user, created = User.objects.get_or_create(
                username=username,
                defaults={
                    "role": role,
                    "is_active": True,
                    "is_staff": is_staff,
                    "is_superuser": is_superuser,
                    "first_name": first_name,
                    "last_name": last_name,
                },
            )
            fields_to_update = []
            if not created:
                if user.role != role:
                    user.role = role
                    fields_to_update.append("role")
                if user.is_staff != is_staff:
                    user.is_staff = is_staff
                    fields_to_update.append("is_staff")
                if user.is_superuser != is_superuser:
                    user.is_superuser = is_superuser
                    fields_to_update.append("is_superuser")
                if not user.is_active:
                    user.is_active = True
                    fields_to_update.append("is_active")
                if user.first_name != first_name:
                    user.first_name = first_name
                    fields_to_update.append("first_name")
                if user.last_name != last_name:
                    user.last_name = last_name
                    fields_to_update.append("last_name")
            user.set_password(password)
            fields_to_update.append("password")
            user.save(update_fields=sorted(set(fields_to_update)))
            result.append(user)
        return result

    def _create_leads(
        self,
        *,
        partner: Partner,
        sources: list[PartnerSource],
        statuses: dict[str, LeadStatus],
        assignees: list[User],
        users: list[User],
        leads_count: int,
    ) -> int:
        if leads_count <= 0:
            return 0

        first_names = [
            "Emma",
            "Noah",
            "Liam",
            "Olivia",
            "Mason",
            "Sophia",
            "Lucas",
            "Mia",
            "Ethan",
            "Ava",
            "James",
            "Ella",
            "Henry",
            "Grace",
        ]
        last_names = [
            "Carter",
            "Walker",
            "Harris",
            "Turner",
            "King",
            "White",
            "Hall",
            "Young",
            "Scott",
            "Green",
            "Adams",
            "Baker",
        ]
        companies = [
            "North Star Logistics",
            "Blue Peak Fitness",
            "Urban Bloom Studio",
            "Silverline Dental",
            "Atlas Property Group",
            "Fresh Market Hub",
            "Beacon Tech Works",
            "Lakeview Wellness",
        ]
        seq = self._next_lead_seq(partner=partner)
        now = timezone.now()
        created = 0
        manager_users = [user for user in assignees if user.role == UserRole.MANAGER]
        ret_users = [user for user in assignees if user.role == UserRole.RET]
        any_assignees = assignees or list(User.objects.filter(role__in=[UserRole.MANAGER, UserRole.RET], is_active=True).order_by("id"))
        actor_user = next((u for u in users if u.role in {UserRole.ADMIN, UserRole.SUPERUSER, UserRole.TEAMLEADER}), None)
        geo_codes = ["US", "CA", "GB", "DE", "PL", "CH", "AE", "ES", "IT", "FR", "SE", "NL"]

        for idx in range(leads_count):
            lead_seq = seq + idx
            full_name = f"{first_names[lead_seq % len(first_names)]} {last_names[lead_seq % len(last_names)]}"
            company = companies[lead_seq % len(companies)]
            source = sources[lead_seq % len(sources)]

            if lead_seq % 10 == 0:
                status_obj = statuses["WON"]
            elif lead_seq % 7 == 0:
                status_obj = statuses["LOST"]
            elif lead_seq % 5 == 0:
                status_obj = statuses["PROPOSAL"]
            elif lead_seq % 3 == 0:
                status_obj = statuses["QUALIFIED"]
            elif lead_seq % 2 == 0:
                status_obj = statuses["CONTACTED"]
            else:
                status_obj = statuses["NEW"]

            first_manager = manager_users[lead_seq % len(manager_users)] if manager_users else None
            received_at = now - timedelta(days=(lead_seq % 30), hours=(lead_seq * 3) % 24)
            next_contact_at = (
                None
                if status_obj.conversion_bucket in {LeadStatus.ConversionBucket.WON, LeadStatus.ConversionBucket.LOST}
                else received_at + timedelta(days=1 + (lead_seq % 5))
            )
            first_assigned_at = received_at + timedelta(hours=1) if first_manager else None
            closed_at = None
            closing_author = None
            manager = first_manager
            assigned_at = first_assigned_at

            if status_obj.code == "WON":
                closed_at = min(now - timedelta(minutes=5), received_at + timedelta(days=2 + (lead_seq % 5), hours=3))
                if manager_users:
                    if first_manager and len(manager_users) > 1 and lead_seq % 3 == 0:
                        candidate = manager_users[(lead_seq + 1) % len(manager_users)]
                        if candidate.id == first_manager.id:
                            candidate = manager_users[(lead_seq + 2) % len(manager_users)]
                        closing_author = candidate
                    else:
                        closing_author = first_manager or manager_users[lead_seq % len(manager_users)]
                else:
                    closing_author = first_manager

                if ret_users and closing_author and lead_seq % 2 == 0:
                    manager = ret_users[lead_seq % len(ret_users)]
                    assigned_at = closed_at + timedelta(hours=1)
                else:
                    manager = closing_author
                    assigned_at = closed_at
            elif status_obj.code == "LOST":
                closing_author = first_manager
                closed_at = min(now - timedelta(minutes=5), received_at + timedelta(days=1 + (lead_seq % 4), hours=2))

            if manager is None and any_assignees:
                manager = any_assignees[lead_seq % len(any_assignees)]
                assigned_at = assigned_at or (received_at + timedelta(hours=1))

            phone = self._unique_phone(start_seq=lead_seq)
            email = f"lead{lead_seq}@demo-crm.local"
            geo = geo_codes[lead_seq % len(geo_codes)]

            lead = Lead.objects.create(
                partner=partner,
                manager=manager,
                first_manager=first_manager,
                source=source,
                status=status_obj,
                geo=geo,
                full_name=full_name,
                phone=phone,
                email=email,
                priority=self._priority_for_seq(lead_seq),
                next_contact_at=next_contact_at,
                last_contacted_at=received_at + timedelta(hours=4) if status_obj.code != "NEW" else None,
                assigned_at=assigned_at,
                first_assigned_at=first_assigned_at,
                custom_fields={
                    "company": company,
                    "city": self._city_for_seq(lead_seq),
                    "segment": "SMB" if lead_seq % 2 == 0 else "Mid-Market",
                },
                received_at=received_at,
            )
            self._seed_lead_audit_logs(
                lead=lead,
                statuses=statuses,
                actor_user=actor_user,
                first_manager=first_manager,
                closing_author=closing_author,
                final_manager=manager,
                first_assigned_at=first_assigned_at,
                closed_at=closed_at,
                received_at=received_at,
            )
            created += 1

        return created

    def _seed_lead_audit_logs(
        self,
        *,
        lead: Lead,
        statuses: dict[str, LeadStatus],
        actor_user: User | None,
        first_manager: User | None,
        closing_author: User | None,
        final_manager: User | None,
        first_assigned_at,
        closed_at,
        received_at,
    ) -> None:
        def _manager_payload(user: User | None):
            if not user:
                return None
            return {
                "id": str(user.id),
                "username": user.username,
                "first_name": (user.first_name or "").strip(),
                "last_name": (user.last_name or "").strip(),
                "role": user.role,
                "is_active": user.is_active,
            }

        def _update_ts(log_obj, dt_obj):
            LeadAuditLog.objects.filter(pk=log_obj.pk).update(created_at=dt_obj)

        if first_manager and first_assigned_at:
            assigned_log = LeadAuditLog.objects.create(
                lead=lead,
                event_type=LeadAuditEvent.MANAGER_ASSIGNED,
                actor_user=actor_user,
                source=LeadAuditSource.SYSTEM,
                payload_before={"lead_id": str(lead.id), "manager": None},
                payload_after={"lead_id": str(lead.id), "manager": _manager_payload(first_manager)},
            )
            _update_ts(assigned_log, first_assigned_at)

        if closing_author and first_manager and closing_author.id != first_manager.id and closed_at:
            reassigned_log = LeadAuditLog.objects.create(
                lead=lead,
                event_type=LeadAuditEvent.MANAGER_REASSIGNED,
                actor_user=actor_user,
                source=LeadAuditSource.SYSTEM,
                payload_before={"lead_id": str(lead.id), "manager": _manager_payload(first_manager)},
                payload_after={"lead_id": str(lead.id), "manager": _manager_payload(closing_author)},
            )
            _update_ts(reassigned_log, closed_at - timedelta(hours=1))

        if final_manager and closing_author and final_manager.id != closing_author.id and closed_at:
            handoff_log = LeadAuditLog.objects.create(
                lead=lead,
                event_type=LeadAuditEvent.MANAGER_REASSIGNED,
                actor_user=actor_user,
                source=LeadAuditSource.SYSTEM,
                payload_before={"lead_id": str(lead.id), "manager": _manager_payload(closing_author)},
                payload_after={"lead_id": str(lead.id), "manager": _manager_payload(final_manager)},
            )
            _update_ts(handoff_log, closed_at + timedelta(hours=1))

        if lead.status and lead.status.code != "NEW":
            status_changed_at = closed_at or min(
                timezone.now() - timedelta(minutes=3),
                received_at + timedelta(days=1, hours=2),
            )
            status_log = LeadAuditLog.objects.create(
                lead=lead,
                event_type=LeadAuditEvent.STATUS_CHANGED,
                from_status=statuses["NEW"],
                to_status=lead.status,
                actor_user=actor_user,
                source=LeadAuditSource.SYSTEM,
                payload_before={
                    "lead_id": str(lead.id),
                    "status_id": str(statuses["NEW"].id),
                    "status_code": statuses["NEW"].code,
                },
                payload_after={
                    "lead_id": str(lead.id),
                    "status_id": str(lead.status_id) if lead.status_id else None,
                    "status_code": lead.status.code if lead.status else None,
                },
            )
            _update_ts(status_log, status_changed_at)

    def _create_comments(self, *, partner: Partner, authors: list[User], comments_count: int) -> int:
        if comments_count <= 0:
            return 0

        leads = list(Lead.objects.filter(partner=partner).order_by("-received_at", "id")[:500])
        if not leads:
            return 0

        usable_authors = [u for u in authors if u.role in {UserRole.ADMIN, UserRole.TEAMLEADER, UserRole.MANAGER, UserRole.RET}]
        if not usable_authors:
            usable_authors = list(
                User.objects.filter(
                    role__in=[UserRole.ADMIN, UserRole.TEAMLEADER, UserRole.MANAGER, UserRole.RET],
                    is_active=True,
                ).order_by("id")
            )

        templates = [
            "Reached out and confirmed the main business need",
            "Follow-up call done, waiting for internal approval",
            "Sent pricing options and timeline details",
            "Client asked for a short product walkthrough",
            "Clarified budget range and implementation scope",
            "Scheduled next contact for contract discussion",
            "Received positive signal, prepare final proposal",
            "Need one more call with decision maker",
        ]
        next_steps = [
            "send proposal",
            "prepare contract draft",
            "book demo call",
            "check legal requirements",
            "confirm start date",
            "align payment terms",
        ]

        now = timezone.now()
        created = 0
        for idx in range(comments_count):
            lead = leads[idx % len(leads)]
            author = usable_authors[idx % len(usable_authors)] if usable_authors else None
            body = f"{templates[idx % len(templates)]}. Next step: {next_steps[idx % len(next_steps)]}."
            is_pinned = (idx + 1) % 12 == 0
            comment = LeadComment.objects.create(lead=lead, author=author, body=body, is_pinned=is_pinned)

            shifted_ts = now - timedelta(hours=(comments_count - idx) % 72)
            LeadComment.objects.filter(pk=comment.pk).update(created_at=shifted_ts, updated_at=shifted_ts)
            created += 1

        return created

    def _next_lead_seq(self, *, partner: Partner) -> int:
        max_id = Lead.all_objects.filter(partner=partner).aggregate(max_id=models.Max("id"))["max_id"] or 0
        return max_id + 1

    def _unique_phone(self, *, start_seq: int) -> str:
        seq = start_seq
        while True:
            phone = f"+1555{seq:07d}"
            exists = Lead.all_objects.filter(phone=phone).exists()
            if not exists:
                return phone
            seq += 1

    def _priority_for_seq(self, seq: int) -> int:
        priorities = [Lead.Priority.NORMAL, Lead.Priority.NORMAL, Lead.Priority.HIGH, Lead.Priority.LOW, Lead.Priority.URGENT]
        return priorities[seq % len(priorities)]

    def _city_for_seq(self, seq: int) -> str:
        cities = ["Warsaw", "Berlin", "Prague", "Vienna", "Madrid", "Milan", "Lisbon", "Dublin"]
        return cities[seq % len(cities)]
