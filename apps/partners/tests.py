from rest_framework.test import APITestCase
from rest_framework_simplejwt.tokens import RefreshToken
from django.core.cache import cache
from django.contrib.auth import get_user_model
from unittest.mock import patch

from datetime import timedelta
from django.utils import timezone

from apps.iam.models import UserRole
from apps.leads.models import Lead, LeadStatus
from apps.partners.models import Partner, PartnerSource, PartnerToken
from apps.partners.pagination import PartnerLeadPagination
from apps.partners.throttling import PartnerTokenRateThrottle

User = get_user_model()


class PartnerLeadApiTests(APITestCase):
    def setUp(self):
        self.partner = Partner.objects.create(name="Acme", code="acme")
        self.source = PartnerSource.objects.create(
            partner=self.partner,
            name="Google",
            code="google",
            is_active=True,
        )
        self.raw_token = "tok_live_partner_test_1234567890"
        self.token = PartnerToken.build(partner=self.partner, raw_token=self.raw_token, name="test")
        self.token.save()

    def test_partner_lead_create_rejects_duplicate_phone(self):
        url = "/api/v1/partner/leads/"
        payload = {
            "phone": "+155500001",
            "source": self.source.code,
            "full_name": "John Doe",
            "email": "lead@example.com",
            "custom_fields": {"channel": "google"},
        }
        headers = {"HTTP_X_PARTNER_TOKEN": self.raw_token}

        first = self.client.post(url, payload, format="json", **headers)
        second = self.client.post(url, payload, format="json", **headers)

        self.assertEqual(first.status_code, 201)
        self.assertTrue(first.data["created"])
        self.assertEqual(second.status_code, 409)
        self.assertFalse(second.data["created"])
        self.assertTrue(second.data["duplicate_rejected"])
        self.assertNotIn("id", second.data)
        self.assertNotIn("status", second.data)
        self.assertEqual(
            second.data,
            {
                "source": self.source.code,
                "geo": "",
                "age": None,
                "full_name": "John Doe",
                "phone": "+155500001",
                "email": "lead@example.com",
                "custom_fields": {"channel": "google"},
                "created": False,
                "duplicate_rejected": True,
            },
        )
        self.assertEqual(Lead.objects.filter(phone="+155500001").count(), 1)

    def test_partner_duplicate_response_does_not_leak_existing_foreign_lead_data(self):
        other_partner = Partner.objects.create(name="Other", code="other")
        Lead.objects.create(
            partner=other_partner,
            phone="+155500777",
            full_name="Foreign Existing",
            email="foreign@example.com",
            custom_fields={"secret": "value"},
        )

        response = self.client.post(
            "/api/v1/partner/leads/",
            {
                "phone": "+155500777",
                "full_name": "Attempted Name",
                "email": "attempt@example.com",
                "custom_fields": {"campaign": "new"},
            },
            format="json",
            HTTP_X_PARTNER_TOKEN=self.raw_token,
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.data,
            {
                "source": "",
                "geo": "",
                "age": None,
                "full_name": "Attempted Name",
                "phone": "+155500777",
                "email": "attempt@example.com",
                "custom_fields": {"campaign": "new"},
                "created": False,
                "duplicate_rejected": True,
            },
        )

    def test_partner_lead_with_invalid_token_returns_401(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {"email": "lead@example.com"},
            format="json",
            HTTP_X_PARTNER_TOKEN="short",
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.data["error"]["code"], "authentication_failed")

    def test_partner_lead_with_unknown_source_returns_400(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {"phone": "+155500099", "source": "unknown", "email": "unknown@example.com"},
            format="json",
            HTTP_X_PARTNER_TOKEN=self.raw_token,
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_partner_can_set_geo_on_create(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {
                "phone": "+19990001",
                "geo": "ru",
                "age": 27,
            },
            format="json",
            HTTP_X_PARTNER_TOKEN=self.raw_token,
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["geo"], "RU")
        self.assertEqual(response.data["age"], 27)
        self.assertEqual(
            set(response.data.keys()),
            {
                "id",
                "source",
                "received_at",
                "geo",
                "age",
                "status",
                "full_name",
                "phone",
                "email",
                "custom_fields",
                "created",
                "duplicate_rejected",
            },
        )
        lead = Lead.objects.get(partner=self.partner, phone="+19990001")
        self.assertEqual(response.data["received_at"], lead.received_at.isoformat().replace("+00:00", "Z"))
        self.assertEqual(
            response.data["status"],
            {
                "id": str(lead.status_id),
                "code": lead.status.code,
                "name": lead.status.name,
                "work_bucket": lead.status.work_bucket,
            },
        )
        self.assertEqual(lead.geo, "RU")
        self.assertEqual(lead.age, 27)

    def test_partner_can_create_lead_with_null_custom_fields(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {
                "phone": "+19990003",
                "custom_fields": None,
            },
            format="json",
            HTTP_X_PARTNER_TOKEN=self.raw_token,
        )

        self.assertEqual(response.status_code, 201)
        self.assertIsNone(response.data["custom_fields"])
        lead = Lead.objects.get(partner=self.partner, phone="+19990003")
        self.assertIsNone(lead.custom_fields)

    def test_partner_lead_with_invalid_geo_returns_400(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {
                "phone": "+19990002",
                "geo": "RUS",
            },
            format="json",
            HTTP_X_PARTNER_TOKEN=self.raw_token,
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"]["code"], "validation_error")


class PartnerLeadFilterApiTests(APITestCase):
    def setUp(self):
        self.partner = Partner.objects.create(name="Acme", code="acme-filters")
        self.google_source = PartnerSource.objects.create(
            partner=self.partner,
            name="Google",
            code="google",
            is_active=True,
        )
        self.fb_source = PartnerSource.objects.create(
            partner=self.partner,
            name="Facebook",
            code="facebook",
            is_active=True,
        )
        self.raw_token = "tok_live_partner_filters_1234567890"
        self.token = PartnerToken.build(partner=self.partner, raw_token=self.raw_token, name="filters")
        self.token.save()
        self.headers = {"HTTP_X_PARTNER_TOKEN": self.raw_token}
        self.status_new = LeadStatus.objects.create(
            code="PARTNER_FILTER_NEW",
            name="Partner Filter New",
            is_default_for_new_leads=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
        )
        self.status_won = LeadStatus.objects.create(
            code="PARTNER_FILTER_WON",
            name="Partner Filter Won",
            work_bucket=LeadStatus.WorkBucket.NON_WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )

        now = timezone.now()
        self.lead_google = Lead.objects.create(
            partner=self.partner,
            source=self.google_source,
            status=self.status_new,
            phone="+19990101",
            age=21,
            custom_fields={"name": "Google Lead"},
            received_at=now - timedelta(hours=2),
        )
        self.lead_facebook = Lead.objects.create(
            partner=self.partner,
            source=self.fb_source,
            status=self.status_won,
            phone="+19990102",
            age=35,
            custom_fields={"name": "Facebook Lead"},
            received_at=now - timedelta(hours=1),
        )
        self.lead_without_source = Lead.objects.create(
            partner=self.partner,
            source=None,
            phone="+19990103",
            custom_fields={"name": "No Source Lead"},
            received_at=now,
        )

        other_partner = Partner.objects.create(name="Other", code="other-filters")
        Lead.objects.create(
            partner=other_partner,
            source=None,
            phone="+19990200",
            custom_fields={"name": "Foreign Lead"},
            received_at=now - timedelta(hours=1),
        )

    def test_filter_by_source(self):
        response = self.client.get("/api/v1/partner/leads/", {"source": "google"}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["phone"], self.lead_google.phone)
        self.assertEqual(response.data["results"][0]["source"], "google")

    def test_filter_by_phone(self):
        response = self.client.get("/api/v1/partner/leads/", {"phone": self.lead_facebook.phone}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["phone"], self.lead_facebook.phone)
        self.assertEqual(response.data["results"][0]["source"], "facebook")

    def test_filter_by_age(self):
        response = self.client.get("/api/v1/partner/leads/", {"age": 35}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["phone"], self.lead_facebook.phone)
        self.assertEqual(response.data["results"][0]["age"], 35)

    def test_filter_by_age_from_and_age_to(self):
        response = self.client.get("/api/v1/partner/leads/", {"age_from": 20, "age_to": 25}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["phone"], self.lead_google.phone)

    def test_filter_by_status_in(self):
        response = self.client.get(
            "/api/v1/partner/leads/",
            {"status__in": f"{self.status_new.code},{self.status_won.code}"},
            **self.headers,
        )

        self.assertEqual(response.status_code, 200)
        phones = {item["phone"] for item in response.data["results"]}
        self.assertEqual(phones, {self.lead_google.phone, self.lead_facebook.phone})

    def test_filter_by_received_from_and_to(self):
        response = self.client.get(
            "/api/v1/partner/leads/",
            {
                "received_from": (timezone.now() - timedelta(hours=1, minutes=30)).isoformat(),
                "received_to": (timezone.now() - timedelta(minutes=30)).isoformat(),
            },
            **self.headers,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["phone"], self.lead_facebook.phone)

    def test_partner_leads_list_supports_ordering(self):
        response = self.client.get("/api/v1/partner/leads/", {"ordering": "received_at"}, **self.headers)

        self.assertEqual(response.status_code, 200)
        phones = [item["phone"] for item in response.data["results"]]
        self.assertEqual(
            phones,
            [
                self.lead_google.phone,
                self.lead_facebook.phone,
                self.lead_without_source.phone,
            ],
        )

    def test_list_has_pagination_shape(self):
        response = self.client.get("/api/v1/partner/leads/", {}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertIn("count", response.data)
        self.assertIn("next", response.data)
        self.assertIn("previous", response.data)
        self.assertIn("results", response.data)

    def test_page_size_is_limited_by_max_page_size(self):
        with patch.object(PartnerLeadPagination, "page_size", 2), patch.object(PartnerLeadPagination, "max_page_size", 2):
            response = self.client.get("/api/v1/partner/leads/", {"page_size": 999}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 3)
        self.assertEqual(len(response.data["results"]), 2)
        self.assertIsNotNone(response.data["next"])

    def test_invalid_page_returns_standard_error_shape(self):
        response = self.client.get("/api/v1/partner/leads/", {"page": 999}, **self.headers)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data["error"]["code"], "not_found")

    def test_throttle_returns_429_after_limit(self):
        cache.clear()
        with patch.object(PartnerTokenRateThrottle, "THROTTLE_RATES", {"partner_token": "2/min"}):
            first = self.client.get("/api/v1/partner/leads/", {}, **self.headers)
            second = self.client.get("/api/v1/partner/leads/", {}, **self.headers)
            third = self.client.get("/api/v1/partner/leads/", {}, **self.headers)
        cache.clear()

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(third.status_code, 429)
        self.assertEqual(third.data["error"]["code"], "throttled")


class InternalPartnerSourceApiTests(APITestCase):
    def _access_token_for(self, user):
        refresh = RefreshToken.for_user(user)
        return str(refresh.access_token)

    def _auth(self, user):
        token = self._access_token_for(user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def test_admin_can_crud_partner_source(self):
        admin = self._create_user("admin_partner_source", UserRole.ADMIN)
        partner = Partner.objects.create(name="Northwind", code="northwind")
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/partners/sources/",
            {
                "partner": str(partner.id),
                "code": "instagram",
                "name": "Instagram",
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(create_resp.status_code, 201)
        source_id = create_resp.data["id"]
        self.assertEqual(create_resp.data["partner_code"], partner.code)

        list_resp = self.client.get("/api/v1/partners/sources/", {"partner": str(partner.id)})
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.data["count"], 1)
        self.assertEqual(list_resp.data["results"][0]["id"], source_id)

        patch_resp = self.client.patch(
            f"/api/v1/partners/sources/{source_id}/",
            {"name": "Instagram Ads"},
            format="json",
        )
        self.assertEqual(patch_resp.status_code, 200)
        self.assertEqual(patch_resp.data["name"], "Instagram Ads")

        soft_delete_resp = self.client.post(f"/api/v1/partners/sources/{source_id}/soft_delete/", {}, format="json")
        self.assertEqual(soft_delete_resp.status_code, 204)
        self.assertFalse(PartnerSource.objects.filter(id=source_id).exists())
        self.assertTrue(PartnerSource.all_objects.filter(id=source_id).exists())

        restore_resp = self.client.post(f"/api/v1/partners/sources/{source_id}/restore/", {}, format="json")
        self.assertEqual(restore_resp.status_code, 200)
        self.assertTrue(PartnerSource.objects.filter(id=source_id).exists())

    def test_manager_cannot_create_partner_source(self):
        manager = self._create_user("manager_partner_source", UserRole.MANAGER)
        partner = Partner.objects.create(name="Manager Partner", code="manager-partner")
        self._auth(manager)

        response = self.client.post(
            "/api/v1/partners/sources/",
            {"partner": str(partner.id), "code": "seo", "name": "SEO", "is_active": True},
            format="json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_admin_cannot_hard_delete_partner_source(self):
        admin = self._create_user("admin_partner_source_delete", UserRole.ADMIN)
        partner = Partner.objects.create(name="Delete Partner", code="delete-partner")
        source = PartnerSource.objects.create(partner=partner, code="google", name="Google")
        self._auth(admin)

        response = self.client.delete(f"/api/v1/partners/sources/{source.id}/")

        self.assertEqual(response.status_code, 403)
        self.assertTrue(PartnerSource.all_objects.filter(id=source.id).exists())

    def test_superuser_can_hard_delete_partner_source(self):
        superuser = self._create_user(
            "su_partner_source_delete",
            UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Delete Partner SU", code="delete-partner-su")
        source = PartnerSource.objects.create(partner=partner, code="facebook", name="Facebook")
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/partners/sources/{source.id}/")

        self.assertEqual(response.status_code, 204)
        self.assertFalse(PartnerSource.all_objects.filter(id=source.id).exists())

    def _create_user(self, username, role, **extra):
        defaults = {"password": "pass12345", "role": role}
        defaults.update(extra)
        return User.objects.create_user(username=username, **defaults)


class InternalPartnerApiTests(APITestCase):
    def _access_token_for(self, user):
        refresh = RefreshToken.for_user(user)
        return str(refresh.access_token)

    def _auth(self, user):
        token = self._access_token_for(user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def _create_user(self, username, role, **extra):
        defaults = {"password": "pass12345", "role": role}
        defaults.update(extra)
        return User.objects.create_user(username=username, **defaults)

    def test_admin_can_crud_partner(self):
        admin = self._create_user("admin_partner_crud", UserRole.ADMIN)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/partners/",
            {"name": "West Coast Agency", "code": "west-coast", "is_active": True},
            format="json",
        )
        self.assertEqual(create_resp.status_code, 201)
        partner_id = create_resp.data["id"]

        list_resp = self.client.get("/api/v1/partners/", {"code": "west-coast"})
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.data["count"], 1)
        self.assertEqual(list_resp.data["results"][0]["id"], partner_id)

        patch_resp = self.client.patch(
            f"/api/v1/partners/{partner_id}/",
            {"name": "West Coast Agency LLC"},
            format="json",
        )
        self.assertEqual(patch_resp.status_code, 200)
        self.assertEqual(patch_resp.data["name"], "West Coast Agency LLC")

        soft_delete_resp = self.client.post(f"/api/v1/partners/{partner_id}/soft_delete/", {}, format="json")
        self.assertEqual(soft_delete_resp.status_code, 204)
        self.assertFalse(Partner.objects.filter(id=partner_id).exists())
        self.assertTrue(Partner.all_objects.filter(id=partner_id).exists())

        restore_resp = self.client.post(f"/api/v1/partners/{partner_id}/restore/", {}, format="json")
        self.assertEqual(restore_resp.status_code, 200)
        self.assertTrue(Partner.objects.filter(id=partner_id).exists())

    def test_manager_cannot_create_partner(self):
        manager = self._create_user("manager_partner_create", UserRole.MANAGER)
        self._auth(manager)

        response = self.client.post(
            "/api/v1/partners/",
            {"name": "Should Fail", "code": "should-fail"},
            format="json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_hard_delete_partner_is_superuser_only(self):
        admin = self._create_user("admin_partner_delete", UserRole.ADMIN)
        superuser = self._create_user(
            "su_partner_delete",
            UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Delete Partner", code="delete-partner-api")

        self._auth(admin)
        admin_resp = self.client.delete(f"/api/v1/partners/{partner.id}/")
        self.assertEqual(admin_resp.status_code, 403)
        self.assertTrue(Partner.all_objects.filter(id=partner.id).exists())

        self._auth(superuser)
        su_resp = self.client.delete(f"/api/v1/partners/{partner.id}/")
        self.assertEqual(su_resp.status_code, 204)
        self.assertFalse(Partner.all_objects.filter(id=partner.id).exists())

    def test_internal_partner_list_supports_ordering(self):
        admin = self._create_user("admin_partner_ordering", UserRole.ADMIN)
        Partner.objects.create(name="Bravo", code="bravo")
        Partner.objects.create(name="Alpha", code="alpha")
        self._auth(admin)

        response = self.client.get("/api/v1/partners/", {"ordering": "code"})

        self.assertEqual(response.status_code, 200)
        codes = [item["code"] for item in response.data["results"][:2]]
        self.assertEqual(codes, ["alpha", "bravo"])


class InternalPartnerTokenApiTests(APITestCase):
    def _access_token_for(self, user):
        refresh = RefreshToken.for_user(user)
        return str(refresh.access_token)

    def _auth(self, user):
        token = self._access_token_for(user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def _create_user(self, username, role, **extra):
        defaults = {"password": "pass12345", "role": role}
        defaults.update(extra)
        return User.objects.create_user(username=username, **defaults)

    def test_admin_can_create_and_list_partner_token(self):
        admin = self._create_user("admin_partner_token", UserRole.ADMIN)
        partner = Partner.objects.create(name="Token Partner", code="token-partner")
        source = PartnerSource.objects.create(partner=partner, code="google", name="Google")
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/partners/tokens/",
            {
                "partner": str(partner.id),
                "name": "frontend-dev",
                "source": str(source.id),
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(create_resp.status_code, 201)
        token_id = create_resp.data["id"]
        self.assertTrue(create_resp.data["issued_token"])
        self.assertTrue(create_resp.data["prefix"])

        list_resp = self.client.get("/api/v1/partners/tokens/", {"partner": str(partner.id)})
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.data["count"], 1)
        self.assertEqual(list_resp.data["results"][0]["id"], token_id)
        self.assertIsNone(list_resp.data["results"][0]["issued_token"])

    def test_admin_cannot_create_token_with_foreign_source(self):
        admin = self._create_user("admin_partner_token_foreign", UserRole.ADMIN)
        partner_a = Partner.objects.create(name="Partner A", code="partner-a")
        partner_b = Partner.objects.create(name="Partner B", code="partner-b")
        foreign_source = PartnerSource.objects.create(partner=partner_b, code="fb", name="Facebook")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/partners/tokens/",
            {
                "partner": str(partner_a.id),
                "name": "bad-token",
                "source": str(foreign_source.id),
            },
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_admin_cannot_set_raw_token_manually(self):
        admin = self._create_user("admin_partner_token_raw", UserRole.ADMIN)
        partner = Partner.objects.create(name="Token Partner Raw", code="token-partner-raw")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/partners/tokens/",
            {
                "partner": str(partner.id),
                "name": "manual-raw",
                "raw_token": "tok_live_manual_should_not_be_allowed_1234567890",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertIn("raw_token", response.data["error"]["details"])

    def test_hard_delete_partner_token_is_superuser_only(self):
        admin = self._create_user("admin_partner_token_delete", UserRole.ADMIN)
        superuser = self._create_user(
            "su_partner_token_delete",
            UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Token Delete Partner", code="token-delete-partner")
        token = PartnerToken.build(partner=partner, raw_token="tok_live_internal_delete_1234567890", name="delete-me")
        token.save()

        self._auth(admin)
        admin_resp = self.client.delete(f"/api/v1/partners/tokens/{token.id}/")
        self.assertEqual(admin_resp.status_code, 403)
        self.assertTrue(PartnerToken.all_objects.filter(id=token.id).exists())

        self._auth(superuser)
        su_resp = self.client.delete(f"/api/v1/partners/tokens/{token.id}/")
        self.assertEqual(su_resp.status_code, 204)
        self.assertFalse(PartnerToken.all_objects.filter(id=token.id).exists())
