from rest_framework.test import APITestCase
from django.core.cache import cache
from unittest.mock import patch

from datetime import timedelta
from django.utils import timezone

from apps.leads.models import Lead
from apps.partners.models import Partner, PartnerSource, PartnerToken
from apps.partners.pagination import PartnerLeadPagination
from apps.partners.throttling import PartnerTokenRateThrottle


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

    def test_partner_lead_create_is_idempotent_by_external_id(self):
        url = "/api/v1/partner/leads/"
        payload = {
            "external_id": "ext-001",
            "source_code": self.source.code,
            "full_name": "John Doe",
            "email": "lead@example.com",
            "custom_fields": {"channel": "google"},
        }
        headers = {"HTTP_X_PARTNER_TOKEN": self.raw_token}

        first = self.client.post(url, payload, format="json", **headers)
        second = self.client.post(url, payload, format="json", **headers)

        self.assertEqual(first.status_code, 201)
        self.assertTrue(first.data["created"])
        self.assertEqual(second.status_code, 200)
        self.assertFalse(second.data["created"])
        self.assertEqual(Lead.objects.filter(partner=self.partner, external_id="ext-001").count(), 1)

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
            {"external_id": "ext-unknown", "source_code": "unknown", "email": "unknown@example.com"},
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

        now = timezone.now()
        self.lead_google = Lead.objects.create(
            partner=self.partner,
            source=self.google_source,
            external_id="ext-google",
            custom_fields={"name": "Google Lead"},
            received_at=now - timedelta(hours=2),
        )
        self.lead_facebook = Lead.objects.create(
            partner=self.partner,
            source=self.fb_source,
            external_id="ext-facebook",
            custom_fields={"name": "Facebook Lead"},
            received_at=now - timedelta(hours=1),
        )
        self.lead_without_source = Lead.objects.create(
            partner=self.partner,
            source=None,
            external_id="ext-no-source",
            custom_fields={"name": "No Source Lead"},
            received_at=now,
        )

        other_partner = Partner.objects.create(name="Other", code="other-filters")
        Lead.objects.create(
            partner=other_partner,
            source=None,
            external_id="ext-foreign",
            custom_fields={"name": "Foreign Lead"},
            received_at=now - timedelta(hours=1),
        )

    def test_filter_by_source_code(self):
        response = self.client.get("/api/v1/partner/leads/", {"source": "google"}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["external_id"], self.lead_google.external_id)

    def test_filter_by_external_id(self):
        response = self.client.get("/api/v1/partner/leads/", {"external_id": "ext-facebook"}, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["external_id"], self.lead_facebook.external_id)

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
        self.assertEqual(response.data["results"][0]["external_id"], self.lead_facebook.external_id)

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
