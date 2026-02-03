from rest_framework.test import APITestCase

from datetime import timedelta
from django.utils import timezone

from apps.leads.models import Lead
from apps.partners.models import Partner, PartnerSource, PartnerToken


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
            "payload": {"email": "lead@example.com"},
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
            {"payload": {"email": "lead@example.com"}},
            format="json",
            HTTP_X_PARTNER_TOKEN="short",
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.data["error"]["code"], "authentication_failed")

    def test_partner_lead_with_unknown_source_returns_400(self):
        response = self.client.post(
            "/api/v1/partner/leads/",
            {"external_id": "ext-unknown", "source_code": "unknown", "payload": {"x": 1}},
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
            payload={"name": "Google Lead"},
            received_at=now - timedelta(hours=2),
        )
        self.lead_facebook = Lead.objects.create(
            partner=self.partner,
            source=self.fb_source,
            external_id="ext-facebook",
            payload={"name": "Facebook Lead"},
            received_at=now - timedelta(hours=1),
        )
        self.lead_without_source = Lead.objects.create(
            partner=self.partner,
            source=None,
            external_id="ext-no-source",
            payload={"name": "No Source Lead"},
            received_at=now,
        )

        other_partner = Partner.objects.create(name="Other", code="other-filters")
        Lead.objects.create(
            partner=other_partner,
            source=None,
            external_id="ext-foreign",
            payload={"name": "Foreign Lead"},
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
