from rest_framework.test import APITestCase


class HealthApiTests(APITestCase):
    def test_health_returns_ok_and_request_id_header(self):
        response = self.client.get("/api/health/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {"status": "ok"})
        self.assertIn("X-Request-ID", response)
