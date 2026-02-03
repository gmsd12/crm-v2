from rest_framework.test import APITestCase

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings


class AuthApiTests(APITestCase):
    def test_me_without_auth_returns_401(self):
        response = self.client.get("/api/v1/auth/me/")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.data["error"]["code"], "not_authenticated")

    def test_login_refresh_logout_flow(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(username="manager1", password="pass12345")

        login = self.client.post(
            "/api/v1/auth/login/",
            {"username": "manager1", "password": "pass12345"},
            format="json",
        )
        self.assertEqual(login.status_code, 200)
        self.assertIn("access", login.data)
        self.assertEqual(login.data["user"]["id"], user.id)
        self.assertEqual(login.data["user"]["username"], user.username)
        self.assertIn(settings.JWT_REFRESH_COOKIE_NAME, login.cookies)

        me = self.client.get(
            "/api/v1/auth/me/",
            HTTP_AUTHORIZATION=f"Bearer {login.data['access']}",
        )
        self.assertEqual(me.status_code, 200)
        self.assertEqual(me.data["id"], user.id)
        self.assertEqual(me.data["username"], user.username)

        refresh = self.client.post("/api/v1/auth/refresh/", {}, format="json")
        self.assertEqual(refresh.status_code, 200)
        self.assertIn("access", refresh.data)

        logout = self.client.post("/api/v1/auth/logout/", {}, format="json")
        self.assertEqual(logout.status_code, 200)
        self.assertEqual(logout.data, {"ok": True})

        refresh_after_logout = self.client.post("/api/v1/auth/refresh/", {}, format="json")
        self.assertEqual(refresh_after_logout.status_code, 401)
        self.assertEqual(refresh_after_logout.data["detail"], "Missing refresh cookie")

    def test_login_with_invalid_credentials_returns_400(self):
        get_user_model().objects.create_user(username="manager2", password="pass12345")

        response = self.client.post(
            "/api/v1/auth/login/",
            {"username": "manager2", "password": "wrong-pass"},
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_refresh_with_invalid_cookie_returns_401(self):
        self.client.cookies[settings.JWT_REFRESH_COOKIE_NAME] = "not-a-jwt-token"

        response = self.client.post("/api/v1/auth/refresh/", {}, format="json")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.data["detail"], "Invalid refresh token")

    @override_settings(CORS_ALLOWED_ORIGINS=["https://allowed.example.com"])
    def test_login_with_blocked_origin_returns_403(self):
        response = self.client.post(
            "/api/v1/auth/login/",
            {"username": "nobody", "password": "nobody"},
            format="json",
            HTTP_ORIGIN="https://blocked.example.com",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["detail"], "Origin not allowed")
