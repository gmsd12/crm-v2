from rest_framework.test import APITestCase
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework import status

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings

from apps.iam.models import UserRole

User = get_user_model()


class AuthApiTests(APITestCase):
    def test_me_without_auth_returns_401(self):
        response = self.client.get("/api/v1/auth/me/")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.data["error"]["code"], "not_authenticated")

    def test_login_refresh_logout_flow(self):
        user = User.objects.create_user(username="manager1", password="pass12345")

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
        self.assertEqual(refresh_after_logout.data["error"]["code"], "not_authenticated")

    def test_login_with_invalid_credentials_returns_400(self):
        User.objects.create_user(username="manager2", password="pass12345")

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
        self.assertEqual(response.data["error"]["code"], "authentication_failed")

    @override_settings(CORS_ALLOWED_ORIGINS=["https://allowed.example.com"])
    def test_login_with_blocked_origin_returns_403(self):
        response = self.client.post(
            "/api/v1/auth/login/",
            {"username": "nobody", "password": "nobody"},
            format="json",
            HTTP_ORIGIN="https://blocked.example.com",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["error"]["code"], "permission_denied")


class IamUsersRBACTests(APITestCase):
    def _access_token_for(self, user):
        refresh = RefreshToken.for_user(user)
        return str(refresh.access_token)

    def _auth(self, user):
        token = self._access_token_for(user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def test_admin_can_list_users(self):
        admin = User.objects.create_user(username="admin_user", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        response = self.client.get("/api/v1/iam/users/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(response.data["results"]), 1)

    def test_manager_cannot_list_users(self):
        manager = User.objects.create_user(username="manager_user", password="pass12345", role=UserRole.MANAGER)
        self._auth(manager)

        response = self.client.get("/api/v1/iam/users/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_ret_cannot_list_users(self):
        ret = User.objects.create_user(username="ret_user", password="pass12345", role=UserRole.RET)
        self._auth(ret)

        response = self.client.get("/api/v1/iam/users/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_teamleader_can_list_users(self):
        teamleader = User.objects.create_user(username="tl_read_user", password="pass12345", role=UserRole.TEAMLEADER)
        self._auth(teamleader)

        response = self.client.get("/api/v1/iam/users/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(response.data["results"]), 1)

    def test_teamleader_cannot_create_user(self):
        teamleader = User.objects.create_user(username="tl_user", password="pass12345", role=UserRole.TEAMLEADER)
        self._auth(teamleader)

        response = self.client.post(
            "/api/v1/iam/users/",
            {"username": "new_user_tl", "password": "pass12345", "role": UserRole.MANAGER},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_teamleader_cannot_update_user(self):
        teamleader = User.objects.create_user(username="tl_update", password="pass12345", role=UserRole.TEAMLEADER)
        victim = User.objects.create_user(username="victim_tl", password="pass12345", role=UserRole.MANAGER)
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/iam/users/{victim.id}/",
            {"is_active": False},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        victim.refresh_from_db()
        self.assertTrue(victim.is_active)

    def test_admin_can_create_user(self):
        admin = User.objects.create_user(username="admin_create", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        response = self.client.post(
            "/api/v1/iam/users/",
            {"username": "new_user_admin", "password": "pass12345", "role": UserRole.MANAGER, "is_active": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["username"], "new_user_admin")
        self.assertEqual(response.data["role"], UserRole.MANAGER)

    def test_superuser_can_create_user(self):
        superuser = User.objects.create_user(
            username="su_user",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        self._auth(superuser)

        response = self.client.post(
            "/api/v1/iam/users/",
            {"username": "new_user_su", "password": "pass12345", "role": UserRole.MANAGER, "is_active": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["username"], "new_user_su")
        self.assertEqual(response.data["role"], UserRole.MANAGER)

    def test_admin_cannot_hard_delete_user(self):
        admin = User.objects.create_user(username="admin_delete", password="pass12345", role=UserRole.ADMIN)
        victim = User.objects.create_user(username="victim_admin", password="pass12345", role=UserRole.MANAGER)
        self._auth(admin)

        response = self.client.delete(f"/api/v1/iam/users/{victim.id}/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertTrue(User.objects.filter(id=victim.id).exists())

    def test_superuser_can_hard_delete_user(self):
        superuser = User.objects.create_user(
            username="su_delete",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        victim = User.objects.create_user(username="victim_su", password="pass12345", role=UserRole.MANAGER)
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/iam/users/{victim.id}/")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(User.objects.filter(id=victim.id).exists())
