from datetime import datetime

from rest_framework import status
from rest_framework.test import APITestCase
from rest_framework_simplejwt.tokens import RefreshToken

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils import timezone

from apps.iam.models import UserRole
from apps.leads.models import (
    Lead,
    LeadAuditEntity,
    LeadComment,
    LeadDeposit,
    LeadDuplicateAttempt,
    LeadStatus,
    LeadAuditEvent,
    LeadAuditLog,
    LeadIdempotencyEndpoint,
    LeadIdempotencyKey,
)
from apps.partners.models import Partner, PartnerSource, PartnerToken

User = get_user_model()


class LeadStatusCatalogApiTests(APITestCase):
    def _access_token_for(self, user):
        refresh = RefreshToken.for_user(user)
        return str(refresh.access_token)

    def _auth(self, user):
        token = self._access_token_for(user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def _set_log_created_at(self, log_obj, dt_obj):
        LeadAuditLog.objects.filter(id=log_obj.id).update(created_at=dt_obj)

    def _set_deposit_created_at(self, deposit_obj, dt_obj):
        LeadDeposit.objects.filter(id=deposit_obj.id).update(created_at=dt_obj)

    def test_teamleader_can_list_statuses(self):
        teamleader = User.objects.create_user(username="tl_status_list", password="pass12345", role=UserRole.TEAMLEADER)
        LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        self._auth(teamleader)

        response = self.client.get("/api/v1/leads/statuses/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(response.data["results"]), 1)

    def test_status_defaults_to_working_and_is_exposed_in_catalog(self):
        admin = User.objects.create_user(username="admin_status_work_bucket", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/statuses/",
            {
                "code": "CALL_LATER",
                "name": "Call Later",
                "order": 30,
                "is_active": True,
            },
            format="json",
        )

        self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(create_resp.data["work_bucket"], LeadStatus.WorkBucket.WORKING)
        status_obj = LeadStatus.objects.get(id=create_resp.data["id"])
        self.assertEqual(status_obj.work_bucket, LeadStatus.WorkBucket.WORKING)

    def test_lead_retrieve_exposes_status_work_bucket(self):
        admin = User.objects.create_user(username="admin_status_on_lead", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Status On Lead", code="partner-status-on-lead")
        status_obj = LeadStatus.objects.create(
            code="CALL_BACK_LATER",
            name="Call Back Later",
            work_bucket=LeadStatus.WorkBucket.RETURN,
            is_default_for_new_leads=True,
        )
        lead = Lead.objects.create(partner=partner, status=status_obj, phone="+15559901", custom_fields={})
        self._auth(admin)

        response = self.client.get(f"/api/v1/leads/records/{lead.id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"]["work_bucket"], LeadStatus.WorkBucket.RETURN)

    def test_manager_and_ret_can_list_statuses(self):
        manager = User.objects.create_user(username="manager_status_list", password="pass12345", role=UserRole.MANAGER)
        LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        self._auth(manager)
        manager_resp = self.client.get("/api/v1/leads/statuses/")
        self.assertEqual(manager_resp.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(manager_resp.data["results"]), 1)

        ret = User.objects.create_user(username="ret_status_list", password="pass12345", role=UserRole.RET)
        self._auth(ret)
        ret_resp = self.client.get("/api/v1/leads/statuses/")
        self.assertEqual(ret_resp.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(ret_resp.data["results"]), 1)

    def test_admin_can_create_and_soft_delete_status(self):
        admin = User.objects.create_user(username="admin_status_write", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/statuses/",
            {
                "code": "CALLBACK",
                "name": "Callback",
                "order": 20,
                "is_default_for_new_leads": False,
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
        status_id = create_resp.data["id"]

        soft_delete_resp = self.client.post(f"/api/v1/leads/statuses/{status_id}/soft_delete/", {}, format="json")
        self.assertEqual(soft_delete_resp.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(LeadStatus.objects.filter(id=status_id).exists())

    def test_admin_cannot_deactivate_status_if_used_by_leads(self):
        admin = User.objects.create_user(username="admin_status_deactivate", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Status Used", code="partner-status-used")
        status_obj = LeadStatus.objects.create(
            code="IN_USE",
            name="In Use",
            is_default_for_new_leads=True,
            is_active=True,
        )
        Lead.objects.create(partner=partner, status=status_obj, custom_fields={})
        self._auth(admin)

        response = self.client.patch(
            f"/api/v1/leads/statuses/{status_obj.id}/",
            {"is_active": False},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        status_obj.refresh_from_db()
        self.assertTrue(status_obj.is_active)

    def test_admin_cannot_soft_delete_status_if_used_by_leads(self):
        admin = User.objects.create_user(username="admin_status_soft_used", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Status Soft Used", code="partner-status-soft-used")
        status_obj = LeadStatus.objects.create(
            code="SOFT_USED",
            name="Soft Used",
            is_default_for_new_leads=True,
            is_active=True,
        )
        Lead.objects.create(partner=partner, status=status_obj, custom_fields={})
        self._auth(admin)

        response = self.client.post(f"/api/v1/leads/statuses/{status_obj.id}/soft_delete/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertTrue(LeadStatus.objects.filter(id=status_obj.id).exists())

    def test_admin_cannot_hard_delete_status(self):
        admin = User.objects.create_user(username="admin_status_delete", password="pass12345", role=UserRole.ADMIN)
        status_obj = LeadStatus.objects.create(
            code="VERIFY",
            name="Verify",
            is_default_for_new_leads=True,
        )
        self._auth(admin)

        response = self.client.delete(f"/api/v1/leads/statuses/{status_obj.id}/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertTrue(LeadStatus.objects.filter(id=status_obj.id).exists())

    def test_superuser_can_hard_delete_status(self):
        superuser = User.objects.create_user(
            username="su_status_delete",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        status_obj = LeadStatus.objects.create(
            code="VERIFY2",
            name="Verify 2",
            is_default_for_new_leads=True,
        )
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/leads/statuses/{status_obj.id}/")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(LeadStatus.all_objects.filter(id=status_obj.id).exists())

    def test_superuser_cannot_hard_delete_status_if_used_by_leads(self):
        superuser = User.objects.create_user(
            username="su_status_delete_used",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner Status Hard Used", code="partner-status-hard-used")
        status_obj = LeadStatus.objects.create(
            code="HARD_USED",
            name="Hard Used",
            is_default_for_new_leads=True,
        )
        Lead.objects.create(partner=partner, status=status_obj, custom_fields={})
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/leads/statuses/{status_obj.id}/")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertTrue(LeadStatus.all_objects.filter(id=status_obj.id).exists())

    def test_audit_log_records_status_create_update_and_soft_delete(self):
        admin = User.objects.create_user(username="admin_audit", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/statuses/",
            {
                "code": "NEW",
                "name": "New",
                "order": 10,
                "is_default_for_new_leads": True,
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
        status_id = create_resp.data["id"]

        update_resp = self.client.patch(
            f"/api/v1/leads/statuses/{status_id}/",
            {"name": "New Updated"},
            format="json",
        )
        self.assertEqual(update_resp.status_code, status.HTTP_200_OK)

        soft_delete_resp = self.client.post(f"/api/v1/leads/statuses/{status_id}/soft_delete/", {}, format="json")
        self.assertEqual(soft_delete_resp.status_code, status.HTTP_204_NO_CONTENT)

        events = list(
            LeadAuditLog.objects.filter(to_status_id=status_id).values_list("event_type", flat=True)
        ) + list(
            LeadAuditLog.objects.filter(from_status_id=status_id).values_list("event_type", flat=True)
        )

        self.assertIn(LeadAuditEvent.STATUS_CREATED, events)
        self.assertIn(LeadAuditEvent.STATUS_UPDATED, events)
        self.assertIn(LeadAuditEvent.STATUS_DELETED_SOFT, events)

    def test_admin_can_change_lead_status_with_valid_transition_and_audit(self):
        admin = User.objects.create_user(username="admin_change_status", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change", code="partner-change")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={"x": 1})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "accepted to work"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"]["code"], "WORK")
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

        audit = LeadAuditLog.objects.filter(
            event_type=LeadAuditEvent.STATUS_CHANGED,
            lead=lead,
            from_status=status_new,
            to_status=status_work,
        ).first()
        self.assertIsNotNone(audit)
        self.assertEqual(audit.reason, "accepted to work")

    def test_change_lead_status_allows_direct_change_without_transition(self):
        admin = User.objects.create_user(username="admin_invalid_transition", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Invalid", code="partner-invalid-transition")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(code="WON", name="Won")
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_won.id), "reason": "force jump"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_won.id)

    def test_change_lead_status_does_not_require_comment(self):
        admin = User.objects.create_user(username="admin_requires_comment", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Comment", code="partner-comment")
        status_lost = LeadStatus.objects.create(code="LOST", name="Lost", is_default_for_new_leads=True)
        status_reopened = LeadStatus.objects.create(code="REOPEN", name="Reopened")
        lead = Lead.objects.create(partner=partner, status=status_lost, custom_fields={})
        self._auth(admin)

        no_reason = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_reopened.id)},
            format="json",
        )
        self.assertEqual(no_reason.status_code, status.HTTP_200_OK)

        with_reason = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_reopened.id), "reason": "lead returned with new budget"},
            format="json",
        )
        self.assertEqual(with_reason.status_code, status.HTTP_400_BAD_REQUEST)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_reopened.id)

    def test_manager_can_change_own_lead_status(self):
        manager = User.objects.create_user(username="manager_change_status", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Manager Change", code="partner-manager-change")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=manager, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "try change"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

    def test_change_status_to_won_sets_manager_outcome(self):
        admin = User.objects.create_user(username="admin_status_won_attrib", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_status_won_attrib", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Won Attribution", code="partner-won-attrib")
        status_work = LeadStatus.objects.create(
            code="WORK",
            name="Work",
            is_default_for_new_leads=True,
        )
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
            status=status_work,
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_won.id), "reason": "closed deal"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()

    def test_change_status_from_lost_to_work_resets_manager_outcome(self):
        admin = User.objects.create_user(username="admin_status_lost_reset", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_status_lost_reset", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Lost Reset", code="partner-lost-reset")
        lost_at = timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0))
        status_lost = LeadStatus.objects.create(
            code="LOST_RESET",
            name="Lost Reset",
            is_default_for_new_leads=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        status_work = LeadStatus.objects.create(code="WORK_RESET", name="Work Reset")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_lost,
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "reopen lead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()

    def test_bulk_change_status_from_lost_to_work_resets_manager_outcome(self):
        admin = User.objects.create_user(username="admin_bulk_lost_reset", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_bulk_lost_reset", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Bulk Lost Reset", code="partner-bulk-lost-reset")
        lost_at = timezone.make_aware(datetime(2026, 1, 7, 11, 0, 0))
        status_lost = LeadStatus.objects.create(
            code="LOST_BULK_RESET",
            name="Lost Bulk Reset",
            is_default_for_new_leads=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        status_work = LeadStatus.objects.create(code="WORK_BULK_RESET", name="Work Bulk Reset")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_lost,
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead.id)],
                "to_status": str(status_work.id),
                "reason": "bulk reopen",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()

    def test_change_status_non_bucket_does_not_reset_won_outcome(self):
        admin = User.objects.create_user(username="admin_status_won_keep", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_status_won_keep", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Won Keep", code="partner-won-keep")
        won_at = timezone.make_aware(datetime(2026, 1, 8, 12, 0, 0))
        status_won = LeadStatus.objects.create(
            code="WON_KEEP",
            name="Won Keep",
            is_default_for_new_leads=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_work = LeadStatus.objects.create(code="WORK_KEEP", name="Work Keep")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_won,
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "ret-side status fix"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()

    def test_manager_cannot_change_foreign_lead_status(self):
        owner = User.objects.create_user(username="manager_change_owner", password="pass12345", role=UserRole.MANAGER)
        manager = User.objects.create_user(username="manager_change_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Manager Deny", code="partner-manager-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=owner, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "try foreign"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_teamleader_can_change_status_for_manager_lead(self):
        teamleader = User.objects.create_user(username="tl_status_manager_ok", password="pass12345", role=UserRole.TEAMLEADER)
        manager_owner = User.objects.create_user(username="manager_status_owner", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner TL Status Manager", code="partner-tl-status-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=manager_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "teamlead handles manager lead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

    def test_teamleader_can_change_status_for_other_teamleader_lead(self):
        teamleader = User.objects.create_user(username="tl_status_other_tl_ok", password="pass12345", role=UserRole.TEAMLEADER)
        teamleader_owner = User.objects.create_user(
            username="tl_status_other_tl_owner",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        partner = Partner.objects.create(name="Partner TL Status Other TL", code="partner-tl-status-other-tl")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=teamleader_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "teamlead handles other teamlead lead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

    def test_teamleader_can_assign_manager_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_single", password="pass12345", role=UserRole.TEAMLEADER)
        manager_target = User.objects.create_user(
            username="manager_target_single",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Single", code="partner-assign-single")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id, "reason": "initial distribution"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)
        self.assertEqual(lead.first_manager_id, manager_target.id)
        self.assertIsNotNone(lead.first_assigned_at)
        self.assertEqual(response.data["manager"]["id"], str(manager_target.id))
        audit = LeadAuditLog.objects.get(lead=lead, event_type=LeadAuditEvent.MANAGER_ASSIGNED)
        self.assertEqual(audit.actor_user_id, teamleader.id)
        self.assertEqual(audit.reason, "initial distribution")
        self.assertEqual(audit.payload_before["manager"], None)
        self.assertEqual(audit.payload_after["manager"]["id"], str(manager_target.id))

    def test_teamleader_can_assign_manager_for_other_teamleader_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_other_tl", password="pass12345", role=UserRole.TEAMLEADER)
        teamleader_owner = User.objects.create_user(
            username="tl_assign_other_tl_owner",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        manager_target = User.objects.create_user(
            username="manager_target_assign_other_tl",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Other TL", code="partner-assign-other-tl")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=teamleader_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id, "reason": "rebalance from teamlead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)

    def test_manager_cannot_assign_manager_to_single_lead(self):
        manager_actor = User.objects.create_user(
            username="manager_actor_assign_single",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        manager_target = User.objects.create_user(
            username="manager_target_assign_single",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Single Deny", code="partner-assign-single-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(manager_actor)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)

    def test_admin_can_reassign_manager_and_write_reassign_audit(self):
        admin = User.objects.create_user(username="admin_reassign_single", password="pass12345", role=UserRole.ADMIN)
        manager_old = User.objects.create_user(username="manager_old_single", password="pass12345", role=UserRole.MANAGER)
        manager_new = User.objects.create_user(username="manager_new_single", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Reassign Single", code="partner-reassign-single")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_old, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_new.id, "reason": "rebalance"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_new.id)
        audit = LeadAuditLog.objects.get(lead=lead, event_type=LeadAuditEvent.MANAGER_REASSIGNED)
        self.assertEqual(audit.reason, "rebalance")
        self.assertEqual(audit.payload_before["manager"]["id"], str(manager_old.id))
        self.assertEqual(audit.payload_after["manager"]["id"], str(manager_new.id))

    def test_teamleader_can_assign_ret_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_ret", password="pass12345", role=UserRole.TEAMLEADER)
        manager_owner = User.objects.create_user(username="manager_assign_ret", password="pass12345", role=UserRole.MANAGER)
        ret_target = User.objects.create_user(
            username="ret_target_single",
            password="pass12345",
            role=UserRole.RET,
        )
        partner = Partner.objects.create(name="Partner Assign RET", code="partner-assign-ret")
        LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=manager_owner,
            first_manager=manager_owner,
            status=status_won,
            custom_fields={},
        )
        LeadDeposit.objects.create(lead=lead, creator=manager_owner, amount="100.00", type=LeadDeposit.Type.FTD)
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": ret_target.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_target.id)
        self.assertEqual(lead.first_manager_id, manager_owner.id)
        self.assertEqual(response.data["manager"]["id"], str(ret_target.id))

    def test_assign_ret_without_ftd_is_allowed(self):
        admin = User.objects.create_user(username="admin_assign_ret_no_ftd", password="pass12345", role=UserRole.ADMIN)
        ret_target = User.objects.create_user(username="ret_target_no_ftd", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Assign RET No FTD", code="partner-assign-ret-no-ftd")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": ret_target.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_target.id)
        self.assertEqual(lead.first_manager_id, ret_target.id)

    def test_teamleader_can_assign_admin_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_admin", password="pass12345", role=UserRole.TEAMLEADER)
        admin_target = User.objects.create_user(username="admin_target_single", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Assign Admin", code="partner-assign-admin")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": admin_target.id, "reason": "escalate lead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, admin_target.id)
        self.assertEqual(response.data["manager"]["id"], str(admin_target.id))

    def test_first_manager_is_preserved_after_reassignments(self):
        admin = User.objects.create_user(username="admin_first_manager", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_first_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_first_b", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_first_manager", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner First Manager", code="partner-first-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_a.id, "reason": "first assignment"},
            format="json",
        )
        lead.refresh_from_db()
        self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": ret_user.id, "reason": "ret handover"},
            format="json",
        )
        self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_b.id, "reason": "back to manager"},
            format="json",
        )

        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager_a.id)
        self.assertEqual(lead.manager_id, manager_b.id)

    def test_admin_can_change_first_manager(self):
        admin = User.objects.create_user(username="admin_change_first_mgr", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_change_first_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_change_first_b", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Change First Manager", code="partner-change-first-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            first_manager=manager_a,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 10, 10, 0, 0)),
            status=status_new,
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-first-manager/",
            {"manager": manager_b.id, "reason": "owner correction"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager_b.id)

    def test_teamleader_can_change_first_manager_and_first_assigned_at_for_manager_lead(self):
        teamleader = User.objects.create_user(
            username="tl_change_first_mgr",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        manager_owner = User.objects.create_user(
            username="manager_first_owner",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        manager_target = User.objects.create_user(
            username="manager_first_target",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner TL Change First Manager", code="partner-tl-change-first-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager_owner,
            first_manager=manager_owner,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 10, 10, 0, 0)),
            status=status_new,
            custom_fields={},
        )
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-first-manager/",
            {
                "manager": manager_target.id,
                "first_assigned_at": "2026-01-09T09:30:00Z",
                "reason": "correction by teamlead",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager_target.id)
        self.assertEqual(lead.first_assigned_at.isoformat(), "2026-01-09T09:30:00+00:00")

    def test_teamleader_can_set_first_manager_to_any_user_role(self):
        teamleader = User.objects.create_user(
            username="tl_change_first_any_role",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        manager_owner = User.objects.create_user(
            username="manager_first_owner_any_role",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        ret_target = User.objects.create_user(
            username="ret_first_target_any_role",
            password="pass12345",
            role=UserRole.RET,
        )
        partner = Partner.objects.create(name="Partner TL Change First Any Role", code="partner-tl-change-first-any-role")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager_owner,
            first_manager=manager_owner,
            status=status_new,
            custom_fields={},
        )
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-first-manager/",
            {"manager": ret_target.id, "reason": "manual correction"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, ret_target.id)

    def test_teamleader_can_change_first_manager_for_other_teamleader_lead(self):
        teamleader = User.objects.create_user(
            username="tl_change_first_other_tl",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        teamleader_owner = User.objects.create_user(
            username="tl_owner_change_first_other_tl",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        manager_target = User.objects.create_user(
            username="manager_target_change_first_other_tl",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner TL Change First Other TL", code="partner-tl-change-first-other-tl")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=teamleader_owner,
            first_manager=teamleader_owner,
            status=status_new,
            custom_fields={},
        )
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-first-manager/",
            {"manager": manager_target.id, "reason": "cross-team correction"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager_target.id)

    def test_manager_cannot_change_first_manager(self):
        manager = User.objects.create_user(username="manager_change_first_deny", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_change_first_deny_b", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Change First Deny", code="partner-change-first-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-first-manager/",
            {"manager": manager_b.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_admin_can_bulk_assign_manager(self):
        admin = User.objects.create_user(username="admin_assign_bulk", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_bulk",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Bulk", code="partner-assign-bulk")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-assign-manager/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "manager": manager_target.id,
                "reason": "bulk distribution",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 2)
        self.assertEqual(response.data["failed_count"], 0)
        self.assertCountEqual(response.data["updated_ids"], [str(lead_1.id), str(lead_2.id)])

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertEqual(lead_1.manager_id, manager_target.id)
        self.assertEqual(lead_2.manager_id, manager_target.id)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.MANAGER_ASSIGNED,
                lead_id__in=[lead_1.id, lead_2.id],
                reason="bulk distribution",
            ).count(),
            2,
        )

    def test_bulk_assign_manager_partial_success_reports_missing(self):
        admin = User.objects.create_user(username="admin_assign_bulk_partial", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_bulk_partial",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Bulk Partial", code="partner-assign-bulk-partial")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        unknown_id = 999999991
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-assign-manager/",
            {
                "lead_ids": [str(lead.id), str(unknown_id)],
                "manager": manager_target.id,
                "reason": "partial distribution",
                "allow_partial": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 1)
        self.assertEqual(response.data["failed_count"], 1)
        self.assertEqual(response.data["failed"][str(unknown_id)], "Unknown lead id")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.MANAGER_ASSIGNED,
                lead=lead,
                reason="partial distribution",
            ).exists()
        )

    def test_ret_cannot_bulk_assign_manager(self):
        ret = User.objects.create_user(username="ret_assign_bulk", password="pass12345", role=UserRole.RET)
        manager_target = User.objects.create_user(
            username="manager_target_bulk_deny",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Bulk Deny", code="partner-assign-bulk-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(ret)

        response = self.client.post(
            "/api/v1/leads/records/bulk-assign-manager/",
            {
                "lead_ids": [str(lead.id)],
                "manager": manager_target.id,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)

    def test_admin_can_unassign_manager_to_single_lead(self):
        admin = User.objects.create_user(username="admin_unassign_single", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_unassign_single",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Single", code="partner-unassign-single")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/unassign-manager/",
            {"reason": "manager on vacation"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)
        self.assertIsNone(response.data["manager"])
        audit = LeadAuditLog.objects.get(lead=lead, event_type=LeadAuditEvent.MANAGER_UNASSIGNED)
        self.assertEqual(audit.actor_user_id, admin.id)
        self.assertEqual(audit.reason, "manager on vacation")
        self.assertEqual(audit.payload_before["manager"]["id"], str(manager_target.id))
        self.assertEqual(audit.payload_after["manager"], None)

    def test_teamleader_can_unassign_manager_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_unassign_single", password="pass12345", role=UserRole.TEAMLEADER)
        manager_target = User.objects.create_user(
            username="manager_target_tl_unassign",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner TL Unassign", code="partner-tl-unassign")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/unassign-manager/",
            {"reason": "queue rebalance"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)
        self.assertIsNone(response.data["manager"])

    def test_teamleader_can_unassign_other_teamleader_lead(self):
        teamleader = User.objects.create_user(username="tl_unassign_other_tl", password="pass12345", role=UserRole.TEAMLEADER)
        teamleader_owner = User.objects.create_user(
            username="tl_owner_unassign_other_tl",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        partner = Partner.objects.create(name="Partner TL Unassign Other TL", code="partner-tl-unassign-other-tl")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=teamleader_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/unassign-manager/",
            {"reason": "queue rebalance from other tl"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)
        self.assertIsNone(response.data["manager"])

    def test_teamleader_cannot_unassign_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_unassign_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_owner = User.objects.create_user(username="ret_unassign_owner", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Unassign RET", code="partner-tl-unassign-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=ret_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/unassign-manager/",
            {"reason": "try protected"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_owner.id)

    def test_admin_can_bulk_unassign_manager(self):
        admin = User.objects.create_user(username="admin_unassign_bulk", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_unassign_bulk",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Bulk", code="partner-unassign-bulk")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-unassign-manager/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "reason": "queue reset",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 2)
        self.assertEqual(response.data["failed_count"], 0)

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertIsNone(lead_1.manager_id)
        self.assertIsNone(lead_2.manager_id)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.MANAGER_UNASSIGNED,
                lead_id__in=[lead_1.id, lead_2.id],
                reason="queue reset",
            ).count(),
            2,
        )

    def test_bulk_unassign_manager_partial_success_reports_missing(self):
        admin = User.objects.create_user(username="admin_unassign_bulk_partial", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_unassign_bulk_partial",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Bulk Partial", code="partner-unassign-bulk-partial")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        unknown_id = 999999992
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-unassign-manager/",
            {
                "lead_ids": [str(lead.id), str(unknown_id)],
                "reason": "partial unassign",
                "allow_partial": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 1)
        self.assertEqual(response.data["failed_count"], 1)
        self.assertEqual(response.data["failed"][str(unknown_id)], "Unknown lead id")
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.MANAGER_UNASSIGNED,
                lead=lead,
                reason="partial unassign",
            ).exists()
        )

    def test_manager_cannot_bulk_unassign_manager(self):
        manager_actor = User.objects.create_user(
            username="manager_actor_unassign_bulk_deny",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        manager_target = User.objects.create_user(
            username="manager_target_unassign_bulk_deny",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Bulk Deny", code="partner-unassign-bulk-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(manager_actor)

        response = self.client.post(
            "/api/v1/leads/records/bulk-unassign-manager/",
            {
                "lead_ids": [str(lead.id)],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)

    def test_assign_manager_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_assign_idem", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_assign_idem",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Idem", code="partner-assign-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "assign-manager-key-1"}
        payload = {"manager": manager_target.id}
        first = self.client.post(f"/api/v1/leads/records/{lead.id}/assign-manager/", payload, format="json", **headers)
        second = self.client.post(f"/api/v1/leads/records/{lead.id}/assign-manager/", payload, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                lead=lead,
                event_type=LeadAuditEvent.MANAGER_ASSIGNED,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadIdempotencyEndpoint.ASSIGN_MANAGER,
                key="assign-manager-key-1",
            ).count(),
            1,
        )

    def test_assign_manager_rejects_same_idempotency_key_with_different_payload(self):
        admin = User.objects.create_user(username="admin_assign_idem_diff", password="pass12345", role=UserRole.ADMIN)
        manager_first = User.objects.create_user(
            username="manager_assign_idem_first",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        manager_second = User.objects.create_user(
            username="manager_assign_idem_second",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Idem Diff", code="partner-assign-idem-diff")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "assign-manager-key-2"}
        first = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_first.id},
            format="json",
            **headers,
        )
        second = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_second.id},
            format="json",
            **headers,
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(second.data["error"]["code"], "validation_error")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_first.id)

    def test_bulk_assign_manager_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_bulk_assign_idem", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_bulk_assign_idem_target",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Bulk Assign Idem", code="partner-bulk-assign-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "bulk-assign-manager-key-1"}
        payload = {
            "lead_ids": [str(lead_1.id), str(lead_2.id)],
            "manager": manager_target.id,
        }
        first = self.client.post("/api/v1/leads/records/bulk-assign-manager/", payload, format="json", **headers)
        second = self.client.post("/api/v1/leads/records/bulk-assign-manager/", payload, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                lead_id__in=[lead_1.id, lead_2.id],
                event_type=LeadAuditEvent.MANAGER_ASSIGNED,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadIdempotencyEndpoint.BULK_ASSIGN_MANAGER,
                key="bulk-assign-manager-key-1",
            ).count(),
            1,
        )

    def test_unassign_manager_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_unassign_idem", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_unassign_idem_target",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Idem", code="partner-unassign-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "unassign-manager-key-1"}
        first = self.client.post(f"/api/v1/leads/records/{lead.id}/unassign-manager/", {}, format="json", **headers)
        second = self.client.post(f"/api/v1/leads/records/{lead.id}/unassign-manager/", {}, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        lead.refresh_from_db()
        self.assertIsNone(lead.manager_id)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                lead=lead,
                event_type=LeadAuditEvent.MANAGER_UNASSIGNED,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadIdempotencyEndpoint.UNASSIGN_MANAGER,
                key="unassign-manager-key-1",
            ).count(),
            1,
        )

    def test_bulk_unassign_manager_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_bulk_unassign_idem", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_bulk_unassign_idem_target",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Bulk Unassign Idem", code="partner-bulk-unassign-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, manager=manager_target, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "bulk-unassign-manager-key-1"}
        payload = {"lead_ids": [str(lead_1.id), str(lead_2.id)]}
        first = self.client.post("/api/v1/leads/records/bulk-unassign-manager/", payload, format="json", **headers)
        second = self.client.post("/api/v1/leads/records/bulk-unassign-manager/", payload, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                lead_id__in=[lead_1.id, lead_2.id],
                event_type=LeadAuditEvent.MANAGER_UNASSIGNED,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadIdempotencyEndpoint.BULK_UNASSIGN_MANAGER,
                key="bulk-unassign-manager-key-1",
            ).count(),
            1,
        )

    def test_change_status_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_change_idem", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change Idem", code="partner-change-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "change-status-key-1"}
        first = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "idem test"},
            format="json",
            **headers,
        )
        second = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "idem test"},
            format="json",
            **headers,
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead=lead,
                to_status=status_work,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint="change_status",
                key="change-status-key-1",
            ).count(),
            1,
        )

    def test_change_status_rejects_same_idempotency_key_with_different_payload(self):
        admin = User.objects.create_user(username="admin_change_idem_diff", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change Idem Diff", code="partner-change-idem-diff")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(code="LOST", name="Lost")
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "change-status-key-2"}
        first = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "idem first"},
            format="json",
            **headers,
        )
        second = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_lost.id), "reason": "idem second"},
            format="json",
            **headers,
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(second.data["error"]["code"], "validation_error")

    def test_change_status_uses_fresh_row_after_stale_prefetch(self):
        from unittest.mock import patch

        admin = User.objects.create_user(username="admin_stale_check", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Stale", code="partner-stale")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        status_won = LeadStatus.objects.create(code="WON", name="Won")
        stale_lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        stale_snapshot = Lead.objects.select_related("status").get(id=stale_lead.id)
        stale_lead.status = status_work
        stale_lead.save(update_fields=["status", "updated_at"])
        self._auth(admin)

        with patch("apps.leads.api.views.LeadViewSet.get_object", return_value=stale_snapshot):
            response = self.client.post(
                f"/api/v1/leads/records/{stale_lead.id}/change-status/",
                {"to_status": str(status_won.id), "reason": "stale retry"},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        stale_lead.refresh_from_db()
        self.assertEqual(stale_lead.status_id, status_won.id)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead=stale_lead,
                to_status=status_won,
            ).exists()
        )

    def test_admin_can_bulk_change_lead_status_and_write_audit(self):
        admin = User.objects.create_user(username="admin_bulk_status", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk", code="partner-bulk")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead_1 = Lead.objects.create(partner=partner, status=status_new, custom_fields={"n": 1})
        lead_2 = Lead.objects.create(partner=partner, status=status_new, custom_fields={"n": 2})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_work.id),
                "reason": "bulk move to work",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 2)
        self.assertCountEqual(response.data["updated_ids"], [str(lead_1.id), str(lead_2.id)])

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertEqual(lead_1.status_id, status_work.id)
        self.assertEqual(lead_2.status_id, status_work.id)

        audits = LeadAuditLog.objects.filter(
            event_type=LeadAuditEvent.STATUS_CHANGED,
            to_status=status_work,
            lead_id__in=[lead_1.id, lead_2.id],
        )
        self.assertEqual(audits.count(), 2)
        self.assertEqual(set(audits.values_list("reason", flat=True)), {"bulk move to work"})

    def test_bulk_change_status_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_bulk_idem", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Idem", code="partner-bulk-idem")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead_1 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "bulk-status-key-1"}
        payload = {
            "lead_ids": [str(lead_1.id), str(lead_2.id)],
            "to_status": str(status_work.id),
            "reason": "bulk idem",
        }
        first = self.client.post("/api/v1/leads/records/bulk-change-status/", payload, format="json", **headers)
        second = self.client.post("/api/v1/leads/records/bulk-change-status/", payload, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        self.assertEqual(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead_id__in=[lead_1.id, lead_2.id],
                to_status=status_work,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint="bulk_change_status",
                key="bulk-status-key-1",
            ).count(),
            1,
        )

    def test_bulk_change_status_updates_all_without_transition_checks(self):
        admin = User.objects.create_user(username="admin_bulk_invalid", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Invalid", code="partner-bulk-invalid")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(code="LOST", name="Lost")
        lead_allowed = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_blocked = Lead.objects.create(partner=partner, status=status_lost, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_allowed.id), str(lead_blocked.id)],
                "to_status": str(status_work.id),
                "reason": "bulk invalid",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 2)

        lead_allowed.refresh_from_db()
        lead_blocked.refresh_from_db()
        self.assertEqual(lead_allowed.status_id, status_work.id)
        self.assertEqual(lead_blocked.status_id, status_work.id)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead_id__in=[lead_allowed.id, lead_blocked.id],
            ).exists()
        )

    def test_bulk_change_status_partial_success_keeps_unknown_only(self):
        admin = User.objects.create_user(username="admin_bulk_partial", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Partial", code="partner-bulk-partial")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(code="LOST", name="Lost")
        lead_allowed = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_blocked = Lead.objects.create(partner=partner, status=status_lost, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_allowed.id), str(lead_blocked.id)],
                "to_status": str(status_work.id),
                "reason": "bulk partial",
                "allow_partial": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 2)
        self.assertEqual(response.data["failed_count"], 0)
        self.assertCountEqual(response.data["updated_ids"], [str(lead_allowed.id), str(lead_blocked.id)])

        lead_allowed.refresh_from_db()
        lead_blocked.refresh_from_db()
        self.assertEqual(lead_allowed.status_id, status_work.id)
        self.assertEqual(lead_blocked.status_id, status_work.id)

        self.assertEqual(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead_id=lead_allowed.id,
            ).count(),
            1,
        )
        self.assertTrue(
            LeadAuditLog.objects.filter(
                event_type=LeadAuditEvent.STATUS_CHANGED,
                lead_id=lead_blocked.id,
            ).exists()
        )

    def test_bulk_change_status_partial_success_reports_unknown_lead_id(self):
        admin = User.objects.create_user(username="admin_bulk_partial_unknown", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Partial Unknown", code="partner-bulk-partial-unknown")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        unknown_id = 999999993
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead.id), str(unknown_id)],
                "to_status": str(status_work.id),
                "reason": "bulk partial with unknown",
                "allow_partial": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 1)
        self.assertEqual(response.data["failed_count"], 1)
        self.assertEqual(response.data["updated_ids"], [str(lead.id)])
        self.assertEqual(response.data["failed"][str(unknown_id)], "Unknown lead id")

        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

    @override_settings(LEADS_BULK_STATUS_CHANGE_MAX_IDS=1)
    def test_bulk_change_status_rejects_when_limit_exceeded(self):
        admin = User.objects.create_user(username="admin_bulk_limit", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Limit", code="partner-bulk-limit")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead_1 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_work.id),
                "reason": "limit test",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertIn("Maximum 1 lead ids allowed per request", str(response.data["error"]["details"]))

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertEqual(lead_1.status_id, status_new.id)
        self.assertEqual(lead_2.status_id, status_new.id)

    def test_bulk_change_status_does_not_require_comment(self):
        admin = User.objects.create_user(username="admin_bulk_comment", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Comment", code="partner-bulk-comment")
        status_lost = LeadStatus.objects.create(code="LOST", name="Lost", is_default_for_new_leads=True)
        status_reopen = LeadStatus.objects.create(code="REOPEN", name="Reopen")
        lead_1 = Lead.objects.create(partner=partner, status=status_lost, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, status=status_lost, custom_fields={})
        self._auth(admin)

        response_no_reason = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_reopen.id),
            },
            format="json",
        )
        self.assertEqual(response_no_reason.status_code, status.HTTP_200_OK)

        response_with_reason = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_reopen.id),
                "reason": "returned to funnel",
            },
            format="json",
        )
        self.assertEqual(response_with_reason.status_code, status.HTTP_400_BAD_REQUEST)

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertEqual(lead_1.status_id, status_reopen.id)
        self.assertEqual(lead_2.status_id, status_reopen.id)

    def test_manager_can_bulk_change_own_lead_status(self):
        manager = User.objects.create_user(username="manager_bulk_change", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Bulk Manager", code="partner-bulk-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=manager, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead.id)],
                "to_status": str(status_work.id),
                "reason": "try bulk change",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated_count"], 1)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_work.id)

    def test_manager_cannot_bulk_change_foreign_lead_status(self):
        owner = User.objects.create_user(username="manager_bulk_owner", password="pass12345", role=UserRole.MANAGER)
        manager = User.objects.create_user(username="manager_bulk_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Bulk Foreign", code="partner-bulk-foreign")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=owner, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead.id)],
                "to_status": str(status_work.id),
                "reason": "try foreign bulk change",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_admin_can_get_leads_metrics(self):
        admin = User.objects.create_user(username="admin_metrics", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_metrics_all", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics", code="partner-metrics")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.NON_WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.NON_WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        status_return = LeadStatus.objects.create(
            code="CALL_LATER_METRICS",
            name="Call Later Metrics",
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.RETURN,
        )

        lead_1 = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_2 = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 8, 10, 0, 0)),
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 8, 10, 0, 0)),
        )
        lead_3 = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2025, 12, 20, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2025, 12, 20, 10, 0, 0)),
        )
        lead_4 = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 20, 10, 0, 0)),
            status=status_return,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 20, 10, 0, 0)),
        )
        dep_1 = LeadDeposit.objects.create(lead=lead_1, creator=manager, amount="150.00", type=LeadDeposit.Type.FTD)
        dep_3 = LeadDeposit.objects.create(lead=lead_3, creator=manager, amount="120.00", type=LeadDeposit.Type.FTD)
        self._set_deposit_created_at(dep_1, timezone.make_aware(datetime(2026, 1, 5, 15, 0, 0)))
        self._set_deposit_created_at(dep_3, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        for removed_key in ("transitions_count", "personal_conversion", "sales_executor", "speed", "stale_leads"):
            self.assertNotIn(removed_key, response.data)
        self.assertEqual(response.data["overview"]["total"], 3)
        self.assertEqual(response.data["overview"]["valid_total"], 3)
        self.assertEqual(response.data["overview"]["invalid_total"], 0)
        self.assertEqual(response.data["overview"]["won_total"], 2)
        self.assertEqual(response.data["overview"]["lost_total"], 1)
        self.assertEqual(response.data["overview"]["working_total"], 0)
        self.assertEqual(response.data["overview"]["return_total"], 1)
        self.assertEqual(response.data["overview"]["non_working_total"], 2)
        self.assertEqual(response.data["conversion"]["cohort"]["count"], 2)
        self.assertEqual(response.data["conversion"]["cohort"]["rate"], 0.6667)
        self.assertEqual(response.data["conversion"]["same_day"]["count"], 1)
        self.assertEqual(response.data["conversion"]["same_day"]["rate"], 0.3333)
        self.assertEqual(response.data["won_by_manager"][0]["manager"]["id"], str(manager.id))
        self.assertEqual(response.data["won_by_manager"][0]["won_total"], 2)

        status_counts = {row["status_code"]: row["count"] for row in response.data["status_breakdown"]}
        self.assertEqual(status_counts["WON"], 1)
        self.assertEqual(status_counts["LOST"], 1)
        self.assertEqual(status_counts["CALL_LATER_METRICS"], 1)
        status_buckets = {row["status_code"]: row["work_bucket"] for row in response.data["status_breakdown"]}
        self.assertEqual(status_buckets["WON"], LeadStatus.WorkBucket.NON_WORKING)
        self.assertEqual(status_buckets["CALL_LATER_METRICS"], LeadStatus.WorkBucket.RETURN)

    def test_admin_can_get_leads_metrics_for_single_partner(self):
        admin = User.objects.create_user(username="admin_metrics_partner", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_metrics_partner", password="pass12345", role=UserRole.MANAGER)
        partner_a = Partner.objects.create(name="Partner A", code="partner-a")
        partner_b = Partner.objects.create(name="Partner B", code="partner-b")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )

        lead_a1 = Lead.objects.create(
            partner=partner_a,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        lead_a2 = Lead.objects.create(
            partner=partner_a,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
        )
        lead_b = Lead.objects.create(
            partner=partner_b,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )
        dep_a1 = LeadDeposit.objects.create(lead=lead_a1, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        dep_b = LeadDeposit.objects.create(lead=lead_b, creator=manager, amount="120.00", type=LeadDeposit.Type.FTD)
        self._set_deposit_created_at(dep_a1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        self._set_deposit_created_at(dep_b, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        log_a1 = LeadAuditLog.objects.create(
            lead=lead_a1,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_a2 = LeadAuditLog.objects.create(
            lead=lead_a2,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_a2, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b = LeadAuditLog.objects.create(
            lead=lead_b,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get(
            f"/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&partner={partner_a.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["partner"]["id"], str(partner_a.id))
        self.assertEqual(response.data["overview"]["total"], 2)
        self.assertEqual(response.data["overview"]["valid_total"], 2)
        self.assertEqual(response.data["overview"]["won_total"], 1)
        self.assertEqual(response.data["overview"]["lost_total"], 1)
        self.assertEqual(response.data["conversion"]["cohort"]["count"], 1)
        self.assertEqual(response.data["conversion"]["cohort"]["rate"], 0.5)
        self.assertEqual(response.data["conversion"]["same_day"]["count"], 0)

    def test_admin_can_get_leads_metrics_grouped_by_partner(self):
        admin = User.objects.create_user(username="admin_metrics_group", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_metrics_group", password="pass12345", role=UserRole.MANAGER)
        partner_a = Partner.objects.create(name="Partner Group A", code="partner-group-a")
        partner_b = Partner.objects.create(name="Partner Group B", code="partner-group-b")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )

        lead_a = Lead.objects.create(
            partner=partner_a,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_b1 = Lead.objects.create(
            partner=partner_b,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )
        lead_b2 = Lead.objects.create(
            partner=partner_b,
            manager=manager,
            first_manager=manager,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 9, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 9, 10, 0, 0)),
        )
        dep_a = LeadDeposit.objects.create(lead=lead_a, creator=manager, amount="130.00", type=LeadDeposit.Type.FTD)
        dep_b2 = LeadDeposit.objects.create(lead=lead_b2, creator=manager, amount="90.00", type=LeadDeposit.Type.FTD)
        self._set_deposit_created_at(dep_a, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        self._set_deposit_created_at(dep_b2, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        log_a = LeadAuditLog.objects.create(
            lead=lead_a,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_b1 = LeadAuditLog.objects.create(
            lead=lead_b1,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_b1, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b2 = LeadAuditLog.objects.create(
            lead=lead_b2,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b2, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&group_by=partner")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["group_by"], "partner")
        items = {item["partner"]["code"]: item for item in response.data["items"]}
        self.assertEqual(items["partner-group-a"]["overview"]["total"], 1)
        self.assertEqual(items["partner-group-a"]["overview"]["won_total"], 1)
        self.assertEqual(items["partner-group-a"]["overview"]["lost_total"], 0)
        self.assertEqual(items["partner-group-a"]["conversion"]["cohort"]["rate"], 1.0)

        self.assertEqual(items["partner-group-b"]["overview"]["total"], 2)
        self.assertEqual(items["partner-group-b"]["overview"]["won_total"], 1)
        self.assertEqual(items["partner-group-b"]["overview"]["lost_total"], 1)
        self.assertEqual(items["partner-group-b"]["conversion"]["cohort"]["rate"], 0.5)

    def test_admin_can_get_leads_metrics_for_single_manager(self):
        admin = User.objects.create_user(username="admin_metrics_manager", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_a_metrics", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_b_metrics", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics Manager", code="partner-metrics-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )

        lead_a1 = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            first_manager=manager_a,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 2, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 2, 10, 0, 0)),
        )
        lead_a2 = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            first_manager=manager_a,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 4, 10, 0, 0)),
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 4, 10, 0, 0)),
        )
        lead_b = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            first_manager=manager_b,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
        )

        log_a1 = LeadAuditLog.objects.create(
            lead=lead_a1,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_a2 = LeadAuditLog.objects.create(
            lead=lead_a2,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_a2, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b = LeadAuditLog.objects.create(
            lead=lead_b,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get(
            f"/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&manager={manager_a.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_metrics_support_owner_and_executor_attribution(self):
        admin = User.objects.create_user(username="admin_metrics_attr", password="pass12345", role=UserRole.ADMIN)
        manager_1 = User.objects.create_user(username="manager_metrics_attr_1", password="pass12345", role=UserRole.MANAGER)
        manager_2 = User.objects.create_user(username="manager_metrics_attr_2", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics Attribution", code="partner-metrics-attr")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )

        lead_owned_and_won = Lead.objects.create(
            partner=partner,
            manager=manager_1,
            first_manager=manager_1,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        lead_owned_won_by_other = Lead.objects.create(
            partner=partner,
            manager=manager_2,
            first_manager=manager_1,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 4, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 4, 10, 0, 0)),
        )
        Lead.objects.create(
            partner=partner,
            manager=manager_1,
            first_manager=manager_1,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
            status=status_new,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&group_by=manager")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_admin_can_get_leads_metrics_for_single_ret_assignee(self):
        admin = User.objects.create_user(username="admin_metrics_ret", password="pass12345", role=UserRole.ADMIN)
        ret_user = User.objects.create_user(username="ret_metrics_user", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Metrics RET", code="partner-metrics-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=ret_user,
            first_manager=ret_user,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        log = LeadAuditLog.objects.create(
            lead=lead,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        self._auth(admin)

        response = self.client.get(
            f"/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&manager={ret_user.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_admin_can_get_leads_metrics_grouped_by_manager(self):
        admin = User.objects.create_user(username="admin_metrics_group_manager", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_a_group", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_b_group", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics Group Manager", code="partner-metrics-group-manager")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )

        lead_a = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            first_manager=manager_a,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        lead_b1 = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            first_manager=manager_b,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_b2 = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            first_manager=manager_b,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )

        log_a = LeadAuditLog.objects.create(
            lead=lead_a,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_b1 = LeadAuditLog.objects.create(
            lead=lead_b1,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_b1, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b2 = LeadAuditLog.objects.create(
            lead=lead_b2,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b2, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&group_by=manager")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_manager_can_get_leads_metrics(self):
        manager = User.objects.create_user(username="manager_metrics", password="pass12345", role=UserRole.MANAGER)
        self._auth(manager)

        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31")

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_leads_metrics_rejects_invalid_date_range(self):
        admin = User.objects.create_user(username="admin_metrics_invalid", password="pass12345", role=UserRole.ADMIN)
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-02-01&date_to=2026-01-01")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_manager_can_create_lead_comment(self):
        manager = User.objects.create_user(username="manager_comment_create", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Create", code="partner-comment-create")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            "/api/v1/leads/comments/",
            {"lead": str(lead.id), "body": "First contact completed"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["author"], manager.id)
        self.assertEqual(str(response.data["lead"]), str(lead.id))
        self.assertEqual(response.data["body"], "First contact completed")
        self.assertTrue(
            LeadComment.objects.filter(
                lead=lead,
                author=manager,
                body="First contact completed",
            ).exists()
        )
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type="lead_comment",
                event_type=LeadAuditEvent.COMMENT_CREATED,
            ).exists()
        )
        lead.refresh_from_db()
        created_comment = LeadComment.objects.get(lead=lead, author=manager, body="First contact completed")
        self.assertEqual(lead.last_contacted_at, created_comment.created_at)

    def test_ret_can_create_lead_comment(self):
        ret = User.objects.create_user(username="ret_comment_create", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Comment RET", code="partner-comment-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        self._auth(ret)

        response = self.client.post(
            "/api/v1/leads/comments/",
            {"lead": str(lead.id), "body": "RET note"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["author"], ret.id)

    def test_manager_cannot_update_foreign_comment(self):
        manager_a = User.objects.create_user(username="manager_comment_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_comment_b", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Update", code="partner-comment-update")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment = LeadComment.objects.create(lead=lead, author=manager_a, body="Initial")
        self._auth(manager_b)

        response = self.client.patch(
            f"/api/v1/leads/comments/{comment.id}/",
            {"body": "Edited by other manager"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        comment.refresh_from_db()
        self.assertEqual(comment.body, "Initial")

    def test_admin_can_update_foreign_comment(self):
        admin = User.objects.create_user(username="admin_comment_update", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_comment_owner", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Admin", code="partner-comment-admin")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment = LeadComment.objects.create(lead=lead, author=manager, body="Initial")
        self._auth(admin)

        response = self.client.patch(
            f"/api/v1/leads/comments/{comment.id}/",
            {"body": "Admin updated"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        comment.refresh_from_db()
        self.assertEqual(comment.body, "Admin updated")
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type="lead_comment",
                event_type=LeadAuditEvent.COMMENT_UPDATED,
            ).exists()
        )

    def test_manager_delete_comment_is_soft_and_can_restore(self):
        manager = User.objects.create_user(username="manager_comment_delete_restore", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Delete", code="partner-comment-delete")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment = LeadComment.objects.create(lead=lead, author=manager, body="To be deleted")
        self._auth(manager)

        delete_response = self.client.delete(f"/api/v1/leads/comments/{comment.id}/")

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(LeadComment.objects.filter(id=comment.id).exists())
        deleted_comment = LeadComment.all_objects.get(id=comment.id)
        self.assertTrue(deleted_comment.is_deleted)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                entity_type="lead_comment",
                entity_id=str(comment.id),
                event_type=LeadAuditEvent.COMMENT_SOFT_DELETED,
            ).exists()
        )

        restore_response = self.client.post(f"/api/v1/leads/comments/{comment.id}/restore/", {}, format="json")

        self.assertEqual(restore_response.status_code, status.HTTP_200_OK)
        restored_comment = LeadComment.all_objects.get(id=comment.id)
        self.assertFalse(restored_comment.is_deleted)
        self.assertTrue(LeadComment.objects.filter(id=comment.id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                entity_type="lead_comment",
                entity_id=str(comment.id),
                event_type=LeadAuditEvent.COMMENT_RESTORED,
            ).exists()
        )

    def test_manager_cannot_restore_foreign_comment(self):
        manager_a = User.objects.create_user(username="manager_comment_restore_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_comment_restore_b", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Restore Deny", code="partner-comment-restore-deny")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment = LeadComment.objects.create(lead=lead, author=manager_a, body="Protected comment")
        comment.delete()
        self._auth(manager_b)

        response = self.client.post(f"/api/v1/leads/comments/{comment.id}/restore/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        self.assertTrue(LeadComment.all_objects.get(id=comment.id).is_deleted)

    def test_admin_can_restore_foreign_comment(self):
        admin = User.objects.create_user(username="admin_comment_restore", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_comment_restore_owner", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Restore", code="partner-comment-restore")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment = LeadComment.objects.create(lead=lead, author=manager, body="Admin will restore")
        comment.delete()
        self._auth(admin)

        response = self.client.post(f"/api/v1/leads/comments/{comment.id}/restore/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(LeadComment.all_objects.get(id=comment.id).is_deleted)

    def test_list_comments_can_be_filtered_by_lead(self):
        manager = User.objects.create_user(username="manager_comment_list", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment List", code="partner-comment-list")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead_a = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        lead_b = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        comment_a = LeadComment.objects.create(lead=lead_a, author=manager, body="A")
        LeadComment.objects.create(lead=lead_b, author=manager, body="B")
        self._auth(manager)

        response = self.client.get("/api/v1/leads/comments/", {"lead": str(lead_a.id)})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["id"], comment_a.id)

    def test_pinned_comment_is_listed_first(self):
        manager = User.objects.create_user(username="manager_comment_pin", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Pin", code="partner-comment-pin")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        first = LeadComment.objects.create(lead=lead, author=manager, body="old regular")
        pinned = LeadComment.objects.create(lead=lead, author=manager, body="important", is_pinned=True)
        self._auth(manager)

        response = self.client.get("/api/v1/leads/comments/", {"lead": str(lead.id)})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["id"], pinned.id)
        self.assertEqual(response.data["results"][1]["id"], first.id)

    def test_list_comments_can_be_filtered_by_authors(self):
        manager_a = User.objects.create_user(username="manager_comment_filter_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_comment_filter_b", password="pass12345", role=UserRole.MANAGER)
        manager_c = User.objects.create_user(username="manager_comment_filter_c", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Authors", code="partner-comment-authors")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, status=status_new, custom_fields={})
        LeadComment.objects.create(lead=lead, author=manager_a, body="A")
        LeadComment.objects.create(lead=lead, author=manager_b, body="B")
        excluded = LeadComment.objects.create(lead=lead, author=manager_c, body="C")
        self._auth(manager_a)

        response = self.client.get(
            "/api/v1/leads/comments/",
            {"lead": str(lead.id), "authors": f"{manager_a.id},{manager_b.id}"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned_ids = {row["id"] for row in response.data["results"]}
        self.assertEqual(len(response.data["results"]), 2)
        self.assertNotIn(excluded.id, returned_ids)

    def test_admin_can_create_lead(self):
        admin = User.objects.create_user(username="admin_lead_create", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Create", code="partner-lead-create")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "full_name": "John Lead",
                "phone": "+123450001",
                "email": "john.lead@example.com",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["partner"]["id"], str(partner.id))
        self.assertEqual(response.data["email"], "john.lead@example.com")
        self.assertTrue(
            LeadAuditLog.objects.filter(
                entity_type="lead",
                entity_id=str(response.data["id"]),
                event_type=LeadAuditEvent.LEAD_CREATED,
            ).exists()
        )

    def test_admin_cannot_create_lead_with_custom_fields(self):
        admin = User.objects.create_user(username="admin_lead_create_cf_null", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Create CF Null", code="partner-lead-create-cf-null")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "phone": "+123450009",
                "custom_fields": None,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        self.assertIn("Only superusers can set custom_fields on create", response.data["error"]["message"])

    def test_superuser_can_create_lead_with_null_custom_fields(self):
        superuser = User.objects.create_user(
            username="su_lead_create_cf_null",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner Lead Create CF Null SU", code="partner-lead-create-cf-null-su")
        self._auth(superuser)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "phone": "+123450010",
                "custom_fields": None,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertIsNone(response.data["custom_fields"])
        lead = Lead.objects.get(id=response.data["id"])
        self.assertIsNone(lead.custom_fields)

    def test_admin_cannot_create_lead_with_geo(self):
        admin = User.objects.create_user(username="admin_lead_create_geo", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Create Geo", code="partner-lead-create-geo")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "full_name": "Geo Denied",
                "phone": "+123456001",
                "geo": "RU",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_superuser_can_create_lead_with_geo(self):
        superuser = User.objects.create_user(
            username="su_lead_create_geo",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner Lead Create Geo SU", code="partner-lead-create-geo-su")
        self._auth(superuser)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "full_name": "Geo Allowed",
                "phone": "+123456002",
                "geo": "ru",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["geo"], "RU")

    def test_teamleader_cannot_create_lead(self):
        teamleader = User.objects.create_user(username="tl_lead_create", password="pass12345", role=UserRole.TEAMLEADER)
        partner = Partner.objects.create(name="Partner Lead Create Deny", code="partner-lead-create-deny")
        self._auth(teamleader)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "full_name": "Denied",
                "phone": "+123456789",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_manager_can_update_allowed_fields_on_own_lead(self):
        manager = User.objects.create_user(username="manager_lead_edit_own", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Lead Edit", code="partner-lead-edit")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            full_name="Before",
            phone="+111",
            priority=Lead.Priority.NORMAL,
            custom_fields={},
        )
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {
                "priority": Lead.Priority.HIGH,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.priority, Lead.Priority.HIGH)
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type="lead",
                event_type=LeadAuditEvent.LEAD_UPDATED,
            ).exists()
        )

    def test_manager_cannot_change_custom_fields_on_own_lead(self):
        manager = User.objects.create_user(username="manager_lead_cf_null", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Lead CF Null", code="partner-lead-cf-null")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            phone="+1110001",
            custom_fields={"stage_note": "hot lead"},
        )
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"custom_fields": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        self.assertIn("Only superusers can edit sensitive fields", response.data["error"]["message"])
        lead.refresh_from_db()
        self.assertEqual(lead.custom_fields, {"stage_note": "hot lead"})

    def test_superuser_can_change_custom_fields(self):
        superuser = User.objects.create_user(
            username="su_lead_cf_update",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner Lead CF Update", code="partner-lead-cf-update")
        lead = Lead.objects.create(
            partner=partner,
            phone="+1110002",
            custom_fields={"stage_note": "old"},
        )
        self._auth(superuser)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"custom_fields": {"stage_note": "new", "x": 1}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["custom_fields"]["stage_note"], "new")
        lead.refresh_from_db()
        self.assertEqual(lead.custom_fields["stage_note"], "new")

    def test_teamleader_can_patch_first_manager_and_first_assigned_at(self):
        teamleader = User.objects.create_user(username="tl_patch_first_mgr", password="pass12345", role=UserRole.TEAMLEADER)
        manager_owner = User.objects.create_user(username="manager_patch_first_owner", password="pass12345", role=UserRole.MANAGER)
        manager_target = User.objects.create_user(username="manager_patch_first_target", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Patch First Manager", code="partner-patch-first-manager")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager_owner,
            first_manager=manager_owner,
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 10, 10, 0, 0)),
            phone="+11101",
            custom_fields={},
        )
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {
                "first_manager": manager_target.id,
                "first_assigned_at": "2026-01-09T09:30:00Z",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager_target.id)
        self.assertEqual(lead.first_assigned_at.isoformat(), "2026-01-09T09:30:00+00:00")

    def test_manager_cannot_patch_first_manager(self):
        manager = User.objects.create_user(username="manager_patch_first_deny", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_patch_first_target_deny", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Patch First Deny", code="partner-patch-first-deny")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            phone="+11102",
            custom_fields={},
        )
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"first_manager": manager_b.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.first_manager_id, manager.id)

    def test_manager_can_set_priority_null_on_own_lead(self):
        manager = User.objects.create_user(username="manager_priority_null", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Priority Null", code="partner-priority-null")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            phone="+11103",
            priority=Lead.Priority.NORMAL,
            custom_fields={},
        )
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"priority": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNone(lead.priority)

    def test_manager_cannot_update_protected_fields_on_own_lead(self):
        manager = User.objects.create_user(username="manager_protected_fields", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Protected", code="partner-protected")
        source_a = PartnerSource.objects.create(partner=partner, code="google", name="Google")
        source_b = PartnerSource.objects.create(partner=partner, code="fb", name="Facebook")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            source=source_a,
            full_name="Before",
            phone="+1111",
            email="before@example.com",
            custom_fields={},
        )
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {
                "full_name": "After",
                "phone": "+2222",
                "email": "after@example.com",
                "source": str(source_b.id),
                "partner": str(partner.id),
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "Before")
        self.assertEqual(lead.phone, "+1111")
        self.assertEqual(lead.email, "before@example.com")
        self.assertEqual(lead.source_id, source_a.id)

    def test_admin_cannot_update_sensitive_partner_fields(self):
        admin = User.objects.create_user(username="admin_sensitive_fields", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Admin Sensitive", code="partner-admin-sensitive")
        partner_other = Partner.objects.create(name="Partner Admin Sensitive Other", code="partner-admin-sensitive-other")
        source_a = PartnerSource.objects.create(partner=partner, code="google-admin", name="Google Admin")
        source_b = PartnerSource.objects.create(partner=partner_other, code="meta-admin", name="Meta Admin")
        lead = Lead.objects.create(
            partner=partner,
            source=source_a,
            geo="RU",
            full_name="Original Name",
            phone="+15550001",
            email="original@example.com",
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {
                "full_name": "Changed Name",
                "phone": "+15550002",
                "email": "changed@example.com",
                "source": str(source_b.id),
                "partner": str(partner_other.id),
                "geo": "CH",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "Original Name")
        self.assertEqual(lead.phone, "+15550001")
        self.assertEqual(lead.email, "original@example.com")
        self.assertEqual(lead.partner_id, partner.id)
        self.assertEqual(lead.source_id, source_a.id)
        self.assertEqual(lead.geo, "RU")

    def test_superuser_can_update_sensitive_partner_fields(self):
        superuser = User.objects.create_user(
            username="su_sensitive_fields",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner SU Sensitive", code="partner-su-sensitive")
        partner_other = Partner.objects.create(name="Partner SU Sensitive Other", code="partner-su-sensitive-other")
        source_a = PartnerSource.objects.create(partner=partner, code="google-su", name="Google SU")
        source_b = PartnerSource.objects.create(partner=partner_other, code="meta-su", name="Meta SU")
        lead = Lead.objects.create(
            partner=partner,
            source=source_a,
            geo="RU",
            full_name="Original SU",
            phone="+16660001",
            email="original-su@example.com",
            custom_fields={},
        )
        self._auth(superuser)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {
                "full_name": "Updated SU",
                "phone": "+16660002",
                "email": "updated-su@example.com",
                "source": str(source_b.id),
                "partner": str(partner_other.id),
                "geo": "ch",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "Updated SU")
        self.assertEqual(lead.phone, "+16660002")
        self.assertEqual(lead.email, "updated-su@example.com")
        self.assertEqual(lead.partner_id, partner_other.id)
        self.assertEqual(lead.source_id, source_b.id)
        self.assertEqual(lead.geo, "CH")

    def test_manager_cannot_update_foreign_lead(self):
        owner = User.objects.create_user(username="manager_lead_owner", password="pass12345", role=UserRole.MANAGER)
        manager = User.objects.create_user(username="manager_lead_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Lead Edit Deny", code="partner-lead-edit-deny")
        lead = Lead.objects.create(partner=partner, manager=owner, full_name="Before", phone="+111", custom_fields={})
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"full_name": "After"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "Before")

    def test_teamleader_cannot_update_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_edit_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_user = User.objects.create_user(username="ret_edit_protected", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Edit RET", code="partner-tl-edit-ret")
        lead = Lead.objects.create(partner=partner, manager=ret_user, full_name="RET Lead", phone="+1311", custom_fields={})
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"priority": Lead.Priority.HIGH},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertEqual(lead.priority, Lead.Priority.NORMAL)

    def test_manager_sees_only_own_leads(self):
        manager = User.objects.create_user(username="manager_list_own", password="pass12345", role=UserRole.MANAGER)
        other = User.objects.create_user(username="manager_list_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner List Own", code="partner-list-own")
        own = Lead.objects.create(partner=partner, manager=manager, full_name="Own Lead", phone="+1001", custom_fields={})
        Lead.objects.create(partner=partner, manager=other, full_name="Foreign Lead", phone="+1002", custom_fields={})
        self._auth(manager)

        response = self.client.get("/api/v1/leads/records/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], own.id)

    def test_leads_list_supports_ordering(self):
        admin = User.objects.create_user(username="admin_list_ordering", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner List Ordering", code="partner-list-ordering")
        lead_old = Lead.objects.create(
            partner=partner,
            phone="+100101",
            received_at=timezone.make_aware(datetime(2026, 1, 1, 10, 0, 0)),
            custom_fields={},
        )
        lead_new = Lead.objects.create(
            partner=partner,
            phone="+100102",
            received_at=timezone.make_aware(datetime(2026, 1, 2, 10, 0, 0)),
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/", {"ordering": "received_at"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["id"], lead_old.id)
        self.assertEqual(response.data["results"][1]["id"], lead_new.id)

    def test_leads_list_supports_age_filter(self):
        admin = User.objects.create_user(username="admin_list_age_filter", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner List Age Filter", code="partner-list-age-filter")
        matching = Lead.objects.create(partner=partner, phone="+100111", age=34, custom_fields={})
        Lead.objects.create(partner=partner, phone="+100112", age=22, custom_fields={})
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/", {"age": 34})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], matching.id)
        self.assertEqual(response.data["results"][0]["age"], 34)

    def test_leads_list_supports_age_ordering(self):
        admin = User.objects.create_user(username="admin_list_age_ordering", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner List Age Ordering", code="partner-list-age-ordering")
        younger = Lead.objects.create(partner=partner, phone="+100121", age=19, custom_fields={})
        older = Lead.objects.create(partner=partner, phone="+100122", age=41, custom_fields={})
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/", {"ordering": "age"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["id"], younger.id)
        self.assertEqual(response.data["results"][1]["id"], older.id)

    def test_leads_list_does_not_include_last_comment_by_default(self):
        admin = User.objects.create_user(username="admin_list_no_last_comment", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_list_no_last_comment", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner List No Last Comment", code="partner-list-no-last-comment")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+100201", custom_fields={})
        LeadComment.objects.create(lead=lead, author=manager, body="Hidden by default")
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertNotIn("last_comment", response.data["results"][0])

    def test_leads_list_can_include_last_comment(self):
        admin = User.objects.create_user(username="admin_list_with_last_comment", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(
            username="manager_list_with_last_comment",
            password="pass12345",
            role=UserRole.MANAGER,
            first_name="Nina",
            last_name="Lopez",
        )
        partner = Partner.objects.create(name="Partner List Last Comment", code="partner-list-last-comment")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+100202", custom_fields={})
        LeadComment.objects.create(lead=lead, author=manager, body="First")
        expected_last = LeadComment.objects.create(lead=lead, author=manager, body="Second", is_pinned=True)
        deleted_newest = LeadComment.objects.create(lead=lead, author=manager, body="Third deleted")
        LeadComment.all_objects.filter(id=deleted_newest.id).update(is_deleted=True, deleted_at=timezone.now())
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/", {"include_last_comment": "true"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        item = response.data["results"][0]
        self.assertIn("last_comment", item)
        self.assertEqual(item["last_comment"]["id"], str(expected_last.id))
        self.assertEqual(item["last_comment"]["body"], "Second")
        self.assertTrue(item["last_comment"]["is_pinned"])
        self.assertEqual(item["last_comment"]["author"]["id"], str(manager.id))
        self.assertEqual(item["last_comment"]["author"]["first_name"], "Nina")
        self.assertEqual(item["last_comment"]["author"]["last_name"], "Lopez")

    def test_lead_retrieve_can_include_last_comment(self):
        admin = User.objects.create_user(username="admin_retrieve_last_comment", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(
            username="manager_retrieve_last_comment",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Retrieve Last Comment", code="partner-retrieve-last-comment")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+100203", custom_fields={})
        expected_last = LeadComment.objects.create(lead=lead, author=manager, body="Retrieve latest")
        self._auth(admin)

        response = self.client.get(f"/api/v1/leads/records/{lead.id}/", {"include_last_comment": "1"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("last_comment", response.data)
        self.assertEqual(response.data["last_comment"]["id"], str(expected_last.id))
        self.assertEqual(response.data["last_comment"]["body"], "Retrieve latest")
        self.assertEqual(response.data["last_comment"]["author"]["id"], str(manager.id))

    def test_manager_cannot_retrieve_foreign_lead(self):
        manager = User.objects.create_user(username="manager_retrieve_own", password="pass12345", role=UserRole.MANAGER)
        other = User.objects.create_user(username="manager_retrieve_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Retrieve Own", code="partner-retrieve-own")
        foreign = Lead.objects.create(partner=partner, manager=other, full_name="Foreign", phone="+1003", custom_fields={})
        self._auth(manager)

        response = self.client.get(f"/api/v1/leads/records/{foreign.id}/")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_lead_timeline_returns_latest_events_and_supports_filter(self):
        admin = User.objects.create_user(username="admin_timeline_list", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_timeline_list", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Timeline", code="partner-timeline")
        status_new = LeadStatus.objects.create(code="NEW_TIMELINE", name="New Timeline", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK_TIMELINE", name="Work Timeline")
        lead = Lead.objects.create(partner=partner, manager=manager, status=status_new, phone="+10031", custom_fields={})

        log_status = LeadAuditLog.objects.create(
            lead=lead,
            event_type=LeadAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_work,
            actor_user=manager,
            payload_before={"status": {"code": status_new.code}},
            payload_after={"status": {"code": status_work.code}},
        )
        log_comment = LeadAuditLog.objects.create(
            lead=lead,
            event_type=LeadAuditEvent.COMMENT_CREATED,
            actor_user=manager,
            entity_type="lead_comment",
            entity_id="1",
            payload_after={"body": "Contacted customer"},
        )
        self._set_log_created_at(log_status, timezone.make_aware(datetime(2026, 2, 1, 10, 0, 0)))
        self._set_log_created_at(log_comment, timezone.make_aware(datetime(2026, 2, 1, 11, 0, 0)))
        self._auth(admin)

        response = self.client.get(f"/api/v1/leads/records/{lead.id}/timeline/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 2)
        self.assertEqual(response.data["results"][0]["event_type"], LeadAuditEvent.COMMENT_CREATED)
        self.assertEqual(response.data["results"][0]["details"], "Contacted customer")
        self.assertEqual(response.data["results"][1]["event_type"], LeadAuditEvent.STATUS_CHANGED)
        self.assertEqual(response.data["results"][1]["details"], "NEW_TIMELINE -> WORK_TIMELINE")
        self.assertEqual(response.data["results"][0]["actor"]["id"], str(manager.id))

        filtered_response = self.client.get(
            f"/api/v1/leads/records/{lead.id}/timeline/",
            {"events": LeadAuditEvent.STATUS_CHANGED},
        )
        self.assertEqual(filtered_response.status_code, status.HTTP_200_OK)
        self.assertEqual(filtered_response.data["count"], 1)
        self.assertEqual(filtered_response.data["results"][0]["event_type"], LeadAuditEvent.STATUS_CHANGED)

        invalid_response = self.client.get(
            f"/api/v1/leads/records/{lead.id}/timeline/",
            {"events": "unknown_event"},
        )
        self.assertEqual(invalid_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(invalid_response.data["error"]["code"], "validation_error")
        events_error = invalid_response.data["error"]["details"]["events"]
        if isinstance(events_error, list):
            events_error = events_error[0]
        self.assertIn("Unknown event types", events_error)

    def test_lead_timeline_blocks_manager_for_foreign_lead(self):
        manager = User.objects.create_user(username="manager_timeline_self", password="pass12345", role=UserRole.MANAGER)
        other = User.objects.create_user(username="manager_timeline_other", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Timeline Scope", code="partner-timeline-scope")
        lead = Lead.objects.create(partner=partner, manager=other, phone="+10032", custom_fields={})
        self._auth(manager)

        response = self.client.get(f"/api/v1/leads/records/{lead.id}/timeline/")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_teamleader_can_set_next_contact_for_manager_lead(self):
        teamleader = User.objects.create_user(username="tl_next_contact", password="pass12345", role=UserRole.TEAMLEADER)
        manager = User.objects.create_user(username="manager_next_contact", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Next Contact", code="partner-next-contact")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+1004", custom_fields={})
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"next_contact_at": "2026-03-01T10:30:00+01:00"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNotNone(lead.next_contact_at)

    def test_teamleader_can_set_next_contact_for_other_teamleader_lead(self):
        teamleader = User.objects.create_user(username="tl_next_contact_other_tl", password="pass12345", role=UserRole.TEAMLEADER)
        teamleader_owner = User.objects.create_user(
            username="tl_owner_next_contact_other_tl",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        partner = Partner.objects.create(name="Partner Next Contact Other TL", code="partner-next-contact-other-tl")
        lead = Lead.objects.create(partner=partner, manager=teamleader_owner, phone="+100401", custom_fields={})
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"next_contact_at": "2026-03-01T10:30:00+01:00"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertIsNotNone(lead.next_contact_at)

    def test_teamleader_cannot_set_next_contact_for_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_next_contact_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_user = User.objects.create_user(username="ret_next_contact", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Next Contact RET", code="partner-next-contact-ret")
        lead = Lead.objects.create(partner=partner, manager=ret_user, phone="+10041", custom_fields={})
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"next_contact_at": "2026-03-01T10:30:00+01:00"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertIsNone(lead.next_contact_at)

    def test_teamleader_cannot_set_next_contact_for_unassigned_lead(self):
        teamleader = User.objects.create_user(username="tl_next_contact_unassigned", password="pass12345", role=UserRole.TEAMLEADER)
        partner = Partner.objects.create(name="Partner Next Contact Unassigned", code="partner-next-contact-unassigned")
        lead = Lead.objects.create(partner=partner, manager=None, phone="+1005", custom_fields={})
        self._auth(teamleader)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"next_contact_at": "2026-03-02T12:00:00+01:00"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertIsNone(lead.next_contact_at)

    def test_admin_can_soft_delete_lead(self):
        admin = User.objects.create_user(username="admin_lead_soft_delete", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Soft Delete", code="partner-lead-soft-delete")
        lead = Lead.objects.create(partner=partner, phone="+111", custom_fields={})
        self._auth(admin)

        response = self.client.post(f"/api/v1/leads/records/{lead.id}/soft_delete/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Lead.objects.filter(id=lead.id).exists())
        self.assertTrue(Lead.all_objects.filter(id=lead.id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                entity_type="lead",
                entity_id=str(lead.id),
                event_type=LeadAuditEvent.LEAD_SOFT_DELETED,
            ).exists()
        )

    def test_admin_cannot_hard_delete_lead(self):
        admin = User.objects.create_user(username="admin_lead_hard_delete", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Hard Delete", code="partner-lead-hard-delete")
        lead = Lead.objects.create(partner=partner, phone="+111", custom_fields={})
        self._auth(admin)

        response = self.client.delete(f"/api/v1/leads/records/{lead.id}/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertTrue(Lead.all_objects.filter(id=lead.id).exists())

    def test_superuser_can_hard_delete_lead(self):
        superuser = User.objects.create_user(
            username="su_lead_hard_delete",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner Lead Hard Delete SU", code="partner-lead-hard-delete-su")
        lead = Lead.objects.create(partner=partner, phone="+111", custom_fields={})
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/leads/records/{lead.id}/")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Lead.all_objects.filter(id=lead.id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                entity_type="lead",
                entity_id=str(lead.id),
                event_type=LeadAuditEvent.LEAD_HARD_DELETED,
            ).exists()
        )

    def test_admin_create_rejects_duplicate_phone(self):
        admin = User.objects.create_user(username="admin_dup_phone", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Dup Phone", code="partner-dup-phone")
        Lead.objects.create(partner=partner, phone="+1111", custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "phone": "+1111",
                "full_name": "Duplicate by phone",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_admin_create_allows_same_email_if_phone_differs(self):
        admin = User.objects.create_user(username="admin_same_email", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Same Email", code="partner-same-email")
        Lead.objects.create(partner=partner, phone="+1111", email="dup@example.com", custom_fields={})
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/records/",
            {
                "partner": str(partner.id),
                "phone": "+2222",
                "email": "dup@example.com",
                "full_name": "Same email allowed",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["phone"], "+2222")

    def test_manager_update_rejects_duplicate_phone(self):
        manager = User.objects.create_user(username="manager_dup_update", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Dup Update", code="partner-dup-update")
        own = Lead.objects.create(partner=partner, manager=manager, phone="+1111", custom_fields={})
        Lead.objects.create(partner=partner, phone="+2222", custom_fields={})
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{own.id}/",
            {"phone": "+2222"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")

    def test_teamleader_cannot_assign_manager_for_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_ret_protected", password="pass12345", role=UserRole.TEAMLEADER)
        manager_target = User.objects.create_user(username="manager_target_tl_ret", password="pass12345", role=UserRole.MANAGER)
        ret_owner = User.objects.create_user(username="ret_owner_assign", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Assign RET", code="partner-tl-assign-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=ret_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id, "reason": "teamlead tries override ret"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_owner.id)

    def test_teamleader_cannot_assign_manager_for_admin_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_admin_protected", password="pass12345", role=UserRole.TEAMLEADER)
        manager_target = User.objects.create_user(username="manager_target_tl_admin", password="pass12345", role=UserRole.MANAGER)
        admin_owner = User.objects.create_user(username="admin_owner_assign", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner TL Assign ADMIN", code="partner-tl-assign-admin")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=admin_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id, "reason": "teamlead tries override admin"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, admin_owner.id)

    def test_teamleader_cannot_change_status_for_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_status_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_owner = User.objects.create_user(username="ret_status_owner", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Status RET", code="partner-tl-status-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")
        lead = Lead.objects.create(partner=partner, manager=ret_owner, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "blocked for ret lead"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_teamleader_can_force_change_status_for_manager_lead(self):
        teamleader = User.objects.create_user(username="tl_force_status_manager", password="pass12345", role=UserRole.TEAMLEADER)
        manager_owner = User.objects.create_user(username="manager_force_status_owner", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner TL Force Status", code="partner-tl-force-status")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        lead = Lead.objects.create(partner=partner, manager=manager_owner, status=status_new, phone="+19991004", custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_lost.id), "force": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_lost.id)

    def test_teamleader_cannot_force_change_status_for_ret_lead(self):
        teamleader = User.objects.create_user(username="tl_force_status_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_owner = User.objects.create_user(username="ret_force_status_owner", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Force Status RET", code="partner-tl-force-status-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        lead = Lead.objects.create(partner=partner, manager=ret_owner, status=status_new, phone="+19991005", custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_lost.id), "force": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_admin_can_force_change_status_for_ret_lead(self):
        admin = User.objects.create_user(username="admin_force_status_ret", password="pass12345", role=UserRole.ADMIN)
        ret_owner = User.objects.create_user(username="ret_force_status_owner_adm", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Admin Force Status RET", code="partner-admin-force-status-ret")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_lost = LeadStatus.objects.create(
            code="LOST",
            name="Lost",
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        lead = Lead.objects.create(partner=partner, manager=ret_owner, status=status_new, phone="+19991006", custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_lost.id), "force": True},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_lost.id)

    def test_admin_can_set_manual_deposit_type(self):
        admin = User.objects.create_user(username="admin_manual_dep_type", password="pass12345", role=UserRole.ADMIN)
        manager_owner = User.objects.create_user(username="manager_manual_dep_type", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Manual Dep Type", code="partner-manual-dep-type")
        lead = Lead.objects.create(partner=partner, manager=manager_owner, phone="+19991007", custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "99.00", "type": LeadDeposit.Type.DEPOSIT},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["type"], LeadDeposit.Type.DEPOSIT)
        lead.refresh_from_db()
        dep = LeadDeposit.objects.get(lead=lead)
        self.assertEqual(lead.last_contacted_at, dep.created_at)

    def test_admin_cannot_create_second_ftd_manually(self):
        admin = User.objects.create_user(username="admin_second_ftd_denied", password="pass12345", role=UserRole.ADMIN)
        manager_owner = User.objects.create_user(username="manager_second_ftd_denied", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Second FTD Denied", code="partner-second-ftd-denied")
        lead = Lead.objects.create(partner=partner, manager=manager_owner, phone="+19991071", custom_fields={})
        LeadDeposit.objects.create(
            lead=lead,
            creator=manager_owner,
            amount="100.00",
            type=LeadDeposit.Type.FTD,
        )
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "50.00", "type": LeadDeposit.Type.FTD},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertIn("type", response.data["error"]["details"])
        type_error = response.data["error"]["details"]["type"]
        if isinstance(type_error, list):
            type_error = type_error[0]
        self.assertEqual(type_error, "FTD already exists for this lead")
        self.assertEqual(LeadDeposit.objects.filter(lead=lead, type=LeadDeposit.Type.FTD, is_deleted=False).count(), 1)

    def test_deposits_endpoint_manager_lists_only_own_created_deposits(self):
        manager = User.objects.create_user(username="manager_dep_list_own", password="pass12345", role=UserRole.MANAGER)
        other = User.objects.create_user(username="manager_dep_list_other", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_dep_list_other", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Deposit List Own", code="partner-deposit-list-own")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991101", custom_fields={})
        own_dep = LeadDeposit.objects.create(lead=lead, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        LeadDeposit.objects.create(lead=lead, creator=other, amount="50.00", type=LeadDeposit.Type.DEPOSIT)
        LeadDeposit.objects.create(lead=lead, creator=ret_user, amount="75.00", type=LeadDeposit.Type.DEPOSIT)
        self._auth(manager)

        response = self.client.get("/api/v1/leads/deposits/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], own_dep.id)

    def test_deposits_endpoint_ret_lists_own_and_manager_deposits(self):
        manager = User.objects.create_user(username="manager_dep_list_for_ret", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_dep_list_scope", password="pass12345", role=UserRole.RET)
        admin_user = User.objects.create_user(username="admin_dep_list_hidden", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Deposit List RET", code="partner-deposit-list-ret")
        lead = Lead.objects.create(partner=partner, manager=ret_user, phone="+19991102", custom_fields={})
        manager_dep = LeadDeposit.objects.create(lead=lead, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        own_dep = LeadDeposit.objects.create(lead=lead, creator=ret_user, amount="60.00", type=LeadDeposit.Type.RELOAD)
        LeadDeposit.objects.create(lead=lead, creator=admin_user, amount="70.00", type=LeadDeposit.Type.DEPOSIT)
        self._auth(ret_user)

        response = self.client.get("/api/v1/leads/deposits/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned_ids = {item["id"] for item in response.data["results"]}
        self.assertEqual(returned_ids, {manager_dep.id, own_dep.id})

    def test_deposits_endpoint_teamleader_lists_manager_and_teamleader_deposits(self):
        teamleader = User.objects.create_user(username="tl_dep_list_scope", password="pass12345", role=UserRole.TEAMLEADER)
        other_teamleader = User.objects.create_user(username="tl_dep_list_scope_other", password="pass12345", role=UserRole.TEAMLEADER)
        manager = User.objects.create_user(username="manager_dep_list_scope", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_dep_list_scope_2", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Deposit List TL", code="partner-deposit-list-tl")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991103", custom_fields={})
        manager_dep = LeadDeposit.objects.create(lead=lead, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        own_dep = LeadDeposit.objects.create(lead=lead, creator=teamleader, amount="44.00", type=LeadDeposit.Type.DEPOSIT)
        other_tl_dep = LeadDeposit.objects.create(lead=lead, creator=other_teamleader, amount="66.00", type=LeadDeposit.Type.DEPOSIT)
        LeadDeposit.objects.create(lead=lead, creator=ret_user, amount="55.00", type=LeadDeposit.Type.DEPOSIT)
        self._auth(teamleader)

        response = self.client.get("/api/v1/leads/deposits/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned_ids = {item["id"] for item in response.data["results"]}
        self.assertEqual(returned_ids, {manager_dep.id, own_dep.id, other_tl_dep.id})

    def test_deposits_endpoint_supports_search_by_lead_name_phone_and_email(self):
        admin = User.objects.create_user(username="admin_dep_search", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_dep_search", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deposit Search", code="partner-deposit-search")
        lead_match = Lead.objects.create(
            partner=partner,
            manager=manager,
            full_name="Nina Petrova",
            phone="+19991201",
            email="nina@example.com",
            custom_fields={},
        )
        lead_other = Lead.objects.create(
            partner=partner,
            manager=manager,
            full_name="John Smith",
            phone="+19991202",
            email="john@example.com",
            custom_fields={},
        )
        dep_match = LeadDeposit.objects.create(lead=lead_match, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        LeadDeposit.objects.create(lead=lead_other, creator=manager, amount="50.00", type=LeadDeposit.Type.DEPOSIT)
        self._auth(admin)

        by_name = self.client.get("/api/v1/leads/deposits/", {"search": "Nina"})
        self.assertEqual(by_name.status_code, status.HTTP_200_OK)
        self.assertEqual(by_name.data["count"], 1)
        self.assertEqual(by_name.data["results"][0]["id"], dep_match.id)

        by_phone = self.client.get("/api/v1/leads/deposits/", {"search": "+19991201"})
        self.assertEqual(by_phone.status_code, status.HTTP_200_OK)
        self.assertEqual(by_phone.data["count"], 1)
        self.assertEqual(by_phone.data["results"][0]["id"], dep_match.id)

        by_email = self.client.get("/api/v1/leads/deposits/", {"search": "nina@example.com"})
        self.assertEqual(by_email.status_code, status.HTTP_200_OK)
        self.assertEqual(by_email.data["count"], 1)
        self.assertEqual(by_email.data["results"][0]["id"], dep_match.id)

    def test_deposit_serializer_includes_lead_full_name(self):
        admin = User.objects.create_user(username="admin_dep_lead_name", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_dep_lead_name", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deposit Lead Name", code="partner-deposit-lead-name")
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            full_name="Alice Walker",
            phone="+19991211",
            custom_fields={},
        )
        dep = LeadDeposit.objects.create(lead=lead, creator=manager, amount="120.00", type=LeadDeposit.Type.FTD)
        self._auth(admin)

        response = self.client.get(f"/api/v1/leads/deposits/{dep.id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["lead"], lead.id)
        self.assertEqual(response.data["lead_full_name"], "Alice Walker")

    def test_deposits_default_ordering_is_newest_first(self):
        admin = User.objects.create_user(username="admin_dep_ordering", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_dep_ordering", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deposit Ordering", code="partner-deposit-ordering")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991212", custom_fields={})
        dep_old = LeadDeposit.objects.create(lead=lead, creator=manager, amount="90.00", type=LeadDeposit.Type.FTD)
        dep_new = LeadDeposit.objects.create(lead=lead, creator=manager, amount="120.00", type=LeadDeposit.Type.DEPOSIT)
        self._set_deposit_created_at(dep_old, timezone.make_aware(datetime(2026, 2, 1, 10, 0, 0)))
        self._set_deposit_created_at(dep_new, timezone.make_aware(datetime(2026, 2, 2, 10, 0, 0)))
        self._auth(admin)

        list_response = self.client.get("/api/v1/leads/deposits/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(list_response.data["results"][0]["id"], dep_new.id)
        self.assertEqual(list_response.data["results"][1]["id"], dep_old.id)

        nested_response = self.client.get(f"/api/v1/leads/records/{lead.id}/deposits/")
        self.assertEqual(nested_response.status_code, status.HTTP_200_OK)
        self.assertEqual(nested_response.data[0]["id"], dep_new.id)
        self.assertEqual(nested_response.data[1]["id"], dep_old.id)

    def test_deposit_stats_monthly_aggregates_ftd_and_non_ftd_amounts(self):
        admin = User.objects.create_user(username="admin_dep_stats_monthly", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_dep_stats_monthly", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_dep_stats_monthly", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Deposit Stats Monthly", code="partner-deposit-stats-monthly")
        lead_jan = Lead.objects.create(partner=partner, manager=manager, phone="+19991213", custom_fields={})
        lead_feb = Lead.objects.create(partner=partner, manager=ret_user, phone="+19991216", custom_fields={})

        jan_ftd = LeadDeposit.objects.create(lead=lead_jan, creator=manager, amount="100.00", type=LeadDeposit.Type.FTD)
        jan_reload = LeadDeposit.objects.create(lead=lead_jan, creator=ret_user, amount="40.00", type=LeadDeposit.Type.RELOAD)
        feb_ftd = LeadDeposit.objects.create(lead=lead_feb, creator=ret_user, amount="200.00", type=LeadDeposit.Type.FTD)
        feb_deposit = LeadDeposit.objects.create(lead=lead_feb, creator=ret_user, amount="25.00", type=LeadDeposit.Type.DEPOSIT)
        soft_deleted = LeadDeposit.objects.create(lead=lead_feb, creator=ret_user, amount="999.00", type=LeadDeposit.Type.DEPOSIT)
        soft_deleted.delete()

        self._set_deposit_created_at(jan_ftd, timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)))
        self._set_deposit_created_at(jan_reload, timezone.make_aware(datetime(2026, 1, 8, 10, 0, 0)))
        self._set_deposit_created_at(feb_ftd, timezone.make_aware(datetime(2026, 2, 3, 10, 0, 0)))
        self._set_deposit_created_at(feb_deposit, timezone.make_aware(datetime(2026, 2, 10, 10, 0, 0)))
        LeadDeposit.all_objects.filter(id=soft_deleted.id).update(
            created_at=timezone.make_aware(datetime(2026, 2, 12, 10, 0, 0))
        )

        self._auth(admin)
        response = self.client.get(
            "/api/v1/leads/deposits/stats/monthly/?date_from=2026-01-01&date_to=2026-02-28"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["summary"]["ftd_count"], 2)
        self.assertEqual(response.data["summary"]["non_ftd_total_amount"], "65.00")
        self.assertEqual(
            response.data["items"],
            [
                {
                    "year": 2026,
                    "month": 2,
                    "month_key": "2026-02",
                    "month_label": "2026-02",
                    "ftd_count": 1,
                    "non_ftd_total_amount": "25.00",
                },
                {
                    "year": 2026,
                    "month": 1,
                    "month_key": "2026-01",
                    "month_label": "2026-01",
                    "ftd_count": 1,
                    "non_ftd_total_amount": "40.00",
                },
            ],
        )

    def test_deposit_stats_ftd_matrix_respects_creator_scope_for_manager(self):
        manager = User.objects.create_user(username="manager_dep_stats_matrix", password="pass12345", role=UserRole.MANAGER)
        other_manager = User.objects.create_user(
            username="manager_dep_stats_matrix_other",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Deposit Stats Matrix", code="partner-deposit-stats-matrix")
        own_lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991214", custom_fields={})
        other_lead = Lead.objects.create(partner=partner, manager=other_manager, phone="+19991215", custom_fields={})
        own_ftd = LeadDeposit.objects.create(lead=own_lead, creator=manager, amount="80.00", type=LeadDeposit.Type.FTD)
        other_ftd = LeadDeposit.objects.create(
            lead=other_lead,
            creator=other_manager,
            amount="90.00",
            type=LeadDeposit.Type.FTD,
        )
        self._set_deposit_created_at(own_ftd, timezone.make_aware(datetime(2026, 1, 15, 10, 0, 0)))
        self._set_deposit_created_at(other_ftd, timezone.make_aware(datetime(2026, 2, 15, 10, 0, 0)))

        self._auth(manager)
        response = self.client.get(
            "/api/v1/leads/deposits/stats/ftd-matrix/?date_from=2026-01-01&date_to=2026-02-28"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data["columns"],
            [
                {"year": 2026, "month": 2, "month_key": "2026-02", "month_label": "2026-02"},
                {"year": 2026, "month": 1, "month_key": "2026-01", "month_label": "2026-01"},
            ],
        )
        self.assertEqual(len(response.data["rows"]), 1)
        row = response.data["rows"][0]
        self.assertEqual(row["user"]["id"], str(manager.id))
        self.assertEqual(row["total_ftd"], 1)
        self.assertEqual(row["cells"], {"2026-01": 1, "2026-02": 0})

    def test_deposit_stats_default_to_current_year_when_dates_missing(self):
        admin = User.objects.create_user(username="admin_dep_stats_default_dates", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(
            username="manager_dep_stats_default_dates",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Deposit Stats Default Dates", code="partner-dep-stats-default-dates")
        current_year = timezone.localdate().year
        current_lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991217", custom_fields={})
        old_lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991218", custom_fields={})
        current_ftd = LeadDeposit.objects.create(
            lead=current_lead,
            creator=manager,
            amount="100.00",
            type=LeadDeposit.Type.FTD,
        )
        old_ftd = LeadDeposit.objects.create(
            lead=old_lead,
            creator=manager,
            amount="90.00",
            type=LeadDeposit.Type.FTD,
        )
        self._set_deposit_created_at(
            current_ftd,
            timezone.make_aware(datetime(current_year, 1, 15, 10, 0, 0)),
        )
        self._set_deposit_created_at(
            old_ftd,
            timezone.make_aware(datetime(current_year - 1, 12, 15, 10, 0, 0)),
        )
        self._auth(admin)

        response = self.client.get("/api/v1/leads/deposits/stats/monthly/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["period"]["date_from"], f"{current_year}-01-01")
        self.assertEqual(response.data["period"]["date_to"], timezone.localdate().isoformat())
        self.assertEqual(response.data["summary"]["ftd_count"], 1)
        self.assertEqual(len(response.data["items"]), timezone.localdate().month)
        self.assertEqual(response.data["items"][0]["month_key"], timezone.localdate().replace(day=1).strftime("%Y-%m"))
        january_item = next(item for item in response.data["items"] if item["month_key"] == f"{current_year}-01")
        self.assertEqual(january_item["ftd_count"], 1)

    def test_deposits_endpoint_admin_can_create_update_soft_delete_restore_and_audit(self):
        admin = User.objects.create_user(username="admin_dep_crud", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_dep_crud", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deposit CRUD", code="partner-deposit-crud")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991104", custom_fields={})
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/deposits/",
            {
                "lead": lead.id,
                "amount": "101.00",
                "type": LeadDeposit.Type.FTD,
                "reason": "manual create",
            },
            format="json",
        )
        self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
        dep_id = create_resp.data["id"]
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type=LeadAuditEntity.LEAD_DEPOSIT,
                entity_id=str(dep_id),
                event_type=LeadAuditEvent.DEPOSIT_CREATED,
            ).exists()
        )

        update_resp = self.client.patch(
            f"/api/v1/leads/deposits/{dep_id}/",
            {
                "amount": "111.00",
                "reason": "adjust amount",
            },
            format="json",
        )
        self.assertEqual(update_resp.status_code, status.HTTP_200_OK)
        self.assertEqual(update_resp.data["amount"], "111.00")
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type=LeadAuditEntity.LEAD_DEPOSIT,
                entity_id=str(dep_id),
                event_type=LeadAuditEvent.DEPOSIT_UPDATED,
            ).exists()
        )

        soft_delete_resp = self.client.post(
            f"/api/v1/leads/deposits/{dep_id}/soft_delete/",
            {},
            format="json",
        )
        self.assertEqual(soft_delete_resp.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(LeadDeposit.objects.filter(id=dep_id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type=LeadAuditEntity.LEAD_DEPOSIT,
                entity_id=str(dep_id),
                event_type=LeadAuditEvent.DEPOSIT_SOFT_DELETED,
            ).exists()
        )

        restore_resp = self.client.post(
            f"/api/v1/leads/deposits/{dep_id}/restore/",
            {},
            format="json",
        )
        self.assertEqual(restore_resp.status_code, status.HTTP_200_OK)
        self.assertTrue(LeadDeposit.objects.filter(id=dep_id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type=LeadAuditEntity.LEAD_DEPOSIT,
                entity_id=str(dep_id),
                event_type=LeadAuditEvent.DEPOSIT_RESTORED,
            ).exists()
        )

    def test_deposits_endpoint_superuser_can_hard_delete_and_audit(self):
        superuser = User.objects.create_user(
            username="su_dep_delete",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        manager = User.objects.create_user(username="manager_dep_delete", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deposit Delete", code="partner-deposit-delete")
        lead = Lead.objects.create(partner=partner, manager=manager, phone="+19991105", custom_fields={})
        dep = LeadDeposit.objects.create(lead=lead, creator=manager, amount="88.00", type=LeadDeposit.Type.FTD)
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/leads/deposits/{dep.id}/")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(LeadDeposit.all_objects.filter(id=dep.id).exists())
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=lead,
                entity_type=LeadAuditEntity.LEAD_DEPOSIT,
                entity_id=str(dep.id),
                event_type=LeadAuditEvent.DEPOSIT_HARD_DELETED,
            ).exists()
        )

    def test_manager_cannot_set_manual_deposit_type(self):
        manager_owner = User.objects.create_user(username="manager_manual_dep_denied", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Manual Dep Denied", code="partner-manual-dep-denied")
        lead = Lead.objects.create(partner=partner, manager=manager_owner, phone="+19991008", custom_fields={})
        self._auth(manager_owner)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "99.00", "type": LeadDeposit.Type.DEPOSIT},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_ret_can_create_reload_when_ftd_absent(self):
        ret_owner = User.objects.create_user(username="ret_dep_no_ftd", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner RET Dep No FTD", code="partner-ret-dep-no-ftd")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=ret_owner,
            status=status_new,
            phone="+19991009",
            custom_fields={},
        )
        self._auth(ret_owner)

        first_dep = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "200.00"},
            format="json",
        )
        self.assertEqual(first_dep.status_code, status.HTTP_201_CREATED)
        self.assertEqual(first_dep.data["type"], LeadDeposit.Type.RELOAD)

        second_dep = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "75.00"},
            format="json",
        )
        self.assertEqual(second_dep.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second_dep.data["type"], LeadDeposit.Type.DEPOSIT)
        self.assertFalse(LeadDeposit.objects.filter(lead=lead, type=LeadDeposit.Type.FTD).exists())

    def test_partner_duplicate_attempt_is_saved_without_creating_new_lead(self):
        partner = Partner.objects.create(name="Partner Dup Attempt", code="partner-dup-attempt")
        source = PartnerSource.objects.create(partner=partner, name="Google", code="google", is_active=True)
        raw_token = "tok_live_partner_dup_attempt_1234567890"
        token = PartnerToken.build(partner=partner, raw_token=raw_token, name="dup-attempt", source=source)
        token.save()
        existing = Lead.objects.create(partner=partner, source=source, phone="+123456", custom_fields={})

        response = self.client.post(
            "/api/v1/partner/leads/",
            {
                "phone": "+123456",
                "email": "duplicate@example.com",
                "full_name": "Duplicate try",
                "custom_fields": {"x": 1},
            },
            format="json",
            HTTP_X_PARTNER_TOKEN=raw_token,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["created"])
        self.assertTrue(response.data["duplicate_rejected"])
        self.assertEqual(Lead.objects.filter(partner=partner).count(), 1)
        attempt = LeadDuplicateAttempt.objects.get(partner=partner)
        self.assertEqual(attempt.existing_lead_id, existing.id)
        self.assertEqual(attempt.phone, "+123456")
        self.assertEqual(attempt.email, "duplicate@example.com")
        self.assertTrue(
            LeadAuditLog.objects.filter(
                lead=existing,
                entity_type="duplicate_attempt",
                entity_id=str(attempt.id),
                event_type=LeadAuditEvent.DUPLICATE_REJECTED,
            ).exists()
        )

    def test_leads_list_supports_manager_and_status_in_filters(self):
        admin = User.objects.create_user(username="admin_leads_filter_in", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_filter_in_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_filter_in_b", password="pass12345", role=UserRole.MANAGER)
        manager_c = User.objects.create_user(username="manager_filter_in_c", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Filter In", code="partner-filter-in")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(code="WORK", name="Work")

        lead_a = Lead.objects.create(partner=partner, manager=manager_a, status=status_new, custom_fields={})
        lead_b = Lead.objects.create(partner=partner, manager=manager_b, status=status_new, custom_fields={})
        Lead.objects.create(partner=partner, manager=manager_c, status=status_new, custom_fields={})
        Lead.objects.create(partner=partner, manager=manager_a, status=status_work, custom_fields={})
        self._auth(admin)

        response = self.client.get(
            "/api/v1/leads/records/",
            {
                "manager__in": f"{manager_a.id},{manager_b.id}",
                "status__in": str(status_new.id),
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        self.assertCountEqual([item["id"] for item in items], [lead_a.id, lead_b.id])

    def test_leads_list_supports_manager_role_filter(self):
        admin = User.objects.create_user(username="admin_leads_role_filter", password="pass12345", role=UserRole.ADMIN)
        manager_user = User.objects.create_user(username="manager_role_filter", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_role_filter", password="pass12345", role=UserRole.RET)
        teamleader_user = User.objects.create_user(username="tl_role_filter", password="pass12345", role=UserRole.TEAMLEADER)
        partner = Partner.objects.create(name="Partner Role Filter", code="partner-role-filter")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)

        lead_manager = Lead.objects.create(
            partner=partner, manager=manager_user, status=status_new, custom_fields={}
        )
        Lead.objects.create(partner=partner, manager=ret_user, status=status_new, custom_fields={})
        Lead.objects.create(
            partner=partner, manager=teamleader_user, status=status_new, custom_fields={}
        )
        Lead.objects.create(partner=partner, manager=None, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/", {"manager_role": UserRole.MANAGER})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], lead_manager.id)

    def test_leads_list_supports_extended_filters(self):
        admin = User.objects.create_user(username="admin_leads_ext_filters", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_ext_filter_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_ext_filter_b", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Extended Filters", code="partner-extended-filters")
        source = PartnerSource.objects.create(partner=partner, name="Google", code="google-ext", is_active=True)
        status_new = LeadStatus.objects.create(code="NEW_EXT_FILTER", name="New Ext", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON_EXT_FILTER",
            name="Won Ext",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )

        lead_match = Lead.objects.create(
            partner=partner,
            source=source,
            manager=manager_a,
            first_manager=manager_a,
            status=status_won,
            geo="RU",
            full_name="Ivan Petrov",
            phone="+700000001",
            email="ivan.petrov@example.com",
            assigned_at=timezone.make_aware(datetime(2026, 1, 10, 10, 0, 0)),
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 10, 10, 5, 0)),
            next_contact_at=timezone.make_aware(datetime(2026, 1, 11, 12, 0, 0)),
            received_at=timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)),
            custom_fields={},
        )
        Lead.objects.create(
            partner=partner,
            source=source,
            manager=manager_b,
            first_manager=manager_b,
            status=status_new,
            geo="CH",
            full_name="Maria Chen",
            phone="+700000002",
            email="maria.chen@example.com",
            assigned_at=timezone.make_aware(datetime(2026, 1, 20, 10, 0, 0)),
            first_assigned_at=timezone.make_aware(datetime(2026, 1, 20, 10, 5, 0)),
            next_contact_at=timezone.make_aware(datetime(2026, 1, 22, 12, 0, 0)),
            received_at=timezone.make_aware(datetime(2026, 1, 20, 9, 0, 0)),
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.get(
            "/api/v1/leads/records/",
            {
                "partner__in": str(partner.id),
                "source__in": str(source.id),
                "manager__in": str(manager_a.id),
                "first_manager__in": str(manager_a.id),
                "status_code": "won_ext_filter",
                "geo": "RU",
                "full_name": "ivan",
                "phone__icontains": "+7000000",
                "email__icontains": "petrov@",
                "received_from": "2026-01-01T00:00:00Z",
                "received_to": "2026-01-15T23:59:59Z",
                "is_unassigned": "false",
                "has_next_contact": "true",
                "has_email": "true",
                "has_phone": "true",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], lead_match.id)

    def test_leads_list_supports_unassigned_and_empty_email_filters(self):
        admin = User.objects.create_user(username="admin_leads_bool_filters", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="manager_bool_filter", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Bool Filters", code="partner-bool-filters")
        status_new = LeadStatus.objects.create(code="NEW_BOOL_FILTER", name="New Bool", is_default_for_new_leads=True)
        unassigned_lead = Lead.objects.create(
            partner=partner,
            manager=None,
            status=status_new,
            phone="+700000011",
            email="",
            custom_fields={},
        )
        Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+700000012",
            email="has@email.test",
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.get(
            "/api/v1/leads/records/",
            {
                "is_unassigned": "true",
                "has_email": "false",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], unassigned_lead.id)

    def test_leads_list_returns_informative_manager_outcome_by(self):
        admin = User.objects.create_user(username="admin_outcome_by_list", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(
            username="manager_outcome_author",
            password="pass12345",
            role=UserRole.MANAGER,
            first_name="Nina",
            last_name="Lopez",
        )
        partner = Partner.objects.create(name="Partner Outcome Author", code="partner-outcome-author")
        status_won = LeadStatus.objects.create(
            code="WON_OUTCOME_AUTHOR",
            name="Won Outcome Author",
            is_default_for_new_leads=True,
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        Lead.objects.create(
            partner=partner,
            manager=manager,
            first_manager=manager,
            status=status_won,
            phone="+700000021",
            custom_fields={},
        )
        self._auth(admin)

        response = self.client.get("/api/v1/leads/records/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        manager_payload = items[0]["manager"]
        self.assertEqual(manager_payload["id"], str(manager.id))
        self.assertEqual(manager_payload["username"], manager.username)
        self.assertEqual(manager_payload["first_name"], "Nina")
        self.assertEqual(manager_payload["last_name"], "Lopez")
        self.assertEqual(manager_payload["role"], UserRole.MANAGER)
        first_manager_payload = items[0]["first_manager"]
        self.assertEqual(first_manager_payload["id"], str(manager.id))
        self.assertEqual(first_manager_payload["first_name"], "Nina")
        self.assertEqual(first_manager_payload["last_name"], "Lopez")

    def test_teamleader_list_hides_ret_admin_super_leads(self):
        teamleader = User.objects.create_user(username="tl_scope_list", password="pass12345", role=UserRole.TEAMLEADER)
        other_teamleader = User.objects.create_user(username="tl_scope_list_other", password="pass12345", role=UserRole.TEAMLEADER)
        manager_user = User.objects.create_user(username="manager_scope_list", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_scope_list", password="pass12345", role=UserRole.RET)
        admin_user = User.objects.create_user(username="admin_scope_list", password="pass12345", role=UserRole.ADMIN)
        super_user = User.objects.create_user(
            username="su_scope_list",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner TL Scope List", code="partner-tl-scope-list")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)

        own_lead = Lead.objects.create(partner=partner, manager=teamleader, status=status_new, phone="+15550001", custom_fields={})
        teamleader_lead = Lead.objects.create(partner=partner, manager=other_teamleader, status=status_new, phone="+15550007", custom_fields={})
        manager_lead = Lead.objects.create(partner=partner, manager=manager_user, status=status_new, phone="+15550002", custom_fields={})
        unassigned_lead = Lead.objects.create(partner=partner, manager=None, status=status_new, phone="+15550003", custom_fields={})
        ret_lead = Lead.objects.create(partner=partner, manager=ret_user, status=status_new, phone="+15550004", custom_fields={})
        admin_lead = Lead.objects.create(partner=partner, manager=admin_user, status=status_new, phone="+15550005", custom_fields={})
        super_lead = Lead.objects.create(partner=partner, manager=super_user, status=status_new, phone="+15550006", custom_fields={})
        self._auth(teamleader)

        response = self.client.get("/api/v1/leads/records/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        items = response.data["results"] if isinstance(response.data, dict) and "results" in response.data else response.data
        returned_ids = {item["id"] for item in items}
        self.assertIn(own_lead.id, returned_ids)
        self.assertIn(teamleader_lead.id, returned_ids)
        self.assertIn(manager_lead.id, returned_ids)
        self.assertIn(unassigned_lead.id, returned_ids)
        self.assertNotIn(ret_lead.id, returned_ids)
        self.assertNotIn(admin_lead.id, returned_ids)
        self.assertNotIn(super_lead.id, returned_ids)

    def test_teamleader_cannot_retrieve_ret_admin_super_leads(self):
        teamleader = User.objects.create_user(username="tl_scope_retrieve", password="pass12345", role=UserRole.TEAMLEADER)
        ret_user = User.objects.create_user(username="ret_scope_retrieve", password="pass12345", role=UserRole.RET)
        admin_user = User.objects.create_user(username="admin_scope_retrieve", password="pass12345", role=UserRole.ADMIN)
        super_user = User.objects.create_user(
            username="su_scope_retrieve",
            password="pass12345",
            role=UserRole.SUPERUSER,
            is_staff=True,
            is_superuser=True,
        )
        partner = Partner.objects.create(name="Partner TL Scope Retrieve", code="partner-tl-scope-retrieve")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        ret_lead = Lead.objects.create(partner=partner, manager=ret_user, status=status_new, phone="+15551001", custom_fields={})
        admin_lead = Lead.objects.create(partner=partner, manager=admin_user, status=status_new, phone="+15551002", custom_fields={})
        super_lead = Lead.objects.create(partner=partner, manager=super_user, status=status_new, phone="+15551003", custom_fields={})
        self._auth(teamleader)

        self.assertEqual(self.client.get(f"/api/v1/leads/records/{ret_lead.id}/").status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.client.get(f"/api/v1/leads/records/{admin_lead.id}/").status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.client.get(f"/api/v1/leads/records/{super_lead.id}/").status_code, status.HTTP_404_NOT_FOUND)

    def test_teamleader_can_create_only_ftd_deposit(self):
        teamleader = User.objects.create_user(username="tl_deposit_ftd_only", password="pass12345", role=UserRole.TEAMLEADER)
        manager_user = User.objects.create_user(username="manager_deposit_ftd_only", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner TL Deposit FTD", code="partner-tl-deposit-ftd")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_user, status=status_new, phone="+15552001", custom_fields={})
        self._auth(teamleader)

        first_dep = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "100.00"},
            format="json",
        )
        self.assertEqual(first_dep.status_code, status.HTTP_201_CREATED)
        self.assertEqual(first_dep.data["type"], LeadDeposit.Type.FTD)

        second_dep = self.client.post(
            f"/api/v1/leads/records/{lead.id}/deposits/",
            {"amount": "50.00"},
            format="json",
        )
        self.assertEqual(second_dep.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(second_dep.data["error"]["code"], "validation_error")

    def test_teamleader_transfer_requires_transfer_author_and_can_attribute_to_manager(self):
        teamleader = User.objects.create_user(username="tl_transfer_author", password="pass12345", role=UserRole.TEAMLEADER)
        manager_user = User.objects.create_user(username="manager_transfer_author", password="pass12345", role=UserRole.MANAGER)
        ret_user = User.objects.create_user(username="ret_transfer_author", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner TL Transfer Author", code="partner-tl-transfer-author")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lead = Lead.objects.create(partner=partner, manager=manager_user, status=status_new, phone="+15553001", custom_fields={})
        self._auth(teamleader)

        missing_author = self.client.post(
            f"/api/v1/leads/records/{lead.id}/close-won-transfer/",
            {"ret_manager": ret_user.id, "to_status": status_won.id, "amount": "120.00", "reason": "won by manager"},
            format="json",
        )
        self.assertEqual(missing_author.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(missing_author.data["error"]["code"], "validation_error")

        ok_response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/close-won-transfer/",
            {
                "ret_manager": ret_user.id,
                "to_status": status_won.id,
                "amount": "120.00",
                "reason": "won by manager",
                "transfer_author": manager_user.id,
            },
            format="json",
        )
        self.assertEqual(ok_response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_user.id)
        ftd = LeadDeposit.objects.get(lead=lead, type=LeadDeposit.Type.FTD)
        self.assertEqual(ftd.creator_id, manager_user.id)

    def test_manager_cannot_override_transfer_author(self):
        manager = User.objects.create_user(username="manager_transfer_override", password="pass12345", role=UserRole.MANAGER)
        manager_other = User.objects.create_user(
            username="manager_transfer_override_other",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        ret_user = User.objects.create_user(username="ret_transfer_override", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Manager Transfer Override", code="partner-manager-transfer-override")
        status_new = LeadStatus.objects.create(code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            code="WON",
            name="Won",
            is_valid=True,
            conversion_bucket=LeadStatus.ConversionBucket.WON,
        )
        lead = Lead.objects.create(partner=partner, manager=manager, status=status_new, phone="+15554001", custom_fields={})
        self._auth(manager)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/close-won-transfer/",
            {
                "ret_manager": ret_user.id,
                "to_status": status_won.id,
                "amount": "130.00",
                "reason": "won",
                "transfer_author": manager_other.id,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
