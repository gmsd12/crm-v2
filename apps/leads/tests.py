from uuid import uuid4

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
    LeadComment,
    LeadDuplicateAttempt,
    LeadStatus,
    LeadStatusAuditEvent,
    LeadStatusAuditLog,
    LeadStatusIdempotencyEndpoint,
    LeadStatusIdempotencyKey,
    LeadStatusTransition,
    Pipeline,
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
        LeadStatusAuditLog.objects.filter(id=log_obj.id).update(created_at=dt_obj)

    def test_teamleader_can_list_statuses(self):
        teamleader = User.objects.create_user(username="tl_status_list", password="pass12345", role=UserRole.TEAMLEADER)
        pipeline = Pipeline.objects.create(code="default", name="Default", is_default=True)
        LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        self._auth(teamleader)

        response = self.client.get("/api/v1/leads/statuses/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(response.data), 1)

    def test_manager_and_ret_cannot_list_statuses(self):
        manager = User.objects.create_user(username="manager_status_list", password="pass12345", role=UserRole.MANAGER)
        self._auth(manager)
        manager_resp = self.client.get("/api/v1/leads/statuses/")
        self.assertEqual(manager_resp.status_code, status.HTTP_403_FORBIDDEN)

        ret = User.objects.create_user(username="ret_status_list", password="pass12345", role=UserRole.RET)
        self._auth(ret)
        ret_resp = self.client.get("/api/v1/leads/statuses/")
        self.assertEqual(ret_resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_admin_can_create_and_soft_delete_status(self):
        admin = User.objects.create_user(username="admin_status_write", password="pass12345", role=UserRole.ADMIN)
        pipeline = Pipeline.objects.create(code="p_soft", name="Pipeline Soft", is_default=True)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/statuses/",
            {
                "pipeline": str(pipeline.id),
                "code": "CALLBACK",
                "name": "Callback",
                "order": 20,
                "is_default_for_new_leads": False,
                "is_active": True,
                "is_terminal": False,
                "counts_for_conversion": True,
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
        pipeline = Pipeline.objects.create(code="p_used_deactivate", name="Pipeline Used Deactivate", is_default=True)
        status_obj = LeadStatus.objects.create(
            pipeline=pipeline,
            code="IN_USE",
            name="In Use",
            is_default_for_new_leads=True,
            is_active=True,
        )
        Lead.objects.create(partner=partner, pipeline=pipeline, status=status_obj, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="p_used_soft", name="Pipeline Used Soft", is_default=True)
        status_obj = LeadStatus.objects.create(
            pipeline=pipeline,
            code="SOFT_USED",
            name="Soft Used",
            is_default_for_new_leads=True,
            is_active=True,
        )
        Lead.objects.create(partner=partner, pipeline=pipeline, status=status_obj, custom_fields={})
        self._auth(admin)

        response = self.client.post(f"/api/v1/leads/statuses/{status_obj.id}/soft_delete/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertTrue(LeadStatus.objects.filter(id=status_obj.id).exists())

    def test_admin_cannot_hard_delete_status(self):
        admin = User.objects.create_user(username="admin_status_delete", password="pass12345", role=UserRole.ADMIN)
        pipeline = Pipeline.objects.create(code="p_admin_del", name="Pipeline Admin Delete", is_default=True)
        status_obj = LeadStatus.objects.create(
            pipeline=pipeline,
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
        pipeline = Pipeline.objects.create(code="p_su_del", name="Pipeline Super Delete", is_default=True)
        status_obj = LeadStatus.objects.create(
            pipeline=pipeline,
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
        pipeline = Pipeline.objects.create(code="p_su_del_used", name="Pipeline Super Delete Used", is_default=True)
        status_obj = LeadStatus.objects.create(
            pipeline=pipeline,
            code="HARD_USED",
            name="Hard Used",
            is_default_for_new_leads=True,
        )
        Lead.objects.create(partner=partner, pipeline=pipeline, status=status_obj, custom_fields={})
        self._auth(superuser)

        response = self.client.delete(f"/api/v1/leads/statuses/{status_obj.id}/")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        self.assertTrue(LeadStatus.all_objects.filter(id=status_obj.id).exists())

    def test_transition_rejects_cross_pipeline_statuses(self):
        admin = User.objects.create_user(username="admin_transition", password="pass12345", role=UserRole.ADMIN)
        p1 = Pipeline.objects.create(code="p1", name="Pipeline 1", is_default=True)
        p2 = Pipeline.objects.create(code="p2", name="Pipeline 2")
        s1 = LeadStatus.objects.create(pipeline=p1, code="NEW", name="New", is_default_for_new_leads=True)
        s2 = LeadStatus.objects.create(pipeline=p2, code="CALL", name="Call")
        self._auth(admin)

        response = self.client.post(
            "/api/v1/leads/status-transitions/",
            {
                "pipeline": str(p1.id),
                "from_status": str(s1.id),
                "to_status": str(s2.id),
                "is_active": True,
                "requires_comment": False,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

    def test_audit_log_records_status_create_update_and_soft_delete(self):
        admin = User.objects.create_user(username="admin_audit", password="pass12345", role=UserRole.ADMIN)
        pipeline = Pipeline.objects.create(code="p_audit", name="Pipeline Audit", is_default=True)
        self._auth(admin)

        create_resp = self.client.post(
            "/api/v1/leads/statuses/",
            {
                "pipeline": str(pipeline.id),
                "code": "NEW",
                "name": "New",
                "order": 10,
                "is_default_for_new_leads": True,
                "is_active": True,
                "is_terminal": False,
                "counts_for_conversion": False,
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
            LeadStatusAuditLog.objects.filter(to_status_id=status_id).values_list("event_type", flat=True)
        ) + list(
            LeadStatusAuditLog.objects.filter(from_status_id=status_id).values_list("event_type", flat=True)
        )

        self.assertIn(LeadStatusAuditEvent.STATUS_CREATED, events)
        self.assertIn(LeadStatusAuditEvent.STATUS_UPDATED, events)
        self.assertIn(LeadStatusAuditEvent.STATUS_DELETED_SOFT, events)

    def test_admin_can_change_lead_status_with_valid_transition_and_audit(self):
        admin = User.objects.create_user(username="admin_change_status", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change", code="partner-change")
        pipeline = Pipeline.objects.create(code="wf_change", name="Workflow Change", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={"x": 1})
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

        audit = LeadStatusAuditLog.objects.filter(
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            lead=lead,
            from_status=status_new,
            to_status=status_work,
        ).first()
        self.assertIsNotNone(audit)
        self.assertEqual(audit.reason, "accepted to work")

    def test_change_lead_status_rejects_invalid_transition(self):
        admin = User.objects.create_user(username="admin_invalid_transition", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Invalid", code="partner-invalid-transition")
        pipeline = Pipeline.objects.create(code="wf_invalid", name="Workflow Invalid", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(pipeline=pipeline, code="WON", name="Won", is_terminal=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_won.id), "reason": "force jump"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_change_lead_status_requires_comment_for_transition(self):
        admin = User.objects.create_user(username="admin_requires_comment", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Comment", code="partner-comment")
        pipeline = Pipeline.objects.create(code="wf_comment", name="Workflow Comment", is_default=True)
        status_lost = LeadStatus.objects.create(pipeline=pipeline, code="LOST", name="Lost", is_default_for_new_leads=True)
        status_reopened = LeadStatus.objects.create(pipeline=pipeline, code="REOPEN", name="Reopened")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_lost,
            to_status=status_reopened,
            is_active=True,
            requires_comment=True,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_lost, custom_fields={})
        self._auth(admin)

        no_reason = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_reopened.id)},
            format="json",
        )
        self.assertEqual(no_reason.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(no_reason.data["error"]["code"], "validation_error")

        with_reason = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_reopened.id), "reason": "lead returned with new budget"},
            format="json",
        )
        self.assertEqual(with_reason.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_reopened.id)

    def test_manager_cannot_change_lead_status(self):
        manager = User.objects.create_user(username="manager_change_status", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Deny", code="partner-deny")
        pipeline = Pipeline.objects.create(code="wf_deny", name="Workflow Deny", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(manager)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/change-status/",
            {"to_status": str(status_work.id), "reason": "try change"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_teamleader_can_assign_manager_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_single", password="pass12345", role=UserRole.TEAMLEADER)
        manager_target = User.objects.create_user(
            username="manager_target_single",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Single", code="partner-assign-single")
        pipeline = Pipeline.objects.create(code="wf_assign_single", name="Workflow Assign Single", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_target.id, "reason": "initial distribution"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_target.id)
        self.assertEqual(response.data["manager"]["id"], str(manager_target.id))
        audit = LeadStatusAuditLog.objects.get(lead=lead, event_type=LeadStatusAuditEvent.MANAGER_ASSIGNED)
        self.assertEqual(audit.actor_user_id, teamleader.id)
        self.assertEqual(audit.reason, "initial distribution")
        self.assertEqual(audit.payload_before["manager"], None)
        self.assertEqual(audit.payload_after["manager"]["id"], str(manager_target.id))

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
        pipeline = Pipeline.objects.create(code="wf_assign_single_deny", name="Workflow Assign Single Deny", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_reassign_single", name="Workflow Reassign Single", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_old, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(admin)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": manager_new.id, "reason": "rebalance"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, manager_new.id)
        audit = LeadStatusAuditLog.objects.get(lead=lead, event_type=LeadStatusAuditEvent.MANAGER_REASSIGNED)
        self.assertEqual(audit.reason, "rebalance")
        self.assertEqual(audit.payload_before["manager"]["id"], str(manager_old.id))
        self.assertEqual(audit.payload_after["manager"]["id"], str(manager_new.id))

    def test_teamleader_can_assign_ret_to_single_lead(self):
        teamleader = User.objects.create_user(username="tl_assign_ret", password="pass12345", role=UserRole.TEAMLEADER)
        ret_target = User.objects.create_user(
            username="ret_target_single",
            password="pass12345",
            role=UserRole.RET,
        )
        partner = Partner.objects.create(name="Partner Assign RET", code="partner-assign-ret")
        pipeline = Pipeline.objects.create(code="wf_assign_ret", name="Workflow Assign RET", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(teamleader)

        response = self.client.post(
            f"/api/v1/leads/records/{lead.id}/assign-manager/",
            {"manager": ret_target.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.manager_id, ret_target.id)
        self.assertEqual(response.data["manager"]["id"], str(ret_target.id))

    def test_admin_can_bulk_assign_manager(self):
        admin = User.objects.create_user(username="admin_assign_bulk", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_bulk",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Assign Bulk", code="partner-assign-bulk")
        pipeline = Pipeline.objects.create(code="wf_assign_bulk", name="Workflow Assign Bulk", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.MANAGER_ASSIGNED,
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
        pipeline = Pipeline.objects.create(code="wf_assign_bulk_partial", name="Workflow Assign Bulk Partial", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        unknown_id = uuid4()
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.MANAGER_ASSIGNED,
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
        pipeline = Pipeline.objects.create(code="wf_assign_bulk_deny", name="Workflow Assign Bulk Deny", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_unassign_single", name="Workflow Unassign Single", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
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
        audit = LeadStatusAuditLog.objects.get(lead=lead, event_type=LeadStatusAuditEvent.MANAGER_UNASSIGNED)
        self.assertEqual(audit.actor_user_id, admin.id)
        self.assertEqual(audit.reason, "manager on vacation")
        self.assertEqual(audit.payload_before["manager"]["id"], str(manager_target.id))
        self.assertEqual(audit.payload_after["manager"], None)

    def test_admin_can_bulk_unassign_manager(self):
        admin = User.objects.create_user(username="admin_unassign_bulk", password="pass12345", role=UserRole.ADMIN)
        manager_target = User.objects.create_user(
            username="manager_target_unassign_bulk",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        partner = Partner.objects.create(name="Partner Unassign Bulk", code="partner-unassign-bulk")
        pipeline = Pipeline.objects.create(code="wf_unassign_bulk", name="Workflow Unassign Bulk", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.MANAGER_UNASSIGNED,
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
        pipeline = Pipeline.objects.create(code="wf_unassign_bulk_partial", name="Workflow Unassign Bulk Partial", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
        unknown_id = uuid4()
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.MANAGER_UNASSIGNED,
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
        pipeline = Pipeline.objects.create(code="wf_unassign_bulk_deny", name="Workflow Unassign Bulk Deny", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_assign_idem", name="Workflow Assign Idem", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                lead=lead,
                event_type=LeadStatusAuditEvent.MANAGER_ASSIGNED,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadStatusIdempotencyEndpoint.ASSIGN_MANAGER,
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
        pipeline = Pipeline.objects.create(code="wf_assign_idem_diff", name="Workflow Assign Idem Diff", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_bulk_assign_idem", name="Workflow Bulk Assign Idem", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                lead_id__in=[lead_1.id, lead_2.id],
                event_type=LeadStatusAuditEvent.MANAGER_ASSIGNED,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadStatusIdempotencyEndpoint.BULK_ASSIGN_MANAGER,
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
        pipeline = Pipeline.objects.create(code="wf_unassign_idem", name="Workflow Unassign Idem", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                lead=lead,
                event_type=LeadStatusAuditEvent.MANAGER_UNASSIGNED,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadStatusIdempotencyEndpoint.UNASSIGN_MANAGER,
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
        pipeline = Pipeline.objects.create(
            code="wf_bulk_unassign_idem",
            name="Workflow Bulk Unassign Idem",
            is_default=True,
        )
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead_1 = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, manager=manager_target, pipeline=pipeline, status=status_new, custom_fields={})
        self._auth(admin)

        headers = {"HTTP_IDEMPOTENCY_KEY": "bulk-unassign-manager-key-1"}
        payload = {"lead_ids": [str(lead_1.id), str(lead_2.id)]}
        first = self.client.post("/api/v1/leads/records/bulk-unassign-manager/", payload, format="json", **headers)
        second = self.client.post("/api/v1/leads/records/bulk-unassign-manager/", payload, format="json", **headers)

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data, second.data)
        self.assertEqual(
            LeadStatusAuditLog.objects.filter(
                lead_id__in=[lead_1.id, lead_2.id],
                event_type=LeadStatusAuditEvent.MANAGER_UNASSIGNED,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint=LeadStatusIdempotencyEndpoint.BULK_UNASSIGN_MANAGER,
                key="bulk-unassign-manager-key-1",
            ).count(),
            1,
        )

    def test_change_status_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_change_idem", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change Idem", code="partner-change-idem")
        pipeline = Pipeline.objects.create(code="wf_change_idem", name="Workflow Change Idem", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead=lead,
                to_status=status_work,
            ).count(),
            1,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint="change_status",
                key="change-status-key-1",
            ).count(),
            1,
        )

    def test_change_status_rejects_same_idempotency_key_with_different_payload(self):
        admin = User.objects.create_user(username="admin_change_idem_diff", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Change Idem Diff", code="partner-change-idem-diff")
        pipeline = Pipeline.objects.create(code="wf_change_idem_diff", name="Workflow Change Idem Diff", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(pipeline=pipeline, code="LOST", name="Lost")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_lost,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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

    def test_change_status_revalidates_after_stale_prefetch(self):
        from unittest.mock import patch

        admin = User.objects.create_user(username="admin_stale_check", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Stale", code="partner-stale")
        pipeline = Pipeline.objects.create(code="wf_stale", name="Workflow Stale", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        status_won = LeadStatus.objects.create(pipeline=pipeline, code="WON", name="Won")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_won,
            is_active=True,
            requires_comment=False,
        )
        stale_lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        stale_snapshot = Lead.objects.select_related("status", "pipeline").get(id=stale_lead.id)
        stale_lead.status = status_work
        stale_lead.pipeline = pipeline
        stale_lead.save(update_fields=["status", "pipeline", "updated_at"])
        self._auth(admin)

        with patch("apps.leads.api.views.LeadViewSet.get_object", return_value=stale_snapshot):
            response = self.client.post(
                f"/api/v1/leads/records/{stale_lead.id}/change-status/",
                {"to_status": str(status_won.id), "reason": "stale retry"},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")
        stale_lead.refresh_from_db()
        self.assertEqual(stale_lead.status_id, status_work.id)
        self.assertFalse(
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead=stale_lead,
                to_status=status_won,
            ).exists()
        )

    def test_admin_can_bulk_change_lead_status_and_write_audit(self):
        admin = User.objects.create_user(username="admin_bulk_status", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk", code="partner-bulk")
        pipeline = Pipeline.objects.create(code="wf_bulk", name="Workflow Bulk", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={"n": 1})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={"n": 2})
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

        audits = LeadStatusAuditLog.objects.filter(
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            to_status=status_work,
            lead_id__in=[lead_1.id, lead_2.id],
        )
        self.assertEqual(audits.count(), 2)
        self.assertEqual(set(audits.values_list("reason", flat=True)), {"bulk move to work"})

    def test_bulk_change_status_is_idempotent_with_same_key(self):
        admin = User.objects.create_user(username="admin_bulk_idem", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Idem", code="partner-bulk-idem")
        pipeline = Pipeline.objects.create(code="wf_bulk_idem", name="Workflow Bulk Idem", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id__in=[lead_1.id, lead_2.id],
                to_status=status_work,
            ).count(),
            2,
        )
        self.assertEqual(
            LeadStatusIdempotencyKey.objects.filter(
                actor_user=admin,
                endpoint="bulk_change_status",
                key="bulk-status-key-1",
            ).count(),
            1,
        )

    def test_bulk_change_status_rejects_batch_with_invalid_transition_and_keeps_all(self):
        admin = User.objects.create_user(username="admin_bulk_invalid", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Invalid", code="partner-bulk-invalid")
        pipeline = Pipeline.objects.create(code="wf_bulk_invalid", name="Workflow Bulk Invalid", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(pipeline=pipeline, code="LOST", name="Lost")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead_allowed = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_blocked = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_lost, custom_fields={})
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

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

        lead_allowed.refresh_from_db()
        lead_blocked.refresh_from_db()
        self.assertEqual(lead_allowed.status_id, status_new.id)
        self.assertEqual(lead_blocked.status_id, status_lost.id)
        self.assertFalse(
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id__in=[lead_allowed.id, lead_blocked.id],
            ).exists()
        )

    def test_bulk_change_status_partial_success_updates_valid_and_reports_errors(self):
        admin = User.objects.create_user(username="admin_bulk_partial", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Partial", code="partner-bulk-partial")
        pipeline = Pipeline.objects.create(code="wf_bulk_partial", name="Workflow Bulk Partial", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        status_lost = LeadStatus.objects.create(pipeline=pipeline, code="LOST", name="Lost")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead_allowed = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_blocked = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_lost, custom_fields={})
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
        self.assertEqual(response.data["updated_count"], 1)
        self.assertEqual(response.data["failed_count"], 1)
        self.assertEqual(response.data["updated_ids"], [str(lead_allowed.id)])
        self.assertEqual(response.data["failed"][str(lead_blocked.id)], "Transition is not allowed")

        lead_allowed.refresh_from_db()
        lead_blocked.refresh_from_db()
        self.assertEqual(lead_allowed.status_id, status_work.id)
        self.assertEqual(lead_blocked.status_id, status_lost.id)

        self.assertEqual(
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id=lead_allowed.id,
            ).count(),
            1,
        )
        self.assertFalse(
            LeadStatusAuditLog.objects.filter(
                event_type=LeadStatusAuditEvent.STATUS_CHANGED,
                lead_id=lead_blocked.id,
            ).exists()
        )

    def test_bulk_change_status_partial_success_reports_unknown_lead_id(self):
        admin = User.objects.create_user(username="admin_bulk_partial_unknown", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Partial Unknown", code="partner-bulk-partial-unknown")
        pipeline = Pipeline.objects.create(code="wf_bulk_partial_unknown", name="Workflow Bulk Partial Unknown", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        unknown_id = uuid4()
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
        pipeline = Pipeline.objects.create(code="wf_bulk_limit", name="Workflow Bulk Limit", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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

    def test_bulk_change_status_requires_comment_for_transition(self):
        admin = User.objects.create_user(username="admin_bulk_comment", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Bulk Comment", code="partner-bulk-comment")
        pipeline = Pipeline.objects.create(code="wf_bulk_comment", name="Workflow Bulk Comment", is_default=True)
        status_lost = LeadStatus.objects.create(pipeline=pipeline, code="LOST", name="Lost", is_default_for_new_leads=True)
        status_reopen = LeadStatus.objects.create(pipeline=pipeline, code="REOPEN", name="Reopen")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_lost,
            to_status=status_reopen,
            is_active=True,
            requires_comment=True,
        )
        lead_1 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_lost, custom_fields={})
        lead_2 = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_lost, custom_fields={})
        self._auth(admin)

        response_no_reason = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_reopen.id),
            },
            format="json",
        )
        self.assertEqual(response_no_reason.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response_no_reason.data["error"]["code"], "validation_error")

        response_with_reason = self.client.post(
            "/api/v1/leads/records/bulk-change-status/",
            {
                "lead_ids": [str(lead_1.id), str(lead_2.id)],
                "to_status": str(status_reopen.id),
                "reason": "returned to funnel",
            },
            format="json",
        )
        self.assertEqual(response_with_reason.status_code, status.HTTP_200_OK)

        lead_1.refresh_from_db()
        lead_2.refresh_from_db()
        self.assertEqual(lead_1.status_id, status_reopen.id)
        self.assertEqual(lead_2.status_id, status_reopen.id)

    def test_manager_cannot_bulk_change_lead_status(self):
        manager = User.objects.create_user(username="manager_bulk_change", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Bulk Deny", code="partner-bulk-deny")
        pipeline = Pipeline.objects.create(code="wf_bulk_deny", name="Workflow Bulk Deny", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_work = LeadStatus.objects.create(pipeline=pipeline, code="WORK", name="Work")
        LeadStatusTransition.objects.create(
            pipeline=pipeline,
            from_status=status_new,
            to_status=status_work,
            is_active=True,
            requires_comment=False,
        )
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.status_id, status_new.id)

    def test_admin_can_get_leads_metrics(self):
        admin = User.objects.create_user(username="admin_metrics", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Metrics", code="partner-metrics")
        pipeline = Pipeline.objects.create(code="wf_metrics", name="Workflow Metrics", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        status_lost = LeadStatus.objects.create(
            pipeline=pipeline,
            code="LOST",
            name="Lost",
            is_terminal=True,
            counts_for_conversion=False,
        )

        lead_1 = Lead.objects.create(
            partner=partner,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_2 = Lead.objects.create(
            partner=partner,
            pipeline=pipeline,
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 8, 10, 0, 0)),
        )
        lead_3 = Lead.objects.create(
            partner=partner,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2025, 12, 20, 10, 0, 0)),
        )
        lead_4 = Lead.objects.create(
            partner=partner,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 20, 10, 0, 0)),
        )

        log_1 = LeadStatusAuditLog.objects.create(
            lead=lead_1,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))

        log_2 = LeadStatusAuditLog.objects.create(
            lead=lead_2,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_2, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))

        log_3 = LeadStatusAuditLog.objects.create(
            lead=lead_3,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_3, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["leads_received"], 3)
        self.assertEqual(response.data["transitions_count"], 3)
        self.assertEqual(response.data["won_count"], 2)
        self.assertEqual(response.data["lost_count"], 1)
        self.assertEqual(response.data["overall_conversion"]["cohort_received"], 3)
        self.assertEqual(response.data["overall_conversion"]["cohort_won"], 2)
        self.assertEqual(response.data["overall_conversion"]["rate"], 0.6667)
        self.assertEqual(response.data["speed"]["median_time_to_win_seconds"], 428400.0)
        self.assertEqual(response.data["speed"]["median_time_to_lost_seconds"], 255600.0)
        speed_by_status = {row["status_code"]: row["median_seconds"] for row in response.data["speed"]["median_time_in_status"]}
        self.assertEqual(speed_by_status["NEW"], 342000.0)
        self.assertEqual(response.data["stale_leads"]["count"], 0)
        self.assertEqual(response.data["stale_leads"]["total_active_non_terminal"], 0)
        self.assertEqual(response.data["stale_leads"]["rate"], 0.0)

        status_counts = {row["status_code"]: row["count"] for row in response.data["leads_in_status"]}
        self.assertEqual(status_counts["WON"], 3)
        self.assertEqual(status_counts["LOST"], 1)

    def test_admin_can_get_leads_metrics_for_single_partner(self):
        admin = User.objects.create_user(username="admin_metrics_partner", password="pass12345", role=UserRole.ADMIN)
        partner_a = Partner.objects.create(name="Partner A", code="partner-a")
        partner_b = Partner.objects.create(name="Partner B", code="partner-b")
        pipeline = Pipeline.objects.create(code="wf_metrics_partner", name="Workflow Metrics Partner", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        status_lost = LeadStatus.objects.create(
            pipeline=pipeline,
            code="LOST",
            name="Lost",
            is_terminal=True,
            counts_for_conversion=False,
        )

        lead_a1 = Lead.objects.create(
            partner=partner_a,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        lead_a2 = Lead.objects.create(
            partner=partner_a,
            pipeline=pipeline,
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
        )
        lead_b = Lead.objects.create(
            partner=partner_b,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )

        log_a1 = LeadStatusAuditLog.objects.create(
            lead=lead_a1,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_a2 = LeadStatusAuditLog.objects.create(
            lead=lead_a2,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_a2, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b = LeadStatusAuditLog.objects.create(
            lead=lead_b,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
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
        self.assertEqual(response.data["leads_received"], 2)
        self.assertEqual(response.data["transitions_count"], 2)
        self.assertEqual(response.data["won_count"], 1)
        self.assertEqual(response.data["lost_count"], 1)
        self.assertEqual(response.data["overall_conversion"]["cohort_received"], 2)
        self.assertEqual(response.data["overall_conversion"]["cohort_won"], 1)
        self.assertEqual(response.data["overall_conversion"]["rate"], 0.5)

    def test_admin_can_get_leads_metrics_grouped_by_partner(self):
        admin = User.objects.create_user(username="admin_metrics_group", password="pass12345", role=UserRole.ADMIN)
        partner_a = Partner.objects.create(name="Partner Group A", code="partner-group-a")
        partner_b = Partner.objects.create(name="Partner Group B", code="partner-group-b")
        pipeline = Pipeline.objects.create(code="wf_metrics_group", name="Workflow Metrics Group", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        status_lost = LeadStatus.objects.create(
            pipeline=pipeline,
            code="LOST",
            name="Lost",
            is_terminal=True,
            counts_for_conversion=False,
        )

        lead_a = Lead.objects.create(
            partner=partner_a,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_b1 = Lead.objects.create(
            partner=partner_b,
            pipeline=pipeline,
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )
        lead_b2 = Lead.objects.create(
            partner=partner_b,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 9, 10, 0, 0)),
        )

        log_a = LeadStatusAuditLog.objects.create(
            lead=lead_a,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_b1 = LeadStatusAuditLog.objects.create(
            lead=lead_b1,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_b1, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b2 = LeadStatusAuditLog.objects.create(
            lead=lead_b2,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
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
        self.assertEqual(items["partner-group-a"]["leads_received"], 1)
        self.assertEqual(items["partner-group-a"]["won_count"], 1)
        self.assertEqual(items["partner-group-a"]["lost_count"], 0)
        self.assertEqual(items["partner-group-a"]["overall_conversion"]["rate"], 1.0)

        self.assertEqual(items["partner-group-b"]["leads_received"], 2)
        self.assertEqual(items["partner-group-b"]["won_count"], 1)
        self.assertEqual(items["partner-group-b"]["lost_count"], 1)
        self.assertEqual(items["partner-group-b"]["overall_conversion"]["rate"], 0.5)

    def test_admin_can_get_leads_metrics_for_single_manager(self):
        admin = User.objects.create_user(username="admin_metrics_manager", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_a_metrics", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_b_metrics", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics Manager", code="partner-metrics-manager")
        pipeline = Pipeline.objects.create(code="wf_metrics_manager", name="Workflow Metrics Manager", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        status_lost = LeadStatus.objects.create(
            pipeline=pipeline,
            code="LOST",
            name="Lost",
            is_terminal=True,
            counts_for_conversion=False,
        )

        lead_a1 = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 2, 10, 0, 0)),
        )
        lead_a2 = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            pipeline=pipeline,
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 4, 10, 0, 0)),
        )
        lead_b = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 6, 10, 0, 0)),
        )

        log_a1 = LeadStatusAuditLog.objects.create(
            lead=lead_a1,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a1, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_a2 = LeadStatusAuditLog.objects.create(
            lead=lead_a2,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_a2, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b = LeadStatusAuditLog.objects.create(
            lead=lead_b,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get(
            f"/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&manager={manager_a.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["manager"]["id"], str(manager_a.id))
        self.assertEqual(response.data["leads_received"], 2)
        self.assertEqual(response.data["won_count"], 1)
        self.assertEqual(response.data["lost_count"], 1)
        self.assertEqual(response.data["overall_conversion"]["cohort_received"], 2)
        self.assertEqual(response.data["overall_conversion"]["cohort_won"], 1)
        self.assertEqual(response.data["overall_conversion"]["rate"], 0.5)

    def test_admin_can_get_leads_metrics_for_single_ret_assignee(self):
        admin = User.objects.create_user(username="admin_metrics_ret", password="pass12345", role=UserRole.ADMIN)
        ret_user = User.objects.create_user(username="ret_metrics_user", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Metrics RET", code="partner-metrics-ret")
        pipeline = Pipeline.objects.create(code="wf_metrics_ret", name="Workflow Metrics RET", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=ret_user,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        log = LeadStatusAuditLog.objects.create(
            lead=lead,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        self._auth(admin)

        response = self.client.get(
            f"/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&manager={ret_user.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["manager"]["id"], str(ret_user.id))
        self.assertEqual(response.data["leads_received"], 1)
        self.assertEqual(response.data["won_count"], 1)

    def test_admin_can_get_leads_metrics_grouped_by_manager(self):
        admin = User.objects.create_user(username="admin_metrics_group_manager", password="pass12345", role=UserRole.ADMIN)
        manager_a = User.objects.create_user(username="manager_a_group", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_b_group", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Metrics Group Manager", code="partner-metrics-group-manager")
        pipeline = Pipeline.objects.create(code="wf_metrics_group_manager", name="Workflow Metrics Group Manager", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        status_won = LeadStatus.objects.create(
            pipeline=pipeline,
            code="WON",
            name="Won",
            is_terminal=True,
            counts_for_conversion=True,
        )
        status_lost = LeadStatus.objects.create(
            pipeline=pipeline,
            code="LOST",
            name="Lost",
            is_terminal=True,
            counts_for_conversion=False,
        )

        lead_a = Lead.objects.create(
            partner=partner,
            manager=manager_a,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 3, 10, 0, 0)),
        )
        lead_b1 = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            pipeline=pipeline,
            status=status_lost,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 5, 10, 0, 0)),
        )
        lead_b2 = Lead.objects.create(
            partner=partner,
            manager=manager_b,
            pipeline=pipeline,
            status=status_won,
            custom_fields={},
            received_at=timezone.make_aware(datetime(2026, 1, 7, 10, 0, 0)),
        )

        log_a = LeadStatusAuditLog.objects.create(
            lead=lead_a,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_a, timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0)))
        log_b1 = LeadStatusAuditLog.objects.create(
            lead=lead_b1,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_lost,
            actor_user=admin,
        )
        self._set_log_created_at(log_b1, timezone.make_aware(datetime(2026, 1, 11, 9, 0, 0)))
        log_b2 = LeadStatusAuditLog.objects.create(
            lead=lead_b2,
            event_type=LeadStatusAuditEvent.STATUS_CHANGED,
            from_status=status_new,
            to_status=status_won,
            actor_user=admin,
        )
        self._set_log_created_at(log_b2, timezone.make_aware(datetime(2026, 1, 12, 9, 0, 0)))

        self._auth(admin)
        response = self.client.get("/api/v1/leads/records/metrics/?date_from=2026-01-01&date_to=2026-01-31&group_by=manager")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["group_by"], "manager")
        items = {item["manager"]["username"]: item for item in response.data["items"]}
        self.assertEqual(items["manager_a_group"]["leads_received"], 1)
        self.assertEqual(items["manager_a_group"]["won_count"], 1)
        self.assertEqual(items["manager_a_group"]["lost_count"], 0)
        self.assertEqual(items["manager_a_group"]["overall_conversion"]["rate"], 1.0)

        self.assertEqual(items["manager_b_group"]["leads_received"], 2)
        self.assertEqual(items["manager_b_group"]["won_count"], 1)
        self.assertEqual(items["manager_b_group"]["lost_count"], 1)
        self.assertEqual(items["manager_b_group"]["overall_conversion"]["rate"], 0.5)

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
        pipeline = Pipeline.objects.create(code="wf_comment_create", name="Workflow Comment Create", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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

    def test_ret_can_create_lead_comment(self):
        ret = User.objects.create_user(username="ret_comment_create", password="pass12345", role=UserRole.RET)
        partner = Partner.objects.create(name="Partner Comment RET", code="partner-comment-ret")
        pipeline = Pipeline.objects.create(code="wf_comment_ret", name="Workflow Comment RET", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_comment_update", name="Workflow Comment Update", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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
        pipeline = Pipeline.objects.create(code="wf_comment_admin", name="Workflow Comment Admin", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
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

    def test_list_comments_can_be_filtered_by_lead(self):
        manager = User.objects.create_user(username="manager_comment_list", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment List", code="partner-comment-list")
        pipeline = Pipeline.objects.create(code="wf_comment_list", name="Workflow Comment List", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead_a = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        lead_b = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        comment_a = LeadComment.objects.create(lead=lead_a, author=manager, body="A")
        LeadComment.objects.create(lead=lead_b, author=manager, body="B")
        self._auth(manager)

        response = self.client.get("/api/v1/leads/comments/", {"lead": str(lead_a.id)})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], comment_a.id)

    def test_pinned_comment_is_listed_first(self):
        manager = User.objects.create_user(username="manager_comment_pin", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Pin", code="partner-comment-pin")
        pipeline = Pipeline.objects.create(code="wf_comment_pin", name="Workflow Comment Pin", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        first = LeadComment.objects.create(lead=lead, author=manager, body="old regular")
        pinned = LeadComment.objects.create(lead=lead, author=manager, body="important", is_pinned=True)
        self._auth(manager)

        response = self.client.get("/api/v1/leads/comments/", {"lead": str(lead.id)})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["id"], pinned.id)
        self.assertEqual(response.data[1]["id"], first.id)

    def test_list_comments_can_be_filtered_by_authors(self):
        manager_a = User.objects.create_user(username="manager_comment_filter_a", password="pass12345", role=UserRole.MANAGER)
        manager_b = User.objects.create_user(username="manager_comment_filter_b", password="pass12345", role=UserRole.MANAGER)
        manager_c = User.objects.create_user(username="manager_comment_filter_c", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Comment Authors", code="partner-comment-authors")
        pipeline = Pipeline.objects.create(code="wf_comment_authors", name="Workflow Comment Authors", is_default=True)
        status_new = LeadStatus.objects.create(pipeline=pipeline, code="NEW", name="New", is_default_for_new_leads=True)
        lead = Lead.objects.create(partner=partner, pipeline=pipeline, status=status_new, custom_fields={})
        LeadComment.objects.create(lead=lead, author=manager_a, body="A")
        LeadComment.objects.create(lead=lead, author=manager_b, body="B")
        excluded = LeadComment.objects.create(lead=lead, author=manager_c, body="C")
        self._auth(manager_a)

        response = self.client.get(
            "/api/v1/leads/comments/",
            {"lead": str(lead.id), "authors": f"{manager_a.id},{manager_b.id}"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned_ids = {row["id"] for row in response.data}
        self.assertEqual(len(response.data), 2)
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
                "email": "john.lead@example.com",
                "currency": "usd",
                "custom_fields": {"note": "new lead"},
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["partner"]["id"], str(partner.id))
        self.assertEqual(response.data["email"], "john.lead@example.com")
        self.assertEqual(response.data["currency"], "USD")

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

    def test_manager_can_update_only_own_lead(self):
        manager = User.objects.create_user(username="manager_lead_edit_own", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Partner Lead Edit", code="partner-lead-edit")
        lead = Lead.objects.create(partner=partner, manager=manager, full_name="Before", phone="+111", custom_fields={})
        self._auth(manager)

        response = self.client.patch(
            f"/api/v1/leads/records/{lead.id}/",
            {"full_name": "After"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "After")

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

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["error"]["code"], "permission_denied")
        lead.refresh_from_db()
        self.assertEqual(lead.full_name, "Before")

    def test_admin_can_soft_delete_lead(self):
        admin = User.objects.create_user(username="admin_lead_soft_delete", password="pass12345", role=UserRole.ADMIN)
        partner = Partner.objects.create(name="Partner Lead Soft Delete", code="partner-lead-soft-delete")
        lead = Lead.objects.create(partner=partner, phone="+111", custom_fields={})
        self._auth(admin)

        response = self.client.post(f"/api/v1/leads/records/{lead.id}/soft_delete/", {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Lead.objects.filter(id=lead.id).exists())
        self.assertTrue(Lead.all_objects.filter(id=lead.id).exists())

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

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"]["code"], "validation_error")

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
                "external_id": "dup-ext-1",
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
