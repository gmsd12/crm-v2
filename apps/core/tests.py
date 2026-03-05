from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils import timezone
from rest_framework.test import APITestCase

from apps.core.models import Notification, NotificationPolicy, NotificationPreference
from apps.core.notifications import (
    NotificationEmitPayload,
    emit,
    emit_manager_no_activity_notifications,
    emit_partner_duplicate_attempt_notification,
    emit_next_contact_overdue_notifications,
    get_or_create_policy,
    process_due_notifications,
)
from apps.iam.models import UserRole
from apps.leads.models import Lead, LeadDuplicateAttempt, LeadStatus
from apps.partners.models import Partner, PartnerSource

User = get_user_model()


class HealthApiTests(APITestCase):
    def test_health_returns_ok_and_request_id_header(self):
        response = self.client.get("/api/health/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {"status": "ок"})
        self.assertIn("X-Request-ID", response)


class NotificationApiTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="notif_user", password="pass12345", role=UserRole.MANAGER)
        self.other = User.objects.create_user(username="notif_other", password="pass12345", role=UserRole.MANAGER)
        self.client.force_authenticate(user=self.user)

    def test_notification_list_unread_count_mark_read_and_mark_all_read(self):
        n1 = Notification.objects.create(
            event_type="lead_assigned",
            recipient=self.user,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="A",
            body="first",
        )
        Notification.objects.create(
            event_type="comment_added",
            recipient=self.user,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="B",
            body="second",
        )
        Notification.objects.create(
            event_type="lead_assigned",
            recipient=self.other,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="C",
            body="foreign",
        )

        list_resp = self.client.get("/api/v1/notifications/")
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.data["count"], 2)

        unread_resp = self.client.get("/api/v1/notifications/unread-count/")
        self.assertEqual(unread_resp.status_code, 200)
        self.assertEqual(unread_resp.data["unread_count"], 2)

        mark_one_resp = self.client.post(f"/api/v1/notifications/{n1.id}/mark-read/", {}, format="json")
        self.assertEqual(mark_one_resp.status_code, 200)
        self.assertTrue(mark_one_resp.data["is_read"])

        unread_after_one = self.client.get("/api/v1/notifications/unread-count/")
        self.assertEqual(unread_after_one.data["unread_count"], 1)

        mark_all_resp = self.client.post("/api/v1/notifications/mark-all-read/", {}, format="json")
        self.assertEqual(mark_all_resp.status_code, 200)
        self.assertEqual(mark_all_resp.data["updated_count"], 1)

        unread_after_all = self.client.get("/api/v1/notifications/unread-count/")
        self.assertEqual(unread_after_all.data["unread_count"], 0)

    def test_assign_and_comment_create_notifications(self):
        admin = User.objects.create_user(username="notif_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_manager", password="pass12345", role=UserRole.MANAGER)
        manager_2 = User.objects.create_user(username="notif_manager_2", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Notif Partner", code="notif-partner")
        status_new = LeadStatus.objects.create(code="NOTIF_NEW", name="Notif New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+15550888",
            custom_fields={},
        )

        self.client.force_authenticate(user=admin)
        with self.captureOnCommitCallbacks(execute=True):
            assign_resp = self.client.post(
                f"/api/v1/leads/records/{lead.id}/assign-manager/",
                {"manager": manager_2.id, "reason": "reassign"},
                format="json",
            )
        self.assertEqual(assign_resp.status_code, 200)
        self.assertTrue(
            Notification.objects.filter(
                event_type="lead_assigned",
                recipient=manager_2,
                lead=lead,
            ).exists()
        )

        with self.captureOnCommitCallbacks(execute=True):
            comment_resp = self.client.post(
                "/api/v1/leads/comments/",
                {"lead": lead.id, "body": "New context"},
                format="json",
            )
        self.assertEqual(comment_resp.status_code, 201)
        lead.refresh_from_db()
        self.assertTrue(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=lead.manager,
                lead=lead,
            ).exists()
        )

    @override_settings(NOTIFICATIONS_BULK_SUMMARY_THRESHOLD=2)
    def test_bulk_assign_notifications_are_summarized_above_threshold(self):
        actor_admin = User.objects.create_user(username="notif_bulk_actor", password="pass12345", role=UserRole.ADMIN)
        watcher_admin = User.objects.create_user(username="notif_bulk_watcher", password="pass12345", role=UserRole.ADMIN)
        old_manager = User.objects.create_user(username="notif_bulk_old_mgr", password="pass12345", role=UserRole.MANAGER)
        new_manager = User.objects.create_user(username="notif_bulk_new_mgr", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Bulk Assign Partner", code="bulk-assign-partner")
        status_new = LeadStatus.objects.create(code="BULK_ASSIGN_NEW", name="Bulk Assign New", is_default_for_new_leads=True)
        lead_ids = []
        for idx in range(3):
            lead = Lead.objects.create(
                partner=partner,
                manager=old_manager,
                status=status_new,
                phone=f"+1555900{idx}",
                custom_fields={},
            )
            lead_ids.append(lead.id)

        NotificationPreference.objects.create(
            user=watcher_admin,
            event_type="lead_assigned",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=watcher_admin,
        )
        NotificationPreference.objects.create(
            user=watcher_admin,
            event_type="lead_unassigned",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=watcher_admin,
        )

        self.client.force_authenticate(user=actor_admin)
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                "/api/v1/leads/records/bulk-assign-manager/",
                {"lead_ids": lead_ids, "manager": new_manager.id, "reason": "bulk assign"},
                format="json",
            )
        self.assertEqual(response.status_code, 200)

        assigned_notifications = Notification.objects.filter(recipient=watcher_admin, event_type="lead_assigned")
        unassigned_notifications = Notification.objects.filter(recipient=watcher_admin, event_type="lead_unassigned")
        self.assertEqual(assigned_notifications.count(), 1)
        self.assertEqual(unassigned_notifications.count(), 1)

        assigned = assigned_notifications.first()
        unassigned = unassigned_notifications.first()
        self.assertEqual(assigned.payload.get("mode"), "bulk_summary")
        self.assertEqual(unassigned.payload.get("mode"), "bulk_summary")
        self.assertEqual(assigned.payload.get("lead_count"), 3)
        self.assertEqual(unassigned.payload.get("lead_count"), 3)
        self.assertEqual(assigned.payload.get("status_counts", {}).get("BULK_ASSIGN_NEW"), 3)
        self.assertEqual(unassigned.payload.get("status_counts", {}).get("BULK_ASSIGN_NEW"), 3)
        self.assertFalse(assigned.payload.get("lead_ids_sample"))
        self.assertFalse(unassigned.payload.get("lead_ids_sample"))
        self.assertIsNone(assigned.lead_id)
        self.assertIsNone(unassigned.lead_id)

    def test_emit_next_contact_overdue_notifications_repeats_by_slot_and_skips_contacted(self):
        manager = User.objects.create_user(username="notif_overdue_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Overdue Partner", code="overdue-partner")
        status_work = LeadStatus.objects.create(
            code="NOTIF_WORKING",
            name="Notif Working",
            is_default_for_new_leads=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_work,
            phone="+15550777",
            next_contact_at=timezone.now() - timedelta(hours=2),
            custom_fields={},
        )

        now = timezone.now()
        created_first = emit_next_contact_overdue_notifications(now=now)
        created_second = emit_next_contact_overdue_notifications(now=now)
        self.assertEqual(created_first, 1)
        self.assertEqual(created_second, 0)
        created_third = emit_next_contact_overdue_notifications(now=now + timedelta(minutes=16))
        self.assertEqual(created_third, 1)

        lead.last_contacted_at = lead.next_contact_at + timedelta(minutes=1)
        lead.save(update_fields=["last_contacted_at", "updated_at"])
        created_after_contact = emit_next_contact_overdue_notifications(now=now + timedelta(minutes=32))
        self.assertEqual(created_after_contact, 0)
        self.assertEqual(
            Notification.objects.filter(event_type="next_contact_overdue", recipient=manager, lead=lead).count(),
            2,
        )

    def test_overdue_recipients_include_teamleader_and_admin_by_preference(self):
        manager = User.objects.create_user(username="notif_scope_manager", password="pass12345", role=UserRole.MANAGER)
        teamleader = User.objects.create_user(username="notif_scope_tl", password="pass12345", role=UserRole.TEAMLEADER)
        admin = User.objects.create_user(username="notif_scope_admin", password="pass12345", role=UserRole.ADMIN)
        superuser = User.objects.create_user(username="notif_scope_super", password="pass12345", role=UserRole.SUPERUSER)
        partner = Partner.objects.create(name="Scope Partner", code="scope-partner")
        status_work = LeadStatus.objects.create(
            code="NOTIF_SCOPE_WORKING",
            name="Notif Scope Working",
            is_default_for_new_leads=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_work,
            phone="+15550771",
            next_contact_at=timezone.now() - timedelta(minutes=20),
            custom_fields={},
        )

        now = timezone.now()
        created_slot_1 = emit_next_contact_overdue_notifications(now=now)
        self.assertEqual(created_slot_1, 2)  # manager + teamleader by default policy

        policy = NotificationPolicy.objects.get(event_type="next_contact_overdue")
        policy.apply_to_admins = True
        policy.apply_to_superusers = True
        policy.save(update_fields=["apply_to_admins", "apply_to_superusers", "updated_at"])
        NotificationPreference.objects.create(
            user=admin,
            event_type="next_contact_overdue",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )
        NotificationPreference.objects.create(
            user=superuser,
            event_type="next_contact_overdue",
            enabled=False,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        created_slot_2 = emit_next_contact_overdue_notifications(now=now + timedelta(minutes=16))
        self.assertEqual(created_slot_2, 3)  # manager + teamleader + admin
        self.assertEqual(
            Notification.objects.filter(event_type="next_contact_overdue", lead=lead, recipient=admin).count(),
            1,
        )
        self.assertEqual(
            Notification.objects.filter(event_type="next_contact_overdue", lead=lead, recipient=superuser).count(),
            0,
        )

    def test_notification_stream_returns_sse_events(self):
        Notification.objects.create(
            event_type="lead_assigned",
            recipient=self.user,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="Assigned",
            body="Lead assigned",
        )

        response = self.client.get("/api/v1/notifications/stream/?once=1&last_id=0")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/event-stream")
        self.assertEqual(response["Cache-Control"], "no-cache")
        body = b"".join(response.streaming_content).decode("utf-8")
        self.assertIn("event: unread_count", body)
        self.assertIn("event: notifications", body)

    def test_scheduled_notification_becomes_visible_after_due_processing(self):
        scheduled_for = timezone.now() + timedelta(minutes=10)
        notification = emit(
            NotificationEmitPayload(
                event_type="next_contact_overdue",
                recipient_id=self.user.id,
                title="Scheduled reminder",
                scheduled_for=scheduled_for,
            )
        )
        self.assertIsNotNone(notification)
        notification.refresh_from_db()
        self.assertEqual(notification.status, Notification.Status.PENDING)

        list_before = self.client.get("/api/v1/notifications/")
        self.assertEqual(list_before.status_code, 200)
        self.assertEqual(list_before.data["count"], 0)

        processed_before = process_due_notifications(now=timezone.now(), limit=50)
        self.assertEqual(processed_before, 0)

        processed_after = process_due_notifications(now=scheduled_for + timedelta(seconds=1), limit=50)
        self.assertEqual(processed_after, 1)
        notification.refresh_from_db()
        self.assertEqual(notification.status, Notification.Status.SENT)

        list_after = self.client.get("/api/v1/notifications/")
        self.assertEqual(list_after.status_code, 200)
        self.assertEqual(list_after.data["count"], 1)

    def test_settings_me_patch_allowed_for_teamleader_admin_super_only(self):
        teamleader = User.objects.create_user(username="notif_settings_tl", password="pass12345", role=UserRole.TEAMLEADER)
        admin = User.objects.create_user(username="notif_settings_admin", password="pass12345", role=UserRole.ADMIN)
        superuser = User.objects.create_user(username="notif_settings_super", password="pass12345", role=UserRole.SUPERUSER)
        manager = User.objects.create_user(username="notif_settings_manager", password="pass12345", role=UserRole.MANAGER)

        self.client.force_authenticate(user=manager)
        denied = self.client.patch(
            "/api/v1/notifications/settings/me/",
            {"event_type": "next_contact_overdue", "enabled": False},
            format="json",
        )
        self.assertEqual(denied.status_code, 403)

        for user in (teamleader, admin, superuser):
            self.client.force_authenticate(user=user)
            ok = self.client.patch(
                "/api/v1/notifications/settings/me/",
                {
                    "event_type": "next_contact_overdue",
                    "enabled": True,
                    "repeat_minutes": 20,
                    "watch_scope": "all",
                },
                format="json",
            )
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.data["resolved"]["repeat_minutes"], 20)
            self.assertEqual(ok.data["resolved"]["watch_scope"], "all")

    def test_comment_added_can_notify_admin_when_policy_and_preference_enable_it(self):
        admin = User.objects.create_user(username="notif_comment_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_comment_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Comment Partner", code="comment-partner")
        status_new = LeadStatus.objects.create(code="NOTIF_COMMENT_NEW", name="Notif Comment New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+15550666",
            custom_fields={},
        )

        policy = get_or_create_policy("comment_added")
        policy.apply_to_admins = True
        policy.save(update_fields=["apply_to_admins", "updated_at"])
        NotificationPreference.objects.create(
            user=admin,
            event_type="comment_added",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        self.client.force_authenticate(user=manager)
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                "/api/v1/leads/comments/",
                {"lead": lead.id, "body": "Anna added context"},
                format="json",
            )
        self.assertEqual(response.status_code, 201)
        self.assertTrue(
            Notification.objects.filter(event_type="comment_added", recipient=admin, lead=lead).exists()
        )

    def test_manager_settings_list_contains_only_allowed_event_types(self):
        manager = User.objects.create_user(username="notif_allowed_manager", password="pass12345", role=UserRole.MANAGER)
        self.client.force_authenticate(user=manager)

        response = self.client.get("/api/v1/notifications/settings/me/")
        self.assertEqual(response.status_code, 200)
        event_types = {item["event_type"] for item in response.data["items"]}
        self.assertIn("lead_unassigned", event_types)
        self.assertIn("lead_assigned", event_types)
        self.assertNotIn("manager_no_activity", event_types)
        self.assertNotIn("partner_duplicate_attempt", event_types)

    def test_api_error_messages_are_localized_to_russian(self):
        manager = User.objects.create_user(username="notif_ru_manager", password="pass12345", role=UserRole.MANAGER)
        self.client.force_authenticate(user=manager)
        response = self.client.patch(
            "/api/v1/notifications/settings/me/",
            {"event_type": "next_contact_overdue", "enabled": False},
            format="json",
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("Только тимлиды", response.data["detail"])

    def test_notification_titles_are_returned_as_stored(self):
        Notification.objects.create(
            event_type="lead_assigned",
            recipient=self.user,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="Назначен новый лид: Ivan Petrov",
            body="Лид #12 назначен пользователю manager_anna",
        )
        response = self.client.get("/api/v1/notifications/")
        self.assertEqual(response.status_code, 200)
        item = response.data["results"][0]
        self.assertEqual(item["title"], "Назначен новый лид: Ivan Petrov")
        self.assertEqual(item["body"], "Лид #12 назначен пользователю manager_anna")

    def test_user_delete_cascades_recipient_notifications(self):
        doomed = User.objects.create_user(username="notif_delete_user", password="pass12345", role=UserRole.MANAGER)
        Notification.objects.create(
            event_type="lead_assigned",
            recipient=doomed,
            status=Notification.Status.SENT,
            sent_at=timezone.now(),
            title="to be deleted",
            body="recipient cascade check",
        )
        self.assertEqual(Notification.objects.filter(recipient=doomed).count(), 1)
        doomed.delete()
        self.assertEqual(Notification.objects.filter(recipient_id=doomed.id).count(), 0)

    def test_lead_unassigned_notifies_previous_manager_and_admin(self):
        admin = User.objects.create_user(username="notif_unassign_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_unassign_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Unassign Partner", code="unassign-partner")
        status_new = LeadStatus.objects.create(code="UNASSIGN_NEW", name="Unassign New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+15550111",
            custom_fields={},
        )
        NotificationPreference.objects.create(
            user=admin,
            event_type="lead_unassigned",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        self.client.force_authenticate(user=admin)
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                f"/api/v1/leads/records/{lead.id}/unassign-manager/",
                {"reason": "cleanup"},
                format="json",
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            Notification.objects.filter(event_type="lead_unassigned", recipient=manager, lead=lead).exists()
        )
        self.assertTrue(
            Notification.objects.filter(event_type="lead_unassigned", recipient=admin, lead=lead).exists()
        )

    def test_lead_status_changed_notifies_only_for_important_cases(self):
        admin = User.objects.create_user(username="notif_status_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_status_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Status Partner", code="status-partner")
        status_work_1 = LeadStatus.objects.create(
            code="STATUS_WORK_1",
            name="Status Work 1",
            is_default_for_new_leads=True,
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
        )
        status_work_2 = LeadStatus.objects.create(
            code="STATUS_WORK_2",
            name="Status Work 2",
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.IGNORE,
        )
        status_lost = LeadStatus.objects.create(
            code="STATUS_LOST",
            name="Status Lost",
            is_valid=False,
            work_bucket=LeadStatus.WorkBucket.NON_WORKING,
            conversion_bucket=LeadStatus.ConversionBucket.LOST,
        )
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_work_1,
            phone="+15550112",
            custom_fields={},
        )
        NotificationPreference.objects.create(
            user=admin,
            event_type="lead_status_changed",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        self.client.force_authenticate(user=manager)
        with self.captureOnCommitCallbacks(execute=True):
            response_1 = self.client.post(
                f"/api/v1/leads/records/{lead.id}/change-status/",
                {"to_status": status_work_2.id, "reason": "regular move"},
                format="json",
            )
        self.assertEqual(response_1.status_code, 200)
        self.assertFalse(
            Notification.objects.filter(event_type="lead_status_changed", recipient=admin, lead=lead).exists()
        )

        with self.captureOnCommitCallbacks(execute=True):
            response_2 = self.client.post(
                f"/api/v1/leads/records/{lead.id}/change-status/",
                {"to_status": status_lost.id, "reason": "lost"},
                format="json",
            )
        self.assertEqual(response_2.status_code, 200)

        with self.captureOnCommitCallbacks(execute=True):
            response_3 = self.client.post(
                f"/api/v1/leads/records/{lead.id}/change-status/",
                {"to_status": status_work_2.id, "reason": "back to valid"},
                format="json",
            )
        self.assertEqual(response_3.status_code, 200)
        self.assertEqual(
            Notification.objects.filter(event_type="lead_status_changed", recipient=admin, lead=lead).count(),
            2,
        )

    def test_deposit_created_notifies_only_for_ftd(self):
        admin = User.objects.create_user(username="notif_dep_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_dep_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Deposit Partner", code="deposit-partner")
        status_new = LeadStatus.objects.create(code="DEP_NEW", name="Dep New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+15550113",
            custom_fields={},
        )
        NotificationPreference.objects.create(
            user=admin,
            event_type="deposit_created",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        self.client.force_authenticate(user=manager)
        with self.captureOnCommitCallbacks(execute=True):
            ftd_resp = self.client.post(
                f"/api/v1/leads/records/{lead.id}/deposits/",
                {"amount": "100.00"},
                format="json",
            )
        self.assertEqual(ftd_resp.status_code, 201)
        self.assertEqual(
            Notification.objects.filter(event_type="deposit_created", recipient=admin, lead=lead).count(),
            1,
        )

        self.client.force_authenticate(user=admin)
        with self.captureOnCommitCallbacks(execute=True):
            reload_resp = self.client.post(
                f"/api/v1/leads/records/{lead.id}/deposits/",
                {"amount": "200.00", "type": 2},
                format="json",
            )
        self.assertEqual(reload_resp.status_code, 201)
        self.assertEqual(
            Notification.objects.filter(event_type="deposit_created", recipient=admin, lead=lead).count(),
            1,
        )

    @override_settings(NOTIFICATIONS_MANAGER_NO_ACTIVITY_THRESHOLD=2)
    def test_manager_no_activity_notifications_are_emitted_by_threshold_and_slot(self):
        admin = User.objects.create_user(username="notif_noact_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_noact_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="No Activity Partner", code="no-activity-partner")
        status_work = LeadStatus.objects.create(
            code="NOACT_WORK",
            name="No Activity Work",
            is_default_for_new_leads=True,
            is_valid=True,
            work_bucket=LeadStatus.WorkBucket.WORKING,
        )
        now = timezone.now()
        Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_work,
            phone="+15550121",
            next_contact_at=now - timedelta(hours=3),
            custom_fields={},
        )
        Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_work,
            phone="+15550122",
            next_contact_at=now - timedelta(hours=2),
            custom_fields={},
        )
        NotificationPreference.objects.create(
            user=admin,
            event_type="manager_no_activity",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=15,
            updated_by=admin,
        )

        created_1 = emit_manager_no_activity_notifications(now=now)
        created_2 = emit_manager_no_activity_notifications(now=now)
        created_3 = emit_manager_no_activity_notifications(now=now + timedelta(minutes=16))
        self.assertEqual(created_1, 1)
        self.assertEqual(created_2, 0)
        self.assertEqual(created_3, 1)
        self.assertEqual(
            Notification.objects.filter(event_type="manager_no_activity", recipient=admin).count(),
            2,
        )

    @override_settings(
        NOTIFICATIONS_PARTNER_DUPLICATE_THRESHOLD=3,
        NOTIFICATIONS_PARTNER_DUPLICATE_WINDOW_MINUTES=60,
    )
    def test_partner_duplicate_attempt_alert_is_threshold_based(self):
        admin = User.objects.create_user(username="notif_dup_admin", password="pass12345", role=UserRole.ADMIN)
        manager = User.objects.create_user(username="notif_dup_manager", password="pass12345", role=UserRole.MANAGER)
        partner = Partner.objects.create(name="Dup Partner", code="dup-partner")
        source = PartnerSource.objects.create(partner=partner, code="SRC_DUP", name="Dup Source", is_active=True)
        status_new = LeadStatus.objects.create(code="DUP_NEW", name="Dup New", is_default_for_new_leads=True)
        lead = Lead.objects.create(
            partner=partner,
            manager=manager,
            status=status_new,
            phone="+15550131",
            custom_fields={},
        )
        NotificationPreference.objects.create(
            user=admin,
            event_type="partner_duplicate_attempt",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.ALL,
            repeat_minutes=60,
            updated_by=admin,
        )

        created_counts = []
        for idx in range(4):
            attempt = LeadDuplicateAttempt.objects.create(
                partner=partner,
                source=source,
                existing_lead=lead,
                phone=f"+1555099{idx}",
                full_name=f"Dup #{idx}",
                email=f"dup{idx}@example.com",
            )
            created_counts.append(emit_partner_duplicate_attempt_notification(attempt_id=attempt.id))

        self.assertEqual(created_counts, [0, 0, 1, 0])
        self.assertEqual(
            Notification.objects.filter(event_type="partner_duplicate_attempt", recipient=admin).count(),
            1,
        )
