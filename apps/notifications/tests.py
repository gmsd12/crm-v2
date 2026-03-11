from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from rest_framework.test import APITestCase
from django.test import TransactionTestCase
from django.utils import timezone

from apps.iam.models import User, UserRole
from apps.leads.models import Lead, LeadComment, LeadStatus
from apps.notifications.handlers.lead_events import emit_comment_added_notification
from apps.notifications.handlers.followups import (
    emit_next_contact_overdue_notifications,
    reschedule_next_contact_planned_notifications,
)
from apps.notifications.events import NotificationEvent
from apps.notifications.models import (
    Notification,
    NotificationDelivery,
    NotificationDeliveryAttempt,
    NotificationOutbox,
    NotificationPolicy,
    NotificationPreference,
    NotificationWatchTarget,
)
from apps.notifications.policies import resolve_user_notification_settings
from apps.notifications.publishers import (
    publish_comment_added,
    publish_next_contact_planned_resync,
)
from apps.notifications.runtime import NotificationEmitPayload, emit, process_due_notifications
from apps.partners.models import Partner


class NotificationOutboxTests(TransactionTestCase):
    def setUp(self):
        self.manager = User.objects.create_user(
            username="notif_manager",
            password="pass12345",
            role=UserRole.MANAGER,
        )
        self.author = User.objects.create_user(
            username="notif_author",
            password="pass12345",
            role=UserRole.ADMIN,
        )
        self.partner = Partner.objects.create(name="Partner", code="partner")
        self.working_status = LeadStatus.objects.create(
            code="working",
            name="Working",
            work_bucket=LeadStatus.WorkBucket.WORKING,
            is_active=True,
        )

    def test_publish_comment_added_creates_processed_outbox_and_notification(self):
        lead = Lead.objects.create(
            partner=self.partner,
            manager=self.manager,
            status=self.working_status,
            phone="700000001",
            full_name="Lead One",
        )
        comment = LeadComment.objects.create(
            lead=lead,
            author=self.author,
            body="New note for the lead",
        )

        event = publish_comment_added(comment_id=comment.id)
        event.refresh_from_db()

        self.assertEqual(event.status, NotificationOutbox.Status.PROCESSED)
        notification = Notification.objects.get(
            event_type="comment_added",
            recipient=self.manager,
            lead=lead,
        )
        self.assertEqual(notification.status, Notification.Status.SENT)
        self.assertEqual(notification.payload.get("comment_id"), str(comment.id))
        delivery = NotificationDelivery.objects.get(notification=notification)
        self.assertEqual(delivery.status, NotificationDelivery.Status.SENT)
        self.assertEqual(delivery.notification_id, notification.id)
        self.assertTrue(
            NotificationDeliveryAttempt.objects.filter(
                delivery=delivery,
                status=NotificationDeliveryAttempt.Status.SENT,
            ).exists()
        )

    def test_publish_next_contact_resync_creates_new_outbox_events_for_repeated_updates(self):
        lead = Lead.objects.create(
            partner=self.partner,
            manager=self.manager,
            status=self.working_status,
            phone="700000002",
            full_name="Lead Two",
            next_contact_at=timezone.now() + timedelta(hours=2),
        )

        first_event = publish_next_contact_planned_resync(lead_id=lead.id, remind_before_minutes=15)
        second_event = publish_next_contact_planned_resync(lead_id=lead.id, remind_before_minutes=15)

        self.assertNotEqual(first_event.id, second_event.id)
        self.assertEqual(
            NotificationOutbox.objects.filter(
                event_type="next_contact_planned_sync",
                aggregate_id=str(lead.id),
            ).count(),
            2,
        )

    def test_process_due_notifications_reads_delivery_queue_and_syncs_inbox(self):
        scheduled_for = timezone.now() + timedelta(minutes=5)
        notification = emit(
            NotificationEmitPayload(
                event_type="comment_added",
                recipient_id=self.manager.id,
                actor_user_id=self.author.id,
                title="Scheduled notification",
                body="Body",
                payload={"sample": True},
                dedupe_key="scheduled:delivery:test",
                scheduled_for=scheduled_for,
            )
        )

        self.assertIsNotNone(notification)
        notification.refresh_from_db()
        delivery = NotificationDelivery.objects.get(notification=notification)
        self.assertEqual(notification.status, Notification.Status.PENDING)
        self.assertEqual(delivery.status, NotificationDelivery.Status.PENDING)

        processed_before = process_due_notifications(now=timezone.now(), limit=50)
        self.assertEqual(processed_before, 0)

        processed_after = process_due_notifications(now=scheduled_for + timedelta(seconds=1), limit=50)
        self.assertEqual(processed_after, 1)

        notification.refresh_from_db()
        delivery.refresh_from_db()
        self.assertEqual(notification.status, Notification.Status.SENT)
        self.assertEqual(delivery.status, NotificationDelivery.Status.SENT)
        self.assertEqual(notification.sent_at, delivery.sent_at)
        self.assertTrue(
            NotificationDeliveryAttempt.objects.filter(
                delivery=delivery,
                status=NotificationDeliveryAttempt.Status.SENT,
                sequence=1,
            ).exists()
        )

    def test_delivery_failure_marks_delivery_and_inbox_failed(self):
        scheduled_for = timezone.now() + timedelta(minutes=5)
        notification = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.COMMENT_ADDED,
                recipient_id=self.manager.id,
                actor_user_id=self.author.id,
                title="Will fail",
                dedupe_key="failure:test",
                scheduled_for=scheduled_for,
            )
        )

        self.assertIsNotNone(notification)
        delivery = NotificationDelivery.objects.get(notification=notification)

        with patch("apps.notifications.runtime.NotificationDelivery.mark_sent", side_effect=RuntimeError("send failed")):
            processed = process_due_notifications(now=scheduled_for + timedelta(seconds=1), limit=50)

        self.assertEqual(processed, 0)
        notification.refresh_from_db()
        delivery.refresh_from_db()
        attempt = NotificationDeliveryAttempt.objects.get(delivery=delivery, sequence=1)

        self.assertEqual(notification.status, Notification.Status.FAILED)
        self.assertEqual(delivery.status, NotificationDelivery.Status.FAILED)
        self.assertEqual(delivery.last_error, "send failed")
        self.assertEqual(attempt.status, NotificationDeliveryAttempt.Status.FAILED)
        self.assertEqual(attempt.error_message, "send failed")

    def test_planned_reminder_cancellation_marks_inbox_cancelled(self):
        next_contact_at = timezone.now() + timedelta(minutes=20)
        lead = Lead.objects.create(
            partner=self.partner,
            manager=self.manager,
            status=self.working_status,
            phone="700000003",
            full_name="Lead Three",
            next_contact_at=next_contact_at,
        )

        created = reschedule_next_contact_planned_notifications(lead_id=lead.id, remind_before_minutes=15)
        self.assertEqual(created, 1)

        notification = Notification.objects.get(
            event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
            recipient=self.manager,
            lead=lead,
        )
        delivery = NotificationDelivery.objects.get(notification=notification)

        lead.last_contacted_at = lead.next_contact_at
        lead.save(update_fields=["last_contacted_at", "updated_at"])

        processed = process_due_notifications(now=delivery.scheduled_for + timedelta(seconds=1), limit=50)
        self.assertEqual(processed, 0)

        notification.refresh_from_db()
        delivery.refresh_from_db()
        attempt = NotificationDeliveryAttempt.objects.get(delivery=delivery, sequence=1)

        self.assertEqual(notification.status, Notification.Status.CANCELLED)
        self.assertEqual(delivery.status, NotificationDelivery.Status.CANCELLED)
        self.assertEqual(attempt.status, NotificationDeliveryAttempt.Status.CANCELLED)
        self.assertEqual(attempt.error_message, "Delivery precondition failed")

    def test_planned_reminder_uses_payload_as_canonical_datetime_and_generic_body(self):
        next_contact_at = timezone.now() + timedelta(minutes=20)
        lead = Lead.objects.create(
            partner=self.partner,
            manager=self.manager,
            status=self.working_status,
            phone="700000004",
            full_name="Lead Four",
            next_contact_at=next_contact_at,
        )

        created = reschedule_next_contact_planned_notifications(lead_id=lead.id, remind_before_minutes=15)
        self.assertEqual(created, 1)

        notification = Notification.objects.get(
            event_type=NotificationEvent.NEXT_CONTACT_PLANNED,
            recipient=self.manager,
            lead=lead,
        )

        self.assertEqual(notification.body, "Контакт запланирован")
        self.assertEqual(notification.payload.get("next_contact_at"), next_contact_at.isoformat())

    def test_overdue_reminder_uses_payload_as_canonical_datetime_and_generic_body(self):
        next_contact_at = timezone.now() - timedelta(minutes=20)
        lead = Lead.objects.create(
            partner=self.partner,
            manager=self.manager,
            status=self.working_status,
            phone="700000005",
            full_name="Lead Five",
            next_contact_at=next_contact_at,
        )

        created = emit_next_contact_overdue_notifications(now=timezone.now(), limit=50)
        self.assertEqual(created, 1)

        notification = Notification.objects.get(
            event_type=NotificationEvent.NEXT_CONTACT_OVERDUE,
            recipient=self.manager,
            lead=lead,
        )

        self.assertEqual(notification.body, "Следующий контакт просрочен")
        self.assertEqual(notification.payload.get("next_contact_at"), next_contact_at.isoformat())

    def test_emit_dedupes_on_delivery_record(self):
        scheduled_for = timezone.now() + timedelta(minutes=5)
        first = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.COMMENT_ADDED,
                recipient_id=self.manager.id,
                actor_user_id=self.author.id,
                title="One",
                dedupe_key="dedupe:delivery:test",
                scheduled_for=scheduled_for,
            )
        )
        second = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.COMMENT_ADDED,
                recipient_id=self.manager.id,
                actor_user_id=self.author.id,
                title="Two",
                dedupe_key="dedupe:delivery:test",
                scheduled_for=scheduled_for,
            )
        )

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(Notification.objects.filter(recipient=self.manager, event_type=NotificationEvent.COMMENT_ADDED).count(), 1)
        self.assertEqual(
            NotificationDelivery.objects.filter(
                notification__recipient=self.manager,
                dedupe_key="dedupe:delivery:test",
            ).count(),
            1,
        )

    def test_delivery_retry_uses_next_attempt_sequence_when_previous_attempt_exists(self):
        scheduled_for = timezone.now() + timedelta(minutes=5)
        notification = emit(
            NotificationEmitPayload(
                event_type=NotificationEvent.COMMENT_ADDED,
                recipient_id=self.manager.id,
                actor_user_id=self.author.id,
                title="Retry me",
                dedupe_key="retry:sequence:test",
                scheduled_for=scheduled_for,
            )
        )

        self.assertIsNotNone(notification)
        delivery = NotificationDelivery.objects.get(notification=notification)
        NotificationDeliveryAttempt.objects.create(
            delivery=delivery,
            sequence=1,
            status=NotificationDeliveryAttempt.Status.STARTED,
            started_at=timezone.now(),
        )

        processed = process_due_notifications(now=scheduled_for + timedelta(seconds=1), limit=50)

        self.assertEqual(processed, 1)
        delivery.refresh_from_db()
        notification.refresh_from_db()
        self.assertEqual(delivery.status, NotificationDelivery.Status.SENT)
        self.assertEqual(notification.status, Notification.Status.SENT)
        self.assertTrue(
            NotificationDeliveryAttempt.objects.filter(
                delivery=delivery,
                sequence=2,
                status=NotificationDeliveryAttempt.Status.SENT,
            ).exists()
        )


class NotificationAudienceSettingsApiTests(APITestCase):
    def setUp(self):
        self.admin = User.objects.create_user(username="notif_admin_cfg", password="pass12345", role=UserRole.ADMIN)
        self.manager_1 = User.objects.create_user(username="notif_manager_cfg_1", password="pass12345", role=UserRole.MANAGER)
        self.manager_2 = User.objects.create_user(username="notif_manager_cfg_2", password="pass12345", role=UserRole.MANAGER)
        self.client.force_authenticate(user=self.admin)

    def test_settings_patch_persists_watch_targets(self):
        response = self.client.patch(
            "/api/v1/notifications/settings/me/",
            {
                "event_type": "comment_added",
                "enabled": True,
                "watch_scope": "team",
                "watched_user_ids": [self.manager_1.id],
                "watched_roles": [UserRole.MANAGER],
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["resolved"]["watch_scope"], "team")
        self.assertEqual(response.data["watch_targets"]["roles"], [UserRole.MANAGER])
        self.assertEqual(response.data["watch_targets"]["user_ids"], [self.manager_1.id])
        self.assertEqual(
            NotificationWatchTarget.objects.filter(
                watcher=self.admin,
                event_type="comment_added",
            ).count(),
            2,
        )

        settings_response = self.client.get("/api/v1/notifications/settings/me/")
        self.assertEqual(settings_response.status_code, 200)
        item = next(entry for entry in settings_response.data["items"] if entry["event_type"] == "comment_added")
        self.assertEqual(item["watch_targets"]["roles"], [UserRole.MANAGER])
        self.assertEqual(item["watch_targets"]["user_ids"], [self.manager_1.id])

    def test_settings_patch_rejects_unknown_watched_user(self):
        response = self.client.patch(
            "/api/v1/notifications/settings/me/",
            {
                "event_type": "comment_added",
                "watch_scope": "team",
                "watched_user_ids": [999999],
            },
            format="json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Unknown or inactive users", response.data["detail"])


class NotificationAudienceResolutionTests(TransactionTestCase):
    def setUp(self):
        self.admin = User.objects.create_user(username="notif_admin_watch", password="pass12345", role=UserRole.ADMIN)
        self.manager_1 = User.objects.create_user(username="notif_manager_watch_1", password="pass12345", role=UserRole.MANAGER)
        self.manager_2 = User.objects.create_user(username="notif_manager_watch_2", password="pass12345", role=UserRole.MANAGER)
        self.teamleader = User.objects.create_user(
            username="notif_teamleader_watch",
            password="pass12345",
            role=UserRole.TEAMLEADER,
        )
        self.author = User.objects.create_user(username="notif_author_watch", password="pass12345", role=UserRole.ADMIN)
        self.partner = Partner.objects.create(name="Watch Partner", code="watch-partner")
        self.status = LeadStatus.objects.create(
            code="watching",
            name="Watching",
            work_bucket=LeadStatus.WorkBucket.WORKING,
            is_active=True,
        )
        NotificationPreference.objects.create(
            user=self.admin,
            event_type="comment_added",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.TEAM,
            repeat_minutes=15,
            updated_by=self.admin,
        )

    def _create_comment(self, *, manager, suffix: str) -> LeadComment:
        lead = Lead.objects.create(
            partner=self.partner,
            manager=manager,
            status=self.status,
            phone=f"7001000{suffix}",
            full_name=f"Lead {suffix}",
        )
        return LeadComment.objects.create(
            lead=lead,
            author=self.author,
            body=f"Comment {suffix}",
        )

    def test_role_watch_target_notifies_only_matching_roles(self):
        NotificationWatchTarget.objects.create(
            watcher=self.admin,
            event_type="comment_added",
            target_role=UserRole.MANAGER,
        )

        manager_comment = self._create_comment(manager=self.manager_1, suffix="1")
        teamleader_comment = self._create_comment(manager=self.teamleader, suffix="2")

        emit_comment_added_notification(comment_id=manager_comment.id)
        emit_comment_added_notification(comment_id=teamleader_comment.id)

        self.assertTrue(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=self.admin,
                lead_id=manager_comment.lead_id,
            ).exists()
        )
        self.assertFalse(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=self.admin,
                lead_id=teamleader_comment.lead_id,
            ).exists()
        )

    def test_user_watch_target_notifies_only_selected_user_leads(self):
        NotificationWatchTarget.objects.create(
            watcher=self.admin,
            event_type="comment_added",
            target_user=self.manager_1,
        )

        manager_1_comment = self._create_comment(manager=self.manager_1, suffix="3")
        manager_2_comment = self._create_comment(manager=self.manager_2, suffix="4")

        emit_comment_added_notification(comment_id=manager_1_comment.id)
        emit_comment_added_notification(comment_id=manager_2_comment.id)

        self.assertTrue(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=self.admin,
                lead_id=manager_1_comment.lead_id,
            ).exists()
        )
        self.assertFalse(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=self.admin,
                lead_id=manager_2_comment.lead_id,
            ).exists()
        )

    def test_primary_recipient_still_gets_own_notification_with_team_scope(self):
        NotificationPreference.objects.create(
            user=self.manager_1,
            event_type="comment_added",
            enabled=True,
            watch_scope=NotificationPolicy.WatchScope.TEAM,
            repeat_minutes=15,
            updated_by=self.manager_1,
        )
        comment = self._create_comment(manager=self.manager_1, suffix="5")

        emit_comment_added_notification(comment_id=comment.id)

        self.assertTrue(
            Notification.objects.filter(
                event_type="comment_added",
                recipient=self.manager_1,
                lead_id=comment.lead_id,
            ).exists()
        )

    def test_preference_resolution_is_event_scoped_for_in_app(self):
        pref = NotificationPreference.objects.get(
            user=self.admin,
            event_type="comment_added",
        )
        pref.enabled = False
        pref.repeat_minutes = 60
        pref.watch_scope = NotificationPolicy.WatchScope.ALL
        pref.updated_by = self.admin
        pref.save(update_fields=["enabled", "repeat_minutes", "watch_scope", "updated_by", "updated_at"])

        settings = resolve_user_notification_settings(
            user=self.admin,
            event_type="comment_added",
        )

        self.assertFalse(settings["enabled"])
        self.assertEqual(settings["repeat_minutes"], 60)
        self.assertEqual(settings["watch_scope"], NotificationPolicy.WatchScope.ALL)
