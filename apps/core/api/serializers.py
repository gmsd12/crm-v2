from __future__ import annotations

from rest_framework import serializers

from apps.core.models import Notification, NotificationPolicy, NotificationPreference


class NotificationSerializer(serializers.ModelSerializer):
    actor = serializers.SerializerMethodField()
    lead = serializers.SerializerMethodField()

    class Meta:
        model = Notification
        fields = [
            "id",
            "event_type",
            "channel",
            "status",
            "scheduled_for",
            "sent_at",
            "title",
            "body",
            "payload",
            "is_read",
            "read_at",
            "created_at",
            "actor",
            "lead",
        ]
        read_only_fields = fields

    @staticmethod
    def _user_payload(user):
        if not user:
            return None
        return {
            "id": str(user.id),
            "username": user.username,
            "first_name": (user.first_name or "").strip(),
            "last_name": (user.last_name or "").strip(),
            "role": getattr(user, "role", None),
        }

    def get_actor(self, obj):
        return self._user_payload(obj.actor_user)

    def get_lead(self, obj):
        lead = obj.lead
        if not lead:
            return None
        return {
            "id": str(lead.id),
            "full_name": (lead.full_name or "").strip(),
            "phone": (lead.phone or "").strip(),
        }


class NotificationPolicySerializer(serializers.ModelSerializer):
    class Meta:
        model = NotificationPolicy
        fields = [
            "id",
            "event_type",
            "enabled_by_default",
            "default_repeat_minutes",
            "default_watch_scope",
            "apply_to_teamleaders",
            "apply_to_admins",
            "apply_to_superusers",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "event_type", "created_at", "updated_at"]


class NotificationPreferenceSerializer(serializers.ModelSerializer):
    class Meta:
        model = NotificationPreference
        fields = [
            "id",
            "user",
            "event_type",
            "enabled",
            "repeat_minutes",
            "watch_scope",
            "updated_by",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "user", "event_type", "updated_by", "created_at", "updated_at"]


class NotificationPreferenceUpdateSerializer(serializers.Serializer):
    event_type = serializers.CharField(max_length=64)
    enabled = serializers.BooleanField(required=False, allow_null=True)
    repeat_minutes = serializers.IntegerField(required=False, allow_null=True, min_value=1, max_value=1440)
    watch_scope = serializers.ChoiceField(
        choices=NotificationPolicy.WatchScope.choices,
        required=False,
        allow_null=True,
    )
