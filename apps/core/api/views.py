import asyncio
import json

from asgiref.sync import sync_to_async
from django.utils import timezone
from django_filters import rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend
from django.http import StreamingHttpResponse
from rest_framework import filters as drf_filters, status, viewsets
from rest_framework.decorators import action, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.core.api.serializers import (
    NotificationPolicySerializer,
    NotificationPreferenceSerializer,
    NotificationPreferenceUpdateSerializer,
    NotificationSerializer,
)
from apps.core.models import Notification, NotificationPolicy, NotificationPreference
from apps.core.notifications import (
    DEFAULT_POLICY_CONFIG,
    event_types_for_user,
    get_or_create_policy,
    resolve_user_notification_settings,
)
from apps.iam.models import UserRole


@api_view(["GET"])
def health(request):
    request.logger.info("проверка здоровья")

    return Response({"status": "ок"})


class NotificationFilter(django_filters.FilterSet):
    class Meta:
        model = Notification
        fields = ["event_type", "channel", "status", "is_read", "lead"]


class NotificationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Notification.objects.select_related("actor_user", "lead", "recipient").all().order_by("-created_at", "-id")
    serializer_class = NotificationSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, drf_filters.OrderingFilter, drf_filters.SearchFilter]
    filterset_class = NotificationFilter
    ordering = ["-created_at", "-id"]
    ordering_fields = ["id", "created_at", "read_at", "scheduled_for", "sent_at", "event_type", "is_read", "lead__id"]
    search_fields = ["title", "body", "event_type", "lead__full_name", "lead__phone"]

    def get_queryset(self):
        queryset = super().get_queryset()
        return queryset.filter(recipient_id=self.request.user.id, status=Notification.Status.SENT)

    @action(detail=False, methods=["get"], url_path="unread-count")
    def unread_count(self, request):
        unread = self.get_queryset().filter(is_read=False).count()
        return Response({"unread_count": unread}, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="mark-read")
    def mark_read(self, request, pk=None):
        instance = self.get_object()
        instance.mark_read(at=timezone.now())
        return Response(NotificationSerializer(instance).data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"], url_path="mark-all-read")
    def mark_all_read(self, request):
        now = timezone.now()
        updated = self.get_queryset().filter(is_read=False).update(is_read=True, read_at=now, updated_at=now)
        return Response({"updated_count": updated}, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get", "patch"], url_path="settings/me")
    def settings_me(self, request):
        role = getattr(request.user, "role", None)
        if request.method.upper() == "PATCH" and role not in {UserRole.TEAMLEADER, UserRole.ADMIN, UserRole.SUPERUSER}:
            return Response({"detail": "Только тимлиды, админы и суперпользователи могут менять настройки уведомлений"}, status=403)

        for event_type in DEFAULT_POLICY_CONFIG.keys():
            get_or_create_policy(event_type)
        allowed_event_types = event_types_for_user(user=request.user)

        if request.method.upper() == "GET":
            prefs_map = {
                pref.event_type: pref
                for pref in NotificationPreference.objects.filter(user_id=request.user.id)
            }
            items = []
            for event_type in allowed_event_types:
                policy = get_or_create_policy(event_type)
                resolved = resolve_user_notification_settings(user=request.user, event_type=event_type)
                pref = prefs_map.get(event_type)
                items.append(
                    {
                        "event_type": event_type,
                        "policy": NotificationPolicySerializer(policy).data,
                        "preference": NotificationPreferenceSerializer(pref).data if pref else None,
                        "resolved": {
                            "enabled": resolved["enabled"],
                            "repeat_minutes": resolved["repeat_minutes"],
                            "watch_scope": resolved["watch_scope"],
                        },
                    }
                )
            return Response({"items": items}, status=200)

        serializer = NotificationPreferenceUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        event_type = serializer.validated_data["event_type"]
        if event_type not in allowed_event_types:
            return Response({"detail": "Этот тип уведомления недоступен для вашей роли"}, status=403)
        get_or_create_policy(event_type)
        pref, _ = NotificationPreference.objects.get_or_create(
            user_id=request.user.id,
            event_type=event_type,
        )
        for field in ("enabled", "repeat_minutes", "watch_scope"):
            if field in serializer.validated_data:
                setattr(pref, field, serializer.validated_data.get(field))
        pref.updated_by_id = request.user.id
        pref.save()
        resolved = resolve_user_notification_settings(user=request.user, event_type=event_type)
        return Response(
            {
                "preference": NotificationPreferenceSerializer(pref).data,
                "resolved": {
                    "enabled": resolved["enabled"],
                    "repeat_minutes": resolved["repeat_minutes"],
                    "watch_scope": resolved["watch_scope"],
                },
            },
            status=200,
        )

    @staticmethod
    def _sse_encode(*, event: str, data: dict | list | str, event_id: int | None = None) -> str:
        if isinstance(data, (dict, list)):
            payload = json.dumps(data, ensure_ascii=False)
        else:
            payload = str(data)
        lines = []
        if event_id is not None:
            lines.append(f"id: {event_id}")
        lines.append(f"event: {event}")
        for line in payload.splitlines() or [""]:
            lines.append(f"data: {line}")
        return "\n".join(lines) + "\n\n"

    @action(detail=False, methods=["get"], url_path="stream")
    def stream(self, request):
        try:
            last_id = int(request.query_params.get("last_id", "0"))
        except (TypeError, ValueError):
            last_id = 0
        if last_id < 0:
            last_id = 0

        try:
            poll_interval = int(request.query_params.get("poll_interval", "3"))
        except (TypeError, ValueError):
            poll_interval = 3
        poll_interval = max(1, min(poll_interval, 30))
        once = (request.query_params.get("once") or "").strip().lower() in {"1", "true", "yes"}

        user_queryset = self.get_queryset()
        user_id = request.user.id

        def event_stream_once():
            current_last_id = last_id
            unread_count = user_queryset.filter(is_read=False).count()
            yield self._sse_encode(event="unread_count", data={"unread_count": unread_count})

            items = list(
                user_queryset.filter(id__gt=current_last_id).order_by("id")[:50]
            )
            if items:
                current_last_id = items[-1].id
                payload = NotificationSerializer(items, many=True).data
                yield self._sse_encode(event="notifications", data={"items": payload}, event_id=current_last_id)

            fresh_unread = user_queryset.filter(is_read=False).count()
            if fresh_unread != unread_count:
                unread_count = fresh_unread
                yield self._sse_encode(event="unread_count", data={"unread_count": unread_count})

        async def event_stream_async():
            current_last_id = last_id
            unread_count = await sync_to_async(self._stream_unread_count, thread_sensitive=True)(user_id)
            yield self._sse_encode(event="unread_count", data={"unread_count": unread_count})

            heartbeat_started = asyncio.get_running_loop().time()
            while True:
                try:
                    payload, new_last_id = await sync_to_async(
                        self._stream_fetch_notifications, thread_sensitive=True
                    )(user_id, current_last_id, 50)
                    if payload:
                        current_last_id = new_last_id
                        yield self._sse_encode(event="notifications", data={"items": payload}, event_id=current_last_id)

                    fresh_unread = await sync_to_async(self._stream_unread_count, thread_sensitive=True)(user_id)
                    if fresh_unread != unread_count:
                        unread_count = fresh_unread
                        yield self._sse_encode(event="unread_count", data={"unread_count": unread_count})

                    now = asyncio.get_running_loop().time()
                    if now - heartbeat_started >= 15:
                        heartbeat_started = now
                        yield ": ping\n\n"

                    await asyncio.sleep(poll_interval)
                except (asyncio.CancelledError, GeneratorExit):
                    break

        response = StreamingHttpResponse(
            event_stream_once() if once else event_stream_async(),
            content_type="text/event-stream",
        )
        response["Cache-Control"] = "no-cache"
        response["X-Accel-Buffering"] = "no"
        return response

    @staticmethod
    def _stream_unread_count(user_id: int) -> int:
        return Notification.objects.filter(
            recipient_id=user_id,
            status=Notification.Status.SENT,
            is_read=False,
        ).count()

    @staticmethod
    def _stream_fetch_notifications(user_id: int, current_last_id: int, limit: int) -> tuple[list[dict], int]:
        items = list(
            Notification.objects.select_related("actor_user", "lead", "recipient")
            .filter(
                recipient_id=user_id,
                status=Notification.Status.SENT,
                id__gt=current_last_id,
            )
            .order_by("id")[:limit]
        )
        if not items:
            return [], current_last_id
        payload = NotificationSerializer(items, many=True).data
        return payload, items[-1].id


class NotificationPolicyViewSet(viewsets.ModelViewSet):
    queryset = NotificationPolicy.objects.all().order_by("event_type")
    serializer_class = NotificationPolicySerializer
    permission_classes = [IsAuthenticated]
    http_method_names = ["get", "patch", "head", "options"]
    lookup_field = "event_type"

    def get_queryset(self):
        for event_type in DEFAULT_POLICY_CONFIG.keys():
            get_or_create_policy(event_type)
        return super().get_queryset()

    def partial_update(self, request, *args, **kwargs):
        role = getattr(request.user, "role", None)
        if role not in {UserRole.ADMIN, UserRole.SUPERUSER}:
            return Response({"detail": "Только админы и суперпользователи могут менять политики уведомлений"}, status=403)
        return super().partial_update(request, *args, **kwargs)
