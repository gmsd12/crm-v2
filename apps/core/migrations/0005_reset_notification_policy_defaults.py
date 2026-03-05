from django.db import migrations


def set_policy_defaults(apps, schema_editor):
    NotificationPolicy = apps.get_model("core", "NotificationPolicy")

    defaults = {
        "lead_assigned": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "comment_added": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "lead_unassigned": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "lead_status_changed": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "deposit_created": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "manager_no_activity": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "partner_duplicate_attempt": {
            "enabled_by_default": True,
            "default_repeat_minutes": 60,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
        "next_contact_overdue": {
            "enabled_by_default": True,
            "default_repeat_minutes": 15,
            "default_watch_scope": "own",
            "apply_to_teamleaders": True,
            "apply_to_admins": True,
            "apply_to_superusers": True,
        },
    }

    for event_type, cfg in defaults.items():
        NotificationPolicy.objects.update_or_create(
            event_type=event_type,
            defaults=cfg,
        )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0004_alter_notification_recipient"),
    ]

    operations = [
        migrations.RunPython(set_policy_defaults, noop_reverse),
    ]
