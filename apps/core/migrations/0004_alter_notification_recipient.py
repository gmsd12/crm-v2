from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0003_notificationpolicy_notificationpreference"),
    ]

    operations = [
        migrations.AlterField(
            model_name="notification",
            name="recipient",
            field=models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="notifications", to="iam.user"),
        ),
    ]
