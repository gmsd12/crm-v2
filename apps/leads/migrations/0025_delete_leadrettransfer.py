from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0024_remove_lead_transferred_to_ret_at"),
    ]

    operations = [
        migrations.DeleteModel(
            name="LeadRetTransfer",
        ),
    ]
