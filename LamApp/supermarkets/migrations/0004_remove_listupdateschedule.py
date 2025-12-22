# LamApp/supermarkets/migrations/0004_remove_listupdateschedule.py
# Migration to remove obsolete ListUpdateSchedule model

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('supermarkets', '0003_add_checkpoint_fields'),
    ]

    operations = [
        # Remove ListUpdateSchedule model (now automatic for all scheduled storages)
        migrations.DeleteModel(
            name='ListUpdateSchedule',
        ),
        
        # Remove ListUpdateLog model (no longer needed)
        migrations.DeleteModel(
            name='ListUpdateLog',
        ),
    ]