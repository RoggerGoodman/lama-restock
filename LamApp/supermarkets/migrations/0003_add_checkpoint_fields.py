# LamApp/supermarkets/migrations/0003_add_checkpoint_fields.py
# Generated migration for checkpoint fields

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('supermarkets', '0002_storage_last_list_update_listupdatelog_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='restocklog',
            name='current_stage',
            field=models.CharField(
                choices=[
                    ('pending', 'Pending Start'),
                    ('updating_stats', 'Updating Product Stats'),
                    ('stats_updated', 'Stats Updated'),
                    ('calculating_order', 'Calculating Order'),
                    ('order_calculated', 'Order Calculated'),
                    ('executing_order', 'Executing Order'),
                    ('completed', 'Completed'),
                    ('failed', 'Failed'),
                ],
                default='pending',
                max_length=30
            ),
        ),
        migrations.AddField(
            model_name='restocklog',
            name='stats_updated_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='restocklog',
            name='order_calculated_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='restocklog',
            name='order_executed_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='restocklog',
            name='retry_count',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='restocklog',
            name='max_retries',
            field=models.IntegerField(default=3),
        ),
    ]