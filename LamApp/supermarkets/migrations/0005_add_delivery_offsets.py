# LamApp/supermarkets/migrations/0004_add_delivery_offsets.py
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('supermarkets', '0004_remove_listupdateschedule'),
    ]

    operations = [
        migrations.AddField(
            model_name='restockschedule',
            name='monday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Monday order (0=same day, 1=next day, etc.)'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='tuesday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Tuesday order'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='wednesday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Wednesday order'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='thursday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Thursday order'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='friday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Friday order'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='saturday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Saturday order'
            ),
        ),
        migrations.AddField(
            model_name='restockschedule',
            name='sunday_delivery_offset',
            field=models.IntegerField(
                default=1,
                help_text='Days until delivery after Sunday order'
            ),
        ),
    ]