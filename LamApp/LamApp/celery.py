# LamApp/LamApp/celery.py
"""
Celery configuration for LamaRestock project.
This file configures Celery to work with Django and Redis.
"""
import os
from celery import Celery
from celery.schedules import crontab

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'LamApp.settings')

# Create Celery app
app = Celery('LamApp')

# Load config from Django settings with 'CELERY' prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()

# Configure Celery Beat schedule
app.conf.beat_schedule = {
    # Loss recording at 22:30 every day
    'record-losses-nightly': {
        'task': 'supermarkets.tasks.record_losses_all_supermarkets',
        'schedule': crontab(hour=22, minute=30),
    },
    
    # Stats update at 5:00 AM every day
    'update-stats-morning': {
        'task': 'supermarkets.tasks.update_stats_all_scheduled_storages',
        'schedule': crontab(hour=5, minute=0),
    },
    
    # List updates check at 3:00 AM every day
    'check-list-updates': {
        'task': 'supermarkets.tasks.run_scheduled_list_updates',
        'schedule': crontab(hour=3, minute=0),
    },
}

# Celery configuration
app.conf.update(
    # Time zone
    timezone='UTC',
    enable_utc=True,
    
    # Task configuration
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Result backend (optional - stores task results)
    result_backend='redis://localhost:6379/1',
    
    # Task time limits (30 minutes max per task)
    task_time_limit=1800,
    task_soft_time_limit=1700,
    
    # Worker configuration
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
)

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')