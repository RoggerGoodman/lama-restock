# LamApp/supermarkets/scheduler.py
"""
Background scheduler for automated restock operations.
Orders run at 6:00 AM on scheduled days.
Product lists update at 3:00 AM based on configuration.
Loss recording runs at 22:30 the day before orders.
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
import logging
import atexit
import os

logger = logging.getLogger(__name__)

# Global flag to prevent multiple scheduler instances
_scheduler = None


def start():
    """
    Start the background scheduler.
    PRODUCTION-SAFE: Only starts in ONE process.
    """
    global _scheduler
    
    # Prevent multiple instances
    if _scheduler is not None:
        logger.warning("Scheduler already running, skipping initialization")
        return
    
    # Skip for management commands (except runserver)
    import sys
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command in ['makemigrations', 'migrate', 'shell', 'createsuperuser', 
                       'test', 'collectstatic']:
            logger.info(f"Skipping scheduler for command: {command}")
            return
    
    # For runserver: only start in reloader process
    if len(sys.argv) > 1 and sys.argv[1] == 'runserver':
        if os.environ.get('RUN_MAIN') != 'true':
            logger.info("Skipping scheduler in main runserver process")
            return
    
    # PRODUCTION SAFETY: Check if scheduler should run in this worker
    # Only start if ENABLE_SCHEDULER is not explicitly set to 'false'
    enable_scheduler = os.environ.get('ENABLE_SCHEDULER', 'true').lower()
    if enable_scheduler == 'false':
        logger.info("Scheduler disabled via ENABLE_SCHEDULER environment variable")
        return
    
    # PRODUCTION SAFETY: Use worker ID to ensure only one worker runs scheduler
    worker_id = os.environ.get('WORKER_ID', '1')
    if worker_id != '1':
        logger.info(f"Scheduler skipped for worker {worker_id} (only worker 1 runs scheduler)")
        return
    
    _scheduler = BackgroundScheduler()
    
    # Schedule 1: Check for restock orders daily at 6:00 AM
    _scheduler.add_job(
        check_and_run_restock_orders,
        CronTrigger(hour=6, minute=0),
        id='restock_orders',
        replace_existing=True
    )
    
    # Schedule 2: Run losses recording daily at 22:30
    _scheduler.add_job(
        run_losses_recording,
        CronTrigger(hour=22, minute=30),
        id='losses_recording',
        replace_existing=True
    )
    
    # Schedule 3: Check for product list updates daily at 3:00 AM
    _scheduler.add_job(
        run_list_updates,
        CronTrigger(hour=3, minute=0),
        id='list_updates',
        replace_existing=True
    )
    
    _scheduler.start()
    logger.info("="*60)
    logger.info("SCHEDULER STARTED SUCCESSFULLY")
    logger.info("="*60)
    logger.info("Scheduled jobs:")
    logger.info("  - Restock Orders:    Daily at 06:00")
    logger.info("  - Loss Recording:    Daily at 22:30")
    logger.info("  - List Updates:      Daily at 03:00")
    logger.info("="*60)
    
    atexit.register(lambda: shutdown_scheduler())


def shutdown_scheduler():
    """Safely shutdown the scheduler"""
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown()
        _scheduler = None
        logger.info("Scheduler shutdown complete")


def check_and_run_restock_orders():
    """
    Check if any restock orders should run today.
    Runs at 6:00 AM every day.
    """
    from .models import RestockSchedule
    from .automation_services import AutomatedRestockService
    
    now = datetime.datetime.now()
    current_weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    # Map weekday to field name
    weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    today_field = weekday_fields[current_weekday]
    
    logger.info(f"Checking for orders on {today_field} at {now.strftime('%H:%M')}")
    
    # Find all schedules where today is enabled
    all_schedules = RestockSchedule.objects.select_related('storage', 'storage__supermarket').all()
    
    for schedule in all_schedules:
        try:
            # Check if today is an order day
            is_order_day = getattr(schedule, today_field)
            
            if not is_order_day:
                continue
            
            logger.info(f"Running restock for {schedule.storage.name} (order day: {today_field})")
            
            # Run the full workflow
            service = AutomatedRestockService(schedule.storage)
            try:
                # Coverage will be calculated automatically based on schedule
                service.run_full_restock_workflow(coverage=None)
                logger.info(f" Successfully completed restock for {schedule.storage.name}")
            except Exception as e:
                logger.exception(f"✗ Failed to run restock for {schedule.storage.name}")
            finally:
                service.close()
                    
        except Exception as e:
            logger.exception(f"Error checking schedule for {schedule}")
            continue


def run_losses_recording():
    """
    Run losses recording for all SUPERMARKETS that have orders scheduled for tomorrow.
    
    CRITICAL: This runs ONCE PER SUPERMARKET, not once per storage.
    Downloads losses for ALL storages in a supermarket in one operation.
    
    This runs once daily at 22:30.
    """
    from .models import Supermarket, RestockSchedule
    from .automation_services import AutomatedRestockService
    from datetime import datetime, timedelta
    
    # Get tomorrow's weekday
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_weekday = tomorrow.weekday()  # 0=Monday, 6=Sunday
    
    # Map weekday to field name
    weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    tomorrow_field = weekday_fields[tomorrow_weekday]
    
    logger.info(f"Checking for losses recording (tomorrow is {tomorrow_field})")
    
    # Get all supermarkets that have at least one order tomorrow
    supermarkets_to_process = set()
    
    for schedule in RestockSchedule.objects.select_related('storage__supermarket').all():
        # Check if tomorrow is an order day
        is_order_tomorrow = getattr(schedule, tomorrow_field)
        
        if is_order_tomorrow:
            supermarkets_to_process.add(schedule.storage.supermarket)
    
    logger.info(f"Found {len(supermarkets_to_process)} supermarket(s) with orders tomorrow")
    
    # Process each supermarket ONCE
    for supermarket in supermarkets_to_process:
        try:
            logger.info(f"Recording losses for supermarket: {supermarket.name}")
            
            # Get the first storage for this supermarket (they share the same credentials)
            first_storage = supermarket.storages.first()
            
            if not first_storage:
                logger.warning(f"No storages found for {supermarket.name}")
                continue
            
            # Use automation service to record losses
            service = AutomatedRestockService(first_storage)
            service.record_losses()
            service.close()
            
            logger.info(f" Successfully recorded losses for {supermarket.name}")
            
        except Exception as e:
            logger.exception(f"✗ Failed to record losses for {supermarket.name}")
            continue


def run_list_updates():
    """
    Check and run product list updates for storages that need it.
    Runs daily at 3:00 AM.
    """
    from .list_update_service import run_scheduled_list_updates
    
    try:
        logger.info("Starting scheduled list updates check")
        run_scheduled_list_updates()
        logger.info("Completed scheduled list updates check")
    except Exception as e:
        logger.exception("Error during scheduled list updates")
