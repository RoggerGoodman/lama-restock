# LamApp/supermarkets/scheduler.py
"""
Background scheduler for automated restock operations.
Orders run at 6:00 AM on scheduled days.
Product lists update at 3:00 AM based on configuration.
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
import logging
import atexit

logger = logging.getLogger(__name__)


def start():
    """
    Start the background scheduler.
    This is called when Django starts (in apps.py ready() method).
    """
    scheduler = BackgroundScheduler()
    
    # Schedule 1: Check for restock orders daily at 6:00 AM
    scheduler.add_job(
        check_and_run_restock_orders,
        CronTrigger(hour=6, minute=0),
        id='restock_orders',
        replace_existing=True
    )
    
    # Schedule 2: Run losses recording daily at 22:30
    scheduler.add_job(
        run_losses_recording,
        CronTrigger(hour=22, minute=30),
        id='losses_recording',
        replace_existing=True
    )
    
    # Schedule 3: Check for product list updates daily at 3:00 AM
    scheduler.add_job(
        run_list_updates,
        CronTrigger(hour=3, minute=0),
        id='list_updates',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started - Orders: 6AM, Losses: 22:30, List Updates: 3AM")
    
    # Make sure the scheduler stops when Django stops
    atexit.register(lambda: scheduler.shutdown())


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
                logger.info(f"Successfully completed restock for {schedule.storage.name}")
            except Exception as e:
                logger.exception(f"Failed to run restock for {schedule.storage.name}")
            finally:
                service.close()
                    
        except Exception as e:
            logger.exception(f"Error checking schedule for {schedule}")
            continue


def run_losses_recording():
    """
    Run losses recording for all storages that have orders scheduled for tomorrow.
    This runs once daily at 22:30.
    """
    from .models import Storage, RestockSchedule
    from .automation_services import AutomatedRestockService
    from datetime import datetime, timedelta
    
    # Get tomorrow's weekday
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_weekday = tomorrow.weekday()  # 0=Monday, 6=Sunday
    
    # Map weekday to field name
    weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    tomorrow_field = weekday_fields[tomorrow_weekday]
    
    logger.info(f"Checking for losses recording (tomorrow is {tomorrow_field})")
    
    # Find all schedules where tomorrow is an order day
    storages_to_process = []
    
    for storage in Storage.objects.select_related('schedule', 'supermarket'):
        try:
            schedule = storage.schedule
            # Check if tomorrow is an order day
            is_order_tomorrow = getattr(schedule, tomorrow_field)
            
            if is_order_tomorrow:
                storages_to_process.append(storage)
                
        except RestockSchedule.DoesNotExist:
            continue
    
    logger.info(f"Found {len(storages_to_process)} storages with orders tomorrow")
    
    # Process each storage
    for storage in storages_to_process:
        try:
            logger.info(f"Recording losses for {storage.name}")
            service = AutomatedRestockService(storage)
            service.record_losses()
            service.close()
            logger.info(f"Successfully recorded losses for {storage.name}")
            
        except Exception as e:
            logger.exception(f"Failed to record losses for {storage.name}")
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
