# LamApp/supermarkets/scheduler.py
"""
Background scheduler with checkpoint-aware retry logic.
Orders run at 6:00 AM on scheduled days.
Product lists update at 3:00 AM based on configuration.
Loss recording runs at 22:30 the day before orders.
Automatic retry of failed operations at 7:00 AM.
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
    
    # Schedule 4: Retry failed operations at 7:00 AM (after initial run)
    _scheduler.add_job(
        retry_failed_operations,
        CronTrigger(hour=7, minute=0),
        id='retry_failed',
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
    logger.info("  - Retry Failed:      Daily at 07:00")
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
    Uses checkpoint-based execution with automatic retry.
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
            
            # Run the full workflow with checkpoint support
            service = AutomatedRestockService(schedule.storage)
            try:
                # Coverage will be calculated automatically based on schedule
                # This now uses checkpoint-based execution
                log = service.run_full_restock_workflow(coverage=None)
                logger.info(
                    f"✓ Successfully completed restock for {schedule.storage.name} "
                    f"(Log #{log.id}: {log.products_ordered} products, {log.total_packages} packages)"
                )
            except Exception as e:
                logger.exception(f" Failed to run restock for {schedule.storage.name}")
                # Log is saved with checkpoint info, will be retried at 7 AM
            finally:
                service.close()
                    
        except Exception as e:
            logger.exception(f"Error checking schedule for {schedule}")
            continue


def retry_failed_operations():
    """
    Automatically retry failed restock operations from their last checkpoint.
    Runs at 7:00 AM, one hour after the main restock run.
    """
    from .models import RestockLog
    from .automation_services import AutomatedRestockService
    from datetime import timedelta
    
    logger.info("[SCHEDULER] Checking for failed operations to retry...")
    
    # Get failed logs from the last 24 hours that can be retried
    yesterday = datetime.datetime.now() - timedelta(days=1)
    
    failed_logs = RestockLog.objects.filter(
        status='failed',
        started_at__gte=yesterday
    ).select_related('storage', 'storage__supermarket')
    
    retry_count = 0
    success_count = 0
    
    for log in failed_logs:
        if not log.can_retry():
            logger.info(
                f"[SCHEDULER] Skipping log #{log.id}: "
                f"max retries ({log.max_retries}) reached"
            )
            continue
        
        try:
            logger.info(
                f"[SCHEDULER] Retrying log #{log.id} for {log.storage.name} "
                f"from checkpoint {log.current_stage}"
            )
            
            service = AutomatedRestockService(log.storage)
            
            try:
                updated_log = service.retry_from_checkpoint(log)
                
                if updated_log.status == 'completed':
                    success_count += 1
                    logger.info(
                        f"✓ [SCHEDULER] Retry successful for log #{log.id} "
                        f"({updated_log.products_ordered} products, "
                        f"{updated_log.total_packages} packages)"
                    )
                else:
                    logger.warning(
                        f" [SCHEDULER] Retry failed for log #{log.id}, "
                        f"will retry again if attempts remaining"
                    )
                
                retry_count += 1
                
            finally:
                service.close()
                
        except Exception as e:
            logger.exception(f"[SCHEDULER] Error retrying log #{log.id}")
            continue
    
    if retry_count > 0:
        logger.info(
            f"[SCHEDULER] Retry summary: {success_count}/{retry_count} successful retries"
        )
    else:
        logger.info("[SCHEDULER] No failed operations to retry")


def run_losses_recording():
    """
    Run losses recording for all SUPERMARKETS that have orders scheduled for tomorrow.
    """
    from .models import Supermarket, RestockSchedule
    from .automation_services import AutomatedRestockService
    from datetime import datetime, timedelta
    
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_weekday = tomorrow.weekday()
    
    weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    tomorrow_field = weekday_fields[tomorrow_weekday]
    
    logger.info(f"[SCHEDULER] Checking for losses recording (tomorrow is {tomorrow_field})")
    
    supermarkets_to_process = set()
    
    for schedule in RestockSchedule.objects.select_related('storage__supermarket').all():
        is_order_tomorrow = getattr(schedule, tomorrow_field)
        
        if is_order_tomorrow:
            supermarkets_to_process.add(schedule.storage.supermarket)
    
    logger.info(f"[SCHEDULER] Found {len(supermarkets_to_process)} supermarket(s) with orders tomorrow")
    
    for supermarket in supermarkets_to_process:
        try:
            logger.info(f"[SCHEDULER] Recording losses for: {supermarket.name}")
            
            first_storage = supermarket.storages.first()
            
            if not first_storage:
                logger.warning(f"[SCHEDULER] No storages found for {supermarket.name}")
                continue
            
            service = AutomatedRestockService(first_storage)
            service.record_losses()
            service.close()
            
            logger.info(f"✓ [SCHEDULER] Losses recorded for {supermarket.name}")
            
        except Exception as e:
            logger.exception(f" [SCHEDULER] Failed to record losses for {supermarket.name}")
            continue


def run_list_updates():
    """Check and run product list updates"""
    from .list_update_service import run_scheduled_list_updates
    
    try:
        logger.info("[SCHEDULER] Starting list updates check")
        run_scheduled_list_updates()
        logger.info("[SCHEDULER] List updates check completed")
    except Exception as e:
        logger.exception("[SCHEDULER] Error during list updates")