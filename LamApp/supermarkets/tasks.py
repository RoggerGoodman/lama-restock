# LamApp/supermarkets/tasks.py
"""
Celery tasks for automated operations.
Replaces scheduler.py with proper distributed task queue.
"""
from celery import shared_task
from celery.utils.log import get_task_logger
import datetime
from django.utils import timezone

logger = get_task_logger(__name__)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900  # 15 minutes in seconds
)
def record_losses_all_supermarkets(self):
    """
    Record losses for all supermarkets that have orders scheduled for tomorrow.
    Runs at 22:30 every day.
    
    CRITICAL: This uses database connections, so each supermarket is processed separately
    to avoid SQLite threading issues.
    """
    from .models import Supermarket, RestockSchedule
    from .automation_services import AutomatedRestockService
    
    try:
        tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
        tomorrow_weekday = tomorrow.weekday()
        
        weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        tomorrow_field = weekday_fields[tomorrow_weekday]
        
        logger.info(f"[CELERY] Checking for losses recording (tomorrow is {tomorrow_field})")
        
        supermarkets_to_process = set()
        
        for schedule in RestockSchedule.objects.select_related('storage__supermarket').all():
            is_order_tomorrow = getattr(schedule, tomorrow_field)
            
            if is_order_tomorrow:
                supermarkets_to_process.add(schedule.storage.supermarket)
        
        logger.info(f"[CELERY] Found {len(supermarkets_to_process)} supermarket(s) with orders tomorrow")
        
        success_count = 0
        error_count = 0
        
        for supermarket in supermarkets_to_process:
            try:
                logger.info(f"[CELERY] Recording losses for: {supermarket.name}")
                
                first_storage = supermarket.storages.first()
                
                if not first_storage:
                    logger.warning(f"[CELERY] No storages found for {supermarket.name}")
                    continue
                
                service = AutomatedRestockService(first_storage)
                
                try:
                    service.record_losses()
                    logger.info(f"✓ [CELERY] Losses recorded for {supermarket.name}")
                    success_count += 1
                except Exception as e:
                    logger.exception(f"✗ [CELERY] Failed to record losses for {supermarket.name}")
                    error_count += 1
                finally:
                    service.close()
                    
            except Exception as e:
                logger.exception(f"✗ [CELERY] Error processing {supermarket.name}")
                error_count += 1
                continue
        
        result_msg = f"Loss recording complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")
        
        if error_count > 0 and success_count == 0:
            # All failed - retry the entire task
            raise Exception(f"All loss recordings failed ({error_count} supermarkets)")
        
        return result_msg
        
    except Exception as exc:
        logger.exception("[CELERY] Fatal error in loss recording task")
        # Retry with exponential backoff
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900  # 15 minutes
)
def update_stats_all_scheduled_storages(self):
    """
    Update product stats for all storages with active schedules at 5:00 AM.
    After successful update, trigger order tasks for storages with orders today.
    
    CRITICAL: Each storage is processed in its own subtask to isolate database connections.
    """
    from .models import Storage, RestockSchedule
    from django.db.models import Q
    
    try:
        logger.info("[CELERY] Starting morning stats update for all scheduled storages")
        
        # Get all storages with active schedules
        storages = Storage.objects.filter(
            schedule__isnull=False
        ).select_related('schedule', 'supermarket')
        
        if not storages.exists():
            logger.info("[CELERY] No storages with schedules found")
            return "No storages to process"
        
        logger.info(f"[CELERY] Found {storages.count()} storage(s) with schedules")
        
        # Update stats for each storage sequentially
        # This ensures database connections are properly managed
        for storage in storages:
            try:
                logger.info(f"[CELERY] Updating stats for {storage.name}")
                
                # Call subtask to update stats
                update_stats_for_storage.apply_async(
                    args=[storage.id],
                    retry=True,
                    retry_policy={
                        'max_retries': 3,
                        'interval_start': 900,  # 15 min
                        'interval_step': 0,
                        'interval_max': 900,
                    }
                )
                
            except Exception as e:
                logger.exception(f"[CELERY] Error queuing stats update for {storage.name}")
                continue
        
        logger.info("[CELERY] All stats update tasks queued successfully")
        
        # After stats are updated, trigger order checks
        # This runs AFTER the stats updates complete
        check_and_run_orders_today.apply_async(countdown=600)  # Wait 10 minutes for stats to finish
        
        return f"Stats update queued for {storages.count()} storages"
        
    except Exception as exc:
        logger.exception("[CELERY] Fatal error in stats update task")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def update_stats_for_storage(self, storage_id):
    """
    Update product stats for a single storage.
    This is a subtask called by update_stats_all_scheduled_storages.
    
    CRITICAL: Creates its own database connection to avoid threading issues.
    """
    from .models import Storage
    from .automation_services import AutomatedRestockService
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[CELERY-SUBTASK] Updating stats for {storage.name}")
        
        service = AutomatedRestockService(storage)
        
        try:
            # Create a log to track the update
            from .models import RestockLog
            log = RestockLog.objects.create(
                storage=storage,
                status='processing',
                current_stage='updating_stats'
            )
            
            service.update_product_stats_checkpoint(log)
            
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.save()
            
            logger.info(f"✓ [CELERY-SUBTASK] Stats updated for {storage.name}")
            return f"Stats updated for {storage.name}"
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[CELERY-SUBTASK] Error updating stats for storage {storage_id}")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def check_and_run_orders_today(self):
    """
    Check which storages have orders scheduled for today and run them.
    This is called AFTER stats are updated (at ~5:10 AM).
    
    CRITICAL: Each order is run in its own subtask to isolate database connections.
    """
    from .models import RestockSchedule
    
    try:
        now = datetime.datetime.now()
        current_weekday = now.weekday()  # 0=Monday, 6=Sunday
        
        weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        today_field = weekday_fields[current_weekday]
        
        logger.info(f"[CELERY] Checking for orders on {today_field} at {now.strftime('%H:%M')}")
        
        all_schedules = RestockSchedule.objects.select_related('storage', 'storage__supermarket').all()
        
        orders_queued = 0
        
        for schedule in all_schedules:
            try:
                is_order_day = getattr(schedule, today_field)
                
                if not is_order_day:
                    continue
                
                logger.info(f"[CELERY] Queueing restock for {schedule.storage.name} (order day: {today_field})")
                
                # Queue the order task
                run_restock_for_storage.apply_async(
                    args=[schedule.storage.id],
                    retry=True,
                    retry_policy={
                        'max_retries': 3,
                        'interval_start': 900,
                        'interval_step': 0,
                        'interval_max': 900,
                    }
                )
                
                orders_queued += 1
                    
            except Exception as e:
                logger.exception(f"[CELERY] Error checking schedule for {schedule}")
                continue
        
        result_msg = f"Queued {orders_queued} restock orders for today"
        logger.info(f"[CELERY] {result_msg}")
        
        return result_msg
        
    except Exception as exc:
        logger.exception("[CELERY] Fatal error in order check task")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def run_restock_for_storage(self, storage_id):
    """
    Run full restock workflow for a single storage.
    This is a subtask called by check_and_run_orders_today.
    
    Uses checkpoint-based execution with automatic retry.
    """
    from .models import Storage
    from .automation_services import AutomatedRestockService
    
    try:
        storage = Storage.objects.select_related('supermarket', 'schedule').get(id=storage_id)
        
        logger.info(f"[CELERY-ORDER] Running restock for {storage.name}")
        
        service = AutomatedRestockService(storage)
        
        try:
            # Coverage will be calculated automatically based on schedule
            log = service.run_full_restock_workflow(coverage=None)
            
            logger.info(
                f"✓ [CELERY-ORDER] Successfully completed restock for {storage.name} "
                f"(Log #{log.id}: {log.products_ordered} products, {log.total_packages} packages)"
            )
            
            return f"Restock completed for {storage.name}: {log.products_ordered} products, {log.total_packages} packages"
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[CELERY-ORDER] Error running restock for storage {storage_id}")
        
        # Log will have checkpoint info for manual retry if needed
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def run_scheduled_list_updates(self):
    """
    Check and run scheduled product list updates.
    Runs at 3:00 AM every day.
    """
    from .list_update_service import run_scheduled_list_updates as run_updates
    
    try:
        logger.info("[CELERY] Starting scheduled list updates check")
        
        run_updates()
        
        logger.info("[CELERY] List updates check completed")
        
        return "List updates completed"
        
    except Exception as exc:
        logger.exception("[CELERY] Error during list updates")
        raise self.retry(exc=exc)