# LamApp/supermarkets/tasks.py
"""
Celery tasks for automated operations.
Replaces scheduler.py with proper distributed task queue.
"""
from celery import shared_task
from celery.utils.log import get_task_logger
import datetime
from django.utils import timezone
from django.conf import settings
from .scripts.logger import logger
import os


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900  # 15 minutes in seconds
)
def record_losses_all_supermarkets(self):
    """
    Record losses for ALL supermarkets every night at 22:30.
    
    This runs daily regardless of order schedules because losses 
    (broken/expired/internal use) happen every day and need to be tracked.
    """
    from .models import Supermarket
    from .automation_services import AutomatedRestockService
    
    try:
        logger.info("[CELERY] Starting nightly loss recording for all supermarkets")
        
        # Get ALL supermarkets (not just those with orders tomorrow)
        supermarkets = Supermarket.objects.all()
        
        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"
        
        logger.info(f"[CELERY] Found {supermarkets.count()} supermarket(s) to process")
        
        success_count = 0
        error_count = 0
        
        for supermarket in supermarkets:
            try:
                logger.info(f"[CELERY] Recording losses for: {supermarket.name}")
                
                first_storage = supermarket.storages.first()
                
                if not first_storage:
                    logger.warning(f"[CELERY] No storages found for {supermarket.name}")
                    error_count += 1
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
        
        result_msg = f"Loss recording complete: {success_count} successful, {error_count} failed out of {supermarkets.count()} total"
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
    default_retry_delay=900,
    queue='selenium'
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
    default_retry_delay=900,
    queue='selenium'
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
    default_retry_delay=900,
    queue='selenium'
)
def run_scheduled_list_updates(self):
    """
    Update product lists for ALL storages with active order schedules.
    Runs at 3:00 AM every day.
    
    This ensures product lists are always fresh for order calculations.
    """
    from .models import Storage
    from .list_update_service import ListUpdateService
    
    try:
        logger.info("[CELERY] Starting automatic list updates for scheduled storages")
        
        # Get all storages that have active order schedules
        storages = Storage.objects.filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')
        
        if not storages.exists():
            logger.info("[CELERY] No storages with schedules found")
            return "No storages to update"
        
        logger.info(f"[CELERY] Found {storages.count()} storage(s) with schedules")
        
        success_count = 0
        error_count = 0
        
        for storage in storages:
            try:
                logger.info(f"[CELERY] Updating product list for {storage.name}")
                
                service = ListUpdateService(storage)
                
                try:
                    result = service.update_and_import()
                    
                    if result['success']:
                        logger.info(f"✓ [CELERY] List updated for {storage.name}")
                        success_count += 1
                    else:
                        logger.warning(f"⚠ [CELERY] List update failed for {storage.name}: {result['message']}")
                        error_count += 1
                        
                finally:
                    service.close()
                    
            except Exception as e:
                logger.exception(f"✗ [CELERY] Error updating list for {storage.name}")
                error_count += 1
                continue
        
        result_msg = f"List updates complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")
        
        return result_msg
        
    except Exception as exc:
        logger.exception("[CELERY] Fatal error in list update task")
        raise self.retry(exc=exc)
    

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium'
)
def manual_restock_task(self, storage_id, coverage=None):
    """
    User-initiated restock (replaces synchronous run_restock_view).
    
    Benefits:
    - Doesn't block Gunicorn workers
    - Proper progress tracking
    - Automatic retry on failure
    - Can run 5-15 minutes without timeout
    """
    from .models import Storage, RestockLog
    from .automation_services import AutomatedRestockService
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket', 'schedule').get(id=storage_id)
        
        logger.info(f"[MANUAL RESTOCK] Starting for {storage.name}")
        
        # Create log (will be updated by service)
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            current_stage='pending',
            started_at=timezone.now()
        )
        
        service = AutomatedRestockService(storage)
        
        try:
            # Run full workflow with checkpoint support
            service.run_full_restock_workflow(coverage=coverage, log=log)
            
            logger.info(
                f"✅ [MANUAL RESTOCK] Completed for {storage.name} "
                f"(Log #{log.id}: {log.products_ordered} products)"
            )
            
            return {
                'success': True,
                'log_id': log.id,
                'products_ordered': log.products_ordered,
                'total_packages': log.total_packages
            }
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[MANUAL RESTOCK] Error for storage {storage_id}")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium'
)
def manual_stats_update_task(self, storage_id):
    """
    Update stats without ordering (replaces update_stats_only_view).
    
    This is one of the slowest operations (5-10 minutes).
    Moving to Celery prevents Gunicorn timeouts.
    """
    from .models import Storage, RestockLog
    from .automation_services import AutomatedRestockService
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[MANUAL STATS] Starting for {storage.name}")
        
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            current_stage='updating_stats'
        )
        
        service = AutomatedRestockService(storage)
        
        try:
            service.update_product_stats_checkpoint(log, manual=True)
            
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.save()
            
            logger.info(f"✅ [MANUAL STATS] Completed for {storage.name}")
            
            return {
                'success': True,
                'log_id': log.id,
                'storage_name': storage.name
            }
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[MANUAL STATS] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium'
)
def add_products_unified_task(self, storage_id, products_list, settore):
    """    
    Args:
        storage_id: Storage ID
        products_list: List of (cod, var) tuples
        settore: Settore name
    """
    from .models import Storage
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from pathlib import Path
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[ADD PRODUCTS] Starting for {storage.name}: {len(products_list)} products")
        
        service = RestockService(storage)
        
        # Create WebLister instance
        temp_dir = Path(settings.BASE_DIR) / 'temp_add_products'
        temp_dir.mkdir(exist_ok=True)
        
        lister = WebLister(
            username=storage.supermarket.username,
            password=storage.supermarket.password,
            storage_name=storage.name,
            download_dir=str(temp_dir),
            headless=True
        )
        
        try:
            lister.login()
            
            added = []
            failed = []
            
            for cod, var in products_list:
                try:
                    logger.info(f"[ADD PRODUCTS] Fetching {cod}.{var}...")
                    
                    # Use gather_missing_product_data for consistency
                    product_data = lister.gather_missing_product_data(cod, var)
                    
                    if not product_data:
                        logger.warning(f"[ADD PRODUCTS] Product {cod}.{var} not found")
                        failed.append((cod, var, "Not found in PAC2000A"))
                        continue
                    
                    description, package, multiplier, availability, cost, price, category = product_data
                    
                    # Add to database
                    service.db.add_product(
                        cod=cod,
                        v=var,
                        descrizione=description or f"Product {cod}.{var}",
                        rapp=multiplier or 1,
                        pz_x_collo=package or 12,
                        settore=settore,
                        disponibilita=availability or "Si"
                    )
                    
                    # Initialize stats
                    service.db.init_product_stats(cod, var, [], [], 0, False)
                    
                    # Add economics
                    if price and cost:
                        cur = service.db.cursor()
                        cur.execute("""
                            INSERT INTO economics (cod, v, price_std, cost_std, category)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (cod, v) DO NOTHING
                        """, (cod, var, price, cost, category or "Unknown"))
                        service.db.conn.commit()
                    
                    added.append((cod, var))
                    logger.info(f"[ADD PRODUCTS] ✅ Added {cod}.{var}")
                    
                except Exception as e:
                    logger.exception(f"[ADD PRODUCTS] Error adding {cod}.{var}")
                    failed.append((cod, var, str(e)))
            
            logger.info(f"[ADD PRODUCTS] ✅ Complete: {len(added)} added, {len(failed)} failed")
            
            return {
                'success': True,
                'products_added': len(added),
                'products_failed': len(failed),
                'added': added[:50],
                'failed': failed[:20],
                'storage_name': storage.name
            }
            
        finally:
            lister.driver.quit()
            service.close()
            
    except Exception as exc:
        logger.exception(f"[ADD PRODUCTS] Error for storage {storage_id}")
        raise self.retry(exc=exc)

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium'
)
def manual_list_update_task(self, storage_id):
    """
    Manual list update (replaces manual_list_update_view).
    
    Can take 5-10 minutes to download and import.
    """
    from .models import Storage
    from .list_update_service import ListUpdateService
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[LIST UPDATE] Starting for {storage.name}")
        
        service = ListUpdateService(storage)
        
        try:
            result = service.update_and_import()
            
            if result['success']:
                logger.info(f"✅ [LIST UPDATE] Completed for {storage.name}")
            else:
                logger.warning(f"⚠️ [LIST UPDATE] Failed for {storage.name}: {result['message']}")
            
            return result
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[LIST UPDATE] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=600
)
def assign_clusters_task(self, storage_id, pdf_file_path, cluster):
    """
    UPDATED: Assign cluster to products from PDF (not CSV).
    
    Args:
        storage_id: Storage ID
        pdf_file_path: Full path to PDF file
        cluster: Cluster name (REQUIRED, user-provided)
    """
    from .models import Storage
    from .services import RestockService
    from .scripts.inventory_reader import assign_clusters_from_pdf
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[ASSIGN CLUSTERS] Starting for {storage.name}: cluster='{cluster}'")
        
        service = RestockService(storage)
        
        try:
            result = assign_clusters_from_pdf(service.db, pdf_file_path, cluster)
            
            if result['success']:
                logger.info(
                    f"✅ [ASSIGN CLUSTERS] Completed: "
                    f"{result['assigned']} assigned, {result['skipped']} skipped"
                )
            else:
                logger.error(f"❌ [ASSIGN CLUSTERS] Failed: {result['error']}")
            
            return {
                'success': result['success'],
                'storage_name': storage.name,
                'cluster': cluster,
                'assigned': result.get('assigned', 0),
                'skipped': result.get('skipped', 0),
                'error': result.get('error')
            }
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[ASSIGN CLUSTERS] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def process_promos_task(self, supermarket_id, pdf_file_path):
    """
    Process promo PDF file.
    
    Args:
        supermarket_id: Supermarket ID
        pdf_file_path: Full path to PDF file
    """
    from .models import Supermarket
    from .services import RestockService
    from pathlib import Path
    import os
    
    try:
        supermarket = Supermarket.objects.get(id=supermarket_id)
        
        logger.info(f"[PROCESS PROMOS] Starting for {supermarket.name}")
        
        storage = supermarket.storages.first()
        
        if not storage:
            raise ValueError(f"No storages found for {supermarket.name}")
        
        service = RestockService(storage)
        
        try:
            # Parse PDF
            promo_list = service.helper.parse_promo_pdf(pdf_file_path)
            
            # Update database
            service.db.update_promos(promo_list)
            
            logger.info(f"✅ [PROCESS PROMOS] Completed: {len(promo_list)} promo items")
            
            # Clean up file
            try:
                os.remove(pdf_file_path)
                logger.info(f"Deleted temp file: {pdf_file_path}")
            except Exception as e:
                logger.warning(f"Could not delete temp file: {e}")
            
            return {
                'success': True,
                'supermarket_name': supermarket.name,
                'promo_count': len(promo_list)
            }
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[PROCESS PROMOS] Error for supermarket {supermarket_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    queue='selenium'
)
def verify_stock_with_auto_add_task(self, storage_id, pdf_file_path, cluster=None):
    """
    ENHANCED: Bulk stock verification with automatic product addition.
    
    Workflow:
    1. Parse PDF to get all products and their stock levels
    2. Record losses (broken/expired/internal)
    3. Separate products into: existing vs missing
    4. For missing products: Auto-fetch data using gather_missing_product_data
    5. Add missing products to database
    6. Verify stock for ALL products (existing + newly added)
    7. Return comprehensive report
    
    Args:
        storage_id: Storage ID
        pdf_file_path: Full path to PDF file
        cluster: Optional cluster name (user-provided)
    """
    from .models import Storage
    from .automation_services import AutomatedRestockService
    from .scripts.inventory_reader import parse_pdf
    from .scripts.web_lister import WebLister
    from pathlib import Path
    import pandas as pd
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[VERIFY+AUTO-ADD] Starting for {storage.name}")
        if cluster:
            logger.info(f"[VERIFY+AUTO-ADD] Cluster: {cluster}")
        
        service = AutomatedRestockService(storage)
        
        try:
            # STEP 1: Record losses first (as before)
            logger.info(f"[VERIFY+AUTO-ADD] Step 1: Recording losses...")
            service.record_losses()
            
            # STEP 2: Parse PDF to get all products
            logger.info(f"[VERIFY+AUTO-ADD] Step 2: Parsing PDF...")
            parsed_entries = parse_pdf(pdf_file_path)
            
            if not parsed_entries:
                return {
                    'success': False,
                    'error': 'No valid entries found in PDF'
                }
            
            logger.info(f"[VERIFY+AUTO-ADD] Found {len(parsed_entries)} products in PDF")
            
            # STEP 3: Separate existing vs missing products
            existing_products = []
            missing_products = []
            
            for entry in parsed_entries:
                cod = entry['cod']
                var = entry['v']
                qty = entry['qty']
                
                try:
                    # Check if product exists
                    service.db.get_stock(cod, var)
                    existing_products.append((cod, var, qty))
                except ValueError:
                    # Product not in DB
                    missing_products.append((cod, var, qty))
            
            logger.info(
                f"[VERIFY+AUTO-ADD] Found {len(existing_products)} existing, "
                f"{len(missing_products)} missing products"
            )
            
            # STEP 4: Auto-add missing products
            added_products = []
            failed_additions = []
            
            if missing_products:
                logger.info(f"[VERIFY+AUTO-ADD] Step 4: Auto-adding {len(missing_products)} missing products...")
                
                # Create WebLister instance for fetching product data
                temp_dir = Path(settings.BASE_DIR) / 'temp_auto_add'
                temp_dir.mkdir(exist_ok=True)
                
                lister = WebLister(
                    username=storage.supermarket.username,
                    password=storage.supermarket.password,
                    storage_name=storage.name,
                    download_dir=str(temp_dir),
                    headless=True
                )
                
                try:
                    lister.login()
                    
                    for cod, var, qty in missing_products:
                        try:
                            logger.info(f"[AUTO-ADD] Fetching data for {cod}.{var}...")
                            
                            # Fetch product data from PAC2000A
                            product_data = lister.gather_missing_product_data(cod, var)
                            
                            if not product_data:
                                logger.warning(f"[AUTO-ADD] ❌ Product {cod}.{var} not found in PAC2000A")
                                failed_additions.append({
                                    'cod': cod,
                                    'var': var,
                                    'reason': 'Not found in PAC2000A system'
                                })
                                continue
                            
                            # Unpack data
                            description, package, multiplier, availability, cost, price, category = product_data
                            
                            # Add to products table
                            service.db.add_product(
                                cod=cod,
                                v=var,
                                descrizione=description or f"Product {cod}.{var}",
                                rapp=multiplier or 1,
                                pz_x_collo=package or 12,
                                settore=storage.settore,
                                disponibilita=availability or "Si"
                            )
                            
                            # Initialize stats with the quantity from PDF
                            service.db.init_product_stats(
                                cod=cod,
                                v=var,
                                sold=[],
                                bought=[],
                                stock=qty,
                                verified=True  # Mark as verified immediately
                            )
                            
                            # Add economics data if available
                            if price and cost and price > 0 and cost > 0:
                                cur = service.db.cursor()
                                cur.execute("""
                                    INSERT INTO economics (cod, v, price_std, cost_std, category)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (cod, v) DO UPDATE SET
                                        price_std = excluded.price_std,
                                        cost_std = excluded.cost_std,
                                        category = excluded.category
                                """, (cod, var, price, cost, category or "Unknown"))
                                service.db.conn.commit()
                            
                            # Assign cluster if provided
                            if cluster:
                                service.db.verify_stock(cod, var, new_stock=None, cluster=cluster)
                            
                            added_products.append({
                                'cod': cod,
                                'var': var,
                                'qty': qty,
                                'description': description
                            })
                            
                            logger.info(f"[AUTO-ADD] ✅ Successfully added {cod}.{var} - {description}")
                            
                        except Exception as e:
                            logger.exception(f"[AUTO-ADD] ❌ Error adding {cod}.{var}")
                            failed_additions.append({
                                'cod': cod,
                                'var': var,
                                'reason': str(e)
                            })
                
                finally:
                    lister.driver.quit()
                    logger.info("[AUTO-ADD] Closed WebLister")
            
            # STEP 5: Verify stock for existing products
            logger.info(f"[VERIFY+AUTO-ADD] Step 5: Verifying {len(existing_products)} existing products...")
            
            verified_count = 0
            stock_changes = []
            
            for cod, var, new_qty in existing_products:
                try:
                    old_stock = service.db.get_stock(cod, var)
                    
                    # Update stock
                    service.db.verify_stock(cod, var, new_qty, cluster)
                    
                    if old_stock != new_qty:
                        stock_changes.append({
                            'cod': cod,
                            'var': var,
                            'old_stock': old_stock,
                            'new_stock': new_qty,
                            'difference': new_qty - old_stock
                        })
                    
                    verified_count += 1
                    
                except Exception as e:
                    logger.warning(f"[VERIFY] Error verifying {cod}.{var}: {e}")
                    continue
            
            # STEP 6: Clean up PDF file
            try:
                os.remove(pdf_file_path)
                logger.info(f"[VERIFY+AUTO-ADD] Deleted PDF: {pdf_file_path}")
            except Exception as e:
                logger.warning(f"Could not delete PDF: {e}")
            
            # STEP 7: Generate report
            result = {
                'success': True,
                'storage_name': storage.name,
                'cluster': cluster,
                'total_products': len(parsed_entries),
                'existing_verified': verified_count,
                'products_added': len(added_products),
                'failed_additions': len(failed_additions),
                'stock_changes': stock_changes[:50],  # Limit for session storage
                'added_products': added_products[:50],
                'failed_additions': failed_additions[:20]
            }
            
            logger.info(
                f"[VERIFY+AUTO-ADD] ✅ Complete: "
                f"{verified_count} verified, {len(added_products)} added, "
                f"{len(failed_additions)} failed"
            )
            
            return result
            
        finally:
            service.close()
            
    except Exception as exc:
        logger.exception(f"[VERIFY+AUTO-ADD] ❌ Error for storage {storage_id}")
        raise self.retry(exc=exc)