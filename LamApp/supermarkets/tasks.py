# LamApp/supermarkets/tasks.py
"""
Celery tasks for automated operations.
Replaces scheduler.py with proper distributed task queue.
"""
from celery import shared_task
import datetime
from django.utils import timezone
from django.conf import settings
import logging
from .automation_services import AutomatedRestockService

logger = logging.getLogger(__name__)

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
                
                with AutomatedRestockService(first_storage) as service:
                    try:
                        service.record_losses()
                        logger.info(f"✓ [CELERY] Losses recorded for {supermarket.name}")
                        success_count += 1
                    except Exception as e:
                        logger.exception(f"✗ [CELERY] Failed to record losses for {supermarket.name}")
                        error_count += 1
                    
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
    Update product stats for all storages with active schedules at 6:00 AM.

    CRITICAL: Each storage is processed in its own subtask. When stats update succeeds
    for a storage, that subtask will trigger orders for that specific storage (if scheduled).
    This ensures orders NEVER run for a storage whose stats update failed.
    """
    from .models import Storage

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

        # Determine which day it is for order scheduling
        now = datetime.datetime.now()
        current_weekday = now.weekday()  # 0=Monday, 6=Sunday
        weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        today_field = weekday_fields[current_weekday]

        # Update stats for each storage
        # Each subtask will trigger orders ONLY if stats succeed
        for storage in storages:
            try:
                # Check if this storage has orders scheduled for today
                is_order_day = getattr(storage.schedule, today_field, False)

                logger.info(
                    f"[CELERY] Queueing stats update for {storage.name} "
                    f"(order day: {is_order_day})"
                )

                # Call subtask to update stats
                # Pass is_order_day so the subtask knows whether to trigger orders on success
                update_stats_for_storage.apply_async(
                    args=[storage.id, is_order_day],
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

        return f"Stats update queued for {storages.count()} storages"

    except Exception as exc:
        logger.exception("[CELERY] Fatal error in stats update task")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def update_stats_for_storage(self, storage_id, trigger_order_on_success=False):
    """
    Update product stats for a single storage.

    CRITICAL: If trigger_order_on_success=True and stats update succeeds,
    this task will trigger run_restock_for_storage for this specific storage.
    This ensures orders ONLY run after successful stats update.

    Args:
        storage_id: The storage ID to update stats for
        trigger_order_on_success: If True, queue restock order after successful stats update
    """
    from .models import Storage

    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)

        logger.info(
            f"[CELERY-SUBTASK] Updating stats for {storage.name} "
            f"(will trigger order: {trigger_order_on_success})"
        )

        with AutomatedRestockService(storage) as service:
            from .models import RestockLog
            log = RestockLog.objects.create(
                storage=storage,
                status='processing',
                current_stage='updating_stats',
                operation_type='stats_update'
            )

            service.update_product_stats_checkpoint(log)

            log.status = 'completed'
            log.completed_at = timezone.now()
            log.save()

            logger.info(f"✓ [CELERY-SUBTASK] Stats updated for {storage.name}")

        # CRITICAL: Only trigger order if stats succeeded AND it's an order day
        if trigger_order_on_success:
            logger.info(
                f"[CELERY-SUBTASK] Stats succeeded for {storage.name}, "
                f"now triggering restock order"
            )
            run_restock_for_storage.apply_async(
                args=[storage_id],
                retry=True,
                retry_policy={
                    'max_retries': 3,
                    'interval_start': 900,
                    'interval_step': 0,
                    'interval_max': 900,
                }
            )

        return f"Stats updated for {storage.name}"

    except Exception as exc:
        # Stats failed - DO NOT trigger order
        logger.exception(
            f"[CELERY-SUBTASK] Error updating stats for storage {storage_id}. "
            f"Order will NOT be triggered."
        )
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def run_restock_for_storage(self, storage_id, coverage=None, skip_stats_update=True):
    """
    Run restock for a single storage. Used for both scheduled and manual restocks.

    Args:
        storage_id: The storage ID to run restock for
        coverage: Optional coverage parameter for order calculation
        skip_stats_update: If True, skip stats update (default True since stats
                          are updated separately in the morning workflow)
    """
    from .models import Storage, RestockLog

    try:
        storage = Storage.objects.select_related('supermarket', 'schedule').get(id=storage_id)
        logger.info(
            f"[CELERY-ORDER] Running restock for {storage.name} "
            f"(coverage={coverage}, skip_stats={skip_stats_update})"
        )

        # Report progress
        def report_progress(progress, message):
            self.update_state(
                state='PROGRESS',
                meta={'progress': progress, 'status': message}
            )
            logger.info(f"[RESTOCK] {progress}% - {message}")

        self.update_state(
            state='PROGRESS',
            meta={'progress': 5, 'status': 'Starting restock...'}
        )

        # Create log upfront for tracking
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            current_stage='pending',
            operation_type='full_restock',
            started_at=timezone.now()
        )

        with AutomatedRestockService(storage) as service:
            service.run_full_restock_workflow(
                coverage=coverage,
                log=log,
                skip_stats_update=skip_stats_update,
                progress_callback=report_progress
            )

            logger.info(
                f"✓ [CELERY-ORDER] Successfully completed restock for {storage.name} "
                f"(Log #{log.id}: {log.products_ordered} products, {log.total_packages} packages)"
            )

            result = {
                'success': True,
                'log_id': log.id,
                'storage_name': storage.name,
                'products_ordered': log.products_ordered,
                'total_packages': log.total_packages,
                'redirect_url': f'/logs/{log.id}/'
            }

            self.update_state(
                state='SUCCESS',
                meta=result
            )

            return result

    except Exception as exc:
        logger.exception(f"[CELERY-ORDER] Error running restock for storage {storage_id}")
        self.update_state(
            state='FAILURE',
            meta={'error': str(exc)}
        )
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
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
                
                with ListUpdateService(storage) as service:
                    result = service.update_and_import()
                    
                    if result['success']:
                        logger.info(f"✓ [CELERY] List updated for {storage.name}")
                        success_count += 1
                    else:
                        logger.warning(f"⚠ [CELERY] List update failed for {storage.name}: {result['message']}")
                        error_count += 1                   
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
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def manual_stats_update_task(self, storage_id):
    """Update stats without ordering - WITH PROGRESS"""
    from .models import Storage, RestockLog
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[MANUAL STATS] Starting for {storage.name}")
        
        # ✅ Report progress
        self.update_state(
            state='PROGRESS',
            meta={'progress': 10, 'status': 'Connecting to PAC2000A...'}
        )
        
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            current_stage='updating_stats',
            operation_type='stats_update'
        )
        
        def report_progress(progress, message):
            self.update_state(
                state='PROGRESS',
                meta={'progress': progress, 'status': message}
            )
        
        with AutomatedRestockService(storage) as service:
            service.update_product_stats_checkpoint(log, progress_callback=report_progress)
            
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.save()

            logger.info(f"✅ [MANUAL STATS] Completed for {storage.name}")

            # ✅ CRITICAL FIX: Explicitly set state to SUCCESS
            # Without this, the task stays in PROGRESS state and frontend keeps polling
            result = {
                'success': True,
                'log_id': log.id,
                'storage_name': storage.name,
                'storage_id': storage_id,
                'redirect_url': f'/storages/{storage_id}/'
            }

            self.update_state(
                state='SUCCESS',
                meta=result
            )

            return result      
    except Exception as exc:
        logger.exception(f"[MANUAL STATS] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def add_products_unified_task(self, storage_id, products_list, settore):
    """Add products with auto-fetch and Scrapper-based stats initialization"""
    from .models import Storage, RestockLog
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from .scripts.scrapper import Scrapper  # ← ADDED
    from .scripts.helpers import Helper  # ← ADDED
    from pathlib import Path
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[ADD PRODUCTS] Starting for {storage.name}: {len(products_list)} products")
        
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='product_addition'
        )
        
        with RestockService(storage) as service:
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
                products_for_scrapper = []  # ← NEW: Collect for scrapper
                
                for cod, var in products_list:
                    try:
                        logger.info(f"[ADD PRODUCTS] Fetching {cod}.{var}...")
                        
                        product_data = lister.gather_missing_product_data(cod, var)
                        
                        if not product_data:
                            logger.warning(f"[ADD PRODUCTS] Product {cod}.{var} not found")
                            failed.append((cod, var, "Not found in PAC2000A"))
                            continue
                        
                        description, package, multiplier, availability, cost, price, category = product_data
                        
                        # Add to products table
                        service.db.add_product(
                            cod=cod,
                            v=var,
                            descrizione=description or f"Product {cod}.{var}",
                            rapp=multiplier or 1,
                            pz_x_collo=package or 12,
                            settore=settore,
                            disponibilita=availability or "Si"
                        )
                        
                        # ✅ FIXED: Collect for scrapper (don't init stats yet!)
                        products_for_scrapper.append((cod, var, False, package or 12))
                        
                        # Add economics data
                        if price and cost:
                            cost = float(cost)
                            price = float(price)
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
                
                lister.driver.quit()
                if products_for_scrapper:
                    logger.info(f"[ADD PRODUCTS] Initializing stats for {len(products_for_scrapper)} products via Scrapper...")
                    
                    helper = Helper()
                    scrapper = Scrapper(
                        username=storage.supermarket.username,
                        password=storage.supermarket.password,
                        helper=helper,
                        db=service.db
                    )
                    scrapper.navigate()
                    
                    # Process products via scrapper
                    scrapper_report = scrapper.process_products(products_for_scrapper)
                    logger.info(f"[ADD PRODUCTS] Scrapper initialized {scrapper_report['initialized']} products")
                
                # Update log
                log.status = 'completed'
                log.completed_at = timezone.now()
                log.products_ordered = len(added)
                log.save()
                
                logger.info(f"[ADD PRODUCTS] ✅ Complete: {len(added)} added, {len(failed)} failed")
                
                return {
                    'success': True,
                    'products_added': len(added),
                    'products_failed': len(failed),
                    'added': added[:50],
                    'failed': failed[:20],
                    'storage_name': storage.name,
                    'storage_id': storage_id
                }   
            finally:
                scrapper.driver.quit()
           
    except Exception as exc:
        logger.exception(f"[ADD PRODUCTS] Error for storage {storage_id}")
        raise self.retry(exc=exc)

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def manual_list_update_task(self, storage_id):
    """Manual list update"""
    from .models import Storage, RestockLog
    from .list_update_service import ListUpdateService
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[LIST UPDATE] Starting for {storage.name}")
        
        # UPDATED: Create log with operation_type
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='list_update'  # NEW
        )
        
        with ListUpdateService(storage) as service:
            result = service.update_and_import()
            
            if result['success']:
                log.status = 'completed'
                log.completed_at = timezone.now()
                logger.info(f"✅ [LIST UPDATE] Completed for {storage.name}")
            else:
                log.status = 'failed'
                log.error_message = result['message']
                logger.warning(f"⚠️ [LIST UPDATE] Failed for {storage.name}: {result['message']}")
            
            log.save()
            
            # Add storage_id for redirect
            result['storage_id'] = storage_id
            return result            
    except Exception as exc:
        logger.exception(f"[LIST UPDATE] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=600
)
def assign_clusters_task(self, storage_id, pdf_file_path, cluster):
    """Assign clusters from PDF"""
    from .models import Storage, RestockLog
    from .services import RestockService
    from .scripts.inventory_reader import assign_clusters_from_pdf
    from django.utils import timezone
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[ASSIGN CLUSTERS] Starting for {storage.name}: cluster='{cluster}'")
        
        # UPDATED: Create log with operation_type
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='cluster_assignment'  # NEW
        )
        
        with RestockService(storage) as service:
            result = assign_clusters_from_pdf(service.db, pdf_file_path, cluster)
            
            if result['success']:
                log.status = 'completed'
                log.products_ordered = result.get('assigned', 0)  # Reuse field
                logger.info(
                    f"✅ [ASSIGN CLUSTERS] Completed: "
                    f"{result['assigned']} assigned, {result['skipped']} skipped"
                )
            else:
                log.status = 'failed'
                log.error_message = result.get('error')
                logger.error(f"❌ [ASSIGN CLUSTERS] Failed: {result['error']}")
            
            log.completed_at = timezone.now()
            log.save()
            
            return {
                'success': result['success'],
                'storage_name': storage.name,
                'storage_id': storage_id,  # For redirect
                'cluster': cluster,
                'assigned': result.get('assigned', 0),
                'skipped': result.get('skipped', 0),
                'error': result.get('error')
            }          
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
        
        with RestockService(storage) as service:
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
    except Exception as exc:
        logger.exception(f"[PROCESS PROMOS] Error for supermarket {supermarket_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def verify_stock_with_auto_add_task(self, storage_id, pdf_file_path, cluster=None):
    """
    Bulk stock verification with automatic product addition.
    NOW PROPERLY TRACKS PROGRESS AND UPDATES LOG.
    """
    from .models import Storage, RestockLog
    from .automation_services import AutomatedRestockService
    from .scripts.inventory_reader import parse_pdf
    from .scripts.web_lister import WebLister
    from .scripts.scrapper import Scrapper
    from .scripts.helpers import Helper
    from pathlib import Path
    import os
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[VERIFY+AUTO-ADD] Starting for {storage.name}")
        
        # ✅ Report initial progress
        self.update_state(
            state='PROGRESS',
            meta={'progress': 5, 'status': 'Starting stock verification...'}
        )

        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='verification'
        )
        
        with AutomatedRestockService(storage) as service:
            # Step 1: Record losses
            self.update_state(
                state='PROGRESS',
                meta={'progress': 10, 'status': 'Recording losses...'}
            )
            logger.info(f"[VERIFY+AUTO-ADD] Step 1: Recording losses...")
            service.record_losses()
            
            # Step 2: Parse PDF
            self.update_state(
                state='PROGRESS',
                meta={'progress': 20, 'status': 'Parsing inventory PDF...'}
            )
            logger.info(f"[VERIFY+AUTO-ADD] Step 2: Parsing PDF...")
            parsed_entries = parse_pdf(pdf_file_path)
            
            if not parsed_entries:
                log.status = 'failed'
                log.error_message = 'No valid entries found in PDF'
                log.save()
                return {'success': False, 'error': 'No valid entries found in PDF'}
            
            logger.info(f"[VERIFY+AUTO-ADD] Found {len(parsed_entries)} products in PDF")
            
            # Step 3: Separate existing vs missing
            self.update_state(
                state='PROGRESS',
                meta={'progress': 30, 'status': f'Analyzing {len(parsed_entries)} products...'}
            )
            
            existing_products = []
            missing_products = []
            
            for entry in parsed_entries:
                cod = entry['cod']
                var = entry['v']
                qty = entry['qty']
                
                try:
                    service.db.get_stock(cod, var)
                    existing_products.append((cod, var, qty))
                except ValueError:
                    missing_products.append((cod, var, qty))
            
            logger.info(
                f"[VERIFY+AUTO-ADD] Found {len(existing_products)} existing, "
                f"{len(missing_products)} missing products"
            )
            
            # Step 4: Auto-add missing products
            added_products = []
            failed_additions = []
            products_for_scrapper = []
            
            if missing_products:
                self.update_state(
                    state='PROGRESS',
                    meta={'progress': 40, 'status': f'Auto-adding {len(missing_products)} missing products (10-20 min)...'}
                )
                logger.info(f"[VERIFY+AUTO-ADD] Step 4: Auto-adding {len(missing_products)} missing products...")
                
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
                    
                    for idx, (cod, var, qty) in enumerate(missing_products, 1):
                        # ✅ Update progress for each product
                        progress = 40 + int((idx / len(missing_products)) * 20)  # 40-60%
                        self.update_state(
                            state='PROGRESS',
                            meta={'progress': progress, 'status': f'Auto-adding product {idx}/{len(missing_products)}: {cod}.{var}...'}
                        )
                        
                        try:
                            logger.info(f"[AUTO-ADD] Fetching data for {cod}.{var}...")
                            
                            product_data = lister.gather_missing_product_data(cod, var)
                            
                            if not product_data:
                                failed_additions.append({
                                    'cod': cod,
                                    'var': var,
                                    'reason': 'Not found in PAC2000A system'
                                })
                                continue
                            
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
                            
                            products_for_scrapper.append((cod, var, True, package or 12))
                            
                            # Add economics data
                            if price and cost:
                                cost = float(cost)
                                price = float(price)
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
                            
                            added_products.append({
                                'cod': cod,
                                'var': var,
                                'qty': qty,
                                'description': description
                            })
                            
                            logger.info(f"[AUTO-ADD] ✅ Successfully added {cod}.{var}")
                            
                        except Exception as e:
                            logger.exception(f"[AUTO-ADD] Error adding {cod}.{var}")
                            failed_additions.append({
                                'cod': cod,
                                'var': var,
                                'reason': str(e)
                            })
                    
                    lister.driver.quit()
                    
                    # Initialize stats using Scrapper
                    if products_for_scrapper:
                        self.update_state(
                            state='PROGRESS',
                            meta={'progress': 65, 'status': f'Initializing stats for {len(products_for_scrapper)} products (5-10 min)...'}
                        )
                        logger.info(f"[VERIFY+AUTO-ADD] Initializing stats for {len(products_for_scrapper)} products via Scrapper...")
                        
                        helper = Helper()
                        scrapper = Scrapper(
                            username=storage.supermarket.username,
                            password=storage.supermarket.password,
                            helper=helper,
                            db=service.db
                        )
                        scrapper.navigate()
                        
                        scrapper_report = scrapper.process_products(products_for_scrapper)
                        logger.info(f"[VERIFY+AUTO-ADD] Scrapper initialized {scrapper_report['initialized']} products")
                        
                        # Now verify stock for newly added products
                        for cod, var, qty in [(p[0], p[1], next((m[2] for m in missing_products if m[0]==p[0] and m[1]==p[1]), 0)) for p in products_for_scrapper]:
                            service.db.verify_stock(cod, var, qty, cluster)
                
                finally:
                    if 'scrapper' in locals():
                        scrapper.driver.quit()
            
            # Step 5: Verify existing products
            self.update_state(
                state='PROGRESS',
                meta={'progress': 75, 'status': f'Verifying {len(existing_products)} existing products...'}
            )
            logger.info(f"[VERIFY+AUTO-ADD] Step 5: Verifying {len(existing_products)} existing products...")
            
            verified_count = 0
            stock_changes = []
            
            for idx, (cod, var, new_qty) in enumerate(existing_products, 1):
                # ✅ Update progress periodically
                if idx % 50 == 0:
                    progress = 75 + int((idx / len(existing_products)) * 15)  # 75-90%
                    self.update_state(
                        state='PROGRESS',
                        meta={'progress': progress, 'status': f'Verifying product {idx}/{len(existing_products)}...'}
                    )
                
                try:
                    old_stock = service.db.get_stock(cod, var)
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
            
            # Clean up
            self.update_state(
                state='PROGRESS',
                meta={'progress': 95, 'status': 'Finalizing verification...'}
            )
            
            try:
                os.remove(pdf_file_path)
            except Exception as e:
                logger.warning(f"Could not delete PDF: {e}")
            
            # ✅ UPDATE LOG WITH PROPER COUNTS
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.products_ordered = verified_count + len(added_products)  # Total verified/added
            log.total_packages = len(added_products)  # Reuse for added count
            log.save()
            
            result = {
                'success': True,
                'storage_name': storage.name,
                'storage_id': storage_id,
                'cluster': cluster,
                'total_products': len(parsed_entries),
                'existing_verified': verified_count,
                'products_added': len(added_products),
                'failed_additions': len(failed_additions),
                'stock_changes': stock_changes[:50],
                'added_products': added_products[:50],
                'failed_additions': failed_additions[:20],
                'redirect_url': f'/inventory/verification-report/?task_id={self.request.id}'  # ✅ Proper redirect
            }
            
            logger.info(
                f"[VERIFY+AUTO-ADD] ✅ Complete: "
                f"{verified_count} verified, {len(added_products)} added"
            )

            # ✅ CRITICAL FIX: Explicitly set state to SUCCESS
            # Without this, the task stays in PROGRESS state and frontend keeps polling
            self.update_state(
                state='SUCCESS',
                meta=result
            )

            return result
            
    except Exception as exc:
        logger.exception(f"[VERIFY+AUTO-ADD] ❌ Error for storage {storage_id}")
        
        # ✅ Report error state
        self.update_state(
            state='FAILURE',
            meta={'error': str(exc)}
        )
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=600,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def order_new_products_task(self, log_id, products_list):
    """
    Place order for new products from restock log.
    
    Args:
        log_id: RestockLog ID
        products_list: List of dicts with {cod, var, qty}
    """
    from .models import RestockLog
    from .scripts.orderer import Orderer
    
    try:
        log = RestockLog.objects.select_related(
            'storage__supermarket'
        ).get(id=log_id)
        
        logger.info(f"[ORDER NEW] Starting for log #{log_id}: {len(products_list)} products")
        
        # Convert to orderer format
        orders_list = [
            (p['cod'], p['var'], p['qty'])
            for p in products_list
        ]
        
        orderer = Orderer(
            username=log.storage.supermarket.username,
            password=log.storage.supermarket.password
        )
        
        try:
            orderer.login()
            successful_orders, order_skipped = orderer.make_orders(
                log.storage.name,
                orders_list
            )
            
            logger.info(
                f"✅ [ORDER NEW] Complete: {len(successful_orders)} ordered, "
                f"{len(order_skipped)} skipped"
            )
            
            return {
                'success': True,
                'log_id': log_id,
                'ordered': len(successful_orders),
                'skipped': len(order_skipped),
                'skipped_products': order_skipped
            }
            
        finally:
            orderer.driver.quit()
            
    except Exception as exc:
        logger.exception(f"[ORDER NEW] Error for log #{log_id}")
        raise self.retry(exc=exc)
    
@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=300
)
def process_ddt_task(self, storage_id, pdf_file_path):
    """
    Process DDT delivery document and add stock.

    Args:
        storage_id: Storage ID
        pdf_file_path: Full path to DDT PDF file
    """
    from .models import Storage, RestockLog
    from .services import RestockService
    from .scripts.ddt_parser import parse_ddt_pdf, process_ddt_deliveries
    import os
    
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        
        logger.info(f"[PROCESS DDT] Starting for {storage.name}")
        
        # Create log
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='verification',  # Reuse this type
            current_stage='processing'
        )
        
        with RestockService(storage) as service:
            # Parse DDT PDF
            logger.info(f"[PROCESS DDT] Parsing PDF: {pdf_file_path}")
            ddt_entries = parse_ddt_pdf(pdf_file_path)
            
            if not ddt_entries:
                log.status = 'failed'
                log.error_message = 'No valid entries found in DDT PDF'
                log.save()
                
                return {
                    'success': False,
                    'error': 'No valid entries found in DDT PDF'
                }
            
            # Process deliveries
            logger.info(f"[PROCESS DDT] Processing {len(ddt_entries)} deliveries")
            result = process_ddt_deliveries(service.db, ddt_entries)
            
            # Update log
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.products_ordered = result['processed']  # Reuse this field
            log.total_packages = result['total_qty_added']  # Reuse for total qty
            log.set_results({
                'ddt_entries': [
                    {'cod': cod, 'var': var, 'qty': qty}
                    for cod, var, qty in ddt_entries[:100]  # Limit for storage
                ],
                'processed': result['processed'],
                'total_qty_added': result['total_qty_added'],
                'skipped': result['skipped'],
                'errors': result['errors'],
                'skipped_products': result['skipped_products'][:20],
                'error_products': result['error_products'][:20]
            })
            log.save()
            
            # Clean up PDF
            try:
                os.remove(pdf_file_path)
                logger.info(f"[PROCESS DDT] Deleted PDF: {pdf_file_path}")
            except Exception as e:
                logger.warning(f"Could not delete PDF: {e}")
            
            logger.info(
                f"[PROCESS DDT] ✅ Complete: {result['processed']} processed, "
                f"{result['total_qty_added']} total units added"
            )
            
            return {
                'success': True,
                'storage_name': storage.name,
                'storage_id': storage_id,
                'log_id': log.id,
                'processed': result['processed'],
                'total_qty_added': result['total_qty_added'],
                'skipped': result['skipped'],
                'errors': result['errors']
            }
            
    except Exception as exc:
        logger.exception(f"[PROCESS DDT] Error for storage {storage_id}")
        raise self.retry(exc=exc)