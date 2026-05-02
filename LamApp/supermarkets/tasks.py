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
    default_retry_delay=900,
)
def update_stats_all_scheduled_storages(self):
    """
    Nightly DDT import — one Celery task per supermarket.
    Fetches invoices once per supermarket and fans out DB writes per storage.
    """
    from .models import Supermarket

    try:
        logger.info("[CELERY] Starting nightly DDT import for all supermarkets")

        supermarkets = Supermarket.objects.prefetch_related('storages').all()
        queued_count = 0

        for supermarket in supermarkets:
            try:
                import_ddt_for_supermarket.apply_async(
                    args=[supermarket.id],
                    retry=True,
                    retry_policy={
                        'max_retries': 3,
                        'interval_start': 900,
                        'interval_step': 0,
                        'interval_max': 900,
                    }
                )
                queued_count += 1

            except Exception:
                logger.exception(f"[CELERY] Error queuing DDT import for {supermarket.name}")
                continue

        logger.info(f"[CELERY] DDT import queued for {queued_count} supermarkets")
        return f"DDT import queued for {queued_count} supermarkets"

    except Exception as exc:
        logger.exception("[CELERY] Fatal error in nightly DDT import")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def import_ddt_for_supermarket(self, supermarket_id):
    """
    Login once, fetch all invoices for the supermarket, then apply deliveries
    per storage — one RestockLog per storage.
    """
    import shutil
    from .models import Supermarket, RestockLog
    from .scripts.web_lister import WebLister

    try:
        supermarket = Supermarket.objects.prefetch_related('storages').get(id=supermarket_id)
        storages = list(supermarket.storages.all())

        if not storages:
            logger.info(f"[DDT] No storages for {supermarket.name}, skipping")
            return

        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

        lister = WebLister(
            username=supermarket.username,
            password=supermarket.password,
            storage_name='',
            download_dir='/tmp',
            id_user=supermarket.id_user,
            x5cper=supermarket.x5cper,
            headless=True,
        )

        try:
            lister.login()

            rows = lister.fetch_scorporo(date_from=yesterday, date_to=yesterday)

            if not rows:
                logger.info(f"[DDT] No DDT rows for {supermarket.name} on {yesterday}")
                return

            # Fetch righe once per distinct invoice number.
            # Track desc_mag → [invoice_numbers] using the first DescMag seen per invoice.
            seen = set()
            all_righe = []
            desc_mag_to_invoices = {}  # desc_mag → [nrcc_stripped, ...]
            for row in rows:
                nrcc = row["X5NRCC"]
                if nrcc in seen:
                    continue
                seen.add(nrcc)
                nrcc_stripped = str(int(nrcc))
                righe = lister.fetch_righe(row)
                # Derive desc_mag for this invoice from its first riga
                if righe:
                    dm = righe[1].get("DescMag", "").strip() if len(righe) > 1 else righe[0].get("DescMag", "").strip()
                    desc_mag_to_invoices.setdefault(dm, []).append(nrcc_stripped)
                all_righe.extend(righe)

            # Group by DescMag — one group per storage
            grouped = lister.process_righe(all_righe)

        finally:
            lister.driver.quit()
            shutil.rmtree(lister.user_data_dir, ignore_errors=True)

        # Fan out: one DB write + RestockLog per matched storage
        for desc_mag, ean_qty in grouped.items():
            matched_storage = None
            for s in storages:
                if desc_mag.upper() in s.name.upper():
                    matched_storage = s
                    break

            if matched_storage is None:
                logger.info(f"[DDT] DescMag '{desc_mag}' matched no storage in {supermarket.name}")
                continue

            log = RestockLog.objects.create(
                storage=matched_storage,
                status='processing',
                current_stage='updating_stats',
                operation_type='ddt_import',
            )

            try:
                storage_invoices = desc_mag_to_invoices.get(desc_mag, [])
                with AutomatedRestockService(matched_storage) as service:
                    service.apply_ddt_for_storage(log, ean_qty, storage_invoices)

                RestockLog.objects.filter(id=log.id).update(
                    status='completed',
                    current_stage='completed',
                    completed_at=timezone.now(),
                )
                logger.info(f"[DDT] Completed for {matched_storage.name}")

            except Exception as e:
                RestockLog.objects.filter(id=log.id).update(
                    status='failed',
                    current_stage='failed',
                    error_message=str(e),
                )
                logger.exception(f"[DDT] Failed for {matched_storage.name}")

    except Exception as exc:
        logger.exception(f"[DDT] Fatal error for supermarket {supermarket_id}")
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
    from .models import Storage, RestockLog
    from .list_update_service import ListUpdateService
    from django.utils import timezone

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
            from .models import is_closure_day
            if is_closure_day(storage.supermarket):
                logger.info(f"[CELERY] Skipping list update for {storage.name} — closure day")
                continue

            # Create a log entry for this scheduled update
            log = RestockLog.objects.create(
                storage=storage,
                status='processing',
                operation_type='list_update'
            )

            try:
                logger.info(f"[CELERY] Updating product list for {storage.name}")

                with ListUpdateService(storage) as service:
                    result = service.update_and_import()

                    if result['success']:
                        log.status = 'completed'
                        log.completed_at = timezone.now()
                        logger.info(f"✓ [CELERY] List updated for {storage.name}")
                        success_count += 1
                    else:
                        log.status = 'failed'
                        log.error_message = result['message']
                        logger.warning(f"⚠ [CELERY] List update failed for {storage.name}: {result['message']}")
                        error_count += 1

                    log.save()

            except Exception as e:
                log.status = 'failed'
                log.error_message = str(e)
                log.save()
                logger.exception(f"✗ [CELERY] Error updating list for {storage.name}")
                error_count += 1
                continue

        result_msg = f"List updates complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")

        # Purge obsolete products (verified=False, disponibilita=No, stock=0)
        # once per supermarket now that all lists are fresh.
        from .models import Supermarket
        purge_total = 0
        supermarket_ids = storages.values_list('supermarket_id', flat=True).distinct()
        for sm in Supermarket.objects.filter(id__in=supermarket_ids):
            try:
                from .scripts.DatabaseManager import DatabaseManager
                from .scripts.helpers import Helper
                db = DatabaseManager(Helper(), supermarket_name=sm.name)
                try:
                    purged = db.purge_obsolete_products()
                    if purged:
                        purge_total += len(purged)
                        for p in purged:
                            logger.info(
                                f"[CELERY] Purged obsolete product {p['cod']}.{p['v']} "
                                f"from {sm.name}"
                            )
                finally:
                    db.close()
            except Exception as e:
                logger.exception(
                    f"[CELERY] Error during obsolete-product purge for {sm.name}"
                )

        if purge_total:
            logger.info(f"[CELERY] Purged {purge_total} obsolete product(s) total")

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
def add_products_unified_task(self, storage_id, products_list, settore):
    """Add products with auto-fetch and Scrapper-based stats initialization"""
    from .models import Storage, RestockLog
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from pathlib import Path
    from django.utils import timezone
    import shutil
    
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
                id_cod_mag=storage.id_cod_mag,
                id_cliente=storage.supermarket.id_cliente,
                id_azienda=storage.supermarket.id_azienda,
                id_marchio=storage.supermarket.id_marchio,
                id_clienti_canale=storage.supermarket.id_clienti_canale,
                id_clienti_area=storage.supermarket.id_clienti_area,
                headless=True
            )

            try:
                lister.login()
                lister.navigate_to_lists()

                added = []
                failed = []

                for cod, var in products_list:
                    try:
                        logger.info(f"[ADD PRODUCTS] Fetching {cod}.{var}...")
                        
                        product_data = lister.gather_missing_product_data(cod, var)
                        
                        if not product_data:
                            logger.warning(f"[ADD PRODUCTS] Product {cod}.{var} not found")
                            failed.append((cod, var, "Not found in Dropzone"))
                            continue
                        
                        description, package, multiplier, availability, cost, price, category, ean = product_data

                        # Add to products table
                        service.db.add_product(
                            cod=cod,
                            v=var,
                            descrizione=description or f"Product {cod}.{var}",
                            rapp=multiplier or 1,
                            pz_x_collo=package or 12,
                            settore=settore,
                            disponibilita=availability or "Si",
                            ean=ean,
                        )
                        
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
                lister.driver.quit()
                shutil.rmtree(lister.user_data_dir, ignore_errors=True)
           
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
    from pathlib import Path
    import os
    import shutil
    
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
                    id_cod_mag=storage.id_cod_mag,
                    id_cliente=storage.supermarket.id_cliente,
                    id_azienda=storage.supermarket.id_azienda,
                    id_marchio=storage.supermarket.id_marchio,
                    id_clienti_canale=storage.supermarket.id_clienti_canale,
                    id_clienti_area=storage.supermarket.id_clienti_area,
                    headless=True
                )
                
                try:
                    lister.login()
                    lister.navigate_to_lists()
                    
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
                                    'reason': 'Not found in Dropzone system'
                                })
                                continue
                            
                            description, package, multiplier, availability, cost, price, category, ean = product_data

                            # Add to products table
                            service.db.add_product(
                                cod=cod,
                                v=var,
                                descrizione=description or f"Product {cod}.{var}",
                                rapp=multiplier or 1,
                                pz_x_collo=package or 12,
                                settore=storage.settore,
                                disponibilita=availability or "Si",
                                ean=ean,
                            )
                            
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
                    
                    # Verify stock for newly added products
                    for p in added_products:
                        service.db.verify_stock(p['cod'], p['var'], p['qty'], cluster)

                finally:
                    lister.driver.quit()
                    shutil.rmtree(lister.user_data_dir, ignore_errors=True)
            
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
def order_promo_products_task(self, user_id, orders_list):
    """
    Place order for promo products from promo products page.

    Args:
        user_id: User ID (for ownership validation)
        orders_list: List of dicts with {storage_id, storage_name, supermarket_id, cod, var, qty}
    """
    from .models import Supermarket
    from .scripts.orderer import Orderer

    try:
        # Group orders by supermarket (each supermarket has its own credentials)
        by_supermarket = {}
        for order in orders_list:
            sm_id = order['supermarket_id']
            if sm_id not in by_supermarket:
                by_supermarket[sm_id] = {
                    'storages': {},
                }
            storage_name = order['storage_name']
            if storage_name not in by_supermarket[sm_id]['storages']:
                by_supermarket[sm_id]['storages'][storage_name] = []
            by_supermarket[sm_id]['storages'][storage_name].append(
                (order['cod'], order['var'], order['qty'])
            )

        total_ordered = 0
        total_skipped = 0
        all_skipped_products = []

        # Process each supermarket
        for sm_id, sm_data in by_supermarket.items():
            supermarket = Supermarket.objects.get(id=sm_id, owner_id=user_id)

            logger.info(f"[ORDER PROMO] Processing supermarket {supermarket.name}")

            orderer = Orderer(
                username=supermarket.username,
                password=supermarket.password
            )

            try:
                orderer.login()

                # Process each storage within this supermarket
                for storage_name, products in sm_data['storages'].items():
                    logger.info(f"[ORDER PROMO] Ordering {len(products)} products for {storage_name}")

                    successful_orders, order_skipped = orderer.make_orders(
                        storage_name,
                        products
                    )

                    total_ordered += len(successful_orders)
                    total_skipped += len(order_skipped)
                    all_skipped_products.extend(order_skipped)

            finally:
                orderer.driver.quit()

        logger.info(
            f"✅ [ORDER PROMO] Complete: {total_ordered} ordered, "
            f"{total_skipped} skipped"
        )

        return {
            'success': True,
            'ordered': total_ordered,
            'skipped': total_skipped,
            'skipped_products': all_skipped_products
        }

    except Exception as exc:
        logger.exception(f"[ORDER PROMO] Error for user #{user_id}")
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
            operation_type='ddt_import',
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
            log.products_ordered = result['processed']
            log.total_packages = result['total_qty_added']
            log.set_results({
                'updated': result['processed'],
                'not_found': [
                    {'cod': p['cod'], 'v': p['var'], 'descrizione': ''}
                    for p in result['skipped_products'][:50]
                ],
                'errors': [
                    {'cod': p['cod'], 'v': p['var'], 'error': p['error']}
                    for p in result['error_products'][:20]
                ],
                'invoices': [],
                'unverified_products': []
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


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900
)
def prepend_monthly_loss_zeros(self):
    """
    Prepend [0, 0] to every loss array in extra_losses for ALL supermarkets.
    Runs on the 1st of every month at 00:30.

    This ensures arr[0] always represents the current month, even for
    products that haven't had a new loss registered in months.
    """
    from .models import Supermarket
    from .services import RestockService

    try:
        logger.info("[CELERY] Starting monthly loss zero-prepend for all supermarkets")

        supermarkets = Supermarket.objects.all()

        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"

        success_count = 0
        error_count = 0

        for supermarket in supermarkets:
            try:
                first_storage = supermarket.storages.first()

                if not first_storage:
                    logger.warning(f"[CELERY] No storages found for {supermarket.name}")
                    continue

                with RestockService(first_storage) as service:
                    updated = service.db.prepend_monthly_loss_zeros()
                    logger.info(f"[CELERY] {supermarket.name}: {updated} loss rows updated")
                    success_count += 1

            except Exception as e:
                logger.exception(f"[CELERY] Error prepending zeros for {supermarket.name}")
                error_count += 1
                continue

        result_msg = f"Monthly loss zero-prepend complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")
        return result_msg

    except Exception as exc:
        logger.exception("[CELERY] Fatal error in monthly loss zero-prepend task")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=3600  # 1 hour
)
def create_monthly_stock_snapshots(self):
    """
    Create automatic stock value snapshots for all supermarkets.
    Runs on the 1st of each month at midnight.

    Configured in Celery Beat schedule.
    """
    from .models import Supermarket, StockValueSnapshot, Storage
    from .services import RestockService

    try:
        logger.info("[CELERY] Starting monthly stock value snapshot creation")

        supermarkets = Supermarket.objects.all()

        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"

        success_count = 0
        error_count = 0

        for supermarket in supermarkets:
            try:
                logger.info(f"[SNAPSHOT] Creating snapshot for {supermarket.name}")

                # Get all storages for this supermarket
                storages = Storage.objects.filter(supermarket=supermarket)

                if not storages.exists():
                    logger.warning(f"[SNAPSHOT] No storages found for {supermarket.name}")
                    continue

                # Calculate total value across all storages
                category_totals = {}
                total_value = 0

                for storage in storages:
                    try:
                        with RestockService(storage) as service:
                            settore = storage.settore
                            cursor = service.db.cursor()

                            cursor.execute("""
                                SELECT e.category,
                                    SUM((e.cost_std / p.rapp) * ps.stock) AS value
                                FROM economics e
                                JOIN product_stats ps
                                    ON e.cod = ps.cod AND e.v = ps.v
                                JOIN products p
                                    ON e.cod = p.cod AND e.v = p.v
                                WHERE e.category != '' AND ps.stock > 0
                                    AND p.settore = %s
                                GROUP BY e.category
                            """, (settore,))

                            for row in cursor.fetchall():
                                category_name = row['category']
                                value = float(row['value'] or 0)

                                if category_name in category_totals:
                                    category_totals[category_name] += value
                                else:
                                    category_totals[category_name] = value

                                total_value += value
                    except Exception as e:
                        logger.exception(f"[SNAPSHOT] Error processing storage {storage.name}")
                        continue

                # Build category breakdown with percentages
                category_breakdown = []
                for name, value in sorted(category_totals.items(), key=lambda x: x[1], reverse=True):
                    percentage = (value / total_value * 100) if total_value > 0 else 0
                    category_breakdown.append({
                        'name': name,
                        'value': round(value, 2),
                        'percentage': round(percentage, 1)
                    })

                # Create snapshot
                StockValueSnapshot.create_snapshot(
                    supermarket=supermarket,
                    total_value=total_value,
                    category_breakdown=category_breakdown,
                    is_manual=False
                )

                logger.info(f"✓ [SNAPSHOT] Created snapshot for {supermarket.name}: €{total_value:.2f}")
                success_count += 1

            except Exception as e:
                logger.exception(f"✗ [SNAPSHOT] Error creating snapshot for {supermarket.name}")
                error_count += 1
                continue

        result_msg = f"Stock snapshots complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")

        return result_msg

    except Exception as exc:
        logger.exception("[CELERY] Fatal error in monthly snapshot task")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def sync_storages_task(self, supermarket_id):
    """Sync storages from Dropzone for a supermarket."""
    from .models import Supermarket
    from .services import StorageService

    try:
        supermarket = Supermarket.objects.get(id=supermarket_id)
        logger.info(f"[SYNC STORAGES] Starting for {supermarket.name}")
        StorageService.sync_storages(supermarket)
        logger.info(f"[SYNC STORAGES] Complete for {supermarket.name}")
        return {
            'success': True,
            'synced': True,
            'supermarket_id': supermarket_id,
            'message': 'Magazzini sincronizzati con successo.',
        }
    except Exception as exc:
        logger.exception(f"[SYNC STORAGES] Error for supermarket #{supermarket_id}")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=600,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def backfill_ean_and_id_for_verified_products(self):
    """
    For every storage with a schedule, fetch and store the EAN for all verified
    products whose ean column is NULL. Runs at 3:30 AM, after the nightly list update.
    """
    from .models import Storage
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from pathlib import Path
    import shutil
    import time

    try:
        storages = Storage.objects.filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')

        if not storages.exists():
            logger.info("[EAN BACKFILL] No storages with schedules found")
            return "No storages to process"

        total_updated = 0
        total_failed = 0

        for storage in storages:
            # Query missing EANs for this storage's settore only
            with RestockService(storage) as service:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT p.cod, p.v
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE ps.verified = TRUE AND p.ean IS NULL AND p.settore = %s
                """, (storage.settore,))
                missing = cur.fetchall()

            if not missing:
                logger.info(f"[EAN BACKFILL] No verified products with missing EAN in {storage.name}")
                continue

            logger.info(f"[EAN BACKFILL] Found {len(missing)} products to fill in {storage.name}")

            temp_dir = Path(settings.BASE_DIR) / 'temp_ean_backfill'
            temp_dir.mkdir(exist_ok=True)

            lister = WebLister(
                username=storage.supermarket.username,
                password=storage.supermarket.password,
                storage_name=storage.name,
                download_dir=str(temp_dir),
                id_cod_mag=storage.id_cod_mag,
                id_cliente=storage.supermarket.id_cliente,
                id_azienda=storage.supermarket.id_azienda,
                id_marchio=storage.supermarket.id_marchio,
                id_clienti_canale=storage.supermarket.id_clienti_canale,
                id_clienti_area=storage.supermarket.id_clienti_area,
                headless=True
            )

            try:
                lister.login()
                lister.navigate_to_lists()

                with RestockService(storage) as service:
                    for row in missing:
                        cod, v = row['cod'], row['v']
                        try:
                            product_data = lister.gather_missing_product_data(cod, v)
                            if not product_data:
                                logger.debug(f"[EAN BACKFILL] No data returned for {cod}.{v}")
                                total_failed += 1
                                continue

                            ean = product_data[7]

                            if ean is None:
                                logger.debug(f"[EAN BACKFILL] No EAN found for {cod}.{v}")
                                total_failed += 1
                                continue

                            cur = service.db.cursor()
                            cur.execute(
                                "UPDATE products SET ean = %s WHERE cod = %s AND v = %s",
                                (ean, cod, v)
                            )
                            service.db.conn.commit()
                            total_updated += 1
                            logger.info(f"[EAN BACKFILL] {cod}.{v} -> EAN {ean}")

                        except Exception as e:
                            logger.warning(f"[EAN BACKFILL] Failed for {cod}.{v}: {e}")
                            total_failed += 1

                        time.sleep(0.1)

            finally:
                lister.driver.quit()
                shutil.rmtree(lister.user_data_dir, ignore_errors=True)

        result_msg = f"EAN backfill complete: {total_updated} updated, {total_failed} failed/missing"
        logger.info(f"[EAN BACKFILL] {result_msg}")
        return result_msg

    except Exception as exc:
        logger.exception("[EAN BACKFILL] Fatal error")
        raise self.retry(exc=exc)


@shared_task(queue='selenium', acks_late=True, reject_on_worker_lost=True)
def fetch_single_ean(storage_id, cod, v):
    """
    Fetch and store the EAN for a single product (cod, v).
    Triggered manually from the delivery check page.
    """
    from .models import Storage
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from pathlib import Path
    import shutil

    storage = Storage.objects.select_related('supermarket').get(id=storage_id)

    temp_dir = Path(settings.BASE_DIR) / 'temp_ean_backfill'
    temp_dir.mkdir(exist_ok=True)

    lister = WebLister(
        username=storage.supermarket.username,
        password=storage.supermarket.password,
        storage_name=storage.name,
        download_dir=str(temp_dir),
        id_cod_mag=storage.id_cod_mag,
        id_cliente=storage.supermarket.id_cliente,
        id_azienda=storage.supermarket.id_azienda,
        id_marchio=storage.supermarket.id_marchio,
        id_clienti_canale=storage.supermarket.id_clienti_canale,
        id_clienti_area=storage.supermarket.id_clienti_area,
        headless=True,
    )

    try:
        lister.login()
        lister.navigate_to_lists()
        product_data = lister.gather_missing_product_data(cod, v)
        if not product_data or product_data[7] is None:
            return {'ean': None, 'message': f'EAN non trovato per {cod}.{v}'}

        ean = product_data[7]
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("UPDATE products SET ean = %s WHERE cod = %s AND v = %s", (ean, cod, v))
            service.db.conn.commit()

        logger.info(f"[EAN FETCH] {cod}.{v} -> EAN {ean}")
        return {'ean': ean, 'message': f'EAN {ean} salvato per {cod}.{v}'}

    finally:
        lister.driver.quit()
        shutil.rmtree(lister.user_data_dir, ignore_errors=True)


@shared_task(queue='selenium', acks_late=True, reject_on_worker_lost=True)
def fetch_product_from_ean(storage_id, ean, qty=None, loss_type=None):
    """
    Given an EAN that was absent from the products table, look up the product
    in Dropzone, update products.ean if the product is in our catalog, and
    return the result so the UI can show what happened.
    """
    from .models import Storage
    from .services import RestockService
    from .scripts.web_lister import WebLister
    from pathlib import Path
    import shutil

    storage = Storage.objects.select_related('supermarket').get(id=storage_id)

    temp_dir = Path(settings.BASE_DIR) / 'temp_ean_backfill'
    temp_dir.mkdir(exist_ok=True)

    lister = WebLister(
        username=storage.supermarket.username,
        password=storage.supermarket.password,
        storage_name=storage.name,
        download_dir=str(temp_dir),
        id_cod_mag=storage.id_cod_mag,
        id_cliente=storage.supermarket.id_cliente,
        id_azienda=storage.supermarket.id_azienda,
        id_marchio=storage.supermarket.id_marchio,
        id_clienti_canale=storage.supermarket.id_clienti_canale,
        id_clienti_area=storage.supermarket.id_clienti_area,
        headless=True,
    )

    try:
        lister.login()
        lister.navigate_to_lists()
        cod_v = lister.gather_product_data_by_ean(ean)
        if cod_v is None:
            return {'success': False, 'ean': ean, 'message': f'EAN {ean} not found in Dropzone'}

        cod, v = cod_v

        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("SELECT 1 FROM products WHERE cod=%s AND v=%s", (cod, v))
            if cur.fetchone() is None:
                return {'success': False, 'ean': ean, 'message': f'Product {cod}.{v} not in catalog'}

        # Fetch authoritative latest EAN from Dropzone (barcode_data[-1])
        product_data = lister.gather_missing_product_data(cod, v)
        latest_ean = product_data[7] if product_data else None

        new_ean = latest_ean if latest_ean is not None else ean

        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("UPDATE products SET ean=%s WHERE cod=%s AND v=%s", (new_ean, cod, v))
            service.db.conn.commit()

            if qty and loss_type:
                service.db.register_losses(cod, v, qty, loss_type)
                logger.info(f"[EAN FIX] Registered {loss_type} loss: {cod}.{v} qty={qty}")

        logger.info(f"[EAN FIX] EAN {ean} -> {cod}.{v}, stored EAN={new_ean}")
        return {'success': True, 'ean': ean, 'cod': cod, 'v': v, 'new_ean': new_ean, 'message': f'EAN aggiornato per {cod}.{v} ({new_ean})'}

    finally:
        lister.driver.quit()
        shutil.rmtree(lister.user_data_dir, ignore_errors=True)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def run_scheduled_orders(self):
    """
    Fan-out task: queue run_restock_for_storage for every storage whose
    schedule includes today as an order day.  Fires at 06:00 daily via
    Celery Beat (after stats are updated at 05:00).
    """
    from .models import Storage, is_closure_day
    import datetime

    try:
        today_index = datetime.date.today().weekday()  # 0=Monday … 6=Sunday

        storages = Storage.objects.filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')

        if not storages.exists():
            logger.info("[CELERY-SCHED] No storages with schedules found")
            return "No storages to check"

        queued = 0
        skipped = 0

        from .models import SalesSyncLog
        yesterday = datetime.date.today() - datetime.timedelta(days=1)

        for storage in storages:
            if is_closure_day(storage.supermarket):
                logger.info(f"[CELERY-SCHED] Skipping {storage.name} — closure day")
                skipped += 1
                continue

            order_days = storage.schedule.get_order_days()
            if today_index not in order_days:
                logger.info(f"[CELERY-SCHED] {storage.name} — no order today (day {today_index})")
                skipped += 1
                continue

            if storage.supermarket.sync_api_token and not is_closure_day(storage.supermarket, yesterday):
                last_sync = SalesSyncLog.objects.filter(
                    supermarket=storage.supermarket
                ).order_by('-sync_date').first()
                if not last_sync or last_sync.sync_date < yesterday:
                    last_date = last_sync.sync_date if last_sync else 'mai'
                    logger.warning(
                        f"[CELERY-SCHED] BLOCCATO {storage.name} — sync VENSETAR non aggiornato "
                        f"(ultimo: {last_date}, atteso: {yesterday}). Ordine annullato."
                    )
                    skipped += 1
                    continue

            run_restock_for_storage.apply_async(
                args=[storage.id],
                kwargs={'skip_stats_update': True},
            )
            logger.info(f"[CELERY-SCHED] Queued restock for {storage.name}")
            queued += 1

        msg = f"Scheduled orders: {queued} queued, {skipped} skipped"
        logger.info(f"[CELERY-SCHED] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[CELERY-SCHED] Fatal error in run_scheduled_orders")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def cleanup_old_restock_logs(self, max_age_days=180, min_keep_per_storage=10):
    """
    Delete RestockLog rows older than max_age_days, but always keep the
    most recent min_keep_per_storage logs per storage so history is never
    completely wiped.  Runs weekly (Sunday 01:00).
    """
    from datetime import timedelta
    from .models import Storage, RestockLog

    try:
        cutoff = timezone.now() - timedelta(days=max_age_days)

        # Collect IDs to preserve (newest N per storage)
        keep_ids = set()
        for storage_id in Storage.objects.values_list('id', flat=True):
            ids = list(
                RestockLog.objects
                .filter(storage_id=storage_id)
                .order_by('-started_at')
                .values_list('id', flat=True)[:min_keep_per_storage]
            )
            keep_ids.update(ids)

        deleted, _ = (
            RestockLog.objects
            .filter(started_at__lt=cutoff)
            .exclude(id__in=keep_ids)
            .delete()
        )

        msg = (
            f"Log cleanup complete: {deleted} rows deleted "
            f"(older than {max_age_days} days, kept last {min_keep_per_storage} per storage)"
        )
        logger.info(f"[CELERY-CLEANUP] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[CELERY-CLEANUP] Fatal error in cleanup_old_restock_logs")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def cleanup_old_sales_sync_logs(self, max_age_days=90, min_keep_per_supermarket=30):
    """
    Delete SalesSyncLog rows older than max_age_days, keeping the most
    recent min_keep_per_supermarket entries per supermarket.
    Runs weekly (Sunday 01:05).
    """
    from datetime import timedelta
    from .models import Supermarket, SalesSyncLog

    try:
        cutoff = timezone.now() - timedelta(days=max_age_days)

        keep_ids = set()
        for sm_id in Supermarket.objects.values_list('id', flat=True):
            ids = list(
                SalesSyncLog.objects
                .filter(supermarket_id=sm_id)
                .order_by('-created_at')
                .values_list('id', flat=True)[:min_keep_per_supermarket]
            )
            keep_ids.update(ids)

        deleted, _ = (
            SalesSyncLog.objects
            .filter(created_at__lt=cutoff)
            .exclude(id__in=keep_ids)
            .delete()
        )

        msg = (
            f"SalesSyncLog cleanup complete: {deleted} rows deleted "
            f"(older than {max_age_days} days, kept last {min_keep_per_supermarket} per supermarket)"
        )
        logger.info(f"[CELERY-CLEANUP] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[CELERY-CLEANUP] Fatal error in cleanup_old_sales_sync_logs")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def cleanup_old_recipe_cost_alerts(self, read_max_age_days=30, unread_max_age_days=90):
    """
    Delete RecipeCostAlert rows that are stale:
    - Read alerts older than read_max_age_days (default 30)
    - Unread alerts older than unread_max_age_days (default 90)
    Runs weekly (Sunday 01:10).
    """
    from datetime import timedelta
    from .models import RecipeCostAlert

    try:
        now = timezone.now()

        deleted_read, _ = RecipeCostAlert.objects.filter(
            is_read=True,
            created_at__lt=now - timedelta(days=read_max_age_days)
        ).delete()

        deleted_unread, _ = RecipeCostAlert.objects.filter(
            is_read=False,
            created_at__lt=now - timedelta(days=unread_max_age_days)
        ).delete()

        msg = (
            f"RecipeCostAlert cleanup complete: {deleted_read} read alerts deleted "
            f"(>{read_max_age_days}d), {deleted_unread} unread alerts deleted (>{unread_max_age_days}d)"
        )
        logger.info(f"[CELERY-CLEANUP] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[CELERY-CLEANUP] Fatal error in cleanup_old_recipe_cost_alerts")
        raise self.retry(exc=exc)