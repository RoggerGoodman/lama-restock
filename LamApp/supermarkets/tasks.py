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
from .demo import real_storages, real_supermarkets
from .logging_context import (
    SupermarketLogContext,
    enter_supermarket_log,
    exit_supermarket_log,
    enter_order_log,
    exit_order_log,
)

logger = logging.getLogger(__name__)


# Days to wait after a promotion ends before measuring its lift. A day closes at its
# 21:30 sync, so 3 days comfortably guarantees the final promo day has landed.
# The exact-day match in get_promos_ended_days_ago is what makes this
# idempotent: each promotion is seen on exactly one nightly pass, so no
# "already measured" marker is needed on the row.
PROMO_MEASURE_AFTER_DAYS = 3


def _measure_finished_promos(db):
    """
    Measure and store the lift of every promotion that ended
    PROMO_MEASURE_AFTER_DAYS ago for this supermarket. Returns how many were
    recorded.

    Deliberately reads sales_sets un-excised: this is measurement, not ordering, so it
    must see the promo days the ordering path removes. Still completed days only —
    measure_promo_lift indexes by "days ago" and the running day would shift every slot.
    """
    from .scripts.helpers import Helper

    recorded = 0

    for row in db.get_promos_ended_days_ago(PROMO_MEASURE_AFTER_DAYS):
        days_lasted = (row["sale_end"] - row["sale_start"]).days + 1

        lift = Helper.measure_promo_lift(
            Helper.sales_history(row["sales_sets"]),
            PROMO_MEASURE_AFTER_DAYS,
            days_lasted,
        )
        if lift is None:
            continue

        price_std, price_s = row["price_std"], row["price_s"]
        if not price_std or not price_s or price_std <= price_s:
            continue
        discount = round((price_std - price_s) / price_std * 100, 2)

        db.append_promo_lift(row["cod"], row["v"], lift, discount)
        recorded += 1
        logger.info(
            f"[PROMO] {row['cod']}.{row['v']}: lift={lift}x at {discount}% off "
            f"({days_lasted}d promo)"
        )

    return recorded


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
        supermarkets = real_supermarkets()
        
        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"
        
        logger.info(f"[CELERY] Found {supermarkets.count()} supermarket(s) to process")
        
        success_count = 0
        error_count = 0
        
        for supermarket in supermarkets:
            with SupermarketLogContext(supermarket.name):
                try:
                    logger.info(f"[CELERY] Recording losses for: {supermarket.name}")

                    first_storage = supermarket.storages.first()

                    if not first_storage:
                        logger.warning(f"[CELERY] No storages found for {supermarket.name}")
                        error_count += 1
                        continue

                    with AutomatedRestockService(first_storage) as service:
                        # Piggybacked on this task because the schema connection
                        # is already open. Runs BEFORE record_losses on purpose:
                        # that is a long job (login, file download, then
                        # DB writes) and a failure part-way through leaves the
                        # connection in an aborted transaction, which would make
                        # every later statement fail. Since a promo is matched on
                        # exactly one night, a measurement lost that way would
                        # never be retried.
                        try:
                            measured = _measure_finished_promos(service.db)
                            if measured:
                                logger.info(f"[PROMO] {supermarket.name}: {measured} promo lift(s) recorded")
                        except Exception:
                            logger.exception(f"[PROMO] Lift measurement failed for {supermarket.name}")
                            # Don't let a poisoned transaction take losses down too
                            try:
                                service.db.conn.rollback()
                            except Exception:
                                pass

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


# Each supermarket starts somewhere in this window, so the hourly run never
# hits Dropzone with every store at once
DOCUMENT_IMPORT_JITTER_SECONDS = 900


@shared_task
def import_documents_all_supermarkets():
    """Hourly 06:00–22:00: queue one document import per supermarket."""
    import random

    queued = 0
    for supermarket in real_supermarkets():
        import_documents_for_supermarket.apply_async(
            args=[supermarket.id],
            countdown=random.randint(0, DOCUMENT_IMPORT_JITTER_SECONDS),
        )
        queued += 1
    logger.info(f"[DOCS] Document import queued for {queued} supermarkets")
    return f"Document import queued for {queued} supermarkets"


@shared_task(bind=True, max_retries=0, acks_late=True)
def import_documents_for_supermarket(self, supermarket_id):
    """
    Read the last week of Dropzone DDTs and credit notes, then book every
    delivery whose day has come. No retries: the next hourly run is the retry.
    """
    from .models import Supermarket
    from .document_import import DocumentImporter

    supermarket = Supermarket.objects.get(id=supermarket_id)
    _log_ctx = enter_supermarket_log(supermarket.name)
    try:
        try:
            DocumentImporter(supermarket).run()
        except Exception:
            # Still book deliveries already in the ledger that fall due today
            logger.exception(f"[DOCS] Import failed for {supermarket.name}")

        today = datetime.date.today()
        for storage in supermarket.storages.all():
            try:
                with AutomatedRestockService(storage) as service:
                    service.apply_due_deliveries(today)
            except Exception:
                logger.exception(f"[DDT] Booking deliveries failed for {storage.name}")
    finally:
        exit_supermarket_log(_log_ctx)


@shared_task
def run_daily_calibration():
    """
    08:00 daily task — completes calibration reports seeded when a delivery is booked.

    Booking the day's first delivery stores a pre-delivery stock snapshot as a pending
    OrderCalibrationReport. This task runs the full classification on every pending
    report of the last week, using the stored pre-delivery stock + fresh sales data.
    A delivery booked after 08:00 (a late order) is graded the next morning.
    """
    from .models import OrderCalibrationReport

    since = timezone.now() - datetime.timedelta(days=7)
    pending_reports = (
        OrderCalibrationReport.objects
        .select_related('storage', 'storage__supermarket')
        .filter(generated_at__gte=since)
    )

    ok_count = 0
    failed_count = 0

    for report in pending_reports:
        raw_data = report.get_results()
        if raw_data.get('status') != 'pending':
            continue  # already completed (e.g. task ran twice)

        storage = report.storage
        _log_ctx = enter_supermarket_log(storage.supermarket.name)
        try:
            raw_stock = raw_data.get('raw_stock', {})

            with AutomatedRestockService(storage) as service:
                cal = service.compute_calibration_for_storage(
                    coverage_days=report.coverage_days,
                    raw_stock=raw_stock,
                )

            report.products_evaluated = cal['products_evaluated']
            report.products_ok = cal['products_ok']
            report.products_overstocked = cal['products_overstocked']
            report.products_understocked = cal['products_understocked'] + cal['products_critical']
            report.set_results({
                'products_critical': cal['products_critical'],
                'critical': cal['critical'],
                'understocked': cal['understocked'],
                'overstocked': cal['overstocked'],
                'ok': cal['ok'],
            })
            report.save()
            ok_count += 1
            logger.info(
                f"[CAL] {storage.name}: critical={cal['products_critical']}, "
                f"under={cal['products_understocked']}, over={cal['products_overstocked']}, "
                f"ok={cal['products_ok']}"
            )
        except Exception:
            logger.exception(f"[CAL] Failed for {storage.name}")
            failed_count += 1
        finally:
            exit_supermarket_log(_log_ctx)

    return f"Calibration done: {ok_count} ok, {failed_count} failed"


@shared_task(
    bind=True,
    max_retries=0,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def retry_restock_from_checkpoint(self, log_id):
    """Retry a failed restock log from scratch."""
    from .models import RestockLog

    log = RestockLog.objects.select_related('storage__supermarket', 'storage__schedule').get(id=log_id)
    storage = log.storage

    log.status = 'processing'
    log.current_stage = 'processing'
    log.error_message = None
    log.retry_count = (log.retry_count or 0) + 1
    log.save()

    _log_ctx = enter_supermarket_log(storage.supermarket.name)
    try:
        logger.info(f"[CELERY-RETRY] Retrying log #{log_id} for {storage.name} (fresh run)")

        with AutomatedRestockService(storage) as service:
            # Operator-triggered retry → always park for review.
            service.run_full_restock_workflow(log=log, force_review=True)

        logger.info(f"[CELERY-RETRY] Log #{log_id} completed successfully")
    finally:
        exit_supermarket_log(_log_ctx)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=900,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def run_restock_for_storage(self, storage_id, coverage=None, manual=False):
    """
    Run restock for a single storage. Used for both scheduled and manual restocks.

    Args:
        storage_id: The storage ID to run restock for
        coverage: Optional coverage parameter for order calculation
        manual: True for an operator-triggered run — always parks for review,
                ignoring the schedule's require_order_review toggle.
    """
    from .models import Storage, RestockLog

    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket', 'schedule').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)
        logger.info(
            f"[CELERY-ORDER] Running restock for {storage.name} "
            f"(coverage={coverage})"
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
                progress_callback=report_progress,
                force_review=manual,
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
    finally:
        exit_supermarket_log(_log_ctx)


@shared_task(
    bind=True,
    max_retries=0,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def submit_pending_order(self, log_id):
    """
    Submit a reviewed ('Da inviare') order to Dropzone (the "Invia" action). Runs
    the same order placement as the automated flow. No auto-retry: a human is
    watching this one.
    """
    from .models import Storage, RestockLog

    _log_ctx = None
    try:
        log = RestockLog.objects.select_related('storage__supermarket', 'storage__schedule').get(id=log_id)
        storage = log.storage
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

        if log.status != 'awaiting_review':
            logger.warning(
                f"[CELERY-INVIA] Log #{log_id} is '{log.status}', not 'awaiting_review'; skipping submit"
            )
            return {'success': False, 'log_id': log_id, 'error': 'not_awaiting_review'}

        def report_progress(progress, message):
            self.update_state(state='PROGRESS', meta={'progress': progress, 'status': message})
            logger.info(f"[INVIA] {progress}% - {message}")

        self.update_state(state='PROGRESS', meta={'progress': 5, 'status': 'Invio ordine...'})

        with AutomatedRestockService(storage) as service:
            service.execute_pending_order(log, progress_callback=report_progress)

        logger.info(
            f"✓ [CELERY-INVIA] Order submitted for {storage.name} "
            f"(Log #{log.id}: {log.products_ordered} products, {log.total_packages} packages)"
        )

        result = {
            'success': True,
            'log_id': log.id,
            'storage_name': storage.name,
            'products_ordered': log.products_ordered,
            'total_packages': log.total_packages,
            'redirect_url': f'/logs/{log.id}/',
        }
        self.update_state(state='SUCCESS', meta=result)
        return result

    except Exception as exc:
        logger.exception(f"[CELERY-INVIA] Error submitting order for log {log_id}")
        self.update_state(state='FAILURE', meta={'error': str(exc)})
        raise
    finally:
        if _log_ctx is not None:
            exit_supermarket_log(_log_ctx)


@shared_task(bind=True, max_retries=0)
def recalculate_review_order(self, log_id):
    """
    Re-run the decision maker for the products whose stock was corrected during
    review and apply the new package counts (default 'celery' queue — pure DB, no
    Dropzone). Guarded to awaiting_review.
    """
    from .models import RestockLog

    _log_ctx = None
    try:
        log = RestockLog.objects.select_related('storage__supermarket', 'storage__schedule').get(id=log_id)
        storage = log.storage
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

        if log.status != 'awaiting_review':
            logger.warning(f"[RECALC] Log #{log_id} is '{log.status}', not 'awaiting_review'; skipping")
            return {'success': False, 'log_id': log_id, 'error': 'not_awaiting_review'}

        with AutomatedRestockService(storage) as service:
            changes = service.recalculate_order_for_products(log)

        result = {
            'success': True,
            'log_id': log.id,
            'changes': len(changes),
            'redirect_url': f'/logs/{log.id}/',
        }
        self.update_state(state='SUCCESS', meta=result)
        return result

    except Exception as exc:
        logger.exception(f"[RECALC] Error recalculating order for log {log_id}")
        self.update_state(state='FAILURE', meta={'error': str(exc)})
        raise
    finally:
        if _log_ctx is not None:
            exit_supermarket_log(_log_ctx)


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
        storages = real_storages().filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')

        if not storages.exists():
            logger.info("[CELERY] No storages with schedules found")
            return "No storages to update"

        logger.info(f"[CELERY] Found {storages.count()} storage(s) with schedules")

        success_count = 0
        error_count = 0

        for storage in storages:
            _log_ctx = enter_supermarket_log(storage.supermarket.name)
            try:
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
            finally:
                exit_supermarket_log(_log_ctx)

        result_msg = f"List updates complete: {success_count} successful, {error_count} failed"
        logger.info(f"[CELERY] {result_msg}")

        # Purge obsolete products (verified=False, disponibilita=No, stock=0)
        # once per supermarket now that all lists are fresh.
        from .models import Supermarket
        purge_total = 0
        supermarket_ids = storages.values_list('supermarket_id', flat=True).distinct()
        for sm in Supermarket.objects.filter(id__in=supermarket_ids):
            _log_ctx = enter_supermarket_log(sm.name)
            try:
                from .scripts.DatabaseManager import DatabaseManager
                db = DatabaseManager(supermarket_name=sm.name)
                try:
                    purged = db.purge_obsolete_products()
                    if purged:
                        purge_total += len(purged)
                        for p in purged:
                            logger.info(
                                f"[CELERY] Purged obsolete product {p['cod']}.{p['v']} "
                                f"from {sm.name}"
                            )
                        from .services import delete_blacklist_entries_for_purged
                        delete_blacklist_entries_for_purged(purged, supermarket=sm)
                finally:
                    db.close()
            except Exception as e:
                logger.exception(
                    f"[CELERY] Error during obsolete-product purge for {sm.name}"
                )
            finally:
                exit_supermarket_log(_log_ctx)

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
    from django.utils import timezone
    
    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

        logger.info(f"[ADD PRODUCTS] Starting for {storage.name}: {len(products_list)} products")
        
        log = RestockLog.objects.create(
            storage=storage,
            status='processing',
            operation_type='product_addition'
        )
        
        with RestockService(storage) as service:
            lister = WebLister(
                username=storage.supermarket.username,
                password=storage.supermarket.password,
                storage_name=storage.name,
                id_cod_mag=storage.id_cod_mag,
                id_cliente=storage.supermarket.id_cliente,
                id_azienda=storage.supermarket.id_azienda,
                id_marchio=storage.supermarket.id_marchio,
                id_clienti_canale=storage.supermarket.id_clienti_canale,
                id_clienti_area=storage.supermarket.id_clienti_area
            )

            try:
                lister.login()

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
                lister.close()
           
    except Exception as exc:
        logger.exception(f"[ADD PRODUCTS] Error for storage {storage_id}")
        raise self.retry(exc=exc)
    finally:
        exit_supermarket_log(_log_ctx)

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
    
    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

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
    finally:
        exit_supermarket_log(_log_ctx)

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
    
    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

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
    finally:
        exit_supermarket_log(_log_ctx)

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
    import os
    
    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

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
                
                lister = WebLister(
                    username=storage.supermarket.username,
                    password=storage.supermarket.password,
                    storage_name=storage.name,
                    id_cod_mag=storage.id_cod_mag,
                    id_cliente=storage.supermarket.id_cliente,
                    id_azienda=storage.supermarket.id_azienda,
                    id_marchio=storage.supermarket.id_marchio,
                    id_clienti_canale=storage.supermarket.id_clienti_canale,
                    id_clienti_area=storage.supermarket.id_clienti_area
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
                    lister.close()
            
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
    finally:
        exit_supermarket_log(_log_ctx)


@shared_task(
    bind=True,
    queue='selenium',
    acks_late=True,
    reject_on_worker_lost=True
)
def place_manual_order_task(self, user_id, orders_list):
    """
    Place a user-built order (promo products page, equipment catalog).
    Never retried: a rerun after a partial send would create a second order on Dropzone.

    Args:
        user_id: User ID (for ownership validation)
        orders_list: List of dicts with {storage_id, storage_name, supermarket_id, cod, var, qty}
    """
    from .models import Storage
    from .scripts.orderer import Orderer

    try:
        # One draft order per storage; make_orders expects (cod, var, qty, discount)
        by_storage = {}
        for order in orders_list:
            by_storage.setdefault(order['storage_id'], []).append(
                (order['cod'], order['var'], order['qty'], None)
            )

        total_ordered = 0
        total_skipped = 0
        all_skipped_products = []

        for storage_id, products in by_storage.items():
            storage = Storage.objects.select_related('supermarket').get(
                id=storage_id, supermarket__owner_id=user_id
            )
            supermarket = storage.supermarket
            _sm_log_ctx = enter_supermarket_log(supermarket.name)
            _order_log_ctx = enter_order_log(supermarket.name, storage.name)
            orderer = Orderer(
                username=supermarket.username,
                password=supermarket.password
            )
            try:
                orderer.login()
                logger.info(f"[MANUAL ORDER] Ordering {len(products)} products for {storage.name}")

                successful_orders, order_skipped = orderer.make_orders(storage, products)

                total_ordered += len(successful_orders)
                total_skipped += len(order_skipped)
                all_skipped_products.extend(order_skipped)
            finally:
                orderer.close()
                exit_order_log(_order_log_ctx)
                exit_supermarket_log(_sm_log_ctx)

        logger.info(
            f"✅ [MANUAL ORDER] Complete: {total_ordered} ordered, "
            f"{total_skipped} skipped"
        )

        return {
            'success': True,
            'ordered': total_ordered,
            'skipped': total_skipped,
            'skipped_products': all_skipped_products
        }

    except Exception:
        logger.exception(f"[MANUAL ORDER] Error for user #{user_id}")
        raise


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=300
)
def process_ddt_task(self, storage_id, pdf_file_path, invoice_number=None):
    """
    Process DDT delivery document and add stock.

    Args:
        storage_id: Storage ID
        pdf_file_path: Full path to DDT PDF file
        invoice_number: DDT number typed by the user; recorded so neither a later
            manual upload nor the hourly import books the same DDT again
    """
    from .models import Storage, RestockLog
    from .services import RestockService
    from .scripts.ddt_parser import parse_ddt_pdf, process_ddt_deliveries
    import os
    
    _log_ctx = None
    try:
        storage = Storage.objects.select_related('supermarket').get(id=storage_id)
        _log_ctx = enter_supermarket_log(storage.supermarket.name)

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

            number = (invoice_number or '').strip().lstrip('0')
            if number:
                cancelled = service.db.claim_manual_ddt(storage.settore, number, datetime.date.today())
                if cancelled:
                    logger.info(f"[PROCESS DDT] DDT {number}: automatic booking cancelled, loaded by hand")
            
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
                'invoices': [number] if number else [],
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
    finally:
        exit_supermarket_log(_log_ctx)


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
    from .scripts.DatabaseManager import DatabaseManager

    try:
        logger.info("[CELERY] Starting monthly loss zero-prepend for all supermarkets")

        supermarkets = real_supermarkets()

        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"

        success_count = 0
        error_count = 0

        for supermarket in supermarkets:
            _log_ctx = enter_supermarket_log(supermarket.name)
            try:
                # extra_losses is schema-wide, so this needs the supermarket connection
                # and nothing else — no Storage involved.
                db = DatabaseManager(supermarket_name=supermarket.name)
                try:
                    updated = db.prepend_monthly_loss_zeros()
                    logger.info(f"[CELERY] {supermarket.name}: {updated} loss rows updated")
                    success_count += 1
                finally:
                    db.close()

            except Exception as e:
                logger.exception(f"[CELERY] Error prepending zeros for {supermarket.name}")
                error_count += 1
                continue
            finally:
                exit_supermarket_log(_log_ctx)

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

        supermarkets = real_supermarkets()

        if not supermarkets.exists():
            logger.info("[CELERY] No supermarkets found")
            return "No supermarkets to process"

        success_count = 0
        error_count = 0

        for supermarket in supermarkets:
            _log_ctx = enter_supermarket_log(supermarket.name)
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
            finally:
                exit_supermarket_log(_log_ctx)

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
    acks_late=True,
    reject_on_worker_lost=True
)
def sync_storages_task(self, supermarket_id):
    """Sync storages and client parameters from Dropzone for a supermarket."""
    from .models import Supermarket
    from .services import StorageService
    from .scripts.dropzone_client import DropzoneClient, DropzoneLoginError

    _log_ctx = None
    try:
        supermarket = Supermarket.objects.get(id=supermarket_id)
        _log_ctx = enter_supermarket_log(supermarket.name)
        logger.info(f"[SYNC STORAGES] Starting for {supermarket.name}")

        # Client data first: storage discovery needs id_cliente
        client = DropzoneClient(supermarket.username, supermarket.password)
        try:
            client.login()
        except DropzoneLoginError as exc:
            # Retrying cannot fix wrong credentials: fail now with a readable message
            logger.warning(f"[SYNC STORAGES] Login refused for {supermarket.name}: {exc}")
            if "expired" in str(exc):
                msg = "Password Dropzone scaduta. Cambiala su Dropzone, poi inseriscila qui e premi di nuovo Sincronizza."
            else:
                msg = "Credenziali Dropzone non valide. Controlla username e password, poi premi di nuovo Sincronizza."
            raise DropzoneLoginError(msg) from None
        client_data = client.gather_client_data()

        supermarket.id_cliente = client_data.get('id_cliente')
        supermarket.id_azienda = client_data.get('id_azienda')
        supermarket.id_marchio = client_data.get('id_marchio')
        supermarket.id_clienti_canale = client_data.get('id_clienti_canale')
        supermarket.id_clienti_area = client_data.get('id_clienti_area')
        supermarket.id_user = client_data.get('id_user')
        supermarket.x5cper = client_data.get('x5cper')
        supermarket.save(update_fields=[
            'id_cliente', 'id_azienda', 'id_marchio',
            'id_clienti_canale', 'id_clienti_area', 'id_user', 'x5cper',
        ])
        logger.info(f"[SYNC STORAGES] Client data saved for {supermarket.name}")

        StorageService.sync_storages(supermarket)
        logger.info(f"[SYNC STORAGES] Storages synced for {supermarket.name}")

        return {
            'success': True,
            'synced': True,
            'supermarket_id': supermarket_id,
            'message': 'Magazzini e dati cliente sincronizzati con successo.',
        }
    except DropzoneLoginError:
        raise
    except Exception as exc:
        logger.exception(f"[SYNC STORAGES] Error for supermarket #{supermarket_id}")
        raise self.retry(exc=exc)
    finally:
        exit_supermarket_log(_log_ctx)


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
    import time

    try:
        storages = real_storages().filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')

        if not storages.exists():
            logger.info("[EAN BACKFILL] No storages with schedules found")
            return "No storages to process"

        total_updated = 0
        total_failed = 0

        for storage in storages:
            _log_ctx = enter_supermarket_log(storage.supermarket.name)
            try:
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

                lister = WebLister(
                    username=storage.supermarket.username,
                    password=storage.supermarket.password,
                    storage_name=storage.name,
                    id_cod_mag=storage.id_cod_mag,
                    id_cliente=storage.supermarket.id_cliente,
                    id_azienda=storage.supermarket.id_azienda,
                    id_marchio=storage.supermarket.id_marchio,
                    id_clienti_canale=storage.supermarket.id_clienti_canale,
                    id_clienti_area=storage.supermarket.id_clienti_area
                )

                try:
                    lister.login()

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
                    lister.close()
            finally:
                exit_supermarket_log(_log_ctx)

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

    storage = Storage.objects.select_related('supermarket').get(id=storage_id)
    _log_ctx = enter_supermarket_log(storage.supermarket.name)

    lister = WebLister(
        username=storage.supermarket.username,
        password=storage.supermarket.password,
        storage_name=storage.name,
        id_cod_mag=storage.id_cod_mag,
        id_cliente=storage.supermarket.id_cliente,
        id_azienda=storage.supermarket.id_azienda,
        id_marchio=storage.supermarket.id_marchio,
        id_clienti_canale=storage.supermarket.id_clienti_canale,
        id_clienti_area=storage.supermarket.id_clienti_area
    )

    try:
        lister.login()
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
        lister.close()
        exit_supermarket_log(_log_ctx)


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

    storage = Storage.objects.select_related('supermarket').get(id=storage_id)
    _log_ctx = enter_supermarket_log(storage.supermarket.name)

    lister = WebLister(
        username=storage.supermarket.username,
        password=storage.supermarket.password,
        storage_name=storage.name,
        id_cod_mag=storage.id_cod_mag,
        id_cliente=storage.supermarket.id_cliente,
        id_azienda=storage.supermarket.id_azienda,
        id_marchio=storage.supermarket.id_marchio,
        id_clienti_canale=storage.supermarket.id_clienti_canale,
        id_clienti_area=storage.supermarket.id_clienti_area
    )

    try:
        lister.login()
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
        lister.close()
        exit_supermarket_log(_log_ctx)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def roll_sales_day_for_supermarket(self, supermarket_id, day_iso):
    """Open today's slot in sales_sets for one supermarket."""
    from .models import Supermarket
    from .scripts.DatabaseManager import DatabaseManager
    import datetime

    supermarket = Supermarket.objects.get(id=supermarket_id)
    day = datetime.date.fromisoformat(day_iso)

    _ctx = enter_supermarket_log(supermarket.name)
    db = None
    try:
        db = DatabaseManager(supermarket_name=supermarket.name)
        rolled = db.roll_sales_day(day)
        logger.info(f"[DAY-ROLL] {supermarket.name}: {rolled} products rolled to {day}")
        return f"{supermarket.name}: {rolled}"
    finally:
        if db:
            db.close()
        exit_supermarket_log(_ctx)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def roll_sales_day_all_supermarkets(self):
    """
    Queue a day-roll per supermarket, just after midnight.

    Keeps "slot 0 is today" true from the date change rather than from the first sync at
    08:30, so anything running early does not slice off yesterday as the running day.

    Fans out rather than looping: a full roll touches every product in a schema, and one
    task doing that for every store serially would outgrow the task time limit.
    """
    from .models import Supermarket

    today = timezone.localtime().date().isoformat()
    ids = list(real_supermarkets().values_list('id', flat=True))
    for sm_id in ids:
        roll_sales_day_for_supermarket.apply_async(args=[sm_id, today])

    msg = f"Day roll {today}: queued {len(ids)} supermarkets"
    logger.info(f"[DAY-ROLL] {msg}")
    return msg


# Days of demand we tolerate being blind to. One missed sync at the busiest hour is ~6%,
# so this absorbs a couple of failures without letting a dead feed through.
SYNC_UNSEEN_DEMAND_LIMIT = 0.15


def _unseen_demand_days(supermarket, last_sync_local, now_local, today) -> float:
    """
    Demand that happened since the last sync, expressed in days.

    This is what `stock` is wrong by, which is the thing worth gating on. Elapsed hours
    are a poor proxy: overnight they mean nothing, at midday they mean everything.
    """
    sync_date = last_sync_local.date()

    if sync_date >= today:
        return max(0.0,
                   supermarket.remaining_day_fraction(last_sync_local, on_date=today)
                   - supermarket.remaining_day_fraction(now_local, on_date=today))

    unseen = supermarket.remaining_day_fraction(last_sync_local, on_date=sync_date)
    unseen += max(0, (today - sync_date).days - 1)
    unseen += 1.0 - supermarket.remaining_day_fraction(now_local, on_date=today)
    return unseen


def _realtime_sync_is_usable(supermarket, now_local, today, storage_name) -> bool:
    """
    Whether stock is fresh enough to order against.

    Blocks rather than warns: ordering on stale stock quietly buys the wrong quantity for
    every product at once instead of failing visibly.
    """
    last_sync = supermarket.last_sales_sync_at
    if not last_sync:
        logger.warning(
            f"[CELERY-SCHED] BLOCCATO {storage_name} — nessun sync real-time ricevuto. "
            f"Ordine annullato."
        )
        return False

    last_sync_local = timezone.localtime(last_sync)

    # Without a curve there is no way to weigh the gap, so fall back to calendar days.
    if not supermarket.intraday_curve:
        return (today - last_sync_local.date()).days <= 1

    unseen = _unseen_demand_days(supermarket, last_sync_local, now_local, today)
    if unseen <= SYNC_UNSEEN_DEMAND_LIMIT:
        return True

    logger.warning(
        f"[CELERY-SCHED] BLOCCATO {storage_name} — vendite non sincronizzate pari a "
        f"{unseen:.0%} di una giornata (ultimo sync: {last_sync_local:%Y-%m-%d %H:%M}). "
        f"Ordine annullato."
    )
    return False


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def run_scheduled_orders(self):
    """
    Fan-out task: queue run_restock_for_storage for every storage whose schedule says
    today, once that storage's own configured firing time has arrived.

    Runs every 15 minutes because order times are per storage AND per weekday, which a
    single daily trigger cannot express. OrderDispatch keeps it to once per day.

    Fires late rather than not at all: a missed order costs a stockout, a late one just
    covers a shorter window, which coverage already accounts for.
    """
    from .models import Storage, is_closure_day, ScheduleException, OrderDispatch

    LATE_WARN_MINUTES = 120

    try:
        now_local = timezone.localtime()
        today = now_local.date()
        today_index = today.weekday()  # 0=Monday … 6=Sunday

        storages = real_storages().filter(
            schedule__isnull=False
        ).select_related('supermarket', 'schedule')

        queued = 0
        skipped = 0

        for storage in storages:
            # Routine skips stay at debug: this runs 96 times a day, and at info level
            # every storage would log a line on every pass.
            if is_closure_day(storage.supermarket):
                logger.debug(f"[CELERY-SCHED] Skipping {storage.name} — closure day")
                skipped += 1
                continue

            order_days = storage.schedule.get_order_days()
            if today_index not in order_days:
                skipped += 1
                continue

            skip_exc = ScheduleException.objects.filter(
                schedule=storage.schedule,
                date=today,
                exception_type='skip'
            ).first()
            if skip_exc:
                note = f" ({skip_exc.note})" if skip_exc.note else ""
                logger.info(f"[CELERY-SCHED] Skipping {storage.name} — exception 'skip' on {today}{note}")
                skipped += 1
                continue

            supermarket = storage.supermarket

            order_time = storage.schedule.get_order_time(today_index)
            if now_local.time() < order_time:
                skipped += 1
                continue

            # Same calendar day and now >= order_time, so plain minutes avoid any
            # naive/aware mismatch.
            late_minutes = (
                (now_local.hour * 60 + now_local.minute)
                - (order_time.hour * 60 + order_time.minute)
            )
            if late_minutes > LATE_WARN_MINUTES:
                logger.warning(
                    f"[CELERY-SCHED] {storage.name} firing {late_minutes} min after its "
                    f"{order_time:%H:%M} slot — was the scheduler down?"
                )

            if supermarket.sync_api_token:
                if not _realtime_sync_is_usable(supermarket, now_local, today, storage.name):
                    skipped += 1
                    continue

            # Claim the day before queueing; the unique constraint settles any race.
            _, claimed = OrderDispatch.objects.get_or_create(
                storage=storage,
                order_date=today,
                defaults={'order_time': order_time},
            )
            if not claimed:
                skipped += 1
                continue

            run_restock_for_storage.apply_async(args=[storage.id])
            logger.info(
                f"[CELERY-SCHED] Queued restock for {storage.name} "
                f"(slot {order_time:%H:%M}, fired {now_local:%H:%M})"
            )
            queued += 1

        msg = f"Scheduled orders: {queued} queued, {skipped} skipped"
        if queued:
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


@shared_task
def import_promo_emails_task():
    """Nightly: load promo PDFs mailed to the PROMO_IMAP mailbox (see promos.py)."""
    from .promos import import_promo_emails

    loaded = import_promo_emails()
    logger.info(f"[PROMO MAIL] {loaded} promo email(s) loaded")
    return loaded


@shared_task
def sync_chain_product_links():
    """Nightly product link pass over every store (see chain_links.py)."""
    from .chain_links import sync_all

    reports, retired = sync_all()
    for report in reports:
        for line in report.lines():
            logger.info(f"[CHAIN LINKS] {report.supermarket.name}: {line}")
    msg = (
        f"Product links synced: {sum(len(r.removed) for r in reports)} removed, "
        f"{sum(len(r.added) for r in reports)} added, "
        f"{sum(len(r.verified) for r in reports)} verified, "
        f"{len(retired)} chain link(s) retired, "
        f"{sum(1 for r in reports if r.error)} store(s) failed"
    )
    logger.info(f"[CHAIN LINKS] {msg}")
    return msg


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def cleanup_old_dropzone_documents(self):
    """
    Prune each supermarket's Dropzone document ledger to LEDGER_RETENTION_DAYS.
    Runs weekly (Sunday 01:20).
    """
    from .document_import import LEDGER_RETENTION_DAYS
    from .scripts.DatabaseManager import DatabaseManager

    before = datetime.date.today() - datetime.timedelta(days=LEDGER_RETENTION_DAYS)
    total = 0
    for supermarket in real_supermarkets():
        db = None
        try:
            db = DatabaseManager(supermarket_name=supermarket.name)
            deleted = db.prune_document_ledger(before)
            total += deleted
            logger.info(f"[CELERY-CLEANUP] {supermarket.name}: {deleted} ledger documents dated before {before} deleted")
        except Exception:
            logger.exception(f"[CELERY-CLEANUP] Ledger cleanup failed for {supermarket.name}")
        finally:
            if db:
                db.close()
    return f"Dropzone ledger cleanup: {total} documents deleted (dated before {before})"


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


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def cleanup_old_decision_maker_logs(self, max_age_days=7):
    """
    Delete per-order decision_maker log files (logs/<supermarket-slug>/decision_maker/*.log*)
    older than max_age_days. Runs weekly (Sunday 01:15).
    """
    from datetime import timedelta
    from pathlib import Path

    try:
        cutoff = timezone.now().timestamp() - timedelta(days=max_age_days).total_seconds()
        logs_dir = Path(settings.BASE_DIR) / 'logs'

        deleted = 0
        for path in logs_dir.glob('*/decision_maker/*.log*'):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
                    deleted += 1
            except OSError:
                continue

        msg = f"Decision-maker log cleanup complete: {deleted} file(s) deleted (older than {max_age_days} days)"
        logger.info(f"[CELERY-CLEANUP] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[CELERY-CLEANUP] Fatal error in cleanup_old_decision_maker_logs")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def prepend_monthly_bought_zeros(self):
    """
    1st of month — prepend a 0 to bought_last_24 for every product whose
    last_update_bought is in a previous month, across all supermarkets.
    Also advances last_update_bought to today so that the first invoice of
    the new month correctly accumulates into slot [0] rather than inserting
    a second new-month slot.
    """
    from .models import Supermarket
    from .scripts.DatabaseManager import DatabaseManager

    try:
        supermarkets = real_supermarkets()
        total_updated = 0

        for supermarket in supermarkets:
            _log_ctx = enter_supermarket_log(supermarket.name)
            db = DatabaseManager(supermarket_name=supermarket.name)
            try:
                updated = db.rollover_bought_last_24()
                total_updated += updated
                logger.info(
                    f"[MONTHLY-ROLLOVER] {supermarket.name}: "
                    f"prepended 0 to {updated} product(s)"
                )
            except Exception as e:
                logger.exception(
                    f"[MONTHLY-ROLLOVER] Error for {supermarket.name}"
                )
            finally:
                db.close()
                exit_supermarket_log(_log_ctx)

        msg = f"Monthly bought_last_24 rollover complete: {total_updated} product(s) updated across all supermarkets"
        logger.info(f"[MONTHLY-ROLLOVER] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[MONTHLY-ROLLOVER] Fatal error")
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def prepend_monthly_sold_zeros(self):
    """
    1st of month — prepend a 0 to sold_last_24 for every product with sales
    history, across all supermarkets. Opens a fresh accumulator slot for the
    new month; apply_realtime_sales only ever adds into slot [0].

    Must run before the day's first sync (08:30): the feed books sales on the day they
    happen, so the 1st's own sales belong in the new month's slot.
    """
    from .models import Supermarket
    from .scripts.DatabaseManager import DatabaseManager

    try:
        supermarkets = real_supermarkets()
        total_updated = 0

        for supermarket in supermarkets:
            _log_ctx = enter_supermarket_log(supermarket.name)
            db = DatabaseManager(supermarket_name=supermarket.name)
            try:
                updated = db.rollover_sold_last_24()
                total_updated += updated
                logger.info(
                    f"[MONTHLY-ROLLOVER] {supermarket.name}: "
                    f"prepended 0 to {updated} product(s) (sold_last_24)"
                )
            except Exception:
                logger.exception(
                    f"[MONTHLY-ROLLOVER] Error for {supermarket.name} (sold_last_24)"
                )
            finally:
                db.close()
                exit_supermarket_log(_log_ctx)

        msg = f"Monthly sold_last_24 rollover complete: {total_updated} product(s) updated across all supermarkets"
        logger.info(f"[MONTHLY-ROLLOVER] {msg}")
        return msg

    except Exception as exc:
        logger.exception("[MONTHLY-ROLLOVER] Fatal error (sold_last_24)")
        raise self.retry(exc=exc)