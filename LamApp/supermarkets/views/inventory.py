"""Inventory search, results, "fermi" products and stock adjustments."""

from datetime import date
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..automation_services import AutomatedRestockService
from ..models import Supermarket, Storage, Blacklist, BlacklistEntry, StockCorrection
from ..forms import InventorySearchForm
from ..services import RestockService, delete_blacklist_entries_for_purged
from ..scripts.DatabaseManager import DatabaseManager
from .common import parse_shelf_barcode

logger = logging.getLogger(__name__)


@login_required
def inventory_search_view(request):
    """Main inventory search interface"""

    # Get user's supermarkets for ILIKE search dropdown
    user_supermarkets = Supermarket.objects.filter(owner=request.user)

    # GET-driven: the search only navigates to a results page (no mutation), so it
    # stays reachable for the read-only demo account. `search_type` is present
    # whenever the form was actually submitted.
    if 'search_type' in request.GET:
        form = InventorySearchForm(request.user, request.GET)

        if form.is_valid():
            search_type = form.cleaned_data['search_type']

            if search_type == 'cod_var':
                cod = form.cleaned_data['product_code']
                var = form.cleaned_data['product_var']
                return redirect(f'/inventory/results/cod_var/?cod={cod}&var={var}')

            elif search_type == 'settore_cluster':
                supermarket_id = form.cleaned_data['supermarket']
                settore = form.cleaned_data['settore']
                cluster = form.cleaned_data.get('cluster') or ''

                if cluster:
                    return redirect(f'/inventory/results/settore_cluster/?supermarket_id={supermarket_id}&settore={settore}&cluster={cluster}')
                else:
                    return redirect(f'/inventory/results/settore_cluster/?supermarket_id={supermarket_id}&settore={settore}')

            elif search_type == 'ean':
                ean_code = form.cleaned_data['ean_code'].strip()
                return redirect(f'/inventory/results/ean/?ean={ean_code}')
    else:
        form = InventorySearchForm(request.user)

    # Count fermi products per storage (verified, disponibilita != No, last 14 sales_sets all zero)
    fermi_storages = []
    for sm in user_supermarkets.prefetch_related('storages'):
        for storage in sm.storages.all():
            try:
                with RestockService(storage) as service:
                    cursor = service.db.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) AS cnt
                        FROM product_stats ps
                        JOIN products p ON p.cod = ps.cod AND p.v = ps.v
                        WHERE ps.verified = TRUE
                          AND p.disponibilita != 'No'
                          AND ps.stock <> 0
                          AND p.settore = %s
                          AND (
                              SELECT bool_and(elem::numeric = 0)
                              FROM (
                                  SELECT value AS elem
                                  FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY
                                  -- include today (ordinality 1): a sale today means not fermo
                                  WHERE ordinality BETWEEN 1 AND 14
                              ) recent
                              HAVING count(*) = 14
                          ) = TRUE
                    """, (storage.settore,))
                    cnt = cursor.fetchone()['cnt']
                    if cnt > 0:
                        fermi_storages.append({
                            'storage_id': storage.id,
                            'storage_name': storage.name,
                            'supermarket_name': sm.name,
                            'count': cnt,
                        })
            except Exception as e:
                logger.warning(f"Could not count fermi products for {storage.name}: {e}")

    return render(request, 'inventory/search.html', {
        'form': form,
        'user_supermarkets': user_supermarkets,
        'fermi_storages': fermi_storages,
    })


@login_required
def fermi_products_api_view(request, storage_id):
    storage = get_object_or_404(Storage, pk=storage_id, supermarket__owner=request.user)
    try:
        blacklisted = set(
            BlacklistEntry.objects.filter(blacklist__storage=storage)
            .values_list('product_code', 'product_var')
        )

        with RestockService(storage) as service:
            cursor = service.db.cursor()
            cursor.execute("""
                SELECT p.settore, ps.cod, ps.v, p.descrizione, ps.stock, p.cluster,
                    (
                        SELECT COALESCE(
                            -- ord 1 is today; counted, so a sale today reads as 0 days.
                            (SELECT (MIN(t.ord) - 1)::int
                             FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY AS t(elem, ord)
                             WHERE t.elem::numeric != 0),
                            -- all stored days zero: report the full stored span, which
                            -- caps at 60 and surfaces as "60+" in the UI.
                            jsonb_array_length(ps.sales_sets)
                        )
                    ) AS days_without_sales
                FROM product_stats ps
                JOIN products p ON p.cod = ps.cod AND p.v = ps.v
                WHERE ps.verified = TRUE
                  AND p.disponibilita != 'No'
                  AND ps.stock <> 0
                  AND p.settore = %s
                  AND (
                      SELECT bool_and(elem::numeric = 0)
                      FROM (
                          SELECT value AS elem
                          FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY
                          -- include today (ordinality 1): a sale today means not fermo
                          WHERE ordinality BETWEEN 1 AND 14
                      ) recent
                      HAVING count(*) = 14
                  ) = TRUE
                ORDER BY p.cluster NULLS LAST, p.descrizione
            """, (storage.settore,))
            products = [
                {
                    'settore': row['settore'],
                    'cod': row['cod'],
                    'v': row['v'],
                    'descrizione': row['descrizione'] or f"{row['cod']}.{row['v']}",
                    'stock': row['stock'] if row['stock'] is not None else 0,
                    'cluster': row['cluster'],
                    'days': row['days_without_sales'] if row['days_without_sales'] is not None else 0,
                    'blacklisted': (row['cod'], row['v']) in blacklisted,
                }
                for row in cursor.fetchall()
            ]
        return JsonResponse({'products': products})
    except Exception as e:
        logger.exception(f"Error fetching fermi products for storage {storage_id}")
        return JsonResponse({'error': str(e)}, status=500)


def _todays_deliveries(user, results):
    """
    {"<supermarket>|<cod>.<v>": {pieces, ddt, booked}} for the products on the page
    that a DDT delivers today. Stock counts a delivery from the morning it is due,
    while the truck may come hours later: a shelf count before then is not a reason
    to correct stock.
    """
    wanted = {}
    for r in results:
        wanted.setdefault(r['supermarket_name'], set()).add((r['cod'], r['v']))

    today = date.today()
    out = {}
    for sm in Supermarket.objects.filter(owner=user, name__in=wanted):
        try:
            db = DatabaseManager(supermarket_name=sm.name)
            try:
                lines = db.deliveries_on(today)
            finally:
                db.close()
        except Exception:
            logger.exception(f"Could not read today's deliveries for {sm.name}")
            continue
        for line in lines:
            if (line['cod'], line['v']) not in wanted[sm.name]:
                continue
            entry = out.setdefault(f"{sm.name}|{line['cod']}.{line['v']}",
                                   {'pieces': 0, 'ddt': [], 'booked': True})
            entry['pieces'] += line['pieces']
            if line['doc_number'] not in entry['ddt']:
                entry['ddt'].append(line['doc_number'])
            entry['booked'] = entry['booked'] and line['booked']
    return out


def _set_default_minimum(result, storage, cluster_minimums):
    """Baseline that applies when the product has no override: cluster value if set, else storage."""
    cluster = result.get('cluster')
    if cluster in cluster_minimums:
        result['storage_minimum_stock'] = cluster_minimums[cluster]
        result['default_minimum_label'] = f"Default cluster {cluster}"
    else:
        result['storage_minimum_stock'] = storage.minimum_stock
        result['default_minimum_label'] = "Default magazzino"


@login_required
def inventory_results_view(request, search_type):
    """Display inventory search results - NOW INCLUDES minimum_stock"""
    
    results = []
    search_description = ""
    supermarket = None
    settore_name = None
    
    try:
        if search_type == 'cod_var':
            # Search for specific product
            cod = int(request.GET.get('cod'))
            var = int(request.GET.get('var'))
            search_description = f"Prodotto {cod}.{var}"
            
            found = False
            for sm in Supermarket.objects.filter(owner=request.user):
                storage = sm.storages.first()
                if not storage:
                    continue
                with RestockService(storage) as service:
                    try:
                        cur = service.db.cursor()
                        cur.execute("""
                            SELECT 
                                p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita, 
                                p.settore, p.cluster,
                                ps.stock, ps.last_update_sold, ps.verified, ps.minimum_stock,
                                ps.max_stock, ps.bulk_order
                            FROM products p
                            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                            WHERE p.cod = %s AND p.v = %s AND ps.verified = TRUE
                        """, (cod, var))
                        
                        row = cur.fetchone()
                        if row:
                            found = True
                            result = dict(row)
                            result['supermarket_name'] = sm.name
                            # Find the correct storage based on product's settore
                            product_storage = sm.storages.filter(settore=row['settore']).first()
                            result['storage_id'] = product_storage.id if product_storage else storage.id
                            result['minimum_stock'] = row['minimum_stock']  # None if no per-product override
                            effective_storage = product_storage or storage
                            _set_default_minimum(result, effective_storage, effective_storage.cluster_minimum_stocks())
                            results.append(result)
                    except Exception as e:
                        logger.exception(f"Error searching in {sm.name}")

            if not found:
                return redirect('inventory-product-not-found', cod=cod, var=var)

        elif search_type == 'ean':
            ean_raw = request.GET.get('ean', '').strip()
            if not ean_raw:
                messages.error(request, "EAN mancante")
                return redirect('inventory-search')

            # A scanned shelf label is a crypted cod.v; a product EAN is a real EAN.
            shelf = parse_shelf_barcode(ean_raw)
            if shelf:
                where_sql = "WHERE p.cod = %s AND p.v = %s AND ps.verified = TRUE"
                where_params = (shelf[0], shelf[1])
                search_description = f"Prodotto {shelf[0]}.{shelf[1]}"
            else:
                try:
                    ean = int(ean_raw)
                except ValueError:
                    messages.error(request, "EAN non valido")
                    return redirect('inventory-search')
                where_sql = "WHERE p.ean = %s AND ps.verified = TRUE"
                where_params = (ean,)
                search_description = f"EAN: {ean_raw}"

            found = False
            for sm in Supermarket.objects.filter(owner=request.user):
                storage = sm.storages.first()
                if not storage:
                    continue
                with RestockService(storage) as service:
                    try:
                        cur = service.db.cursor()
                        cur.execute(f"""
                            SELECT
                                p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                                p.settore, p.cluster,
                                ps.stock, ps.last_update_sold, ps.verified, ps.minimum_stock,
                                ps.max_stock, ps.bulk_order
                            FROM products p
                            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                            {where_sql}
                        """, where_params)
                        row = cur.fetchone()
                        if row:
                            found = True
                            result = dict(row)
                            result['supermarket_name'] = sm.name
                            product_storage = sm.storages.filter(settore=row['settore']).first()
                            result['storage_id'] = product_storage.id if product_storage else storage.id
                            result['minimum_stock'] = row['minimum_stock']
                            effective_storage = product_storage or storage
                            _set_default_minimum(result, effective_storage, effective_storage.cluster_minimum_stocks())
                            results.append(result)
                    except Exception:
                        logger.exception(f"Error searching EAN in {sm.name}")

            if not found:
                messages.warning(request, f"Nessun prodotto trovato per EAN: {ean_raw}")
                return redirect('inventory-search')

        elif search_type == 'settore_cluster':
            supermarket_id = request.GET.get('supermarket_id')
            settore = request.GET.get('settore')
            cluster_param = request.GET.get('cluster', '')
            clusters = [c.strip() for c in cluster_param.split(',') if c.strip()]

            if not supermarket_id or not settore:
                messages.error(request, "Parametri di ricerca mancanti")
                return redirect('inventory-search')

            try:
                supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
            except Exception as e:
                logger.exception("Supermarket not found")
                messages.error(request, f"Punto vendita non trovato: {e}")
                return redirect('inventory-search')

            settore_name = settore

            if clusters:
                search_description = f"{supermarket.name} - {settore} - Cluster: {', '.join(clusters)}"
            else:
                search_description = f"{supermarket.name} - {settore} (Tutti i cluster)"

            storage = supermarket.storages.filter(settore=settore).first()

            if not storage:
                messages.warning(request, f"Nessun magazzino trovato per il settore: {settore}")
                return redirect('inventory-search')

            with RestockService(storage) as service:
                try:
                    cur = service.db.cursor()

                    if clusters:
                        placeholders = ','.join(['%s'] * len(clusters))
                        cur.execute(f"""
                            SELECT
                                p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                                p.settore, p.cluster,
                                ps.stock, ps.last_update_sold, ps.verified, ps.minimum_stock,
                                ps.max_stock, ps.bulk_order
                            FROM products p
                            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                            WHERE p.settore = %s AND p.cluster IN ({placeholders}) AND ps.verified = TRUE
                            ORDER BY p.cluster, p.descrizione
                        """, [settore] + clusters)
                    else:
                        cur.execute("""
                            SELECT
                                p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                                p.settore, p.cluster,
                                ps.stock, ps.last_update_sold, ps.verified, ps.minimum_stock,
                                ps.max_stock, ps.bulk_order
                            FROM products p
                            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                            WHERE p.settore = %s AND ps.verified = TRUE
                            ORDER BY p.cluster, p.descrizione
                        """, (settore,))

                    cluster_minimums = storage.cluster_minimum_stocks()
                    for row in cur.fetchall():
                        result = dict(row)
                        result['supermarket_name'] = supermarket.name
                        result['storage_id'] = storage.id
                        result['minimum_stock'] = row['minimum_stock']  # None if no per-product override
                        _set_default_minimum(result, storage, cluster_minimums)
                        results.append(result)

                except Exception as e:
                    logger.exception(f"Database error in settore search")
                    messages.error(request, f"Errore del database: {e}")
                    return redirect('inventory-search')
    
    except Exception as e:
        logger.exception("Error in inventory search")
        messages.error(request, f"Errore di ricerca: {str(e)}")
        return redirect('inventory-search')
    
    context = {
        'results': results,
        'search_description': search_description,
        'search_type': search_type,
        'supermarket': supermarket,
        'settore': settore_name,
        'cluster_param': request.GET.get('cluster', '') if search_type == 'settore_cluster' else '',
        # Per store, since a code/EAN search can return several. The page's stock values
        # describe sales up to these moments, so the modal shows their age.
        'sales_sync_at': {
            name: synced.replace(microsecond=0).isoformat()
            for name, synced in Supermarket.objects.filter(
                owner=request.user,
                name__in={r['supermarket_name'] for r in results},
                last_sales_sync_at__isnull=False,
            ).values_list('name', 'last_sales_sync_at')
        },
        'sync_stale_minutes': AutomatedRestockService.SYNC_STALE_WARN_MINUTES,
        'todays_deliveries': _todays_deliveries(request.user, results),
    }

    return render(request, 'inventory/results.html', context)


@login_required
def inventory_product_not_found_view(request, cod, var):
    """Handle case when product not found"""
    
    # Check all databases to determine why not found
    product_exists = False
    is_verified = False
    supermarket_name = None
    
    for sm in Supermarket.objects.filter(owner=request.user):
        storage = sm.storages.first()
        if not storage:
            continue
        
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("""
                SELECT ps.verified
                FROM products p
                LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.cod = %s AND p.v = %s
            """, (cod, var))
            
            row = cur.fetchone()
            if row:
                product_exists = True
                is_verified = row['verified']
                supermarket_name = sm.name
                break
    
    context = {
        'cod': cod,
        'var': var,
        'product_exists': product_exists,
        'is_verified': is_verified,
        'supermarket_name': supermarket_name,
    }
    
    return render(request, 'inventory/product_not_found.html', context)


@login_required
def get_settores_for_supermarket_view(request, supermarket_id):
    """AJAX endpoint to get settores for a supermarket"""
    try:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        
        settores = list(
            supermarket.storages.values_list('settore', flat=True)
            .distinct()
            .order_by('settore')
        )
        
        logger.info(f"API: Loaded {len(settores)} settores for {supermarket.name}")
        return JsonResponse({'settores': settores})
    
    except Exception as e:
        logger.exception("Error loading settores")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_POST
def inventory_flag_for_purge_ajax_view(request):
    """
    AJAX endpoint to flag product for purge from inventory view.
    If product has stock > 0, adds to "In fase di eliminazione" blacklist.
    If stock = 0, deletes immediately.
    """
    try:
        data = json.loads(request.body)
        cod = int(data['cod'])
        var = int(data['var'])
        storage_id = int(data['storage_id'])

        storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

        if not storage:
            return JsonResponse({'success': False, 'message': 'No storage found'}, status=400)

        caller_stock = data.get('stock')  # set by fermi page to trigger shrinkage + immediate purge

        with RestockService(storage) as service:
            # Check product stock
            cursor = service.db.cursor()
            cursor.execute("""
                SELECT ps.stock
                FROM product_stats ps
                WHERE ps.cod = %s AND ps.v = %s
            """, (cod, var))

            row = cursor.fetchone()

            if not row:
                return JsonResponse({'success': False, 'message': f'Product {cod}.{var} not found'}, status=404)

            stock = row['stock'] if row['stock'] is not None else 0

            if caller_stock is not None:
                # Fermi path: register stock as shrinkage, then purge (preserving the loss record)
                shrinkage_qty = int(caller_stock)
                if shrinkage_qty > 0:
                    service.db.register_losses(cod, var, shrinkage_qty, 'shrinkage')
                    logger.info(f"Registered shrinkage of {shrinkage_qty} units for {cod}.{var}")
                result = service.db.purge_product(cod, var)
                delete_blacklist_entries_for_purged([result], storage=storage)
                return JsonResponse({'success': True, 'message': result['message']})
            elif stock > 0:
                # Has stock - add to "In fase di eliminazione" blacklist
                PURGE_BLACKLIST_NAME = "In fase di eliminazione"

                # Get or create the blacklist
                blacklist, created = Blacklist.objects.get_or_create(
                    storage=storage,
                    name=PURGE_BLACKLIST_NAME,
                    defaults={'description': 'Articoli in attesa di eliminazione automatica quando la giacenza raggiunge 0'}
                )

                if created:
                    logger.info(f"Created blacklist '{PURGE_BLACKLIST_NAME}' for storage {storage.name}")

                # Add product to blacklist (ignore if already exists)
                BlacklistEntry.objects.get_or_create(
                    blacklist=blacklist,
                    product_code=cod,
                    product_var=var
                )

                cursor.execute("""
                    UPDATE products
                    SET purge_flag = TRUE
                    WHERE cod = %s AND v = %s
                """, (cod, var))
                service.db.conn.commit()

                logger.info(f"Product {cod}.{var} added to blacklist '{PURGE_BLACKLIST_NAME}' (stock: {stock})")

                return JsonResponse({
                    'success': True,
                    'message': f'Prodotto {cod}.{var} aggiunto alla lista di eliminazione (giacenza attuale: {stock})'
                })
            else:
                # No stock - delete immediately
                result = service.db.purge_product(cod, var)
                delete_blacklist_entries_for_purged([result], storage=storage)
                return JsonResponse({'success': True, 'message': result['message']})


    except Exception as e:
        logger.exception("Error flagging product for purge")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)


@login_required
@require_POST
def fermi_blacklist_view(request):
    try:
        data = json.loads(request.body)
        cod = int(data['cod'])
        var = int(data['var'])
        storage_id = int(data['storage_id'])

        storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

        blacklist, _ = Blacklist.objects.get_or_create(
            storage=storage,
            name='Non più interessato',
            defaults={'description': 'Prodotti non più di interesse'},
        )
        _, created = BlacklistEntry.objects.get_or_create(
            blacklist=blacklist,
            product_code=cod,
            product_var=var,
        )

        return JsonResponse({'success': True, 'created': created})
    except Exception as e:
        logger.exception("Error adding fermi product to blacklist")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)


@login_required
@require_POST
def inventory_adjust_stock_ajax_view(request):
    """
    FIXED: Now properly handles empty minimum_stock value
    """
    try:
        cod = int(request.POST.get('cod'))
        var = int(request.POST.get('var'))
        adjustment_raw = request.POST.get('adjustment', '').strip()
        reason = request.POST.get('reason')
        source = request.POST.get('source')
        if source not in dict(StockCorrection.SOURCE_CHOICES):
            source = 'inventory'
        supermarket_name = request.POST.get('supermarket')
        minimum_stock = request.POST.get('minimum_stock', '').strip()  # ← FIX: strip whitespace
        cluster = request.POST.get('cluster', '').strip().upper()
        
        supermarket = get_object_or_404(Supermarket, name=supermarket_name, owner=request.user)
        storage = supermarket.storages.first()

        if not storage:
            return JsonResponse({'success': False, 'message': 'No storage found'}, status=400)

        with RestockService(storage) as service:
            current_stock = service.db.get_stock(cod, var)

            logger.info(
                f"[ADJUST] {supermarket_name} - Product {cod}.{var}: "
                f"current_stock={current_stock} adjustment_raw='{adjustment_raw}' "
                f"reason='{reason}' minimum_stock='{minimum_stock}' cluster='{cluster}'"
            )

            # Update minimum_stock if provided AND not empty
            minimum_stock_updated = False
            if minimum_stock:  # ← FIX: Check if not empty string
                try:
                    minimum_stock_val = int(minimum_stock)
                    if minimum_stock_val < 1:
                        return JsonResponse({
                            'success': False, 
                            'message': 'Minimum stock must be at least 1'
                        }, status=400)
                    
                    cursor = service.db.cursor()
                    cursor.execute("""
                        UPDATE product_stats
                        SET minimum_stock = %s
                        WHERE cod = %s AND v = %s
                    """, (minimum_stock_val, cod, var))
                    service.db.conn.commit()
                    minimum_stock_updated = True
                    logger.info(f"Updated minimum_stock for {cod}.{var} to {minimum_stock_val}")
                except ValueError:
                    # Invalid integer - skip update but don't fail entire request
                    logger.warning(f"Invalid minimum_stock value: {minimum_stock}")
            
            # Present only when the modal sends it; empty max_stock clears the ceiling
            if 'max_stock' in request.POST:
                max_stock_raw = request.POST.get('max_stock', '').strip()
                try:
                    max_stock_val = int(max_stock_raw) if max_stock_raw else None
                except ValueError:
                    return JsonResponse({'success': False, 'message': 'Max stock must be a number'}, status=400)
                if max_stock_val is not None and not 1 <= max_stock_val <= 32767:
                    return JsonResponse({'success': False, 'message': 'Max stock must be between 1 and 32767'}, status=400)
                bulk_order = request.POST.get('bulk_order') == '1'
                if bulk_order and max_stock_val is None:
                    return JsonResponse({'success': False, 'message': 'Ordine in blocco richiede una giacenza massima'}, status=400)

                cursor = service.db.cursor()
                cursor.execute("""
                    UPDATE product_stats
                    SET max_stock = %s, bulk_order = %s
                    WHERE cod = %s AND v = %s
                """, (max_stock_val, bulk_order, cod, var))
                service.db.conn.commit()
                logger.info(f"Updated max_stock={max_stock_val} bulk_order={bulk_order} for {cod}.{var}")

            # Update cluster if provided
            cluster_updated = False
            new_cluster_value = None
            if cluster:
                cursor = service.db.cursor()
                
                if cluster == 'NONE':
                    cursor.execute("""
                        UPDATE products
                        SET cluster = NULL
                        WHERE cod = %s AND v = %s
                    """, (cod, var))
                    new_cluster_value = "None"
                else:
                    cursor.execute("""
                        UPDATE products
                        SET cluster = %s
                        WHERE cod = %s AND v = %s
                    """, (cluster, cod, var))
                    new_cluster_value = cluster
                
                service.db.conn.commit()
                cluster_updated = True
                logger.info(f"Updated cluster for {cod}.{var} to {new_cluster_value}")
            
            adjustment = None

            if adjustment_raw != '':
                if reason not in DatabaseManager.CORRECTION_REASONS:
                    return JsonResponse({
                        'success': False,
                        'message': 'Please select a reason for the stock adjustment'
                    }, status=400)

                try:
                    adjustment = int(adjustment_raw)
                except ValueError:
                    return JsonResponse(
                        {'success': False, 'message': 'Adjustment must be a number'},
                        status=400
                    )

            if adjustment is not None:
                result = service.correct_stock(cod, var, adjustment, reason, request.user, source)
                new_stock = result['new_stock']
                loss_type = result['loss_type']

                logger.info(
                    f"Stock adjusted: {supermarket_name} - "
                    f"Product {cod}.{var}: {current_stock} → {new_stock} ({adjustment:+d}) "
                    f"Reason: {reason}, loss: {loss_type}, censored days: {result['blanked_days']}"
                )

                if loss_type:
                    message = f'Stock adjusted and recorded as {loss_type} loss: {current_stock} → {new_stock}'
                else:
                    message = f'Stock adjusted: {current_stock} → {new_stock}'
                return JsonResponse({
                    'success': True,
                    'message': message,
                    'new_stock': new_stock,
                    'loss_recorded': bool(loss_type),
                    'loss_type': loss_type,
                    'loss_amount': abs(adjustment) if loss_type else 0,
                    'minimum_stock_updated': minimum_stock_updated,
                    'cluster_updated': cluster_updated,
                    'new_cluster': new_cluster_value
                })
            else:
                # No stock change, only metadata updated
                logger.info(
                    f"[ADJUST] {supermarket_name} - Product {cod}.{var}: "
                    f"no stock change (stock={current_stock}), metadata only"
                )
                return JsonResponse({
                    'success': True,
                    'message': 'Product updated',
                    'new_stock': current_stock,
                    'loss_recorded': False,
                    'minimum_stock_updated': minimum_stock_updated,
                    'cluster_updated': cluster_updated,
                    'new_cluster': new_cluster_value
                })
                                            
    except Exception as e:
        logger.exception("Error in inventory stock adjustment")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)
