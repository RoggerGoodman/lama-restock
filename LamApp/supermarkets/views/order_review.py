"""Order review before sending ("Da inviare")."""

from django.utils import timezone
import json
from django.shortcuts import redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import RestockLog
from ..services import RestockService
from ..scripts.DatabaseManager import DatabaseManager
from .common import parse_shelf_barcode

logger = logging.getLogger(__name__)


def _avg_daily_sales_for(service, sales_sets, sold_last_24):
    """Same fallback chain the decision maker / calibration use."""
    from ..scripts.helpers import Helper
    history = Helper.sales_history(sales_sets)
    avg = service.helper.avg_daily_sales_from_sales_sets(history, silent=True)
    if avg is None:
        avg, _ = service.helper.calculate_weighted_avg_sales_new(sold_last_24 or [], silent=True)
    return round(avg or 0, 2)


@login_required
def order_review_search(request, pk):
    """
    AJAX search used while reviewing a 'Da inviare' order.

    Accepts one of: cod+var, ean, or q (description ILIKE). Returns each match
    with whether it is already in this order, its ordered qty, live stock,
    avg daily sales, and sale badge. Scoped to the storage's settore so only
    products valid for this order can be added.
    """
    log = get_object_or_404(
        RestockLog, pk=pk, storage__supermarket__owner=request.user
    )
    storage = log.storage

    q = request.GET.get('q', '').strip()
    ean_raw = request.GET.get('ean', '').strip()
    cod_raw = request.GET.get('cod', '').strip()
    var_raw = request.GET.get('var', '').strip()

    # Map order membership from the (possibly edited) stored order.
    orders = log.get_results().get('orders', [])
    in_order = {(o['cod'], o['var']): o for o in orders}

    # Product links: a searched product may be the phased-out side of a link whose
    # partner carries the order. Map each side to its partner so we can warn.
    from ..models import ProductLink
    partner_of = {}
    for pri, sec in ProductLink.build_pairs(storage.supermarket):
        partner_of[pri] = sec
        partner_of[sec] = pri

    try:
        with RestockService(storage) as service:
            cur = service.db.cursor()
            # No disponibilita filter: an explicit scan/cod.v/description lookup should
            # find the product even if it's flagged unavailable; the card warns instead.
            base_select = """
                SELECT p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                       ps.stock, ps.sales_sets, ps.sold_last_24,
                       e.price_std, e.price_s, e.sale_start, e.sale_end,
                       (e.sale_start IS NOT NULL AND e.sale_end IS NOT NULL
                        AND CURRENT_DATE BETWEEN e.sale_start AND e.sale_end) AS on_sale
                FROM products p
                LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                WHERE p.settore = %s
            """
            if cod_raw and var_raw:
                cur.execute(base_select + " AND p.cod = %s AND p.v = %s",
                            (storage.settore, int(cod_raw), int(var_raw)))
            elif ean_raw:
                # A scanned shelf label is a crypted cod.v; a product EAN is a real EAN.
                shelf = parse_shelf_barcode(ean_raw)
                if shelf:
                    cur.execute(base_select + " AND p.cod = %s AND p.v = %s",
                                (storage.settore, shelf[0], shelf[1]))
                else:
                    cur.execute(base_select + " AND p.ean = %s", (storage.settore, int(ean_raw)))
            elif len(q) >= 3:
                cur.execute(base_select + " AND p.descrizione ILIKE %s ORDER BY p.descrizione LIMIT 20",
                            (storage.settore, f'%{q}%'))
            else:
                return JsonResponse({'results': [], 'error': 'Inserisci almeno 3 caratteri, un EAN o cod.var'})

            results = []
            for row in cur.fetchall():
                key = (row['cod'], row['v'])
                ordered = in_order.get(key)
                on_sale = bool(row['on_sale'])
                discount = None
                if on_sale and row['price_std'] and row['price_s']:
                    try:
                        discount = round((1 - float(row['price_s']) / float(row['price_std'])) * 100)
                    except (ZeroDivisionError, TypeError):
                        discount = None
                partner = partner_of.get(key)
                link_info = None
                if partner:
                    link_info = {
                        'cod': partner[0], 'var': partner[1],
                        'partner_in_order': partner in in_order,
                    }
                results.append({
                    'cod': row['cod'],
                    'var': row['v'],
                    'description': row['descrizione'] or f"{row['cod']}.{row['v']}",
                    'in_order': ordered is not None,
                    'order_qty': ordered['qty'] if ordered else None,
                    'stock': row['stock'] if row['stock'] is not None else 0,
                    'avg_daily_sales': _avg_daily_sales_for(service, row['sales_sets'], row['sold_last_24']),
                    'package_size': row['pz_x_collo'] or 0,
                    'on_sale': on_sale,
                    'discount': discount,
                    'unavailable': (row['disponibilita'] == 'No'),
                    'link': link_info,
                })

            # Resolve partner names for any linked matches.
            partner_keys = [(r['link']['cod'], r['link']['var']) for r in results if r['link']]
            if partner_keys:
                placeholders = ','.join(['(%s,%s)'] * len(partner_keys))
                flat = [x for k in partner_keys for x in k]
                cur.execute(
                    f"SELECT cod, v, descrizione FROM products WHERE (cod, v) IN ({placeholders})",
                    flat,
                )
                pnames = {(r['cod'], r['v']): r['descrizione'] for r in cur.fetchall()}
                for r in results:
                    if r['link']:
                        lk = (r['link']['cod'], r['link']['var'])
                        r['link']['name'] = pnames.get(lk) or f"{lk[0]}.{lk[1]}"
        return JsonResponse({'results': results})
    except Exception as e:
        logger.exception(f"Order review search failed for log #{pk}")
        return JsonResponse({'results': [], 'error': str(e)}, status=500)


@login_required
@require_POST
def order_review_edit(request, pk):
    """
    Mutate a 'Da inviare' order during review.

    actions:
      - set_qty  {cod, var, qty}  add/update an order line (qty>0)
      - remove   {cod, var}       drop an order line
      - set_stock{cod, var, stock} correct the live giacenza (product_stats.stock)
    """
    log = get_object_or_404(
        RestockLog, pk=pk, storage__supermarket__owner=request.user
    )
    if log.status != 'awaiting_review':
        return JsonResponse({'success': False, 'message': "L'ordine non è più in revisione"}, status=409)

    try:
        data = json.loads(request.body)
        action = data['action']
        cod = int(data['cod'])
        var = int(data['var'])
    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
        return JsonResponse({'success': False, 'message': 'Richiesta non valida'}, status=400)

    results = log.get_results()
    orders = results.get('orders', [])

    if action == 'set_stock':
        # Operator types the real shelf count (absolute) plus a mandatory reason
        try:
            new_stock = int(data['stock'])
        except (KeyError, ValueError, TypeError):
            return JsonResponse({'success': False, 'message': 'Giacenza non valida'}, status=400)
        if new_stock < 0:
            return JsonResponse({'success': False, 'message': 'Giacenza non valida'}, status=400)
        reason = (data.get('reason') or '').strip()
        if reason not in DatabaseManager.CORRECTION_REASONS:
            return JsonResponse({'success': False, 'message': 'Seleziona un motivo'}, status=400)

        with RestockService(log.storage) as service:
            try:
                current = service.db.get_stock(cod, var) or 0
            except ValueError:
                return JsonResponse({'success': False, 'message': 'Articolo senza giacenza registrata'}, status=404)
            result = service.correct_stock(cod, var, new_stock - current, reason, request.user, 'order_review')
            applied = result['new_stock']
            cur = service.db.cursor()
            cur.execute("SELECT descrizione FROM products WHERE cod = %s AND v = %s", (cod, var))
            row = cur.fetchone()
            name = (row['descrizione'] if row else None) or f"{cod}.{var}"

        # Queue the product for recalculation (dedup).
        pending = results.get('recalc_pending', [])
        if not any(p['cod'] == cod and p['var'] == var for p in pending):
            pending.append({'cod': cod, 'var': var, 'name': name})
        results['recalc_pending'] = pending
        log.set_results(results)
        log.save(update_fields=['results'])
        return JsonResponse({
            'success': True, 'stock': applied,
            'pending': {'cod': cod, 'var': var, 'name': name},
        })

    if action == 'remove':
        orders = [o for o in orders if not (o['cod'] == cod and o['var'] == var)]
        results['recalc_pending'] = [
            p for p in results.get('recalc_pending', [])
            if not (p['cod'] == cod and p['var'] == var)
        ]
        results['manual_qty'] = [
            m for m in results.get('manual_qty', [])
            if not (m['cod'] == cod and m['var'] == var)
        ]

    elif action == 'set_qty':
        try:
            qty = int(data['qty'])
        except (KeyError, ValueError, TypeError):
            return JsonResponse({'success': False, 'message': 'Quantità non valida'}, status=400)
        # A manual qty change shields the product from being overwritten by recalc.
        manual = results.get('manual_qty', [])
        if not any(m['cod'] == cod and m['var'] == var for m in manual):
            manual.append({'cod': cod, 'var': var})
        results['manual_qty'] = manual
        if qty <= 0:
            orders = [o for o in orders if not (o['cod'] == cod and o['var'] == var)]
        else:
            existing = next((o for o in orders if o['cod'] == cod and o['var'] == var), None)
            if existing:
                existing['qty'] = qty
            else:
                orders.append({'cod': cod, 'var': var, 'qty': qty, 'discount': data.get('discount')})
    else:
        return JsonResponse({'success': False, 'message': 'Azione sconosciuta'}, status=400)

    results['orders'] = orders
    log.set_results(results)
    log.products_ordered = len(orders)
    log.total_packages = sum(int(o.get('qty', 0) or 0) for o in orders)
    log.save(update_fields=['results', 'products_ordered', 'total_packages'])

    return JsonResponse({
        'success': True,
        'products_ordered': log.products_ordered,
        'total_packages': log.total_packages,
    })


@login_required
@require_POST
def order_submit(request, pk):
    """Send a reviewed order to Dropzone (the 'Invia' action)."""
    log = get_object_or_404(
        RestockLog, pk=pk, storage__supermarket__owner=request.user
    )
    if log.status != 'awaiting_review':
        return JsonResponse({'success': False, 'message': "L'ordine non è più in revisione"}, status=409)

    from ..tasks import submit_pending_order
    result = submit_pending_order.apply_async(args=[log.id])
    logger.info(f"[INVIA] Queued submit for log #{log.id} (task {result.id})")
    return JsonResponse({'success': True, 'task_id': result.id})


@login_required
@require_POST
def order_recalc(request, pk):
    """Re-run the decision maker for products whose stock was corrected."""
    log = get_object_or_404(
        RestockLog, pk=pk, storage__supermarket__owner=request.user
    )
    if log.status != 'awaiting_review':
        return JsonResponse({'success': False, 'message': "L'ordine non è più in revisione"}, status=409)

    if not log.get_results().get('recalc_pending'):
        return JsonResponse({'success': False, 'message': 'Nessuna giacenza da ricalcolare'}, status=400)

    from ..tasks import recalculate_review_order
    result = recalculate_review_order.apply_async(args=[log.id])
    logger.info(f"[RECALC] Queued recalc for log #{log.id} (task {result.id})")
    return JsonResponse({'success': True, 'task_id': result.id})


@login_required
@require_POST
def order_discard(request, pk):
    """Discard a reviewed order without sending it (the 'Scarta' action)."""
    log = get_object_or_404(
        RestockLog, pk=pk, storage__supermarket__owner=request.user
    )
    if log.status != 'awaiting_review':
        return JsonResponse({'success': False, 'message': "L'ordine non è più in revisione"}, status=409)

    log.status = 'discarded'
    log.current_stage = 'discarded'
    log.completed_at = timezone.now()
    log.save(update_fields=['status', 'current_stage', 'completed_at'])

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True, 'redirect_url': f'/storages/{log.storage_id}/'})
    messages.success(request, "Ordine scartato")
    return redirect('storage-detail', pk=log.storage_id)
