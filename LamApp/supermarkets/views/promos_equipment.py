"""Promo products and equipment ordering."""

import json
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import Supermarket, Storage
from ..services import RestockService
from .common import net_price_of

logger = logging.getLogger(__name__)


@login_required
def promo_products_view(request):
    """
    Display products currently on sale (today BETWEEN sale_start AND sale_end),
    grouped by storage and cluster, showing margins and stock for stocking decisions.
    """
    from datetime import date

    storages = (Storage.objects.filter(supermarket__owner=request.user)
                .select_related('supermarket')
                .order_by('supermarket__name', 'settore'))

    groups = []
    total_products = 0
    today = date.today()

    for storage in storages:
        promo_products = []
        try:
            with RestockService(storage) as service:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT
                        p.cod,
                        p.v,
                        p.descrizione,
                        p.cluster,
                        p.rapp,
                        e.cost_s,
                        e.cost_std,
                        e.price_std,
                        e.iva,
                        e.sale_start,
                        e.sale_end,
                        ps.stock,
                        ps.promo_lifts
                    FROM products p
                    JOIN economics e ON p.cod = e.cod AND p.v = e.v
                    LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE %s BETWEEN e.sale_start AND e.sale_end
                      AND e.cost_s IS NOT NULL
                      AND ps.verified = TRUE
                      AND p.settore = %s
                    ORDER BY p.cod, p.v
                """, (today, storage.settore))

                for row in cur.fetchall():
                    # cost_s/cost_std are per collo (pack); price_std is per
                    # selling piece. Bring costs down to per-piece with rapp so
                    # the margins are correct for multi-packs (rapp != 1).
                    rapp = int(row['rapp'] or 1) or 1
                    cost_s = float(row['cost_s'] or 0) / rapp
                    cost_std = float(row['cost_std'] or 0) / rapp
                    price_std = float(row['price_std'] or 0)
                    net_price = net_price_of(price_std, row['iva'])
                    stock = int(row['stock'] or 0)

                    # Margins on net-of-IVA price (matches supplier Margine)
                    margin_std = ((net_price - cost_std) / net_price * 100) if net_price > 0 else 0
                    margin_promo = ((net_price - cost_s) / net_price * 100) if net_price > 0 else 0
                    margin_gain = margin_promo - margin_std  # Extra margin from promo

                    # Sales increase in the last measured promos (up to 3, newest first)
                    past_promos = [
                        {'pct': round((float(e['lift']) - 1) * 100), 'depth': e.get('discount')}
                        for e in (row['promo_lifts'] or [])
                        if isinstance(e, dict) and e.get('lift') is not None
                    ]

                    promo_products.append({
                        'cod': row['cod'],
                        'v': row['v'],
                        'descrizione': row['descrizione'],
                        'cluster': row['cluster'] or '',
                        'cost_s': cost_s,
                        'cost_s_collo': float(row['cost_s'] or 0),
                        'cost_std': cost_std,
                        'price_std': price_std,
                        'margin_std': round(margin_std, 1),
                        'margin_promo': round(margin_promo, 1),
                        'margin_gain': round(margin_gain, 1),
                        'stock': stock,
                        'sale_start': row['sale_start'],
                        'sale_end': row['sale_end'],
                        'past_promos': past_promos,
                        # Sort key; never-measured products sort last
                        'past_promo_avg': (sum(x['pct'] for x in past_promos) / len(past_promos)
                                           if past_promos else -999),
                    })
        except Exception as e:
            logger.exception(f"Error fetching promo products for storage {storage.id}")
            continue

        if not promo_products:
            continue

        by_cluster = {}
        for p in promo_products:
            by_cluster.setdefault(p['cluster'], []).append(p)
        clusters = []
        # Unclustered products go last
        for cluster in sorted(by_cluster, key=lambda c: (c == '', c)):
            products = sorted(by_cluster[cluster], key=lambda x: x['margin_gain'], reverse=True)
            clusters.append({'name': cluster or 'Senza cluster', 'products': products})

        groups.append({
            'storage': storage,
            'clusters': clusters,
            'count': len(promo_products),
        })
        total_products += len(promo_products)

    context = {
        'groups': groups,
        'total_products': total_products,
        'today': today,
    }

    return render(request, 'inventory/promo_products.html', context)


@login_required
@require_POST
def order_promo_products_view(request):
    """
    Order promo products from the promo products page.
    Receives orders grouped by storage, dispatches Celery task for each storage.
    """
    try:
        data = json.loads(request.body)
        orders_by_storage = data.get('orders_by_storage', {})

        if not orders_by_storage:
            return JsonResponse({
                'success': False,
                'message': 'No products provided'
            }, status=400)

        # Validate all storages belong to user
        storage_ids = [int(sid) for sid in orders_by_storage.keys()]
        user_storages = Storage.objects.filter(
            id__in=storage_ids,
            supermarket__owner=request.user
        ).select_related('supermarket')

        if user_storages.count() != len(storage_ids):
            return JsonResponse({
                'success': False,
                'message': 'Invalid storage access'
            }, status=403)

        # Build storage info map
        storage_map = {s.id: s for s in user_storages}

        # Prepare orders for the task
        all_orders = []
        for storage_id_str, order_data in orders_by_storage.items():
            storage_id = int(storage_id_str)
            storage = storage_map[storage_id]
            for product in order_data['products']:
                all_orders.append({
                    'storage_id': storage_id,
                    'storage_name': storage.name,
                    'supermarket_id': storage.supermarket.id,
                    'cod': product['cod'],
                    'var': product['var'],
                    'qty': product['qty']
                })

        total_products = len(all_orders)
        logger.info(f"Dispatching promo order for {total_products} products across {len(storage_ids)} storages")

        # Dispatch to Celery task
        from ..tasks import place_manual_order_task

        result = place_manual_order_task.apply_async(
            args=[request.user.id, all_orders],
            retry=True,
            retry_policy={
                'max_retries': 3,
                'interval_start': 600,
            }
        )

        return JsonResponse({
            'success': True,
            'task_id': result.id,
            'message': f'Order started for {total_products} promo products'
        })

    except Exception as e:
        logger.exception("Error dispatching promo order")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
def equipment_order_view(request):
    """Catalog of store equipment, grouped by category, with a cart to order from."""
    from ..equipment import catalog_by_category

    supermarkets = Supermarket.objects.filter(owner=request.user).order_by('name')
    return render(request, 'inventory/equipment_order.html', {
        'supermarkets': supermarkets,
        'categories': catalog_by_category(),
    })


@login_required
@require_POST
def order_equipment_view(request):
    """Validate the cart and dispatch it on the supermarket's GENERI VARI storage."""
    from ..equipment import catalog_index, equipment_storage, EQUIPMENT_SETTORE
    from ..tasks import place_manual_order_task

    try:
        data = json.loads(request.body)
        supermarket = get_object_or_404(Supermarket, id=data.get('supermarket_id'), owner=request.user)
        storage = equipment_storage(supermarket)
        if storage is None:
            return JsonResponse({
                'success': False,
                'message': f'{supermarket.name} non ha un magazzino {EQUIPMENT_SETTORE}'
            }, status=400)

        index = catalog_index()
        orders = []
        for line in data.get('items', []):
            cod, var, qty = int(line['cod']), int(line['var']), int(line['qty'])
            if (cod, var) not in index or not 1 <= qty <= 999:
                return JsonResponse({
                    'success': False,
                    'message': f'Articolo o quantità non valida: {cod}.{var} x{qty}'
                }, status=400)
            orders.append({
                'storage_id': storage.id,
                'storage_name': storage.name,
                'supermarket_id': supermarket.id,
                'cod': cod,
                'var': var,
                'qty': qty,
            })

        if not orders:
            return JsonResponse({'success': False, 'message': 'Carrello vuoto'}, status=400)

        logger.info(f"Dispatching equipment order for {supermarket.name}: {len(orders)} items")
        result = place_manual_order_task.apply_async(
            args=[request.user.id, orders],
            retry=True,
            retry_policy={
                'max_retries': 3,
                'interval_start': 600,
            }
        )
        return JsonResponse({'success': True, 'task_id': result.id})

    except (KeyError, TypeError, ValueError):
        return JsonResponse({'success': False, 'message': 'Richiesta non valida'}, status=400)
    except Exception as e:
        logger.exception("Error dispatching equipment order")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)
