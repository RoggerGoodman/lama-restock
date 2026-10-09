"""Stock profitability report."""

import json
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required
import logging

from ..models import Supermarket, Storage
from ..services import RestockService
from .common import net_price_of

logger = logging.getLogger(__name__)


@login_required
def stock_profit_view(request):
    """
    Profitability analysis: how much each product/cluster/settore generates,
    month by month.

    - "Lordo"  = fatturato  = venduto x price_std
    - "Netto"  = margine    = venduto x (price_std - cost_std)

    Quantities come from product_stats.sold_last_24 (24 monthly slots, index 0 =
    current month, same convention as extra_losses). Prices are NOT historicised
    in the DB, so past months are revalued at today's price_std/cost_std — the
    template warns the user about this.

    "Incidenza prodotto" = share of its own cluster's total (netto, with the
    lordo share shown underneath).
    """
    from datetime import datetime

    month_abbr = ['', 'Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu',
                  'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']

    NO_CLUSTER = 'Senza cluster'

    LOSS_TYPES = ['broken', 'expired', 'internal', 'stolen', 'shrinkage']
    LOSS_TYPE_LABELS = {
        'broken': 'Rotto',
        'expired': 'Scaduto',
        'internal': 'Autoconsumo',
        'stolen': 'Rubato',
        'shrinkage': 'Differenza inv.',
    }

    supermarkets = Supermarket.objects.filter(owner=request.user)

    # Auto-select the only supermarket when the user has just one
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)

    storage_id = request.GET.get('storage_id')
    selected_clusters = [c for c in request.GET.getlist('clusters') if c]
    product_code_filter = request.GET.get('product_code', '').strip()

    # lt_submitted marks a real submission, so "no boxes" means cleared, not first load.
    if request.GET.get('lt_submitted'):
        selected_loss_types = [t for t in request.GET.getlist('loss_types') if t in LOSS_TYPES]
    else:
        selected_loss_types = ['expired']
    losses_active = bool(selected_loss_types)

    # Parse product code filter (format: cod.v)
    filter_cod = None
    filter_v = None
    if product_code_filter and '.' in product_code_filter:
        try:
            parts = product_code_filter.split('.', 1)
            filter_cod = int(parts[0])
            filter_v = int(parts[1])
        except (ValueError, IndexError):
            filter_cod = None
            filter_v = None

    # --- Period selection (identical convention to losses analytics) ---
    period_mode = request.GET.get('period_mode', 'current')
    from_month_str = request.GET.get('from_month', '')
    to_month_str = request.GET.get('to_month', '')

    today = datetime.now()
    current_year, current_month = today.year, today.month

    def index_to_yearmonth(idx):
        """Array index -> (year, month). Index 0 = current month."""
        total = current_year * 12 + (current_month - 1) - idx
        return total // 12, total % 12 + 1

    def yearmonth_to_index(year, month):
        """(year, month) -> array index (0 = current, 23 = oldest)."""
        return (current_year - year) * 12 + (current_month - month)

    available_months = []
    for i in range(24):
        y, m = index_to_yearmonth(i)
        available_months.append({
            'value': f'{y}-{m:02d}',
            'label': f'{month_abbr[m]} {y}',
            'is_current': i == 0,
        })

    if period_mode == 'range':
        if not from_month_str:
            y6, m6 = index_to_yearmonth(5)
            from_month_str = f'{y6}-{m6:02d}'
        if not to_month_str:
            to_month_str = f'{current_year}-{current_month:02d}'
        try:
            from_y, from_m = map(int, from_month_str.split('-'))
            to_y, to_m = map(int, to_month_str.split('-'))
            from_idx = yearmonth_to_index(from_y, from_m)
            to_idx = yearmonth_to_index(to_y, to_m)
            start_idx = max(0, min(23, min(from_idx, to_idx)))
            end_idx = max(0, min(23, max(from_idx, to_idx)))
        except (ValueError, AttributeError):
            period_mode = 'current'
            start_idx = end_idx = 0
            from_month_str = to_month_str = f'{current_year}-{current_month:02d}'
    else:
        period_mode = 'current'
        start_idx = end_idx = 0
        from_month_str = to_month_str = f'{current_year}-{current_month:02d}'

    # Build scope
    scope_parts = []
    if supermarket_id:
        scope_parts.append(get_object_or_404(Supermarket, id=supermarket_id, owner=request.user).name)
    if storage_id:
        scope_parts.append(get_object_or_404(Storage, id=storage_id).name)

    scope_description = " -> ".join(scope_parts) if scope_parts else "Tutti i punti vendita"

    if supermarket_id:
        storages = Storage.objects.filter(supermarket_id=supermarket_id, supermarket__owner=request.user)
    else:
        storages = Storage.objects.filter(supermarket__owner=request.user)

    if storage_id:
        storages = storages.filter(id=storage_id)

    # Group storages by supermarket (one DB schema per supermarket)
    supermarkets_to_process = {}
    for storage in storages:
        entry = supermarkets_to_process.setdefault(storage.supermarket.id, {
            'supermarket': storage.supermarket,
            'storages': [],
            'settores': set(),
        })
        entry['storages'].append(storage)
        entry['settores'].add(storage.settore)

    # Pretty settore label ("Magazzino" name) per settore key
    settore_labels = {s.settore: s.name for s in storages}

    all_clusters = set()
    products_rows = []
    missing_economics = 0

    monthly_lordo = [0.0] * 24
    monthly_netto = [0.0] * 24
    monthly_units = [0] * 24
    monthly_loss = [0.0] * 24

    def loss_value_in_window(arr, cost_per_piece, w_start, w_end):
        """Sum loss units and per-piece cost over [w_start, w_end]."""
        units = 0.0
        value = 0.0
        if not isinstance(arr, list):
            return units, value
        rapp = cost_per_piece['rapp']
        for idx in range(w_start, min(w_end + 1, len(arr))):
            item = arr[idx]
            if isinstance(item, list) and len(item) == 2:
                qty, c = item
                if not isinstance(qty, (int, float)):
                    continue
                units += qty
                value += qty * (float(c or 0.0) / rapp)
            elif isinstance(item, (int, float)):
                units += item
                value += item * cost_per_piece['fallback']
        return units, value

    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            first_storage = sm_data['storages'][0]
            sm_index = {}
            with RestockService(first_storage) as service:
                cursor = service.db.cursor()

                settores = sorted(sm_data['settores'])
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.settore, p.cluster, p.rapp,
                        e.price_std, e.cost_std, e.category, e.iva,
                        ps.sold_last_24, ps.price_last_24, ps.cost_last_24
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                    WHERE p.settore = ANY(%s)
                      AND ps.sold_last_24 IS NOT NULL
                    ORDER BY p.descrizione
                """, (settores,))

                for row in cursor.fetchall():
                    cluster = (row['cluster'] or '').strip() or NO_CLUSTER
                    all_clusters.add(cluster)

                    if selected_clusters and cluster not in selected_clusters:
                        continue

                    cod, v = row['cod'], row['v']
                    if filter_cod is not None and filter_v is not None:
                        if cod != filter_cod or v != filter_v:
                            continue

                    sold_array = row['sold_last_24']
                    if not isinstance(sold_array, list):
                        continue

                    # cost_std is per collo; sold counts pieces, so divide by rapp.
                    rapp = int(row['rapp'] or 1) or 1
                    iva = row['iva']
                    cur_price = net_price_of(row['price_std'], iva)
                    cur_cost = float(row['cost_std'] or 0.0) / rapp

                    # Value each month at its snapshot; missing slot falls back to current.
                    price_hist = row['price_last_24'] if isinstance(row['price_last_24'], list) else []
                    cost_hist = row['cost_last_24'] if isinstance(row['cost_last_24'], list) else []

                    def m_price(idx):
                        if idx < len(price_hist) and price_hist[idx] is not None:
                            return net_price_of(price_hist[idx], iva)
                        return cur_price

                    def m_cost(idx):
                        if idx < len(cost_hist) and cost_hist[idx] is not None:
                            return float(cost_hist[idx]) / rapp
                        return cur_cost

                    period_units = 0
                    period_lordo = 0.0
                    period_netto = 0.0
                    for idx in range(start_idx, min(end_idx + 1, len(sold_array))):
                        qty = sold_array[idx]
                        if not isinstance(qty, (int, float)):
                            continue
                        qty = int(qty)
                        pi = m_price(idx)
                        period_units += qty
                        period_lordo += qty * pi
                        period_netto += qty * (pi - m_cost(idx))

                    if period_units <= 0:
                        continue

                    # Missing economics would fake a 0/100% margin; exclude, don't average in.
                    if cur_price <= 0 or cur_cost <= 0:
                        missing_economics += 1
                        continue

                    for idx, qty in enumerate(sold_array[:24]):
                        if not isinstance(qty, (int, float)):
                            continue
                        qty = int(qty)
                        pi = m_price(idx)
                        monthly_units[idx] += qty
                        monthly_lordo[idx] += qty * pi
                        monthly_netto[idx] += qty * (pi - m_cost(idx))

                    lordo = period_lordo
                    netto = period_netto

                    sm_index[(cod, v)] = {
                        'cod': cod,
                        'var': v,
                        'description': row['descrizione'] or f"Articolo {cod}.{v}",
                        'settore': row['settore'],
                        'settore_label': settore_labels.get(row['settore'], row['settore']),
                        'cluster': cluster,
                        'category': row['category'] or '',
                        'price': cur_price,
                        'cost': cur_cost,
                        'rapp': rapp,
                        'units': period_units,
                        'lordo': lordo,
                        'netto': netto,
                        'margin_pct': (netto / lordo * 100) if lordo > 0 else 0.0,
                        'loss_units': 0.0,
                        'loss_value': 0.0,
                        'netto_reale': netto,
                    }

                if selected_loss_types:
                    loss_cols = ', '.join(f'el.{t}' for t in selected_loss_types)
                    cursor.execute(f"""
                        SELECT
                            el.cod, el.v, {loss_cols},
                            p.descrizione, p.settore, p.cluster, p.rapp,
                            e.price_std, e.cost_std, e.category, e.iva
                        FROM extra_losses el
                        JOIN products p ON el.cod = p.cod AND el.v = p.v
                        LEFT JOIN economics e ON el.cod = e.cod AND e.v = el.v
                        WHERE p.settore = ANY(%s)
                    """, (settores,))

                    for row in cursor.fetchall():
                        cluster = (row['cluster'] or '').strip() or NO_CLUSTER
                        all_clusters.add(cluster)

                        if selected_clusters and cluster not in selected_clusters:
                            continue

                        cod, v = row['cod'], row['v']
                        if filter_cod is not None and filter_v is not None:
                            if cod != filter_cod or v != filter_v:
                                continue

                        rapp = int(row['rapp'] or 1) or 1
                        cost_ctx = {'rapp': rapp,
                                    'fallback': float(row['cost_std'] or 0.0) / rapp}

                        loss_units = 0.0
                        loss_value = 0.0
                        for lt in selected_loss_types:
                            u, val = loss_value_in_window(row[lt], cost_ctx, start_idx, end_idx)
                            loss_units += u
                            loss_value += val
                            arr = row[lt]
                            if isinstance(arr, list):
                                for idx in range(min(24, len(arr))):
                                    item = arr[idx]
                                    if isinstance(item, list) and len(item) == 2:
                                        q, c = item
                                        if isinstance(q, (int, float)):
                                            monthly_loss[idx] += q * (float(c or 0.0) / rapp)
                                    elif isinstance(item, (int, float)):
                                        monthly_loss[idx] += item * cost_ctx['fallback']

                        existing = sm_index.get((cod, v))
                        if existing is not None:
                            existing['loss_units'] = loss_units
                            existing['loss_value'] = loss_value
                            existing['netto_reale'] = existing['netto'] - loss_value
                        elif loss_units or loss_value:
                            # Loss-only: no sales in the period, pure drag on the cluster.
                            price = net_price_of(row['price_std'], row['iva'])
                            cost = float(row['cost_std'] or 0.0) / rapp
                            sm_index[(cod, v)] = {
                                'cod': cod,
                                'var': v,
                                'description': row['descrizione'] or f"Articolo {cod}.{v}",
                                'settore': row['settore'],
                                'settore_label': settore_labels.get(row['settore'], row['settore']),
                                'cluster': cluster,
                                'category': row['category'] or '',
                                'price': price,
                                'cost': cost,
                                'rapp': rapp,
                                'units': 0,
                                'lordo': 0.0,
                                'netto': 0.0,
                                'margin_pct': 0.0,
                                'loss_units': loss_units,
                                'loss_value': loss_value,
                                'netto_reale': -loss_value,
                            }

            products_rows.extend(sm_index.values())
        except Exception:
            logger.exception(f"Error computing stock profit for supermarket {sm_id}")
            continue

    # Rank and share by net-of-loss profit when losses are active, else gross margin.
    metric = 'netto_reale' if losses_active else 'netto'

    settore_map = {}
    for p in products_rows:
        s = settore_map.setdefault(p['settore'], {
            'settore': p['settore'],
            'label': p['settore_label'],
            'clusters': {},
            'units': 0,
            'lordo': 0.0,
            'netto': 0.0,
            'loss_value': 0.0,
            'netto_reale': 0.0,
        })
        s['units'] += p['units']
        s['lordo'] += p['lordo']
        s['netto'] += p['netto']
        s['loss_value'] += p['loss_value']
        s['netto_reale'] += p['netto_reale']

        c = s['clusters'].setdefault(p['cluster'], {
            'cluster': p['cluster'],
            'products': [],
            'units': 0,
            'lordo': 0.0,
            'netto': 0.0,
            'loss_value': 0.0,
            'netto_reale': 0.0,
        })
        c['products'].append(p)
        c['units'] += p['units']
        c['lordo'] += p['lordo']
        c['netto'] += p['netto']
        c['loss_value'] += p['loss_value']
        c['netto_reale'] += p['netto_reale']

    total_units = sum(s['units'] for s in settore_map.values())
    total_lordo = sum(s['lordo'] for s in settore_map.values())
    total_netto = sum(s['netto'] for s in settore_map.values())
    total_loss_value = sum(s['loss_value'] for s in settore_map.values())
    total_netto_reale = sum(s['netto_reale'] for s in settore_map.values())
    total_metric = total_netto_reale if losses_active else total_netto

    tree = []
    for s_idx, s in enumerate(sorted(settore_map.values(), key=lambda x: x[metric], reverse=True)):
        clusters = []
        for c_idx, c in enumerate(sorted(s['clusters'].values(), key=lambda x: x[metric], reverse=True)):
            # Incidenza: share of the product's own cluster, on the active metric
            for p in c['products']:
                p['incidenza_netto'] = (p[metric] / c[metric] * 100) if c[metric] else 0.0
                p['incidenza_lordo'] = (p['lordo'] / c['lordo'] * 100) if c['lordo'] else 0.0
            c['products'].sort(key=lambda x: x[metric], reverse=True)
            c['margin_pct'] = (c['netto'] / c['lordo'] * 100) if c['lordo'] > 0 else 0.0
            c['real_margin_pct'] = (c['netto_reale'] / c['lordo'] * 100) if c['lordo'] > 0 else 0.0
            c['share_of_settore'] = (c[metric] / s[metric] * 100) if s[metric] else 0.0
            # Positional slugs: cluster/settore names are free text and would
            # break the JS attribute selectors used for expand/collapse.
            c['slug'] = f's{s_idx}c{c_idx}'
            c['product_count'] = len(c['products'])
            clusters.append(c)

        s['clusters'] = clusters
        s['margin_pct'] = (s['netto'] / s['lordo'] * 100) if s['lordo'] > 0 else 0.0
        s['real_margin_pct'] = (s['netto_reale'] / s['lordo'] * 100) if s['lordo'] > 0 else 0.0
        s['share_of_total'] = (s[metric] / total_metric * 100) if total_metric else 0.0
        s['slug'] = f's{s_idx}'
        s['product_count'] = sum(c['product_count'] for c in clusters)
        tree.append(s)

    # Cluster ranking cards (across the whole filtered scope)
    cluster_totals = {}
    for p in products_rows:
        ct = cluster_totals.setdefault(p['cluster'], {'cluster': p['cluster'], 'lordo': 0.0,
                                                      'netto': 0.0, 'netto_reale': 0.0,
                                                      'loss_value': 0.0, 'units': 0})
        ct['lordo'] += p['lordo']
        ct['netto'] += p['netto']
        ct['netto_reale'] += p['netto_reale']
        ct['loss_value'] += p['loss_value']
        ct['units'] += p['units']
    top_clusters = sorted(cluster_totals.values(), key=lambda x: x[metric], reverse=True)[:6]
    for ct in top_clusters:
        ct['share'] = (ct[metric] / total_metric * 100) if total_metric else 0.0

    # Chart data: chronological slice of the selected range (oldest -> newest)
    chart_month_labels = []
    for i in range(end_idx, start_idx - 1, -1):
        y, m = index_to_yearmonth(i)
        chart_month_labels.append(f'{month_abbr[m]} {y}')

    chart_lordo = [round(monthly_lordo[i], 2) for i in range(end_idx, start_idx - 1, -1)]
    chart_netto = [round(monthly_netto[i], 2) for i in range(end_idx, start_idx - 1, -1)]
    chart_units = [monthly_units[i] for i in range(end_idx, start_idx - 1, -1)]
    chart_netto_reale = [round(monthly_netto[i] - monthly_loss[i], 2)
                         for i in range(end_idx, start_idx - 1, -1)]

    if period_mode == 'current':
        selected_period_label = f'Mese corrente ({month_abbr[current_month]} {current_year})'
    else:
        old_y, old_m = index_to_yearmonth(end_idx)
        new_y, new_m = index_to_yearmonth(start_idx)
        selected_period_label = f'{month_abbr[old_m]} {old_y} -> {month_abbr[new_m]} {new_y}'

    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'scope_description': scope_description,
        'tree': tree,
        'top_clusters': top_clusters,
        'total_units': total_units,
        'total_lordo': total_lordo,
        'total_netto': total_netto,
        'total_margin_pct': (total_netto / total_lordo * 100) if total_lordo > 0 else 0.0,
        'total_loss_value': total_loss_value,
        'total_netto_reale': total_netto_reale,
        'total_real_margin_pct': (total_netto_reale / total_lordo * 100) if total_lordo > 0 else 0.0,
        'losses_active': losses_active,
        'loss_type_options': [
            {'key': t, 'label': LOSS_TYPE_LABELS[t], 'checked': t in selected_loss_types}
            for t in LOSS_TYPES
        ],
        'selected_loss_types': selected_loss_types,
        'total_products': len(products_rows),
        'missing_economics': missing_economics,
        'all_clusters': sorted(all_clusters),
        'selected_clusters': selected_clusters,
        'product_code_filter': product_code_filter,
        'period_mode': period_mode,
        'from_month': from_month_str,
        'to_month': to_month_str,
        'available_months': available_months,
        'selected_period_label': selected_period_label,
        'chart_labels': json.dumps(chart_month_labels),
        'chart_lordo': json.dumps(chart_lordo),
        'chart_netto': json.dumps(chart_netto),
        'chart_netto_reale': json.dumps(chart_netto_reale),
        'chart_units': json.dumps(chart_units),
        'chart_cluster_labels': json.dumps([c['cluster'] for c in top_clusters]),
        'chart_cluster_values': json.dumps([round(c[metric], 2) for c in top_clusters]),
    }

    return render(request, 'stock_profit.html', context)
