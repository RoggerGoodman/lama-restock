"""Stock value, stock snapshots and loss analytics."""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.views.decorators.http import require_POST
import logging

from ..models import Supermarket, Storage, StockValueSnapshot
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
def stock_value_unified_view(request):
    """Unified stock value view with flexible filtering - FIXED CLUSTER SORTING"""
    
    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters from query params
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)
    storage_id = request.GET.get('storage_id')
    settore = request.GET.get('settore')
    cluster = request.GET.get('cluster')
    
    # Build scope description
    scope_parts = []
    if supermarket_id:
        scope_parts.append(get_object_or_404(Supermarket, id=supermarket_id, owner=request.user).name)
    if storage_id:
        scope_parts.append(get_object_or_404(Storage, id=storage_id).name)
    if settore:
        scope_parts.append(f"Settore: {settore}")
    if cluster:
        scope_parts.append(f"Cluster: {cluster}")
    
    scope_description = " → ".join(scope_parts) if scope_parts else "All Supermarkets"
    
    # Get relevant storages
    if supermarket_id:
        storages = Storage.objects.filter(supermarket_id=supermarket_id)
    else:
        storages = Storage.objects.filter(supermarket__owner=request.user)
    
    if storage_id:
        storages = storages.filter(id=storage_id)
    
    # Get available clusters (for the selected storage if any) - FIXED: SORTED ALPHABETICALLY
    clusters = []
    if storage_id:
        storage = Storage.objects.get(id=storage_id)
        with RestockService(storage) as service:
            settore = storage.settore
            cursor = service.db.cursor()
            cursor.execute("""
                SELECT DISTINCT cluster 
                FROM products 
                WHERE cluster IS NOT NULL AND cluster != '' AND settore = %s 
                ORDER BY cluster ASC
            """, (settore,))
            clusters = [row['cluster'] for row in cursor.fetchall()]
    
    # Calculate values
    category_totals = {}
    total_value = 0
    
    for storage in storages:
        try:
            with RestockService(storage) as service:
                settore = storage.settore
                cursor = service.db.cursor()
                
                # Build query based on filters
                query = """
                    SELECT e.category,
                        SUM((e.cost_std / p.rapp) * ps.stock) AS value
                    FROM economics e
                    JOIN product_stats ps
                        ON e.cod = ps.cod AND e.v = ps.v
                    JOIN products p
                        ON e.cod = p.cod AND e.v = p.v
                    WHERE e.category != '' AND ps.stock > 0 AND ps.verified
                """
                params = []
                
                if cluster:
                    query += " AND p.cluster = %s"
                    params.append(cluster)
                query += " AND p.settore = %s"
                params.append(settore)
                query += " GROUP BY e.category"
                
                cursor.execute(query, params)
                
                for row in cursor.fetchall():
                    print(f"row in cursor {row}")
                    category_name = row['category']
                    value = row['value'] or 0
                    
                    if category_name in category_totals:
                        category_totals[category_name] += value
                    else:
                        category_totals[category_name] = value
                    
                    total_value += value
        except Exception as e:
            logger.exception(f"Error calculating value for {storage.name}")
            continue
    
    # Convert to list and sort
    category_values = [
        {'name': name, 'value': value}
        for name, value in category_totals.items()
    ]
    category_values.sort(key=lambda x: x['value'], reverse=True)
    
    # Calculate percentages
    for cat in category_values:
        cat['percentage'] = (cat['value'] / total_value * 100) if total_value > 0 else 0

    # Get existing snapshots for this supermarket (if one is selected)
    snapshots = []
    if supermarket_id:
        snapshots = StockValueSnapshot.objects.filter(
            supermarket_id=supermarket_id
        ).order_by('-created_at')[:36]

    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'clusters': clusters,
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'selected_cluster': cluster or '',
        'scope_description': scope_description,
        'category_values': category_values,
        'total_value': total_value,
        'snapshots': snapshots,
    }

    return render(request, 'stock_value_unified.html', context)


@login_required
@require_POST
def create_stock_snapshot_view(request):
    """Manually create a stock value snapshot for a supermarket."""
    supermarket_id = request.POST.get('supermarket_id')

    if not supermarket_id:
        messages.error(request, "Seleziona un punto vendita per creare uno snapshot.")
        return redirect('stock-value-unified')

    supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
    storages = Storage.objects.filter(supermarket=supermarket)

    if not storages.exists():
        messages.error(request, f"Nessun magazzino trovato per {supermarket.name}.")
        return redirect('stock-value-unified')

    # Calculate total value across all storages (same logic as the view)
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
                    WHERE e.category != '' AND ps.stock > 0 AND ps.verified
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
            logger.exception(f"Error calculating value for {storage.name}")
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

    # Create the snapshot
    snapshot = StockValueSnapshot.create_snapshot(
        supermarket=supermarket,
        total_value=total_value,
        category_breakdown=category_breakdown,
        is_manual=True
    )

    messages.success(
        request,
        f"Snapshot creato per {supermarket.name}: €{total_value:,.2f}"
    )

    return redirect(f"{reverse('stock-value-unified')}?supermarket_id={supermarket_id}")


@login_required
def delete_stock_snapshot_view(request, pk):
    """Delete a stock value snapshot."""
    snapshot = get_object_or_404(StockValueSnapshot, pk=pk, supermarket__owner=request.user)
    supermarket_id = snapshot.supermarket_id

    if request.method == 'POST':
        snapshot.delete()
        messages.success(request, "Snapshot eliminato.")

    return redirect(f"{reverse('stock-value-unified')}?supermarket_id={supermarket_id}")


@login_required
def losses_analytics_unified_view(request):
    """
    FIXED: Type filter now properly filters ALL data including totals and table columns
    Auto-selects single supermarket if user has only one
    """
    from datetime import datetime

    month_abbr = ['', 'Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu',
                  'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']

    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # ✅ FIX: Auto-select single supermarket
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)
    
    storage_id = request.GET.get('storage_id')
    show_type = request.GET.get('show_type', 'all')
    show_category = request.GET.get('show_category', 'all')
    product_code_filter = request.GET.get('product_code', '').strip()

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

    # --- Period selection ---
    # period_mode: 'current' (only this month) or 'range' (from_month to to_month)
    # Array index 0 = current month, 23 = oldest month (24 months ago)
    period_mode = request.GET.get('period_mode', 'current')
    from_month_str = request.GET.get('from_month', '')
    to_month_str = request.GET.get('to_month', '')

    today = datetime.now()
    current_year, current_month = today.year, today.month

    def index_to_yearmonth(idx):
        """Array index → (year, month). Index 0 = current month."""
        total = current_year * 12 + (current_month - 1) - idx
        return total // 12, total % 12 + 1

    def yearmonth_to_index(year, month):
        """(year, month) → array index (0 = current, 23 = oldest)."""
        return (current_year - year) * 12 + (current_month - month)

    # Build available months for the UI dropdowns
    available_months = []
    for i in range(24):
        y, m = index_to_yearmonth(i)
        available_months.append({
            'value': f'{y}-{m:02d}',
            'label': f'{month_abbr[m]} {y}',
            'is_current': i == 0,
        })

    if period_mode == 'range':
        # Default: last 6 months when not yet specified
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
            # Clamp and normalise: start_idx ≤ end_idx (start = more recent)
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
    
    scope_description = " → ".join(scope_parts) if scope_parts else "All Supermarkets"
    
    # Get relevant storages
    if supermarket_id:
        storages = Storage.objects.filter(supermarket_id=supermarket_id)
    else:
        storages = Storage.objects.filter(supermarket__owner=request.user)
    
    if storage_id:
        storages = storages.filter(id=storage_id)
    
    # Group by supermarket
    supermarkets_to_process = {}
    for storage in storages:
        if storage.supermarket.id not in supermarkets_to_process:
            supermarkets_to_process[storage.supermarket.id] = {
                'supermarket': storage.supermarket,
                'storages': [],
                'settores': set()
            }
        supermarkets_to_process[storage.supermarket.id]['storages'].append(storage)
        supermarkets_to_process[storage.supermarket.id]['settores'].add(storage.settore)
    
    # Collect all available categories for filter
    all_categories = set()
    
    # Enhanced statistics with monetary values
    stats = {
        'broken': {
            'total_units': 0, 
            'total_value': 0.0,
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24
        },
        'expired': {
            'total_units': 0, 
            'total_value': 0.0,
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24
        },
        'internal': {
            'total_units': 0, 
            'total_value': 0.0,
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24
        },
        'stolen': {
            'total_units': 0,
            'total_value': 0.0,
            'products': 0,
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24
        },
        'shrinkage': {
            'total_units': 0,
            'total_value': 0.0,
            'products': 0,
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24
        },
    }
    
    # Complete product list
    all_products_list = []
    
    # Process each supermarket's database
    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            first_storage = sm_data['storages'][0]
            with RestockService(first_storage) as service:
                cursor = service.db.cursor()
                
                # Build WHERE clause
                if storage_id:
                    settore_filter = f"WHERE p.settore = '{first_storage.settore}'"
                elif len(sm_data['settores']) < len(sm_data['supermarket'].storages.all()):
                    settores_list = "', '".join(sm_data['settores'])
                    settore_filter = f"WHERE p.settore IN ('{settores_list}')"
                else:
                    settore_filter = ""
                
                query = f"""
                    SELECT
                        el.cod, el.v,
                        el.broken, el.expired, el.internal, el.stolen, el.shrinkage,
                        p.descrizione,
                        p.rapp,
                        e.cost_std,
                        e.category
                    FROM extra_losses el
                    LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                    LEFT JOIN economics e ON el.cod = e.cod AND el.v = e.v
                    {settore_filter}
                    ORDER BY p.descrizione
                """
                
                cursor.execute(query)
                
                loss_types = ['broken', 'expired', 'internal', 'stolen', 'shrinkage']
                
                for row in cursor.fetchall():
                    cod = row['cod']
                    v = row['v']
                    description = row['descrizione'] or f"Product {cod}.{v}"
                    # cost_std is per collo; losses count pieces, so divide by rapp.
                    rapp = int(row['rapp'] or 1) or 1
                    fallback_cost = (row['cost_std'] or 0.0) / rapp
                    category = row['category'] or 'Unknown'

                    # Collect categories
                    if category != 'Unknown':
                        all_categories.add(category)

                    # Skip if product code filter doesn't match
                    if filter_cod is not None and filter_v is not None:
                        if cod != filter_cod or v != filter_v:
                            continue

                    # Skip if category filter doesn't match
                    if show_category != 'all' and category != show_category:
                        continue
                    
                    product_losses = {
                        'cod': cod,
                        'var': v,
                        'description': description,
                        'category': category,
                        'broken_units': 0,
                        'broken_value': 0.0,
                        'expired_units': 0,
                        'expired_value': 0.0,
                        'internal_units': 0,
                        'internal_value': 0.0,
                        'stolen_units': 0,
                        'stolen_value': 0.0,
                        'shrinkage_units': 0,
                        'shrinkage_value': 0.0,
                        'total_units': 0,
                        'total_value': 0.0
                    }
                    
                    # ✅ FIX: Only process selected type if filter is active
                    types_to_process = [show_type] if show_type != 'all' else loss_types
                    
                    for loss_type in types_to_process:
                        loss_json = row[loss_type] or []
                        
                        if loss_json:
                            try:
                                loss_array = loss_json
                                
                                # Sum losses within the selected period window
                                period_losses = 0
                                period_value = 0.0

                                for idx in range(start_idx, end_idx + 1):
                                    if idx >= len(loss_array):
                                        break
                                    item = loss_array[idx]
                                    if isinstance(item, list) and len(item) == 2:
                                        qty, cost = item
                                        period_losses += qty
                                        period_value += qty * (cost / rapp)
                                    else:
                                        qty = item
                                        period_losses += qty
                                        period_value += qty * fallback_cost
                                
                                if period_losses > 0:
                                    stats[loss_type]['total_units'] += period_losses
                                    stats[loss_type]['total_value'] += period_value
                                    stats[loss_type]['products'] += 1
                                    
                                    # Aggregate monthly data
                                    for idx, item in enumerate(loss_array[:24]):
                                        if isinstance(item, list) and len(item) == 2:
                                            qty, cost = item
                                            stats[loss_type]['monthly_units'][idx] += qty
                                            stats[loss_type]['monthly_value'][idx] += qty * (cost / rapp)
                                        else:
                                            qty = item
                                            stats[loss_type]['monthly_units'][idx] += qty
                                            stats[loss_type]['monthly_value'][idx] += qty * fallback_cost
                                    
                                    # Add to product losses (for table)
                                    product_losses[f'{loss_type}_units'] = period_losses
                                    product_losses[f'{loss_type}_value'] = period_value
                                    product_losses['total_units'] += period_losses
                                    product_losses['total_value'] += period_value
                            
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error processing losses for {cod}.{v}: {e}")
                                continue
                    
                    # Add to list if has losses
                    if product_losses['total_units'] > 0:
                        all_products_list.append(product_losses)
        except Exception as e:
            logger.exception(f"Error processing losses for supermarket {sm_id}")
            continue
    
    # Sort products by total value (descending)
    all_products_list.sort(key=lambda x: x['total_value'], reverse=True)
    
    # ✅ FIX: Calculate totals based on type filter
    if show_type == 'all':
        total_units = sum(s['total_units'] for s in stats.values())
        total_value = sum(s['total_value'] for s in stats.values())
    else:
        total_units = stats[show_type]['total_units']
        total_value = stats[show_type]['total_value']
    
    # Build chart data: chronological slice of the selected range (oldest → newest)
    loss_types_list = ['broken', 'expired', 'internal', 'stolen', 'shrinkage']
    chart_month_labels = []
    for i in range(end_idx, start_idx - 1, -1):
        y, m = index_to_yearmonth(i)
        chart_month_labels.append(f'{month_abbr[m]} {y}')

    for lt in loss_types_list:
        stats[lt]['chart_units'] = [stats[lt]['monthly_units'][i] for i in range(end_idx, start_idx - 1, -1)]
        stats[lt]['chart_value'] = [stats[lt]['monthly_value'][i] for i in range(end_idx, start_idx - 1, -1)]

    # Human-readable period label
    if period_mode == 'current':
        selected_period_label = f'Mese corrente ({month_abbr[current_month]} {current_year})'
    else:
        old_y, old_m = index_to_yearmonth(end_idx)
        new_y, new_m = index_to_yearmonth(start_idx)
        selected_period_label = f'{month_abbr[old_m]} {old_y} → {month_abbr[new_m]} {new_y}'

    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'scope_description': scope_description,
        'stats': stats,
        'total_units': total_units,
        'total_value': total_value,
        'all_products': all_products_list,
        'total_products': len(all_products_list),
        'show_type': show_type,
        'show_category': show_category,
        'product_code_filter': product_code_filter,
        'all_categories': sorted(list(all_categories)),
        'period_mode': period_mode,
        'from_month': from_month_str,
        'to_month': to_month_str,
        'available_months': available_months,
        'selected_period_label': selected_period_label,
        'chart_labels': json.dumps(chart_month_labels),
    }
    
    return render(request, 'losses_analytics_unified.html', context)
