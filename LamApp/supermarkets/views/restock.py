"""Running restocks and restock logs."""

from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.views.generic import DetailView, DeleteView
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import Storage, RestockLog
from ..services import RestockService
from .common import net_price_of

logger = logging.getLogger(__name__)


@login_required
def run_restock_view(request, storage_id):
    """
    FIXED: Now properly handles AJAX requests.
    Returns JSON for AJAX, redirect for regular form submission.
    """
    storage = get_object_or_404(
        Storage, 
        id=storage_id, 
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        coverage = (request.POST.get('coverage') or '').strip() or None
        if coverage is not None:
            coverage = float(coverage)

        # ✅ DISPATCH TO CELERY (non-blocking)
        from ..tasks import run_restock_for_storage

        result = run_restock_for_storage.apply_async(
            args=[storage_id, coverage],
            kwargs={'manual': True},  # operator-triggered → always review
            retry=True,
            retry_policy={
                'max_retries': 3,
                'interval_start': 900,
            }
        )
        
        # 🔍 CHECK IF AJAX REQUEST
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        
        if is_ajax:
            # ✅ RETURN JSON FOR AJAX
            return JsonResponse({
                'success': True,
                'task_id': result.id,
                'message': f'Restock check started for {storage.name}'
            })
        else:
            # ✅ RETURN REDIRECT FOR NON-AJAX
            messages.info(
                request,
                f"Restock check started for {storage.name}. "
                f"This will take 10-15 minutes. You can track progress on the next page."
            )
            return redirect('restock-task-progress', task_id=result.id)
    
    return render(request, 'storages/run_restock.html', {'storage': storage})


@login_required
@require_POST
def retry_restock_view(request, log_id):
    """Retry a failed restock operation from its last checkpoint - AJAX FRIENDLY - THREAD-SAFE"""
    log = get_object_or_404(
        RestockLog, 
        id=log_id, 
        storage__supermarket__owner=request.user
    )
    
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    if not log.can_retry():
        error_msg = f"Cannot retry: Maximum retries ({log.max_retries}) reached or operation not in failed state"
        
        if is_ajax:
            return JsonResponse({'success': False, 'message': error_msg}, status=400)
        
        messages.error(request, error_msg)
        return redirect('restock-log-detail', pk=log_id)
    
    try:
        logger.info(f"User-initiated retry for RestockLog #{log_id} (fresh run)")

        from ..tasks import retry_restock_from_checkpoint
        retry_restock_from_checkpoint.apply_async(args=[log_id], queue='selenium')

        if is_ajax:
            return JsonResponse({'success': True, 'log_id': log_id})
        else:
            messages.success(request, f"Nuovo tentativo in coda — l'ordine verrà elaborato a breve.")
            return redirect('restock-log-detail', pk=log_id)
    except Exception as e:
        logger.exception(f"Error retrying restock from checkpoint")
        
        if is_ajax:
            return JsonResponse({'success': False, 'message': str(e)}, status=500)
        
        messages.error(request, f"Tentativo fallito: {str(e)}")
        return redirect('restock-log-detail', pk=log_id)


class RestockLogDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = RestockLog
    template_name = 'restock_logs/detail.html'
    context_object_name = 'log'

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        results = self.object.get_results()
        
        # ✅ HANDLE DIFFERENT OPERATION TYPES
        operation_type = self.object.operation_type
        
        # Set defaults
        context['results'] = results
        context['enriched_orders'] = []
        context['clusters'] = {}
        context['last_sales_sync_at'] = self.object.storage.supermarket.last_sales_sync_at
        context['is_awaiting_review'] = (self.object.status == 'awaiting_review')
        if context['is_awaiting_review']:
            context['recalc_pending'] = results.get('recalc_pending', [])
            context['last_recalc'] = results.get('last_recalc', [])
        else:
            context['recalc_pending'] = []
            context['last_recalc'] = []
        context['summary'] = {
            'total_items': 0,
            'total_packages': self.object.total_packages or 0,
            'total_clusters': 0,
            'total_cost': 0,
            'total_skipped': 0,
            'total_zombie': 0,
            'total_order_skipped': 0,
        }
        
        # ✅ Operations WITHOUT orders (show simple info)
        if operation_type in ['ddt_import', 'list_update', 'cluster_assignment', 'product_addition']:
            # These operations don't have order details
            # Just show the basic log info
            logger.info(f"Displaying {operation_type} log #{self.object.id} - no order enrichment needed")
            return context
        
        # ✅ Operations WITH orders/products (full enrichment)
        if operation_type in ['full_restock', 'order_execution', 'verification']:
            # Get all lists from results
            orders = results.get('orders', [])
            zombie_products = results.get('zombie_products', [])
            order_skipped_products = results.get('order_skipped_products', [])
            
            # Only enrich if we have orders
            if not orders:
                logger.info(f"No orders found in {operation_type} log #{self.object.id}")
                return context
            
            # Enrich orders with product details
            enriched_orders = []
            
            try:
                with RestockService(self.object.storage) as service:
                    # Collect all (cod, var) pairs first
                    product_keys = [
                        (o['cod'], o['var'])
                        for o in orders
                        if 'cod' in o and 'var' in o
                    ]
                    if not product_keys:
                        logger.warning(f"No valid product keys in log #{self.object.id}")
                        return context

                    # Single query for all products
                    placeholders = ','.join(['(%s,%s)'] * len(product_keys))
                    flat_keys = [item for pair in product_keys for item in pair]
                    cur = service.db.cursor()
                    cur.execute(f"""
                        SELECT
                            p.cod, p.v, p.descrizione, p.cluster, p.pz_x_collo, p.rapp,
                            ps.stock, ps.sales_sets, ps.sold_last_24,
                            CASE
                                WHEN e.sale_start IS NOT NULL
                                AND e.sale_end IS NOT NULL
                                AND CURRENT_DATE BETWEEN e.sale_start AND e.sale_end
                                THEN e.cost_s
                                ELSE e.cost_std
                            END AS cost
                        FROM products p
                        LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE (p.cod, p.v) IN ({placeholders})
                    """, flat_keys)

                    # Build lookup dict
                    products_dict = {(row['cod'], row['v']): row for row in cur.fetchall()}

                    clusters = {}
                    
                    for order in orders:
                        product = products_dict.get((order['cod'], order['var']))
                        cod = order['cod']
                        var = order['var']
                        qty = order['qty']
                        discount = order.get('discount')
                        
                        try:                    
                            if product:
                                descrizione = product['descrizione']
                                cluster = product['cluster'] or 'Uncategorized'
                                package_size = product['pz_x_collo'] or 0
                                rapp = product['rapp'] or 1
                                cost = product['cost'] or 0
                                cost = cost/rapp
                                stock = product['stock'] if product['stock'] is not None else 0
                                avg_daily_sales = self._avg_daily_sales(
                                    service, product['sales_sets'], product['sold_last_24']
                                )
                            else:
                                descrizione = f"Product {cod}.{var}"
                                cluster = 'Uncategorized'
                                package_size = 0
                                rapp = 1
                                cost = 0
                                stock = 0
                                avg_daily_sales = 0

                            order_item = {
                                'cod': cod,
                                'var': var,
                                'qty': qty,
                                'name': descrizione,
                                'cluster': cluster,
                                'cost': cost,
                                'total_cost': cost * qty * package_size * rapp,
                                'discount': discount,
                                'on_sale': discount is not None,
                                'stock': stock,
                                'avg_daily_sales': avg_daily_sales,
                            }
                            
                            enriched_orders.append(order_item)
                            
                            if cluster not in clusters:
                                clusters[cluster] = {
                                    'items': [],
                                    'total_packages': 0,
                                    'total_cost': 0,
                                    'count': 0
                                }
                            
                            clusters[cluster]['items'].append(order_item)
                            clusters[cluster]['total_packages'] += qty
                            clusters[cluster]['total_cost'] += cost * qty * package_size
                            clusters[cluster]['count'] += 1
                            
                        except Exception as e:
                            logger.warning(f"Could not enrich order {cod}.{var}: {e}")
                            continue
                    
                    # Enrich all product lists
                    enriched_zombie = self._enrich_product_list(service, zombie_products)
                    enriched_order_skipped = self._enrich_product_list(service, order_skipped_products)

                    # Alphabetical by cluster, and by product name within each cluster,
                    # so the operator can scan for an item quickly.
                    for _cluster in clusters.values():
                        _cluster['items'].sort(key=lambda it: (it.get('name') or '').lower())
                    sorted_clusters = dict(sorted(clusters.items(), key=lambda x: x[0]))

                    # Calculate summary
                    summary = {
                        'total_items': len(enriched_orders),
                        'total_packages': sum(int(o.get('qty', 0) or 0) for o in enriched_orders),
                        'total_clusters': len(sorted_clusters),
                        'total_cost': sum(float(o.get('total_cost', 0) or 0) for o in enriched_orders),
                        'total_zombie': len(enriched_zombie),
                        'total_order_skipped': len(enriched_order_skipped),
                    }

                    context['enriched_orders'] = enriched_orders
                    context['clusters'] = sorted_clusters
                    context['summary'] = summary

                    # Add all lists to context
                    context['enriched_zombie'] = enriched_zombie
                    context['enriched_order_skipped'] = enriched_order_skipped

                    logger.info(
                        f"Context prepared: {len(enriched_orders)} orders, "
                        f"{len(enriched_zombie)} zombie, {len(enriched_order_skipped)} order-skipped"
                    )
            except Exception as e:
                logger.exception(f"Error enriching orders for log #{self.object.id}")
                # Don't fail completely - just show what we have
                context['error_enriching'] = str(e)
                    
        return context
    
    @staticmethod
    def _avg_daily_sales(service, sales_sets, sold_last_24):
        """Same fallback chain the decision maker / calibration use."""
        from ..scripts.helpers import Helper
        history = Helper.sales_history(sales_sets)
        avg = service.helper.avg_daily_sales_from_sales_sets(history, silent=True)
        if avg is None:
            avg, _ = service.helper.calculate_weighted_avg_sales_new(sold_last_24 or [], silent=True)
        return round(avg or 0, 2)

    def _enrich_product_list(self, service, product_list, include_pricing=False):
        """
        Helper to enrich a list of products with database details.
        
        Args:
            include_pricing: If True, includes cost, price, and package info (for new products)
        """
        enriched = []
        
        for item in product_list:
            cod = item.get('cod')
            var = item.get('var')
            reason = item.get('reason', 'Unknown')
            
            try:
                cur = service.db.cursor()
                
                if include_pricing:
                    # Enhanced query for new products with pricing info
                    cur.execute("""
                        SELECT 
                            p.descrizione, p.pz_x_collo, p.rapp, p.disponibilita,
                            ps.stock,
                            e.cost_std, e.price_std, e.iva
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                        WHERE p.cod = %s AND p.v = %s
                    """, (cod, var))
                else:
                    # Standard query for other lists
                    cur.execute("""
                        SELECT p.descrizione, ps.stock, p.disponibilita
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.cod = %s AND p.v = %s
                    """, (cod, var))
                
                row = cur.fetchone()
                
                if row:
                    product_data = {
                        'cod': cod,
                        'var': var,
                        'name': row['descrizione'] or f"Product {cod}.{var}",
                        'stock': row['stock'] or 0,
                        'disponibilita': row['disponibilita'] or 'Unknown',
                        'reason': reason
                    }
                    
                    # Add pricing info only for new products
                    if include_pricing:
                        pz_x_collo = row['pz_x_collo'] or 12
                        rapp = row['rapp'] or 1
                        package_size = pz_x_collo * rapp
                        
                        # cost_std is per collo di cessione (rapp selling pieces);
                        # bring it down to per-piece to match price_std and stock.
                        unit_cost = (row['cost_std'] or 0) / rapp
                        price_std = row['price_std'] or 0
                        net_price = net_price_of(price_std, row['iva'])

                        # Calculate per-unit cost and price
                        package_cost = unit_cost * package_size
                        # Calculate margin (on net-of-IVA price, to match supplier Margine)
                        margin_pct = 0
                        if net_price > 0 and unit_cost > 0:
                            margin_pct = ((net_price - unit_cost) / net_price) * 100

                        product_data.update({
                            'package_size': package_size,
                            'unit_cost': unit_cost,
                            'unit_price': price_std,
                            'package_cost': package_cost,
                            'margin_pct': margin_pct
                        })
                    
                    enriched.append(product_data)
                else:
                    enriched.append({
                        'cod': cod,
                        'var': var,
                        'name': f"Product {cod}.{var}",
                        'stock': 0,
                        'disponibilita': 'Unknown',
                        'reason': reason
                    })
            except Exception as e:
                logger.warning(f"Could not enrich product {cod}.{var}: {e}")
                enriched.append({
                    'cod': cod,
                    'var': var,
                    'name': f"Product {cod}.{var}",
                    'stock': 0,
                    'disponibilita': 'Unknown',
                    'reason': reason
                })
        
        return enriched


class RestockLogDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    """Delete a restock log entry with confirmation"""
    model = RestockLog
    template_name = 'restock_logs/confirm_delete.html'
    context_object_name = 'log'

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user

    def get_success_url(self):
        # Check if there's a 'next' parameter in the request
        next_url = self.request.GET.get('next') or self.request.POST.get('next')
        if next_url:
            return next_url
        # Default: redirect to storage detail page
        return reverse('storage-detail', kwargs={'pk': self.object.storage.id})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Pass 'next' parameter to template for form
        context['next_url'] = self.request.GET.get('next', '')
        return context


@login_required
@require_POST
def dismiss_failed_log(request, pk):
    """Dismiss a failed log from the dashboard warnings"""
    log = get_object_or_404(
        RestockLog,
        pk=pk,
        storage__supermarket__owner=request.user,
        status='failed'
    )
    log.is_dismissed = True
    log.save(update_fields=['is_dismissed'])

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True})

    messages.success(request, "Avviso archiviato")
    return redirect('dashboard')
