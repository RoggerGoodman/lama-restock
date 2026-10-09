"""Storage detail, minimum stock, calibration and list refresh."""

from django.utils import timezone
from datetime import date, timedelta
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse, reverse_lazy
from django.views.generic import DetailView, DeleteView
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from pathlib import Path
from django.conf import settings
import logging

from ..models import Storage, RestockSchedule, RestockLog
from ..scripts.helpers import Helper
from .common import net_price_of

logger = logging.getLogger(__name__)


class StorageDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Storage
    template_name = 'storages/detail.html'
    context_object_name = 'storage'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['recent_logs'] = self.object.restock_logs.exclude(
            operation_type='loss_recording'
        ).select_related(
            'storage__supermarket'
        ).order_by('-started_at')[:20]
        
        context['blacklists'] = self.object.blacklists.prefetch_related(
            'entries'
        ).order_by('name')
        
        try:
            context['schedule'] = self.object.schedule
        except RestockSchedule.DoesNotExist:
            context['schedule'] = None
        
        from ..services import RestockService

        try:
            with RestockService(self.object) as service:
                cursor = service.db.cursor()

                # Load products with negative stock (anomalies)
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.pz_x_collo, ps.stock
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                        AND ps.verified = TRUE
                        AND ps.stock < 0
                    ORDER BY ps.stock ASC
                    LIMIT 50;
                """, (self.object.settore,))

                negative_stock_products = []
                for row in cursor.fetchall():
                    negative_stock_products.append({
                        'cod': row['cod'],
                        'var': row['v'],
                        'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'stock': row['stock'],
                        'package_size': row['pz_x_collo'] or 12
                    })

                context['negative_stock_products'] = negative_stock_products
                logger.info(f"Found {len(negative_stock_products)} products with negative stock")

                # Load out of stock products (verified, stock=0, disponibilita='Si', sold in the first 7 slots)
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.pz_x_collo, ps.stock, ps.sales_sets
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                        AND p.purge_flag = FALSE
                        AND ps.verified = TRUE
                        AND ps.stock = 0
                        AND p.disponibilita = 'Si'
                        AND EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY AS s(val, idx)
                            WHERE s.idx <= 7 AND s.val::numeric <> 0
                        );
                """, (self.object.settore,))

                # Products the supplier had as unavailable in any order run of the last 7 days
                supplier_unavailable = set()
                recent_runs = RestockLog.objects.filter(
                    storage=self.object,
                    operation_type='full_restock',
                    started_at__date__gte=timezone.now().date() - timedelta(days=6),
                ).exclude(results='').only('results')
                for run in recent_runs:
                    for cod, v in run.get_results().get('unavailable_at_order', []):
                        supplier_unavailable.add((cod, v))

                out_of_stock_products = []
                for row in cursor.fetchall():
                    sales_sets = Helper.sales_history(row['sales_sets'])
                    raw = Helper.avg_daily_sales_from_sales_sets(sales_sets, silent=True) if sales_sets else None
                    avg_daily = round(raw, 1) if raw is not None else 0.0
                    out_of_stock_products.append({
                        'cod': row['cod'],
                        'var': row['v'],
                        'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'package_size': row['pz_x_collo'] or 12,
                        'avg_daily_sales': avg_daily,
                        'supplier_unavailable': (row['cod'], row['v']) in supplier_unavailable,
                        # Spike: a day selling at least twice the average (min 4 pcs)
                        'last_7': [
                            {'qty': q, 'spike': q is not None and avg_daily > 0 and q >= max(2 * avg_daily, 4)}
                            for q in (row['sales_sets'] or [])[:7]
                        ],
                    })
                out_of_stock_products.sort(key=lambda x: x['avg_daily_sales'], reverse=True)
                out_of_stock_products = out_of_stock_products[:50]

                context['out_of_stock_products'] = out_of_stock_products
                logger.info(f"Found {len(out_of_stock_products)} out of stock products")

                # Load brand-new available products (verified=False, disponibilita=Si, no history)
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.pz_x_collo, p.rapp,
                        p.first_added_at, e.cost_std, e.price_std, e.iva
                    FROM products p
                    LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                    WHERE p.settore = %s
                        AND p.purge_flag = FALSE
                        AND ps.verified IS NOT TRUE
                        AND p.disponibilita != 'No'
                        AND NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements_text(COALESCE(ps.bought_last_24, '[]'::jsonb)) WITH ORDINALITY AS b(val, idx)
                            WHERE b.idx <= 6 AND b.val::numeric <> 0
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements_text(COALESCE(ps.sold_last_24, '[]'::jsonb)) WITH ORDINALITY AS s(val, idx)
                            WHERE s.idx <= 6 AND s.val::numeric <> 0
                        )
                    ORDER BY
                        CASE WHEN p.first_added_at >= CURRENT_DATE - INTERVAL '7 days' THEN 0 ELSE 1 END,
                        p.first_added_at DESC,
                        p.descrizione;
                """, (self.object.settore,))

                today = date.today()
                available_products = []
                for row in cursor.fetchall():
                    pz_x_collo = row['pz_x_collo'] or 12
                    rapp = row['rapp'] or 1
                    package_size = pz_x_collo * rapp
                    unit_cost = (row['cost_std'] or 0) / rapp
                    price_std = row['price_std'] or 0
                    net_price = net_price_of(price_std, row['iva'])
                    package_cost = unit_cost * package_size
                    margin_pct = 0
                    if net_price > 0 and unit_cost > 0:
                        margin_pct = ((net_price - unit_cost) / net_price) * 100
                    first_added_at = row['first_added_at']
                    is_new = first_added_at is not None and (today - first_added_at).days <= 7
                    available_products.append({
                        'cod': row['cod'],
                        'var': row['v'],
                        'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'package_size': package_size,
                        'unit_cost': unit_cost,
                        'unit_price': price_std,
                        'package_cost': package_cost,
                        'margin_pct': margin_pct,
                        'is_new': is_new,
                    })

                context['available_products'] = available_products
                logger.info(f"Found {len(available_products)} brand-new available products")

        except Exception as e:
            logger.exception("Error loading product anomalies")
            context['negative_stock_products'] = []
            context['out_of_stock_products'] = []
            context['available_products'] = []

        from ..models import OrderCalibrationReport
        context['recent_calibration_reports'] = (
            OrderCalibrationReport.objects
            .filter(storage=self.object)
            .order_by('-generated_at')[:5]
        )

        snapshot_path = Path(settings.BASE_DIR) / 'snapshots' / f'storage_{self.object.pk}.html'
        if snapshot_path.exists():
            import datetime
            mtime = datetime.datetime.fromtimestamp(snapshot_path.stat().st_mtime)
            context['snapshot_url'] = reverse('serve-comparison-snapshot', kwargs={'storage_id': self.object.pk})
            context['snapshot_saved_at'] = mtime
        else:
            context['snapshot_url'] = None

        return context


@login_required
@require_POST
def storage_set_minimum_stock_view(request, pk):
    storage = get_object_or_404(Storage, pk=pk, supermarket__owner=request.user)
    try:
        data = json.loads(request.body)
        value = int(data['minimum_stock'])
        if value < 0:
            return JsonResponse({'success': False, 'error': 'Il valore deve essere >= 0'}, status=400)
        storage.minimum_stock = value
        storage.save(update_fields=['minimum_stock'])
        return JsonResponse({'success': True, 'minimum_stock': storage.minimum_stock})
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        return JsonResponse({'success': False, 'error': 'Valore non valido'}, status=400)


@login_required
def calibration_report_view(request, pk):
    from ..models import OrderCalibrationReport
    from django.core.exceptions import PermissionDenied
    report = get_object_or_404(OrderCalibrationReport, pk=pk)
    if report.storage.supermarket.owner != request.user:
        raise PermissionDenied

    results = report.get_results()

    understocked = results.get('understocked', [])
    for p in understocked:
        p['deficit'] = p.get('eff_min', 0) - p.get('stock', 0)

    overstocked = results.get('overstocked', [])
    for p in overstocked:
        p['excess'] = p.get('stock', 0) - p.get('eff_min', 0) - p.get('package_size', 0)
        avg = p.get('avg_daily_sales', 0)
        p['excess_days'] = round(p['excess'] / avg, 1) if avg > 0 else None

    blue_dot_keys = set()
    try:
        from ..scripts.DatabaseManager import DatabaseManager
        _db = DatabaseManager(supermarket_name=report.storage.supermarket.name)
        try:
            _cur = _db.cursor()
            _cur.execute("SELECT cod, v, internal FROM extra_losses WHERE internal IS NOT NULL")
            blue_dot_keys = set()
            for _r in _cur.fetchall():
                for _entry in (_r['internal'] or [])[:2]:
                    _qty = _entry[0] if isinstance(_entry, list) else _entry
                    if _qty:
                        blue_dot_keys.add((_r['cod'], _r['v']))
                        break
        finally:
            _db.conn.close()
    except Exception:
        pass

    for lst in (results.get('critical', []), understocked, overstocked):
        for p in lst:
            p['has_blue_dot'] = (p.get('cod'), p.get('v')) in blue_dot_keys

    context = {
        'report': report,
        'storage': report.storage,
        'critical': results.get('critical', []),
        'understocked': understocked,
        'overstocked': overstocked,
        'ok_products': results.get('ok', []),
        'products_critical': results.get('products_critical', 0),
        'products_understocked': report.products_understocked,
        'products_overstocked': report.products_overstocked,
        'products_ok': report.products_ok,
        'products_evaluated': report.products_evaluated,
    }
    return render(request, 'storages/valutazione_ordine.html', context)


class StorageDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = Storage
    template_name = 'storages/confirm_delete.html'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_success_url(self):
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.supermarket.pk})

    def delete(self, request, *args, **kwargs):
        messages.success(request, f"Magazzino '{self.get_object().name}' eliminato con successo!")
        return super().delete(request, *args, **kwargs)


@login_required
def manual_list_update_view(request, storage_id):
    """
    REFACTORED: Async list download and import.
    Can take 5-10 minutes to download and import large lists.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        # ✅ DISPATCH TO CELERY
        from ..tasks import manual_list_update_task
        
        result = manual_list_update_task.apply_async(
            args=[storage_id],
            retry=True
        )
        
        messages.info(
            request,
            f"List update started for {storage.name}. "
            f"This will take 5-10 minutes."
        )
        
        return redirect('task-progress', task_id=result.id, storage_id=storage_id)
    
    context = {
        'storage': storage,
        'has_schedule': hasattr(storage, 'schedule') and storage.schedule is not None,
        'last_update': storage.last_list_update,
    }
    
    return render(request, 'storages/manual_list_update.html', context)
