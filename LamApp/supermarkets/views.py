# LamApp/supermarkets/views.py
from django.utils import timezone
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from pathlib import Path
from django.conf import settings
from psycopg2.extras import Json
from .automation_services import AutomatedRestockService
import threading
from LamApp.celery import app as celery_app
from celery.result import AsyncResult

from .models import (
    Supermarket, Storage, RestockSchedule, 
    Blacklist, BlacklistEntry, RestockLog
)
from .forms import (
    RestockScheduleForm, BlacklistForm, PurgeProductsForm, InventorySearchForm,
    BlacklistEntryForm, AddProductsForm, PromoUploadForm,
    StockAdjustmentForm, RecordLossesForm, 
)

from .services import RestockService, StorageService
import logging

logger = logging.getLogger(__name__)


# ============ Authentication Views ============

def signup(request):
    """
    User registration with optional closure control.
    Set REGISTRATION_CLOSED=True in settings to disable public registration.
    """
    # Check if registration is closed
    if getattr(settings, 'REGISTRATION_CLOSED', False):
        messages.error(
            request,
            "Registration is currently closed. Please contact the administrator for access."
        )
        return redirect('login')
    
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, "Account created successfully!")
            return redirect("dashboard")
    else:
        form = UserCreationForm()
    
    return render(request, "registration/signup.html", {"form": form})

from django.contrib.admin.views.decorators import staff_member_required

@staff_member_required
def admin_create_user(request):
    """
    Admin-only view to create new users when registration is closed.
    Only accessible by Django admin users.
    """
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            messages.success(
                request,
                f"User '{user.username}' created successfully!"
            )
            return redirect('admin:index')
    else:
        form = UserCreationForm()
    
    return render(request, "admin/create_user.html", {
        "form": form,
        "title": "Create New User"
    })


# ============ Dashboard Views ============

@login_required
def dashboard_view(request):
    """Main dashboard showing overview of all supermarkets"""
    supermarkets = Supermarket.objects.filter(owner=request.user).prefetch_related(
        'storages', 'storages__schedule', 'storages__restock_logs'
    )
    
    # Get recent logs across all storages
    recent_logs = RestockLog.objects.filter(
        storage__supermarket__owner=request.user
    ).select_related('storage', 'storage__supermarket')[:10]
    
    # Count active schedules properly
    active_schedules = RestockSchedule.objects.filter(
        storage__supermarket__owner=request.user
    ).count()
    
    # Get all storages with schedules for the upcoming operations section
    scheduled_storages = Storage.objects.filter(
        supermarket__owner=request.user,
        schedule__isnull=False
    ).select_related('schedule', 'supermarket')
    
    context = {
        'supermarkets': supermarkets,
        'recent_logs': recent_logs,
        'total_supermarkets': supermarkets.count(),
        'total_storages': sum(s.storages.count() for s in supermarkets),
        'active_schedules': active_schedules,
        'scheduled_storages': scheduled_storages,
    }
    return render(request, 'dashboard.html', context)


def home_view(request):
    """Landing page"""
    if request.user.is_authenticated:
        return redirect('dashboard')
    return render(request, 'home.html')


# ============ Supermarket Views ============

class SupermarketListView(LoginRequiredMixin, ListView):
    model = Supermarket
    template_name = 'supermarkets/list.html'
    context_object_name = 'supermarkets'

    def get_queryset(self):
        return Supermarket.objects.filter(owner=self.request.user).prefetch_related('storages')


class SupermarketDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Supermarket
    template_name = 'supermarkets/detail.html'
    context_object_name = 'supermarket'

    def test_func(self):
        return self.get_object().owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['storages'] = self.object.storages.all().prefetch_related(
            'schedule', 'blacklists', 'restock_logs'
        )
        return context


class SupermarketCreateView(LoginRequiredMixin, CreateView):
    model = Supermarket
    fields = ['name', 'username', 'password']
    template_name = 'supermarkets/form.html'

    def form_valid(self, form):
        form.instance.owner = self.request.user
        response = super().form_valid(form)
        
        # Try to sync storages automatically
        try:
            StorageService.sync_storages(self.object)
            messages.success(
                self.request, 
                f"Supermarket '{self.object.name}' created and storages discovered!"
            )
        except Exception as e:
            logger.exception("Error syncing storages")
            messages.warning(
                self.request,
                f"Supermarket created, but couldn't sync storages: {str(e)}"
            )
        
        return response

    def get_success_url(self):
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.pk})


class SupermarketUpdateView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = Supermarket
    fields = ['name', 'username', 'password']
    template_name = 'supermarkets/form.html'

    def test_func(self):
        return self.get_object().owner == self.request.user

    def get_success_url(self):
        messages.success(self.request, "Supermarket updated successfully!")
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.pk})


class SupermarketDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = Supermarket
    template_name = 'supermarkets/confirm_delete.html'
    success_url = reverse_lazy('supermarket-list')

    def test_func(self):
        return self.get_object().owner == self.request.user

    def delete(self, request, *args, **kwargs):
        messages.success(request, f"Supermarket '{self.get_object().name}' deleted successfully!")
        return super().delete(request, *args, **kwargs)


@login_required
@require_POST
def sync_storages_view(request, pk):
    """Manually sync storages for a supermarket"""
    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)
    
    try:
        StorageService.sync_storages(supermarket)
        messages.success(request, "Storages synced successfully!")
    except Exception as e:
        logger.exception("Error syncing storages")
        messages.error(request, f"Error syncing storages: {str(e)}")
    
    return redirect('supermarket-detail', pk=pk)


# ============ Storage Views ============

class StorageDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Storage
    template_name = 'storages/detail.html'
    context_object_name = 'storage'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['recent_logs'] = self.object.restock_logs.all()[:20]
        context['blacklists'] = self.object.blacklists.all().prefetch_related('entries')
        
        # Check if schedule exists
        try:
            context['schedule'] = self.object.schedule
        except RestockSchedule.DoesNotExist:
            context['schedule'] = None
        return context


class StorageDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = Storage
    template_name = 'storages/confirm_delete.html'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_success_url(self):
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.supermarket.pk})

    def delete(self, request, *args, **kwargs):
        messages.success(request, f"Storage '{self.get_object().name}' deleted successfully!")
        return super().delete(request, *args, **kwargs)


# ============ Restock Schedule Views ============

class RestockScheduleListView(LoginRequiredMixin, ListView):
    model = Storage
    template_name = "schedules/restock_schedule_list.html"
    context_object_name = "storages"
    
    def get_queryset(self):
        return Storage.objects.filter(
            supermarket__owner=self.request.user
        ).select_related('supermarket', 'schedule').order_by('supermarket__name', 'name')


class RestockScheduleView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = RestockSchedule
    form_class = RestockScheduleForm
    template_name = "schedules/restock_schedule.html"
    
    def test_func(self):
        storage = get_object_or_404(Storage, id=self.kwargs.get("storage_id"))
        return storage.supermarket.owner == self.request.user
    
    def get_object(self, queryset=None):
        storage = get_object_or_404(
            Storage, 
            id=self.kwargs.get("storage_id"),
            supermarket__owner=self.request.user
        )
        schedule, created = RestockSchedule.objects.get_or_create(storage=storage)
        if created:
            messages.info(self.request, "Created new schedule for this storage.")
        return schedule

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["storage"] = get_object_or_404(
            Storage,
            id=self.kwargs.get("storage_id"),
            supermarket__owner=self.request.user
        )
        return context

    def form_valid(self, form):
        messages.success(self.request, "Schedule updated successfully!")
        return super().form_valid(form)

    def get_success_url(self):
        return reverse_lazy("storage-detail", kwargs={"pk": self.object.storage.pk})


class RestockScheduleDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = RestockSchedule
    template_name = 'schedules/confirm_delete.html'

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user

    def get_success_url(self):
        return reverse_lazy('storage-detail', kwargs={'pk': self.object.storage.pk})

    def delete(self, request, *args, **kwargs):
        messages.success(request, "Schedule deleted successfully!")
        return super().delete(request, *args, **kwargs)


# ============ Restock Operation Views ============

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
        coverage = request.POST.get('coverage')
        if coverage:
            coverage = float(coverage)
        
        # ✅ DISPATCH TO CELERY (non-blocking)
        from .tasks import manual_restock_task
        
        result = manual_restock_task.apply_async(
            args=[storage_id, coverage],
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
        # Get coverage from log
        coverage = float(log.coverage_used) if log.coverage_used else None
        storage = log.storage
        
        logger.info(f"User-initiated retry for RestockLog #{log_id} from checkpoint {log.current_stage} with coverage={coverage}")
        
        if is_ajax:
            # CRITICAL FIX: Create service in background thread
            
            def run_retry():
                """Background thread worker - creates its own DB connection"""
                
                
                # CRITICAL: Service created in THIS thread
                service = AutomatedRestockService(storage)
                
                try:
                    service.retry_from_checkpoint(log, coverage=coverage)
                except Exception as e:
                    logger.exception(f"Error in background retry for log #{log_id}")
                finally:
                    service.close()
            
            thread = threading.Thread(target=run_retry)
            thread.daemon = True
            thread.start()
            
            return JsonResponse({'success': True, 'log_id': log_id})
        else:
            # Synchronous retry
            
            service = AutomatedRestockService(storage)
            
            try:
                updated_log = service.retry_from_checkpoint(log, coverage=coverage)
                
                messages.success(
                    request, 
                    f"Retry successful! Operation completed from checkpoint: {log.get_current_stage_display()}"
                )
                
                return redirect('restock-log-detail', pk=updated_log.id)
                
            finally:
                service.close()
        
    except Exception as e:
        logger.exception(f"Error retrying restock from checkpoint")
        
        if is_ajax:
            return JsonResponse({'success': False, 'message': str(e)}, status=500)
        
        messages.error(request, f"Retry failed: {str(e)}")
        return redirect('restock-log-detail', pk=log_id)
    
@login_required
@require_POST
def execute_order_view(request, log_id):
    """Execute the order from a restock log"""
    log = get_object_or_404(
        RestockLog, 
        id=log_id, 
        storage__supermarket__owner=request.user
    )
    
    try:
        service = RestockService(log.storage)
        results = log.get_results()
        
        # Convert back to tuple list
        orders_list = [
            (o['cod'], o['var'], o['qty']) 
            for o in results.get('orders', [])
        ]
        
        service.execute_order(orders_list)
        messages.success(request, "Order executed successfully!")
        
    except Exception as e:
        logger.exception("Error executing order")
        messages.error(request, f"Error executing order: {str(e)}")
    finally:
        service.close()
    
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
        
        # Get all lists from results
        orders = results.get('orders', [])
        new_products = results.get('new_products', [])
        skipped_products = results.get('skipped_products', [])
        zombie_products = results.get('zombie_products', [])
        order_skipped_products = results.get('order_skipped_products', [])
        
        # Enrich orders with product details
        enriched_orders = []
        service = RestockService(self.object.storage)
        
        try:
            clusters = {}
            
            for order in orders:
                cod = order['cod']
                var = order['var']
                qty = order['qty']
                
                try:
                    cur = service.db.cursor()
                    cur.execute("""
                        SELECT 
                            p.descrizione,
                            p.cluster,
                            p.pz_x_collo,
                            p.rapp,
                            CASE
                                WHEN e.sale_start IS NOT NULL
                                AND e.sale_end IS NOT NULL
                                AND CURRENT_DATE BETWEEN e.sale_start AND e.sale_end
                                THEN e.cost_s
                                ELSE e.cost_std
                            END AS cost
                        FROM products p
                        LEFT JOIN economics e 
                            ON p.cod = e.cod AND p.v = e.v
                        WHERE p.cod = %s AND p.v = %s;
                    """, (cod, var))
                    
                    row = cur.fetchone()
                    
                    if row:
                        descrizione = row['descrizione']
                        cluster = row['cluster'] or 'Uncategorized'
                        package_size = row['pz_x_collo'] or 0
                        rapp = row['rapp'] or 1
                        cost = row['cost'] or 0
                        cost = cost/rapp
                    else:
                        descrizione = f"Product {cod}.{var}"
                        cluster = 'Uncategorized'
                        package_size = 0
                        rapp = 1
                        cost = 0
                    
                    order_item = {
                        'cod': cod,
                        'var': var,
                        'qty': qty,
                        'name': descrizione,
                        'cluster': cluster,
                        'cost': cost,
                        'total_cost': cost * qty * package_size,
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
            
            # Enrich all three product lists
            enriched_new = self._enrich_product_list(service, new_products)
            enriched_skipped = self._enrich_product_list(service, skipped_products)
            enriched_zombie = self._enrich_product_list(service, zombie_products)
            enriched_order_skipped = self._enrich_product_list(service, order_skipped_products)
            
            # Calculate summary
            summary = {
                'total_items': len(enriched_orders),
                'total_packages': sum(o['qty'] for o in enriched_orders),
                'total_clusters': len(clusters),
                'total_cost': sum(o['total_cost'] for o in enriched_orders),
                'total_new': len(enriched_new),
                'total_skipped': len(enriched_skipped),
                'total_zombie': len(enriched_zombie),
                'total_order_skipped': len(enriched_order_skipped),
            }
            
            context['enriched_orders'] = enriched_orders
            context['clusters'] = clusters
            context['summary'] = summary
            context['results'] = results
            
            # Add all three lists to context
            context['enriched_new'] = enriched_new
            context['enriched_skipped'] = enriched_skipped
            context['enriched_zombie'] = enriched_zombie
            context['enriched_order_skipped'] = enriched_order_skipped
            
            logger.info(
                f"Context prepared: {len(enriched_orders)} orders, "
                f"{len(enriched_new)} new, {len(enriched_skipped)} skipped, "
                f"{len(enriched_zombie)} zombie, {len(enriched_order_skipped)} order-skipped"
            )
            
        finally:
            service.close()
        
        return context
    
    def _enrich_product_list(self, service, product_list):
        """
        Helper to enrich a list of products with database details.
        """
        enriched = []
        
        for item in product_list:
            cod = item.get('cod')
            var = item.get('var')
            reason = item.get('reason', 'Unknown')
            
            try:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT p.descrizione, ps.stock, p.disponibilita
                    FROM products p
                    LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.cod = %s AND p.v = %s
                """, (cod, var))
                
                row = cur.fetchone()
                
                if row:
                    enriched.append({
                        'cod': cod,
                        'var': var,
                        'name': row['descrizione'] or f"Product {cod}.{var}",
                        'stock': row['stock'] or 0,
                        'disponibilita': row['disponibilita'] or 'Unknown',
                        'reason': reason
                    })
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

@login_required
@require_POST
def flag_product_for_purge_view(request, log_id, product_cod, product_var):
    """Flag a skipped product for purging"""
    log = get_object_or_404(
        RestockLog,
        id=log_id,
        storage__supermarket__owner=request.user
    )
    
    try:
        service = RestockService(log.storage)
        result = service.db.flag_for_purge(product_cod, product_var)
        service.close()
        
        if result['action'] == 'flagged':
            messages.success(
                request,
                f"Product {product_cod}.{product_var} flagged for purging (current stock: {result['stock']})"
            )
        elif result['action'] == 'purged':
            messages.success(
                request,
                f"Product {product_cod}.{product_var} purged immediately (no stock)"
            )
    except Exception as e:
        logger.exception("Error flagging product for purge")
        messages.error(request, f"Error: {str(e)}")
    
    return redirect('restock-log-detail', pk=log_id)

# ============ Blacklist Views ============

class BlacklistListView(LoginRequiredMixin, ListView):
    model = Blacklist
    template_name = 'blacklists/list.html'
    context_object_name = 'blacklists'

    def get_queryset(self):
        return Blacklist.objects.filter(
            storage__supermarket__owner=self.request.user
        ).select_related('storage', 'storage__supermarket').prefetch_related('entries')


class BlacklistDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Blacklist
    template_name = 'blacklists/detail.html'
    context_object_name = 'blacklist'

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user


class BlacklistCreateView(LoginRequiredMixin, CreateView):
    model = Blacklist
    form_class = BlacklistForm
    template_name = 'blacklists/form.html'
    
    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        # Filter storages to only those owned by current user
        form.fields['storage'].queryset = Storage.objects.filter(
            supermarket__owner=self.request.user
        )
        return form
    
    def get_success_url(self):
        messages.success(self.request, "Blacklist created successfully!")
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.object.pk})


class BlacklistDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = Blacklist
    template_name = 'blacklists/confirm_delete.html'
    success_url = reverse_lazy('blacklist-list')

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user

    def delete(self, request, *args, **kwargs):
        messages.success(request, f"Blacklist '{self.get_object().name}' deleted successfully!")
        return super().delete(request, *args, **kwargs)


class BlacklistEntryCreateView(LoginRequiredMixin, UserPassesTestMixin, CreateView):
    model = BlacklistEntry
    form_class = BlacklistEntryForm
    template_name = 'blacklists/entries/form.html'
    
    def test_func(self):
        blacklist = get_object_or_404(Blacklist, pk=self.kwargs.get('blacklist_pk'))
        return blacklist.storage.supermarket.owner == self.request.user
    
    def dispatch(self, request, *args, **kwargs):
        self.blacklist = get_object_or_404(Blacklist, pk=self.kwargs.get('blacklist_pk'))
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['blacklist'] = self.blacklist
        return context
    
    def form_valid(self, form):
        form.instance.blacklist = self.blacklist
        messages.success(self.request, "Blacklist entry added!")
        return super().form_valid(form)
    
    def get_success_url(self):
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.blacklist.pk})


class BlacklistEntryDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = BlacklistEntry
    template_name = 'blacklists/entries/confirm_delete.html'
    
    def test_func(self):
        return self.get_object().blacklist.storage.supermarket.owner == self.request.user
    
    def get_success_url(self):
        messages.success(self.request, "Blacklist entry removed!")
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.object.blacklist.pk})


# ============ Data Management Views ============

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
        from .tasks import manual_list_update_task
        
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


@login_required
def upload_promos_view(request, supermarket_id):
    """
    REFACTORED: Async promo processing.
    PDF parsing can take time depending on file size.
    """
    supermarket = get_object_or_404(
        Supermarket,
        id=supermarket_id,
        owner=request.user
    )
    
    if request.method == 'POST':
        form = PromoUploadForm(request.POST, request.FILES)
        
        if form.is_valid():
            pdf_file = request.FILES['pdf_file']
            
            try:
                # Save file temporarily
                temp_dir = Path(settings.BASE_DIR) / 'temp_promos'
                temp_dir.mkdir(exist_ok=True)
                
                file_path = temp_dir / pdf_file.name
                
                with open(file_path, 'wb+') as destination:
                    for chunk in pdf_file.chunks():
                        destination.write(chunk)
                
                # ✅ DISPATCH TO CELERY
                from .tasks import process_promos_task
                
                result = process_promos_task.apply_async(
                    args=[supermarket_id, str(file_path)],
                    retry=True
                )
                
                messages.info(
                    request,
                    f"Processing promo file: {pdf_file.name}. "
                    f"This may take a few minutes."
                )
                
                return redirect('task-progress', task_id=result.id)
                
            except Exception as e:
                logger.exception("Error saving promo file")
                messages.error(request, f"Error: {str(e)}")
                return redirect('supermarket-detail', pk=supermarket_id)
    else:
        form = PromoUploadForm()
    
    return render(request, 'supermarkets/upload_promos.html', {
        'supermarket': supermarket,
        'form': form
    })


# ============ Stock Adjustment Views ============

@login_required
def adjust_stock_view(request, storage_id):
    """Manually adjust stock for a single product"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = StockAdjustmentForm(request.POST)
        
        if form.is_valid():
            product_code = form.cleaned_data['product_code']
            product_var = form.cleaned_data['product_var']
            adjustment = form.cleaned_data['adjustment']
            reason = form.cleaned_data['reason']
            notes = form.cleaned_data.get('notes', '')
            
            try:
                service = RestockService(storage)
                
                # Check if product exists
                try:
                    current_stock = service.db.get_stock(product_code, product_var)
                except ValueError:
                    messages.error(
                        request,
                        f"Product {product_code}.{product_var} not found in database"
                    )
                    service.close()
                    return redirect('adjust-stock', storage_id=storage_id)
                
                # Apply adjustment
                service.db.adjust_stock(product_code, product_var, adjustment)
                new_stock = service.db.get_stock(product_code, product_var)
                
                service.close()
                
                # Log the adjustment
                logger.info(
                    f"Stock adjusted for {storage.name}: "
                    f"Product {product_code}.{product_var} "
                    f"{current_stock} -> {new_stock} ({adjustment:+d}) "
                    f"Reason: {reason}"
                )
                
                messages.success(
                    request,
                    f"Stock adjusted successfully! "
                    f"Product {product_code}.{product_var}: "
                    f"{current_stock} → {new_stock} ({adjustment:+d})"
                )
                
                # Redirect back to form for another adjustment or to storage detail
                if 'adjust_another' in request.POST:
                    return redirect('adjust-stock', storage_id=storage_id)
                else:
                    return redirect('storage-detail', pk=storage_id)
                
            except Exception as e:
                logger.exception("Error adjusting stock")
                messages.error(request, f"Error adjusting stock: {str(e)}")
    else:
        form = StockAdjustmentForm()
    
    return render(request, 'storages/adjust_stock.html', {
        'storage': storage,
        'form': form
    })

# ============ Stock Value Analysis Views ============

@login_required
def stock_value_unified_view(request):
    """Unified stock value view with flexible filtering - FIXED CLUSTER SORTING"""
    
    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters from query params
    supermarket_id = request.GET.get('supermarket_id')
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
        service = RestockService(storage)
        settore = storage.settore
        cursor = service.db.cursor()
        cursor.execute("""
            SELECT DISTINCT cluster 
            FROM products 
            WHERE cluster IS NOT NULL AND cluster != '' AND settore = %s 
            ORDER BY cluster ASC
        """, (settore,))
        clusters = [row['cluster'] for row in cursor.fetchall()]
        service.close()
    
    # Calculate values
    category_totals = {}
    total_value = 0
    
    for storage in storages:
        try:
            service = RestockService(storage)
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
                WHERE e.category != '' AND ps.stock > 0
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
            
            service.close()
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
    }
    
    return render(request, 'stock_value_unified.html', context)

@login_required
def losses_analytics_unified_view(request):
    """
    OVERHAULED: Enhanced loss analytics with monetary values and full product lists.
    Now shows:
    - Losses in € (using cost_s from economics)
    - Complete product list with pagination
    - Better category breakdown
    """
    
    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters
    supermarket_id = request.GET.get('supermarket_id')
    storage_id = request.GET.get('storage_id')
    period = request.GET.get('period', '3')
    show_category = request.GET.get('show_category', 'all')  # NEW: Filter by category
    
    try:
        period_months = int(period)
    except ValueError:
        period_months = 3
    
    # Build scope description
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
    
    # Group storages by supermarket to avoid duplicate DB connections
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
    
    # ✅ NEW: Enhanced statistics with monetary values
    stats = {
        'broken': {
            'total_units': 0, 
            'total_value': 0.0,  # NEW
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24  # NEW
        },
        'expired': {
            'total_units': 0, 
            'total_value': 0.0,  # NEW
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24  # NEW
        },
        'internal': {
            'total_units': 0, 
            'total_value': 0.0,  # NEW
            'products': 0, 
            'monthly_units': [0]*24,
            'monthly_value': [0.0]*24  # NEW
        },
    }
    
    # ✅ NEW: Complete product list (not just top 20)
    all_products_list = []
    
    # Process each supermarket's database ONCE
    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            first_storage = sm_data['storages'][0]
            service = RestockService(first_storage)
            cursor = service.db.cursor()
            
            # Build WHERE clause
            if storage_id:
                settore_filter = f"WHERE p.settore = '{first_storage.settore}'"
            elif len(sm_data['settores']) < len(sm_data['supermarket'].storages.all()):
                settores_list = "', '".join(sm_data['settores'])
                settore_filter = f"WHERE p.settore IN ('{settores_list}')"
            else:
                settore_filter = ""
            
            # ✅ NEW: Enhanced query with cost data
            query = f"""
                SELECT 
                    el.cod, el.v,
                    el.broken, el.expired, el.internal,
                    p.descrizione,
                    e.cost_std,
                    e.category
                FROM extra_losses el
                LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                LEFT JOIN economics e ON el.cod = e.cod AND el.v = e.v
                {settore_filter}
            """
            
            cursor.execute(query)
            
            loss_types = ['broken', 'expired', 'internal']
            
            for row in cursor.fetchall():
                cod = row['cod']
                v = row['v']
                description = row['descrizione'] or f"Product {cod}.{v}"
                cost = row['cost_std'] or 0.0
                category = row['category'] or 'Unknown'
                
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
                    'total_units': 0,
                    'total_value': 0.0
                }
                
                for loss_type in loss_types:
                    loss_json = row[loss_type] or []
                    
                    if loss_json:
                        try:
                            loss_array = loss_json
                            
                            # Calculate for period
                            months_to_include = min(period_months, len(loss_array))
                            period_losses = sum(loss_array[:months_to_include])
                            period_value = period_losses * cost
                            
                            if period_losses > 0:
                                stats[loss_type]['total_units'] += period_losses
                                stats[loss_type]['total_value'] += period_value
                                stats[loss_type]['products'] += 1
                                
                                # Aggregate monthly data
                                for idx, units in enumerate(loss_array[:24]):
                                    stats[loss_type]['monthly_units'][idx] += units
                                    stats[loss_type]['monthly_value'][idx] += units * cost
                                
                                # Add to product losses
                                product_losses[f'{loss_type}_units'] = period_losses
                                product_losses[f'{loss_type}_value'] = period_value
                                product_losses['total_units'] += period_losses
                                product_losses['total_value'] += period_value
                        
                        except (ValueError, TypeError):
                            continue
                
                # Add to list if has losses
                if product_losses['total_units'] > 0:
                    all_products_list.append(product_losses)
            
            service.close()
        except Exception as e:
            logger.exception(f"Error processing losses for supermarket {sm_id}")
            continue
    
    # Sort products by total value (descending)
    all_products_list.sort(key=lambda x: x['total_value'], reverse=True)
    
    # ✅ NEW: Category filtering
    if show_category != 'all':
        filtered_products = [
            p for p in all_products_list 
            if p[f'{show_category}_units'] > 0
        ]
    else:
        filtered_products = all_products_list
    
    # Calculate totals
    total_units = sum(s['total_units'] for s in stats.values())
    total_value = sum(s['total_value'] for s in stats.values())
    
    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'scope_description': scope_description,
        'stats': stats,
        'total_units': total_units,
        'total_value': total_value,  # NEW
        'all_products': filtered_products,  # NEW: Complete list
        'total_products': len(filtered_products),
        'show_category': show_category,
        'period': period_months,
        'period_options': [
            {'value': 1, 'label': 'Last Month'},
            {'value': 3, 'label': 'Last 3 Months'},
            {'value': 6, 'label': 'Last 6 Months'},
            {'value': 12, 'label': 'Last Year'},
            {'value': 24, 'label': 'All Time (24 months)'},
        ]
    }
    
    return render(request, 'losses_analytics_unified.html', context)

@login_required
def add_products_view(request, storage_id):
    """
    UPDATED: Now uses unified task with gather_missing_product_data.
    Replaces old Scrapper-based approach for consistency.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = AddProductsForm(storage, request.POST)
        
        if form.is_valid():
            products_list = form.cleaned_data['products']
            settore = form.cleaned_data['settore']
            
            # ✅ DISPATCH TO UNIFIED TASK (uses gather_missing_product_data)
            from .tasks import add_products_unified_task
            
            result = add_products_unified_task.apply_async(
                args=[storage_id, products_list, settore],
                retry=True
            )
            
            est_time = len(products_list) * 20  # 20 seconds per product
            messages.info(
                request,
                f"Adding {len(products_list)} products using auto-fetch. "
                f"Estimated time: {est_time // 60} minutes. "
                f"Track progress on the next page."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
    else:
        form = AddProductsForm(storage)
    
    return render(request, 'storages/add_products.html', {
        'storage': storage,
        'form': form
    })

@login_required
def purge_products_view(request, storage_id):
    """View to flag/purge products"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = PurgeProductsForm(request.POST)
        
        if form.is_valid():
            products_list = form.cleaned_data['products']
            
            try:
                service = RestockService(storage)
                
                flagged = []
                purged = []
                errors = []
                
                for cod, var in products_list:
                    try:
                        result = service.db.flag_for_purge(cod, var)
                        
                        if result['action'] == 'flagged':
                            flagged.append(result)
                        elif result['action'] == 'purged':
                            purged.append(result)
                    
                    except ValueError as e:
                        errors.append(f"Product {cod}.{var}: {str(e)}")
                    except Exception as e:
                        logger.exception(f"Error processing {cod}.{var}")
                        errors.append(f"Product {cod}.{var}: {str(e)}")
                
                service.close()
                
                # Show results
                if purged:
                    messages.success(
                        request,
                        f"Immediately purged {len(purged)} products with zero stock"
                    )
                
                if flagged:
                    messages.warning(
                        request,
                        f"Flagged {len(flagged)} products for purging (they have stock > 0). "
                        f"They will be automatically purged when stock reaches zero."
                    )
                
                if errors:
                    for error in errors[:5]:
                        messages.error(request, error)
                
                return redirect('storage-detail', pk=storage_id)
                
            except Exception as e:
                logger.exception("Error in purge operation")
                messages.error(request, f"Error: {str(e)}")
    else:        
        form = PurgeProductsForm()
    
    # Get pending purges
    service = RestockService(storage)
    pending_purges = service.db.get_purge_pending()
    service.close()
    
    return render(request, 'storages/purge_products.html', {
        'storage': storage,
        'form': form,
        'pending_purges': pending_purges
    })


@login_required
@require_POST
def check_purge_flagged_view(request, storage_id):
    """Check and purge all flagged products with zero stock"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    try:
        service = RestockService(storage)
        purged = service.db.check_and_purge_flagged()
        service.close()
        
        if purged:
            messages.success(
                request,
                f"Automatically purged {len(purged)} flagged products that reached zero stock"
            )
        else:
            messages.info(request, "No flagged products ready for purging")
        
    except Exception as e:
        logger.exception("Error checking flagged products")
        messages.error(request, f"Error: {str(e)}")
    
    return redirect('purge-products', storage_id=storage_id)

@login_required
def restock_progress_view(request, log_id):
    """
    AJAX endpoint to check restock progress via RestockLog.
    ⚠️ Returns JSON for AJAX polling
    """
    log = get_object_or_404(
        RestockLog,
        id=log_id,
        storage__supermarket__owner=request.user
    )
    
    stage_info = log.get_stage_display_info()
    
    # Get current stage details for better UX
    stage_details = {
        'pending': 'Initializing...',
        'updating_stats': 'Downloading product statistics from PAC2000A... This may take 5-10 minutes.',
        'stats_updated': 'Statistics updated successfully!',
        'calculating_order': 'Analyzing stock levels and calculating order quantities...',
        'order_calculated': 'Order calculation complete!',
        'executing_order': 'Placing order in PAC2000A system...',
        'completed': 'All operations completed successfully!',
        'failed': 'Operation failed. See error details below.'
    }
    
    data = {
        'status': log.status,
        'current_stage': log.current_stage,
        'stage_label': stage_info['label'],
        'stage_details': stage_details.get(log.current_stage, ''),
        'progress': stage_info['progress'],
        'icon': stage_info['icon'],
        'products_ordered': log.products_ordered,
        'total_packages': log.total_packages,
        'error_message': log.error_message if log.status == 'failed' else None,
        'stats_updated_at': log.stats_updated_at.isoformat() if log.stats_updated_at else None,
        'order_calculated_at': log.order_calculated_at.isoformat() if log.order_calculated_at else None,
        'order_executed_at': log.order_executed_at.isoformat() if log.order_executed_at else None,
        'retry_count': log.retry_count,
        'can_retry': log.can_retry(),
    }
    
    return JsonResponse(data)

@login_required
@require_POST
def flag_products_for_purge_view(request, log_id):
    """Flag multiple skipped products for purging"""
    log = get_object_or_404(
        RestockLog,
        id=log_id,
        storage__supermarket__owner=request.user
    )
    
    try:
        data = json.loads(request.body)
        products = data.get('products', [])
        
        if not products:
            return JsonResponse({'success': False, 'message': 'No products provided'}, status=400)
        
        service = RestockService(log.storage)
        
        flagged_count = 0
        purged_count = 0
        errors = []
        
        for product in products:
            cod = product['cod']
            var = product['var']
            
            try:
                result = service.db.flag_for_purge(cod, var)
                
                if result['action'] == 'flagged':
                    flagged_count += 1
                elif result['action'] == 'purged':
                    purged_count += 1
            except Exception as e:
                logger.warning(f"Error flagging {cod}.{var}: {e}")
                errors.append(f"{cod}.{var}: {str(e)}")
        
        service.close()
        
        return JsonResponse({
            'success': True,
            'flagged': flagged_count,
            'purged': purged_count,
            'errors': errors
        })
        
    except Exception as e:
        logger.exception("Error flagging products")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)
    
# ============ Inventory Management Views ============

@login_required
def inventory_search_view(request):
    """Main inventory search interface"""
    
    if request.method == 'POST':
        form = InventorySearchForm(request.user, request.POST)
        
        if form.is_valid():
            search_type = form.cleaned_data['search_type']
            
            if search_type == 'cod_var':
                cod = form.cleaned_data['product_code']
                var = form.cleaned_data['product_var']
                return redirect(f'/inventory/results/cod_var/?cod={cod}&var={var}')
            
            elif search_type == 'cod_all':
                cod = form.cleaned_data['product_code']
                return redirect(f'/inventory/results/cod_all/?cod={cod}')
            
            elif search_type == 'settore_cluster':
                supermarket_id = form.cleaned_data['supermarket']
                settore = form.cleaned_data['settore']
                cluster = form.cleaned_data.get('cluster') or ''
                
                if cluster:
                    return redirect(f'/inventory/results/settore_cluster/?supermarket_id={supermarket_id}&settore={settore}&cluster={cluster}')
                else:
                    return redirect(f'/inventory/results/settore_cluster/?supermarket_id={supermarket_id}&settore={settore}')
        else:
            # Form has errors - re-render with errors
            return render(request, 'inventory/search.html', {'form': form})
    else:
        form = InventorySearchForm(request.user)
    
    return render(request, 'inventory/search.html', {'form': form})


@login_required  
def inventory_results_view(request, search_type):
    """Display inventory search results - FULLY FIXED"""
    
    results = []
    search_description = ""
    supermarket = None
    settore_name = None
    
    try:
        if search_type == 'cod_var':
            # Search for specific product
            cod = int(request.GET.get('cod'))
            var = int(request.GET.get('var'))
            search_description = f"Product {cod}.{var}"
            
            # Search across all supermarkets
            found = False
            for sm in Supermarket.objects.filter(owner=request.user):
                storage = sm.storages.first()
                if not storage:
                    continue
                
                service = RestockService(storage)
                try:
                    cur = service.db.cursor()
                    cur.execute("""
                        SELECT 
                            p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita, 
                            p.settore, p.cluster,
                            ps.stock, ps.last_update, ps.verified
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.cod = %s AND p.v = %s AND ps.verified = TRUE
                    """, (cod, var))
                    
                    row = cur.fetchone()
                    if row:
                        found = True
                        result = dict(row)
                        result['supermarket_name'] = sm.name
                        results.append(result)
                except Exception as e:
                    logger.exception(f"Error searching in {sm.name}")
                finally:
                    service.close()
            
            # If not found, redirect to not found page
            if not found:
                return redirect('inventory-product-not-found', cod=cod, var=var)
        
        elif search_type == 'cod_all':
            # NEW: Search for all variants of a product code
            cod = int(request.GET.get('cod'))
            search_description = f"All variants of product code {cod}"
            
            logger.info(f"Searching for all variants of code: {cod}")
            
            # Search across all supermarkets
            for sm in Supermarket.objects.filter(owner=request.user):
                storage = sm.storages.first()
                if not storage:
                    continue
                
                service = RestockService(storage)
                try:
                    cur = service.db.cursor()
                    cur.execute("""
                        SELECT 
                            p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                            p.settore, p.cluster,
                            ps.stock, ps.last_update, ps.verified
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.cod = %s AND ps.verified = TRUE
                        ORDER BY p.v
                    """, (cod,))
                    
                    for row in cur.fetchall():
                        result = dict(row)
                        result['supermarket_name'] = sm.name
                        results.append(result)
                    
                    logger.info(f"Found {len(results)} variants in {sm.name}")
                except Exception as e:
                    logger.exception(f"Error searching in {sm.name}")
                finally:
                    service.close()
        
        elif search_type == 'settore_cluster':
            # Search by settore and optionally cluster
            supermarket_id = request.GET.get('supermarket_id')
            settore = request.GET.get('settore')
            cluster = request.GET.get('cluster', '')
            
            logger.info(f"Settore search: supermarket={supermarket_id}, settore={settore}, cluster={cluster}")
            
            if not supermarket_id or not settore:
                messages.error(request, "Missing search parameters")
                return redirect('inventory-search')
            
            try:
                supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
            except Exception as e:
                logger.exception("Supermarket not found")
                messages.error(request, f"Supermarket not found: {e}")
                return redirect('inventory-search')
            
            settore_name = settore
            
            if cluster:
                search_description = f"{supermarket.name} - {settore} - Cluster: {cluster}"
            else:
                search_description = f"{supermarket.name} - {settore} (All Clusters)"
            
            storage = supermarket.storages.filter(settore=settore).first()
            
            if not storage:
                messages.warning(request, f"No storage found for settore: {settore}")
                return redirect('inventory-search')
            
            service = RestockService(storage)
            try:
                cur = service.db.cursor()
                
                if cluster:
                    logger.info(f"Querying with cluster filter: {cluster}")
                    cur.execute("""
                        SELECT 
                            p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                            p.settore, p.cluster,
                            ps.stock, ps.last_update, ps.verified
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s AND p.cluster = %s AND ps.verified = TRUE
                        ORDER BY p.descrizione
                    """, (settore, cluster))
                else:
                    logger.info(f"Querying all clusters for settore: {settore}")
                    cur.execute("""
                        SELECT 
                            p.cod, p.v, p.descrizione, p.pz_x_collo, p.disponibilita,
                            p.settore, p.cluster,
                            ps.stock, ps.last_update, ps.verified
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s AND ps.verified = TRUE
                        ORDER BY p.cluster, p.descrizione
                    """, (settore,))
                
                for row in cur.fetchall():
                    result = dict(row)
                    result['supermarket_name'] = supermarket.name
                    results.append(result)
                
                logger.info(f"Found {len(results)} results for settore search")
                
            except Exception as e:
                logger.exception(f"Database error in settore search")
                messages.error(request, f"Database error: {e}")
                return redirect('inventory-search')
            finally:
                service.close()
    
    except Exception as e:
        logger.exception("Error in inventory search")
        messages.error(request, f"Search error: {str(e)}")
        return redirect('inventory-search')
    
    context = {
        'results': results,
        'search_description': search_description,
        'search_type': search_type,
        'supermarket': supermarket,
        'settore': settore_name,
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
        
        service = RestockService(storage)
        try:
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
        finally:
            service.close()
    
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
def get_clusters_for_settore_view(request, supermarket_id, settore):
    """AJAX endpoint to get clusters for a settore"""
    try:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = supermarket.storages.filter(settore=settore).first()
        
        if not storage:
            logger.warning(f"No storage found for settore: {settore}")
            return JsonResponse({'clusters': []})
        
        service = RestockService(storage)
        try:
            cur = service.db.cursor()
            cur.execute("""
                SELECT DISTINCT cluster
                FROM products
                WHERE settore = %s 
                  AND cluster IS NOT NULL 
                  AND cluster != ''
                ORDER BY cluster
            """, (settore,))
            
            clusters = [row['cluster'] for row in cur.fetchall()]
            logger.info(f"API: Loaded {len(clusters)} clusters for settore {settore}")
            
            return JsonResponse({'clusters': clusters})
        finally:
            service.close()
    
    except Exception as e:
        logger.exception("Error loading clusters")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@require_POST  
def inventory_flag_for_purge_ajax_view(request):
    """AJAX endpoint to flag product for purge from inventory view"""
    try:
        data = json.loads(request.body)
        cod = data['cod']
        var = data['var']
        supermarket_name = data['supermarket']
        
        supermarket = get_object_or_404(Supermarket, name=supermarket_name, owner=request.user)
        storage = supermarket.storages.first()
        
        if not storage:
            return JsonResponse({'success': False, 'message': 'No storage found'}, status=400)
        
        service = RestockService(storage)
        try:
            result = service.db.flag_for_purge(cod, var)
            return JsonResponse({'success': True, 'message': result['message']})
        finally:
            service.close()
            
    except Exception as e:
        logger.exception("Error flagging product for purge")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)


@login_required
def update_stats_only_view(request, storage_id):
    """
    REFACTORED: Async stats update.
    Before: 5-10 minute Selenium operation killed by Gunicorn
    After: Dispatches to Celery, returns immediately
    """
    storage = get_object_or_404(
        Storage, 
        id=storage_id, 
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        # ✅ DISPATCH TO CELERY
        from .tasks import manual_stats_update_task
        
        result = manual_stats_update_task.apply_async(
            args=[storage_id],
            retry=True
        )
        
        messages.info(
            request,
            f"Stats update started for {storage.name}. "
            f"This will take 5-10 minutes. Check progress on the next page."
        )
        
        return redirect('task-progress', task_id=result.id, storage_id=storage_id)
    
    return render(request, 'storages/update_stats_only.html', {'storage': storage})

# ============ NEW: Unified Inventory Operations ============
    
@login_required
@require_POST
def auto_add_product_view(request):
    """
    FIXED: Now returns task_id for async tracking instead of blocking.
    Frontend can track progress properly.
    """
    try:
        data = json.loads(request.body)
        cod = int(data['cod'])
        var = int(data['var'])
        supermarket_id = int(data['supermarket_id'])
        storage_id = int(data['storage_id'])

        products_list = [(cod, var)]
        
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = get_object_or_404(Storage, id=storage_id, supermarket=supermarket)
        
        logger.info(f"Auto-adding product {cod}.{var} to {storage.name}")
        
        # ✅ FIX: Dispatch async and return task_id (don't block with .get())
        from .tasks import add_products_unified_task
        
        result = add_products_unified_task.apply_async(
            args=[storage_id, products_list, storage.settore],
            retry=True
        )
        
        # Return task_id for frontend to track
        return JsonResponse({
            'success': True,
            'task_id': result.id,
            'message': f'Auto-adding product {cod}.{var}...'
        })
            
    except Exception as e:
        logger.exception("Error in auto_add_product_view")
        return JsonResponse({
            'success': False,
            'message': f'Error: {str(e)}'
        }, status=500)

@login_required
def verify_stock_unified_enhanced_view(request):
    """
    FIXED: Now properly dispatches Celery task for auto-add verification.
    Handles PDF files and automatically adds missing products.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    if request.method == 'POST':
        supermarket_id = request.POST.get('supermarket_id')
        storage_id = request.POST.get('storage_id')
        cluster = request.POST.get('cluster', '').strip().upper()
        
        if not supermarket_id or not storage_id:
            messages.error(request, "Please select both supermarket and storage")
            return redirect('verify-stock-unified-enhanced')
        
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket_id=supermarket_id,
            supermarket__owner=request.user
        )
        
        if 'pdf_file' not in request.FILES:
            messages.error(request, "No file uploaded")
            return redirect('verify-stock-unified-enhanced')
        
        pdf_file = request.FILES['pdf_file']
        
        if not pdf_file.name.endswith('.pdf'):
            messages.error(request, "File must be .pdf format")
            return redirect('verify-stock-unified-enhanced')
        
        try:
            # Save file to INVENTORY_FOLDER
            inventory_folder = Path(settings.INVENTORY_FOLDER)
            inventory_folder.mkdir(exist_ok=True)
            
            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            file_path = inventory_folder / f"verify_auto_{timestamp}_{pdf_file.name}"
            
            with open(file_path, 'wb+') as destination:
                for chunk in pdf_file.chunks():
                    destination.write(chunk)
            
            # ✅ DISPATCH TO NEW CELERY TASK WITH AUTO-ADD
            from .tasks import verify_stock_with_auto_add_task
            
            result = verify_stock_with_auto_add_task.apply_async(
                args=[storage_id, str(file_path), cluster or None],
                retry=True
            )
            
            cluster_msg = f" (Cluster: {cluster})" if cluster else ""
            messages.info(
                request,
                f"Stock verification with auto-add started for {storage.name}{cluster_msg}. "
                f"Missing products will be automatically fetched and added. "
                f"This may take 10-20 minutes."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
            
        except Exception as e:
            logger.exception("Error starting verification")
            messages.error(request, f"Error: {str(e)}")
            return redirect('verify-stock-unified-enhanced')
    
    # GET request - load existing clusters for dropdown
    clusters_by_storage = {}
    for sm in supermarkets:
        for storage in sm.storages.all():
            service = RestockService(storage)
            try:
                cursor = service.db.cursor()
                cursor.execute("""
                    SELECT DISTINCT cluster 
                    FROM products 
                    WHERE cluster IS NOT NULL AND cluster != '' 
                        AND settore = %s 
                    ORDER BY cluster ASC
                """, (storage.settore,))
                clusters = [row['cluster'] for row in cursor.fetchall()]
                clusters_by_storage[storage.id] = clusters
            finally:
                service.close()
    
    return render(request, 'inventory/verify_stock_unified.html', {
        'supermarkets': supermarkets,
        'clusters_by_storage': json.dumps(clusters_by_storage)
    })


@login_required
def assign_clusters_view(request):
    """
    UPDATED: Now handles PDF files instead of CSV.
    User provides cluster name, not derived from filename.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    if request.method == 'POST':
        supermarket_id = request.POST.get('supermarket_id')
        storage_id = request.POST.get('storage_id')
        cluster = request.POST.get('cluster', '').strip().upper()
        
        if not supermarket_id or not storage_id:
            messages.error(request, "Please select both supermarket and storage")
            return redirect('assign-clusters')
        
        if not cluster:
            messages.error(request, "Please provide a cluster name")
            return redirect('assign-clusters')
        
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket_id=supermarket_id,
            supermarket__owner=request.user
        )
        
        if 'pdf_file' not in request.FILES:
            messages.error(request, "No file uploaded")
            return redirect('assign-clusters')
        
        pdf_file = request.FILES['pdf_file']
        
        if not pdf_file.name.endswith('.pdf'):
            messages.error(request, "File must be .pdf format (not CSV)")
            return redirect('assign-clusters')
        
        try:
            # Save file
            inventory_folder = Path(settings.INVENTORY_FOLDER)
            inventory_folder.mkdir(exist_ok=True)
            
            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            file_path = inventory_folder / f"cluster_{timestamp}_{pdf_file.name}"
            
            with open(file_path, 'wb+') as destination:
                for chunk in pdf_file.chunks():
                    destination.write(chunk)
            
            # ✅ DISPATCH TO CELERY with explicit cluster name
            from .tasks import assign_clusters_task
            
            result = assign_clusters_task.apply_async(
                args=[storage_id, str(file_path), cluster],
                retry=True
            )
            
            messages.info(
                request,
                f"Assigning cluster '{cluster}' to products. This may take a few minutes."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
            
        except Exception as e:
            logger.exception("Error assigning clusters")
            messages.error(request, f"Error: {str(e)}")
            return redirect('assign-clusters')
    
    # Load existing clusters for reference
    clusters_by_storage = {}
    for sm in supermarkets:
        for storage in sm.storages.all():
            service = RestockService(storage)
            try:
                cursor = service.db.cursor()
                cursor.execute("""
                    SELECT DISTINCT cluster 
                    FROM products 
                    WHERE cluster IS NOT NULL AND cluster != '' 
                        AND settore = %s
                    ORDER BY cluster ASC
                """, (storage.settore,))
                clusters = [row['cluster'] for row in cursor.fetchall()]
                clusters_by_storage[storage.id] = clusters
            finally:
                service.close()
    
    return render(request, 'inventory/assign_clusters.html', {
        'supermarkets': supermarkets,
        'clusters_by_storage': json.dumps(clusters_by_storage)
    })

@login_required
def record_losses_unified_view(request):
    """
    UPDATED: Now handles PDF files instead of CSV.
    Processing can take time for large PDFs.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    if request.method == 'POST':
        form = RecordLossesForm(request.POST, request.FILES)
        supermarket_id = request.POST.get('supermarket_id')
        
        if not supermarket_id:
            messages.error(request, "Please select a supermarket")
            return redirect('record-losses-unified')
        
        supermarket = get_object_or_404(
            Supermarket,
            id=supermarket_id,
            owner=request.user
        )
        
        if form.is_valid():
            loss_type = form.cleaned_data['loss_type']
            pdf_file = request.FILES['pdf_file']
            
            # Map loss types to filenames (keep same naming for compatibility)
            filename_mapping = {
                'broken': 'ROTTURE.pdf',
                'expired': 'SCADUTO.pdf',
                'internal': 'UTILIZZO INTERNO.pdf'
            }
            expected_filename = filename_mapping[loss_type]
            
            try:
                losses_folder = Path(settings.LOSSES_FOLDER)
                losses_folder.mkdir(exist_ok=True)
                
                file_path = losses_folder / expected_filename
                
                # Overwrite if exists
                if file_path.exists():
                    file_path.unlink()
                
                with open(file_path, 'wb+') as destination:
                    for chunk in pdf_file.chunks():
                        destination.write(chunk)
                
                logger.info(f"Saved loss PDF: {file_path}")
                
                # Process immediately (synchronous for now - can be celery task later)
                storage = supermarket.storages.first()
                if not storage:
                    messages.error(request, f"No storages found for {supermarket.name}")
                    return redirect('record-losses-unified')
                
                service = RestockService(storage)
                
                try:
                    # Import the new function
                    from .scripts.inventory_reader import process_loss_pdf
                    
                    result = process_loss_pdf(service.db, str(file_path), loss_type)
                    
                    if result['success']:
                        messages.success(
                            request,
                            f"✅ Processed {loss_type} losses: "
                            f"{result['processed']} registered, "
                            f"{result['total_losses']} total units"
                        )
                        
                        if result['absent'] > 0:
                            messages.info(
                                request,
                                f"ℹ️ {result['absent']} products not found in database (skipped)"
                            )
                        
                        if result['errors'] > 0:
                            messages.warning(
                                request,
                                f"⚠️ {result['errors']} errors occurred during processing"
                            )
                    else:
                        messages.error(request, f"Error: {result.get('error', 'Unknown error')}")
                    
                    # Clean up PDF file after processing
                    try:
                        file_path.unlink()
                        logger.info(f"Deleted processed PDF: {file_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete PDF: {e}")
                    
                finally:
                    service.close()
                
                return redirect('record-losses-unified')
                
            except Exception as e:
                logger.exception("Error saving/processing loss PDF")
                messages.error(request, f"Error: {str(e)}")
    else:
        form = RecordLossesForm()
    
    return render(request, 'inventory/record_losses_unified.html', {
        'supermarkets': supermarkets,
        'form': form
    })


@login_required
def verification_report_unified_view(request):
    """
    UPDATED: Now displays auto-added products separately.
    Shows comprehensive report with all three categories.
    """
    # Try to get from Celery task result first
    task_id = request.GET.get('task_id')
    
    if task_id:
        from celery.result import AsyncResult
        from LamApp.celery import app as celery_app
        
        task = AsyncResult(task_id, app=celery_app)
        
        if task.ready() and task.successful():
            report = task.result
        else:
            report = request.session.get('verification_report')
    else:
        report = request.session.get('verification_report')
    
    if not report:
        messages.warning(request, "No verification report available")
        return redirect('inventory-search')
    
    # Calculate statistics
    total_difference = 0
    
    if report.get('stock_changes'):
        total_difference = sum(
            change.get('difference', 0) 
            for change in report['stock_changes']
        )
    
    # Add auto-added products to difference
    if report.get('added_products'):
        total_difference += sum(
            product.get('qty', 0) 
            for product in report['added_products']
        )
    
    report['total_difference'] = total_difference
    
    return render(request, 'inventory/verification_report_unified.html', {
        'report': report
    })


@login_required
@require_POST
def verify_single_product_ajax_view(request):
    """
    AJAX endpoint for verifying single product from inventory modal
    Uses supermarket + settore instead of storage_id
    """
    try:
        data = json.loads(request.body)
        
        supermarket_id = data.get('supermarket_id')
        settore = data.get('settore')
        cod = int(data.get('cod'))
        var = int(data.get('var'))
        stock = int(data.get('stock'))
        cluster = data.get('cluster', None) or None
        
        # Find storage by supermarket + settore
        storage = get_object_or_404(
            Storage,
            supermarket_id=supermarket_id,
            settore=settore,
            supermarket__owner=request.user
        )
        
        service = RestockService(storage)
        
        try:
            service.db.verify_stock(cod, var, stock, cluster)
            
            logger.info(
                f"Single product verified: {storage.supermarket.name} - {settore} - "
                f"{cod}.{var} = {stock}" + (f" (cluster: {cluster})" if cluster else "")
            )
            
            return JsonResponse({
                'success': True,
                'message': f'Product {cod}.{var} verified successfully!'
            })
            
        finally:
            service.close()
            
    except Exception as e:
        logger.exception("Error verifying single product")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


# ============ API Endpoints for Dynamic Loading ============

@login_required
def get_storages_for_supermarket_ajax_view(request, supermarket_id):
    """AJAX endpoint to get storages for a supermarket"""
    try:
        supermarket = get_object_or_404(
            Supermarket,
            id=supermarket_id,
            owner=request.user
        )
        
        storages = list(
            supermarket.storages.values('id', 'settore', 'name')
            .order_by('settore')
        )
        
        return JsonResponse({'storages': storages})
    
    except Exception as e:
        logger.exception("Error loading storages")
        return JsonResponse({'error': str(e)}, status=500)   

# ============ Task Progress Views ============

@login_required
def task_progress_view(request, task_id, storage_id=None):
    """
    Generic progress view for ANY Celery task.
    Shows real-time progress and auto-redirects when complete.
    """
    from celery.result import AsyncResult
    
    # FIX: Bind to our Celery app instance
    task = AsyncResult(task_id, app=celery_app)
    storage = None
    
    if storage_id:
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket__owner=request.user
        )
    
    context = {
        'task_id': task_id,
        'task_state': task.state,
        'storage': storage
    }
    
    return render(request, 'tasks/progress.html', context)


@login_required
def task_status_ajax_view(request, task_id):
    """
    FIXED: Better handling of different task result formats.
    Returns consistent structure for all task types.
    """
    from celery.result import AsyncResult
    
    task = AsyncResult(task_id, app=celery_app)
    
    response_data = {
        'state': task.state,
        'ready': task.ready(),
        'task_id': task_id,
    }
    
    if task.ready():
        if task.successful():
            result = task.result
            response_data['success'] = True
            
            # ✅ FIX: Handle different result types consistently
            if isinstance(result, dict):
                response_data['result'] = result
                
                # Determine redirect URL based on result content
                if 'log_id' in result:
                    # Restock operation
                    response_data['redirect_url'] = f"/logs/{result['log_id']}/"
                elif 'storage_name' in result and 'products_added' in result:
                    # Add products operation
                    response_data['redirect_url'] = "/inventory/"
                    response_data['message'] = result.get('message', 'Operation completed successfully')
                elif 'storage_name' in result:
                    # Generic storage operation
                    response_data['redirect_url'] = "/dashboard/"
                    response_data['message'] = result.get('message', 'Operation completed successfully')
                else:
                    # Unknown format - just show message
                    response_data['message'] = str(result)
            else:
                # Non-dict result
                response_data['result'] = {'message': str(result)}
                response_data['message'] = str(result)
        else:
            # Task failed
            response_data['success'] = False
            error_info = task.info
            
            if isinstance(error_info, Exception):
                response_data['error'] = str(error_info)
            elif isinstance(error_info, dict):
                response_data['error'] = error_info.get('exc_message', str(error_info))
            else:
                response_data['error'] = str(error_info)
    else:
        # Task still running - extract progress info
        if isinstance(task.info, dict):
            response_data['progress'] = task.info.get('progress', 0)
            response_data['status_message'] = task.info.get('status', 'Processing...')
        else:
            response_data['progress'] = 0
            response_data['status_message'] = 'Processing...'
    
    return JsonResponse(response_data)

@login_required
def restock_task_progress_view(request, task_id):
    """
    Specialized progress view for restock operations.
    Uses RestockLog for detailed checkpoint tracking.
    """
    from celery.result import AsyncResult
    
    # FIX: Bind to our Celery app instance
    task = AsyncResult(task_id, app=celery_app)
    
    # Try to find log from task result
    log = None
    if task.ready() and task.successful():
        result = task.result
        if isinstance(result, dict) and 'log_id' in result:
            try:
                log = RestockLog.objects.get(
                    id=result['log_id'],
                    storage__supermarket__owner=request.user
                )
            except RestockLog.DoesNotExist:
                pass
    
    context = {
        'task_id': task_id,
        'task_state': task.state,
        'log': log
    }
    
    return render(request, 'storages/restock_task_progress.html', context)

@login_required
def edit_losses_view(request):
    """
    View to edit recorded losses WITHOUT affecting stock.
    Shows all products with losses and allows editing individual array values.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters
    supermarket_id = request.GET.get('supermarket_id')
    storage_id = request.GET.get('storage_id')
    
    # Build scope
    if supermarket_id:
        supermarkets_filter = Supermarket.objects.filter(id=supermarket_id, owner=request.user)
    else:
        supermarkets_filter = supermarkets
    
    # Get storages
    if storage_id:
        storages = Storage.objects.filter(id=storage_id)
    else:
        storages = Storage.objects.filter(supermarket__in=supermarkets_filter)
    
    # Group by supermarket to avoid duplicate DB connections
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
    
    products_with_losses = []
    
    # Process each supermarket's database
    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            first_storage = sm_data['storages'][0]
            service = RestockService(first_storage)
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
                    el.broken, el.broken_updated,
                    el.expired, el.expired_updated,
                    el.internal, el.internal_updated,
                    p.descrizione,
                    p.settore
                FROM extra_losses el
                LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                {settore_filter}
                ORDER BY p.descrizione
            """
            
            cursor.execute(query)
            
            for row in cursor.fetchall():
                product = {
                    'cod': row['cod'],
                    'var': row['v'],
                    'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                    'settore': row['settore'],
                    'supermarket_name': sm_data['supermarket'].name,
                    'supermarket_id': sm_data['supermarket'].id,
                    'broken': row['broken'] or [],
                    'broken_updated': row['broken_updated'],
                    'expired': row['expired'] or [],
                    'expired_updated': row['expired_updated'],
                    'internal': row['internal'] or [],
                    'internal_updated': row['internal_updated'],
                }
                
                # Only include if has at least one loss recorded
                if product['broken'] or product['expired'] or product['internal']:
                    products_with_losses.append(product)
            
            service.close()
        except Exception as e:
            logger.exception(f"Error loading losses for supermarket {sm_id}")
            continue
    
    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'products': products_with_losses,
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
    }
    
    return render(request, 'inventory/edit_losses.html', context)


@login_required
@require_POST
def edit_loss_ajax_view(request):
    """
    AJAX endpoint to edit a specific loss value.
    Updates extra_losses table WITHOUT affecting stock.
    """
    try:
        data = json.loads(request.body)
        
        supermarket_id = int(data['supermarket_id'])
        cod = int(data['cod'])
        var = int(data['var'])
        loss_type = data['loss_type']  # 'broken', 'expired', or 'internal'
        month_index = int(data['month_index'])  # 0 = most recent month
        new_value = int(data['new_value'])
        
        # Validate loss type
        if loss_type not in ['broken', 'expired', 'internal']:
            return JsonResponse({
                'success': False,
                'message': 'Invalid loss type'
            }, status=400)
        
        # Get supermarket and storage
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = supermarket.storages.first()
        
        if not storage:
            return JsonResponse({
                'success': False,
                'message': 'No storage found'
            }, status=400)
        
        service = RestockService(storage)
        
        try:
            cursor = service.db.cursor()
            
            # Get current array
            cursor.execute(f"""
                SELECT {loss_type}, {loss_type}_updated
                FROM extra_losses
                WHERE cod = %s AND v = %s
            """, (cod, var))
            
            row = cursor.fetchone()
            
            if not row:
                return JsonResponse({
                    'success': False,
                    'message': 'Product not found in extra_losses'
                }, status=404)
            
            current_array = row[loss_type] or []
            
            # Validate month index
            if month_index >= len(current_array):
                return JsonResponse({
                    'success': False,
                    'message': f'Month index {month_index} out of range (array length: {len(current_array)})'
                }, status=400)
            
            # Calculate stock adjustment needed
            old_value = current_array[month_index]
            stock_delta = old_value - new_value  # Positive = add back to stock, negative = remove more
            
            # Update array (WITHOUT calling register_losses to avoid stock adjustment)
            current_array[month_index] = new_value
            
            # Update database - ONLY the extra_losses table
            cursor.execute(f"""
                UPDATE extra_losses
                SET {loss_type} = %s
                WHERE cod = %s AND v = %s
            """, (Json(current_array), cod, var))
            
            service.db.conn.commit()
            
            logger.info(
                f"Loss edited: {supermarket.name} - Product {cod}.{var} - "
                f"{loss_type}[{month_index}]: {old_value} → {new_value} "
                f"(stock NOT adjusted as per requirement)"
            )
            
            return JsonResponse({
                'success': True,
                'message': f'Updated {loss_type} month {month_index}: {old_value} → {new_value}',
                'old_value': old_value,
                'new_value': new_value,
                'stock_delta_not_applied': stock_delta
            })
            
        finally:
            service.close()
            
    except Exception as e:
        logger.exception("Error editing loss value")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
@require_POST
def inventory_adjust_stock_ajax_view(request):
    """
    UPDATED: Now links stock adjustments to loss categories.
    If reason is broken/expired/internal, records in extra_losses automatically.
    """
    try:
        cod = int(request.POST.get('cod'))
        var = int(request.POST.get('var'))
        adjustment = int(request.POST.get('adjustment'))
        reason = request.POST.get('reason')
        supermarket_name = request.POST.get('supermarket')
        
        # Find the supermarket
        supermarket = get_object_or_404(Supermarket, name=supermarket_name, owner=request.user)
        storage = supermarket.storages.first()
        
        if not storage:
            return JsonResponse({'success': False, 'message': 'No storage found'}, status=400)
        
        service = RestockService(storage)
        
        try:
            current_stock = service.db.get_stock(cod, var)
            
            # ✅ NEW: Map reasons to loss types
            loss_type_mapping = {
                'broken': 'broken',
                'expired': 'expired',
                'internal_use': 'internal',
            }
            
            if reason in loss_type_mapping and adjustment < 0:
                # This is a loss adjustment - record in extra_losses
                loss_type = loss_type_mapping[reason]
                loss_amount = abs(adjustment)  # Make positive for loss recording
                
                # register_losses() handles both array update AND stock adjustment
                service.db.register_losses(cod, var, loss_amount, loss_type)
                
                new_stock = service.db.get_stock(cod, var)
                
                logger.info(
                    f"Stock adjusted via loss recording: {supermarket_name} - "
                    f"Product {cod}.{var}: {current_stock} → {new_stock} ({adjustment:+d}) "
                    f"Loss type: {loss_type}, Amount: {loss_amount}"
                )
                
                return JsonResponse({
                    'success': True,
                    'message': f'Stock adjusted and recorded as {loss_type} loss: {current_stock} → {new_stock}',
                    'new_stock': new_stock,
                    'loss_recorded': True,
                    'loss_type': loss_type,
                    'loss_amount': loss_amount
                })
            else:
                # Regular stock adjustment (not a loss)
                service.db.adjust_stock(cod, var, adjustment)
                new_stock = service.db.get_stock(cod, var)
                
                logger.info(
                    f"Stock adjusted: {supermarket_name} - "
                    f"Product {cod}.{var}: {current_stock} → {new_stock} ({adjustment:+d}) "
                    f"Reason: {reason}"
                )
                
                return JsonResponse({
                    'success': True,
                    'message': f'Stock adjusted: {current_stock} → {new_stock}',
                    'new_stock': new_stock,
                    'loss_recorded': False
                })
                
        finally:
            service.close()
            
    except Exception as e:
        logger.exception("Error in inventory stock adjustment")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)