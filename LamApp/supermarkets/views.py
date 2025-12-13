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
import csv
import io
from django.conf import settings
import os
from .automation_services import AutomatedRestockService
import threading
import pandas as pd

from .models import (
    Supermarket, Storage, RestockSchedule, 
    Blacklist, BlacklistEntry, RestockLog, ListUpdateSchedule
)
from .forms import (
    RestockScheduleForm, BlacklistForm, PurgeProductsForm,
    BlacklistEntryForm, AddProductsForm, PromoUploadForm, ListUpdateScheduleForm,
    StockAdjustmentForm, BulkStockAdjustmentForm, RecordLossesForm, SingleProductVerificationForm
)
from .services import RestockService, StorageService
from .scripts.scrapper import Scrapper
import logging

logger = logging.getLogger(__name__)


# ============ Authentication Views ============

def signup(request):
    """User registration"""
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
    ).select_related('schedule', 'supermarket', 'list_update_schedule')
    
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
        
        # Check if list update schedule exists
        try:
            context['list_schedule'] = self.object.list_update_schedule
        except ListUpdateSchedule.DoesNotExist:
            context['list_schedule'] = None
        
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
    """Manually trigger a restock check with checkpoint support - ASYNC FRIENDLY - THREAD-SAFE"""
    storage = get_object_or_404(
        Storage, 
        id=storage_id, 
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        # Check if this is an AJAX request
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        
        try:
            # Get coverage from form or use default
            coverage = request.POST.get('coverage')
            if coverage:
                coverage = float(coverage)
            
            # CRITICAL: Create log BEFORE starting thread
            log = RestockLog.objects.create(
                storage=storage,
                status='processing',
                current_stage='pending',
                started_at=timezone.now()
            )
            
            if is_ajax:
                # CRITICAL FIX: Create service INSIDE background thread to avoid SQLite threading issues
                
                def run_workflow():
                    """Background thread worker - creates its own DB connection"""
                    
                    
                    # CRITICAL: Service (and DB connection) created in THIS thread
                    service = AutomatedRestockService(storage)
                    
                    try:
                        service.run_full_restock_workflow(coverage, log=log)
                    except Exception as e:
                        logger.exception(f"Error in background restock for log #{log.id}")
                    finally:
                        # Clean up DB connection
                        service.close()
                
                thread = threading.Thread(target=run_workflow)
                thread.daemon = True
                thread.start()
                
                return JsonResponse({'log_id': log.id})
            else:
                # Synchronous execution for non-AJAX
                
                service = AutomatedRestockService(storage)
                
                try:
                    log = service.run_full_restock_workflow(coverage, log=log)
                    
                    messages.success(
                        request, 
                        f"Restock completed successfully! "
                        f"{log.products_ordered} products ordered, "
                        f"{log.total_packages} total packages."
                    )
                    
                    return redirect('restock-log-detail', pk=log.id)
                    
                finally:
                    service.close()
            
        except Exception as e:
            logger.exception("Error running restock check")
            
            if is_ajax:
                return JsonResponse({'error': str(e)}, status=500)
            
            messages.error(
                request, 
                f"Error: {str(e)}. Check the log for details and retry options."
            )
            
            # Try to find the failed log to redirect to it
            failed_log = RestockLog.objects.filter(
                storage=storage,
                status='failed'
            ).order_by('-started_at').first()
            
            if failed_log:
                return redirect('restock-log-detail', pk=failed_log.id)
            
            return redirect('storage-detail', pk=storage_id)
    
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
        
        # Get orders from JSON
        orders = results.get('orders', [])
        
        # NEW: Get skipped products from results
        skipped_products = results.get('skipped_products', [])
        
        # Enrich orders with product details from database
        enriched_orders = []
        service = RestockService(self.object.storage)
        
        try:
            # Group by cluster for organization
            clusters = {}
            
            for order in orders:
                cod = order['cod']
                var = order['var']
                qty = order['qty']
                
                # Get product details from database
                try:
                    cur = service.db.conn.cursor()
                    cur.execute("""
                        SELECT 
                            p.descrizione,
                            p.cluster,
                            p.pz_x_collo,
                            p.rapp,
                            CASE
                                WHEN e.sale_start IS NOT NULL
                                AND e.sale_end IS NOT NULL
                                AND DATE('now') BETWEEN e.sale_start AND e.sale_end
                                THEN e.cost_s
                                ELSE e.cost_std
                            END AS cost
                        FROM products p
                        LEFT JOIN economics e 
                            ON p.cod = e.cod AND p.v = e.v
                        WHERE p.cod = ? AND p.v = ?;
                    """, (cod, var))
                    
                    row = cur.fetchone()
                    
                    if row:
                        descrizione = row[0]
                        cluster = row[1] or 'Uncategorized'
                        package_size = row[2] or 0
                        rapp = row[3] or 1
                        cost = row[4] or 0
                        cost = cost/rapp
                    else:
                        descrizione = f"Product {cod}.{var}"
                        cluster = 'Uncategorized'
                        package_size = 0
                        rapp = 1
                        cost = cost/rapp
                    
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
                    
                    # Group by cluster
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
            
             # NEW: Enrich skipped products with details
            enriched_skipped = []
            for skipped in skipped_products:
                cod = skipped.get('cod')
                var = skipped.get('var')
                reason = skipped.get('reason', 'Unknown')
                
                try:
                    cur = service.db.conn.cursor()
                    cur.execute("""
                        SELECT p.descrizione, ps.stock, p.disponibilita
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.cod = ? AND p.v = ?
                    """, (cod, var))
                    
                    row = cur.fetchone()
                    
                    if row:
                        enriched_skipped.append({
                            'cod': cod,
                            'var': var,
                            'name': row[0] or f"Product {cod}.{var}",
                            'stock': row[1] or 0,
                            'disponibilita': row[2] or 'Unknown',
                            'reason': reason
                        })
                    else:
                        # Product not in database
                        enriched_skipped.append({
                            'cod': cod,
                            'var': var,
                            'name': f"Product {cod}.{var}",
                            'stock': 0,
                            'disponibilita': 'Unknown',
                            'reason': reason
                        })
                except Exception as e:
                    logger.warning(f"Could not enrich skipped product {cod}.{var}: {e}")
                    # Add anyway with minimal info
                    enriched_skipped.append({
                        'cod': cod,
                        'var': var,
                        'name': f"Product {cod}.{var}",
                        'stock': 0,
                        'disponibilita': 'Unknown',
                        'reason': reason
                    })
            
            logger.info(f"Enriched {len(enriched_skipped)} skipped products for display")
            
            # Calculate summary
            summary = {
                'total_items': len(enriched_orders),
                'total_packages': sum(o['qty'] for o in enriched_orders),
                'total_clusters': len(clusters),
                'total_cost': sum(o['total_cost'] for o in enriched_orders),
                'total_skipped': len(enriched_skipped)
            }
            
            context['enriched_orders'] = enriched_orders
            context['clusters'] = clusters
            context['summary'] = summary
            context['results'] = results
            context['skipped_products'] = enriched_skipped  # Make sure this is set!
            
            logger.info(f"Context prepared: {len(enriched_orders)} orders, {len(enriched_skipped)} skipped")
            
        finally:
            service.close()
        
        return context

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
def verify_stock_view(request, storage_id):
    """Verify stock from inventory CSV file"""
    storage = get_object_or_404(
        Storage, 
        id=storage_id, 
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        if 'csv_file' not in request.FILES:
            messages.error(request, "No file uploaded")
            return redirect('storage-detail', pk=storage_id)
        
        csv_file = request.FILES['csv_file']
        
        # Validate file extension
        if not csv_file.name.endswith('.csv'):
            messages.error(request, "File must be .csv format")
            return redirect('storage-detail', pk=storage_id)
        
        try:
            # Save file to INVENTORY_FOLDER
            inventory_folder = Path(settings.INVENTORY_FOLDER)
            file_path = inventory_folder / csv_file.name
            
            with open(file_path, 'wb+') as destination:
                for chunk in csv_file.chunks():
                    destination.write(chunk)
            
            # IMPORTANT: Update stats and record losses BEFORE verification
            
            service = AutomatedRestockService(storage)
            
            try:
                logger.info(f"Updating product stats for {storage.name}...")
                service.update_product_stats()
                
                logger.info(f"Recording losses for {storage.name}...")
                service.record_losses()
                
                # Track changes for report
                verification_report = {
                    'total_products': 0,
                    'products_verified': 0,
                    'stock_changes': [],
                    'total_stock_before': 0,
                    'total_stock_after': 0,
                    'verified_at': timezone.now()
                }
                
                # Read CSV and get product list before verification

                df = pd.read_csv(file_path)
                
                # Clean column names
                COD_COL = "Codice"
                V_COL = "Variante"
                STOCK_COL = "Qta Originale"
                
                # Get stock levels BEFORE verification
                stock_before = {}
                for _, row in df.iterrows():
                    try:
                        cod_str = str(row[COD_COL]).replace('.', '').replace(',', '.').split('.')[0]
                        v_str = str(row[V_COL]).replace('.', '').replace(',', '.').split('.')[0]
                        cod = int(cod_str)
                        v = int(v_str)
                        
                        try:
                            stock_before[(cod, v)] = service.db.get_stock(cod, v)
                        except:
                            stock_before[(cod, v)] = None
                    except:
                        continue
                
                # Now verify stock
                logger.info(f"Verifying stock from CSV for {storage.name}...")
                from .scripts.inventory_reader import verify_stocks_from_excel
                verify_stocks_from_excel(service.db)
                
                # Get stock levels AFTER verification and calculate changes
                for _, row in df.iterrows():
                    try:
                        cod_str = str(row[COD_COL]).replace('.', '').replace(',', '.').split('.')[0]
                        v_str = str(row[V_COL]).replace('.', '').replace(',', '.').split('.')[0]
                        stock_str = str(row[STOCK_COL]).replace('.', '').replace(',', '.').split('.')[0]
                        
                        cod = int(cod_str)
                        v = int(v_str)
                        new_stock = int(stock_str)
                        
                        old_stock = stock_before.get((cod, v))
                        
                        if old_stock is not None:
                            verification_report['total_products'] += 1
                            verification_report['total_stock_before'] += old_stock
                            verification_report['total_stock_after'] += new_stock
                            
                            if old_stock != new_stock:
                                verification_report['products_verified'] += 1
                                verification_report['stock_changes'].append({
                                    'cod': cod,
                                    'var': v,
                                    'old_stock': old_stock,
                                    'new_stock': new_stock,
                                    'difference': new_stock - old_stock
                                })
                    except:
                        continue
                
                # Store report in session for display
                request.session['verification_report'] = {
                    'total_products': verification_report['total_products'],
                    'products_verified': verification_report['products_verified'],
                    'stock_changes': verification_report['stock_changes'][:50],  # Limit to 50 for display
                    'total_stock_before': verification_report['total_stock_before'],
                    'total_stock_after': verification_report['total_stock_after'],
                    'verified_at': verification_report['verified_at'].isoformat(),
                    'storage_name': storage.name
                }
                
                messages.success(
                    request, 
                    f"Stock verification completed for {storage.name}! "
                    f"{verification_report['products_verified']} products updated out of {verification_report['total_products']} verified."
                )
                
                return redirect('verification-report', storage_id=storage_id)
                
            finally:
                service.close()
            
        except Exception as e:
            logger.exception("Error verifying stock")
            messages.error(request, f"Error verifying stock: {str(e)}")
            return redirect('storage-detail', pk=storage_id)
    
    return render(request, 'storages/verify_stock.html', {'storage': storage})

@login_required
def verify_single_product_view(request, storage_id):
    """Verify a single product's stock"""
    storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)
    
    if request.method == 'POST':
        form = SingleProductVerificationForm(request.POST)
        
        if form.is_valid():
            cod = form.cleaned_data['product_code']
            var = form.cleaned_data['product_var']
            new_stock = form.cleaned_data['stock']
            cluster = form.cleaned_data.get('cluster') or None
            
            try:
                service = RestockService(storage)
                service.db.verify_stock(cod, var, new_stock, cluster)
                service.close()
                
                messages.success(request, f"Product {cod}.{var} verified! Stock set to {new_stock}")
                
                if 'verify_another' in request.POST:
                    return redirect('verify-single-product', storage_id=storage_id)
                else:
                    return redirect('storage-detail', pk=storage_id)
                    
            except ValueError as e:
                messages.error(request, f"Product not found: {str(e)}")
            except Exception as e:
                logger.exception("Error verifying product")
                messages.error(request, f"Error: {str(e)}")
    else:
        form = SingleProductVerificationForm()
    
    return render(request, 'storages/verify_single_product.html', {
        'storage': storage,
        'form': form
    })


@login_required
def verification_report_view(request, storage_id):
    """Display verification report"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    report = request.session.get('verification_report')
    
    if not report:
        messages.warning(request, "No verification report available")
        return redirect('storage-detail', pk=storage_id)
    
    # Calculate statistics
    if report['stock_changes']:
        total_difference = sum(change['difference'] for change in report['stock_changes'])
        avg_difference = total_difference / len(report['stock_changes'])
        
        increases = [c for c in report['stock_changes'] if c['difference'] > 0]
        decreases = [c for c in report['stock_changes'] if c['difference'] < 0]
        
        report['total_difference'] = total_difference
        report['avg_difference'] = avg_difference
        report['increases_count'] = len(increases)
        report['decreases_count'] = len(decreases)
        report['total_increase'] = sum(c['difference'] for c in increases)
        report['total_decrease'] = sum(c['difference'] for c in decreases)
    
    context = {
        'storage': storage,
        'report': report
    }
    
    return render(request, 'storages/verification_report.html', context)


@login_required
def configure_list_updates_view(request, storage_id):
    """Configure automatic list updates for a storage"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    # Get or create schedule
    try:
        schedule = storage.list_update_schedule
    except ListUpdateSchedule.DoesNotExist:
        schedule = None
    
    if request.method == 'POST':
        if schedule:
            form = ListUpdateScheduleForm(request.POST, instance=schedule)
        else:
            form = ListUpdateScheduleForm(request.POST)
        
        if form.is_valid():
            schedule = form.save(commit=False)
            schedule.storage = storage
            schedule.save()
            
            messages.success(request, "List update schedule configured successfully!")
            return redirect('storage-detail', pk=storage_id)
    else:
        if schedule:
            form = ListUpdateScheduleForm(instance=schedule)
        else:
            form = ListUpdateScheduleForm()
    
    return render(request, 'storages/configure_list_updates.html', {
        'storage': storage,
        'form': form,
        'schedule': schedule
    })


@login_required
def manual_list_update_view(request, storage_id):
    """Manually trigger product list download and import"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        try:
            from .list_update_service import ListUpdateService
            
            service = ListUpdateService(storage)
            result = service.update_and_import()
            service.close()
            
            if result['success']:
                messages.success(request, result['message'])
            else:
                messages.error(request, result['message'])
                
        except Exception as e:
            logger.exception("Error in manual list update")
            messages.error(request, f"Error: {str(e)}")
        
        return redirect('storage-detail', pk=storage_id)
    
    return render(request, 'storages/manual_list_update.html', {
        'storage': storage
    })


@login_required
def upload_promos_view(request, supermarket_id):
    """
    Upload and process promo PDF file - SUPERMARKET LEVEL
    FIXED: Now passes credentials to Scrapper
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
                
                # Get database connection
                first_storage = supermarket.storages.first()
                
                if not first_storage:
                    messages.error(request, "No storages found for this supermarket. Sync storages first.")
                    return redirect('supermarket-detail', pk=supermarket_id)
                
                service = RestockService(first_storage)
                
                
                
                scrapper = Scrapper(
                    username=supermarket.username,
                    password=supermarket.password,
                    helper=service.helper,
                    db=service.db
                )
                
                # Parse and update promos
                promo_list = scrapper.parse_promo_pdf(str(file_path))
                
                service.db.update_promos(promo_list)
                service.close()
                
                # Clean up
                os.remove(file_path)
                
                messages.success(
                    request,
                    f"Successfully processed {len(promo_list)} promo items for {supermarket.name}!"
                )
                
            except Exception as e:
                logger.exception("Error processing promos")
                messages.error(request, f"Error processing promos: {str(e)}")
            
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


@login_required
def bulk_adjust_stock_view(request, storage_id):
    """Bulk adjust stock via CSV upload"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = BulkStockAdjustmentForm(request.POST, request.FILES)
        
        if form.is_valid():
            csv_file = request.FILES['csv_file']
            default_reason = form.cleaned_data['reason']
            
            try:                
                # Read CSV
                decoded_file = csv_file.read().decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(decoded_file))
                
                service = RestockService(storage)
                
                adjustments_made = []
                errors = []
                
                for row_num, row in enumerate(csv_reader, start=2):
                    try:
                        # Parse row
                        product_code = int(row.get('Product Code', row.get('product_code', 0)))
                        product_var = int(row.get('Variant', row.get('variant', row.get('Product Variant', 1))))
                        adjustment = int(row.get('Adjustment', row.get('adjustment', 0)))
                        
                        if adjustment == 0:
                            continue
                        
                        # Get current stock
                        try:
                            current_stock = service.db.get_stock(product_code, product_var)
                        except ValueError:
                            errors.append(f"Row {row_num}: Product {product_code}.{product_var} not found")
                            continue
                        
                        # Apply adjustment
                        service.db.adjust_stock(product_code, product_var, adjustment)
                        new_stock = service.db.get_stock(product_code, product_var)
                        
                        adjustments_made.append({
                            'code': product_code,
                            'var': product_var,
                            'old': current_stock,
                            'new': new_stock,
                            'adjustment': adjustment
                        })
                        
                    except (KeyError, ValueError) as e:
                        errors.append(f"Row {row_num}: Invalid data - {str(e)}")
                        continue
                
                service.close()
                
                # Show results
                if adjustments_made:
                    messages.success(
                        request,
                        f"Successfully adjusted {len(adjustments_made)} products!"
                    )
                
                if errors:
                    messages.warning(
                        request,
                        f"Encountered {len(errors)} errors. Check logs for details."
                    )
                    for error in errors[:5]:  # Show first 5 errors
                        messages.error(request, error)
                
                return redirect('storage-detail', pk=storage_id)
                
            except Exception as e:
                logger.exception("Error in bulk stock adjustment")
                messages.error(request, f"Error processing CSV: {str(e)}")
    else:
        form = BulkStockAdjustmentForm()
    
    return render(request, 'storages/bulk_adjust_stock.html', {
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
        cursor = service.db.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT cluster 
            FROM products 
            WHERE cluster IS NOT NULL AND cluster != '' AND settore = ? 
            ORDER BY cluster ASC
        """, (settore,))
        clusters = [row[0] for row in cursor.fetchall()]
        service.close()
    
    # Calculate values
    category_totals = {}
    total_value = 0
    
    for storage in storages:
        try:
            service = RestockService(storage)
            settore = storage.settore
            cursor = service.db.conn.cursor()
            
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
                query += " AND p.cluster = ?"
                params.append(cluster)
            query += " AND p.settore = ?"
            params.append(settore)
            query += " GROUP BY e.category"
            
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                category_name = row[0]
                value = row[1] or 0
                
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
def record_losses_view(request, supermarket_id):
    """Manually record losses for ALL storages in supermarket"""
    supermarket = get_object_or_404(
        Supermarket,
        id=supermarket_id,
        owner=request.user
    )
    
    if request.method == 'POST':
        form = RecordLossesForm(request.POST, request.FILES)
        
        if form.is_valid():
            loss_type = form.cleaned_data['loss_type']
            csv_file = request.FILES['csv_file']
            
            # Map to filename
            filename_mapping = {
                'broken': 'ROTTURE.csv',
                'expired': 'SCADUTO.csv',
                'internal': 'UTILIZZO INTERNO.csv'
            }
            expected_filename = filename_mapping[loss_type]
            
            try:
                # Save file to LOSSES_FOLDER
                losses_folder = Path(settings.LOSSES_FOLDER)
                losses_folder.mkdir(exist_ok=True)
                
                file_path = losses_folder / expected_filename
                
                if file_path.exists():
                    file_path.unlink()
                
                with open(file_path, 'wb+') as destination:
                    for chunk in csv_file.chunks():
                        destination.write(chunk)
                
                # Process for FIRST storage (they share same DB)
                first_storage = supermarket.storages.first()
                
                if not first_storage:
                    messages.error(request, "No storages found for this supermarket")
                    return redirect('supermarket-detail', pk=supermarket_id)
                
                service = RestockService(first_storage)
                
                try:
                    from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
                    
                    # This processes ALL storages' data from the CSV
                    verify_lost_stock_from_excel_combined(service.db)
                    
                    messages.success(
                        request,
                        f"Successfully recorded {loss_type} losses for {supermarket.name}! "
                        f"All storages have been updated."
                    )
                    
                except Exception as e:
                    logger.exception("Error processing loss file")
                    messages.error(request, f"Error processing losses: {str(e)}")
                finally:
                    service.close()
                
                return redirect('supermarket-detail', pk=supermarket_id)
                
            except Exception as e:
                logger.exception("Error saving loss file")
                messages.error(request, f"Error saving file: {str(e)}")
    else:
        form = RecordLossesForm()
    
    return render(request, 'supermarkets/record_losses.html', {
        'supermarket': supermarket,
        'form': form
    })


@login_required
def losses_analytics_unified_view(request):
    """Unified loss analytics with flexible filtering - FIXED"""
    
    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters
    supermarket_id = request.GET.get('supermarket_id')
    storage_id = request.GET.get('storage_id')
    period = request.GET.get('period', '3')
    
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
    
    # FIX: Use a single database connection per supermarket to avoid duplicates
    # Group storages by supermarket
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
    
    # Aggregate loss statistics
    stats = {
        'broken': {'total': 0, 'products': 0, 'monthly': [0]*24},
        'expired': {'total': 0, 'products': 0, 'monthly': [0]*24},
        'internal': {'total': 0, 'products': 0, 'monthly': [0]*24},
    }
    
    top_products_dict = {}
    
    # Process each supermarket's database ONCE
    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            # Use first storage to get DB connection (they share same DB per supermarket)
            first_storage = sm_data['storages'][0]
            service = RestockService(first_storage)
            cursor = service.db.conn.cursor()
            
            # Build WHERE clause based on selected storage
            if storage_id:
                # Specific storage selected - filter by settore
                settore_filter = f"WHERE p.settore = '{first_storage.settore}'"
            elif len(sm_data['settores']) < len(sm_data['supermarket'].storages.all()):
                # Multiple but not all storages selected - filter by settores
                settores_list = "', '".join(sm_data['settores'])
                settore_filter = f"WHERE p.settore IN ('{settores_list}')"
            else:
                # All storages - no filter
                settore_filter = ""
            
            query = f"""
                SELECT 
                    el.cod, el.v,
                    el.broken, el.expired, el.internal,
                    p.descrizione
                FROM extra_losses el
                LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                {settore_filter}
            """
            
            cursor.execute(query)
            
            loss_types = ['broken', 'expired', 'internal']
            
            for row in cursor.fetchall():
                cod = row[0]
                v = row[1]
                description = row[5] or f"Product {cod}.{v}"
                
                product_key = (cod, v, description)
                if product_key not in top_products_dict:
                    top_products_dict[product_key] = 0
                
                for i, loss_type in enumerate(loss_types):
                    loss_json = row[2 + i]
                    
                    if loss_json:
                        try:
                            loss_array = json.loads(loss_json)
                            
                            # Calculate for period
                            months_to_include = min(period_months, len(loss_array))
                            period_losses = sum(loss_array[:months_to_include])
                            
                            if period_losses > 0:
                                stats[loss_type]['total'] += period_losses
                                stats[loss_type]['products'] += 1
                                
                                # Aggregate monthly data
                                for idx, val in enumerate(loss_array[:24]):
                                    stats[loss_type]['monthly'][idx] += val
                                
                                # Add to top products
                                top_products_dict[product_key] += period_losses
                        except:
                            continue
            
            service.close()
        except Exception as e:
            logger.exception(f"Error processing losses for supermarket {sm_id}")
            continue
    
    # Convert top products dict to list
    top_products = [
        {
            'cod': cod,
            'var': v,
            'description': desc,
            'total_losses': total
        }
        for (cod, v, desc), total in top_products_dict.items()
    ]
    
    top_products.sort(key=lambda x: x['total_losses'], reverse=True)
    top_products = top_products[:20]
    
    # Calculate total
    total_losses = sum(s['total'] for s in stats.values())
    
    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'scope_description': scope_description,
        'stats': stats,
        'total_losses': total_losses,
        'top_products': top_products,
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
    View to add products by code
    FIXED: Now passes credentials to Scrapper
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
            
            try:
                
                from .scripts.helpers import Helper                
                service = RestockService(storage)
                helper = Helper()
                
                scrapper = Scrapper(
                    username=storage.supermarket.username,
                    password=storage.supermarket.password,
                    helper=helper,
                    db=service.db
                )
                
                try:
                    scrapper.navigate()
                    scrapper.init_products_and_stats_from_list(products_list, settore)
                    
                    messages.success(
                        request,
                        f"Successfully registered {len(products_list)} product(s) in settore {settore}"
                    )
                    
                    return redirect('storage-detail', pk=storage_id)
                    
                finally:
                    scrapper.driver.quit()
                    service.close()
                    
            except Exception as e:
                logger.exception("Error adding products")
                messages.error(request, f"Error adding products: {str(e)}")
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
    """AJAX endpoint to check restock progress - ENHANCED with detailed stage info"""
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