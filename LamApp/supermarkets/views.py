# LamApp/supermarkets/views.py
from django.utils import timezone
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse, reverse_lazy
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
    Supermarket, Storage, RestockSchedule, ScheduleException,
    Blacklist, BlacklistEntry, RestockLog,
    Recipe, RecipeProductItem, RecipeExternalItem, RecipeCostAlert,
    StockValueSnapshot
)
from .forms import (
    RestockScheduleForm, BlacklistForm, PurgeProductsForm, InventorySearchForm,
    BlacklistEntryForm, AddProductsForm, PromoUploadForm,
    RecordLossesForm, DDTUploadForm, DayWeightsForm,
)

from .services import RestockService
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
    supermarkets = Supermarket.objects.filter(owner=request.user).prefetch_related(
        'storages__schedule',
        'storages__restock_logs',
        'storages__blacklists'
    )
    
    # ✅ FIXED: Only show KEY operations (full_restock and order_execution)
    recent_logs = RestockLog.objects.filter(
        storage__supermarket__owner=request.user,
        operation_type__in=['full_restock', 'order_execution']  # ← FILTER KEY OPERATIONS ONLY
    ).select_related('storage', 'storage__supermarket').order_by('storage__name', '-started_at')

    # Group recent logs by storage, limit 5 per storage
    from collections import OrderedDict
    recent_logs_by_storage = OrderedDict()
    for log in recent_logs:
        storage_key = (log.storage.id, log.storage.name, log.storage.supermarket.name)
        if storage_key not in recent_logs_by_storage:
            recent_logs_by_storage[storage_key] = []
        # Limit to 5 most recent operations per storage
        if len(recent_logs_by_storage[storage_key]) < 5:
            recent_logs_by_storage[storage_key].append(log)
    
    # Get failed logs that need attention (last 24h, not dismissed)
    from datetime import timedelta
    last_24h = timezone.now() - timedelta(hours=24)
    failed_logs = RestockLog.objects.filter(
        storage__supermarket__owner=request.user,
        status='failed',
        started_at__gte=last_24h,
        is_dismissed=False
    ).select_related('storage', 'storage__supermarket').order_by('-started_at')[:5]
    
    # Pending verifications count (efficient)
    pending_verifications = 0
    
    # Group storages by supermarket to minimize DB connections
    supermarkets_with_storages = {}
    for sm in supermarkets:
        if sm.storages.exists():
            supermarkets_with_storages[sm.id] = sm
    
    # Process each supermarket's database once
    top_pending_products = []
    for sm_id, sm in list(supermarkets_with_storages.items())[:3]:  # Limit to 3 supermarkets
        try:
            storage = sm.storages.first()
            with RestockService(storage) as service:
                cursor = service.db.cursor()
                settores = list(sm.storages.values_list('settore', flat=True).distinct())
                
                if not settores:
                    continue
                
                settore_placeholders = ','.join(['%s'] * len(settores))
                
                query = f"""
                    SELECT 
                        p.cod, p.v, p.descrizione,
                        ps.stock
                    FROM product_stats ps
                    JOIN products p ON ps.cod = p.cod AND ps.v = p.v
                    WHERE ps.verified = FALSE
                    AND ps.bought_last_24 IS NOT NULL
                    AND jsonb_typeof(ps.bought_last_24) = 'array'
                    AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(ps.bought_last_24)
                    )
                    AND p.settore IN ({settore_placeholders})
                    LIMIT 5
                """
                
                cursor.execute(query, settores)
                
                for row in cursor.fetchall():
                    top_pending_products.append({
                        'supermarket': sm.name,
                        'cod': row['cod'],
                        'var': row['v'],
                        'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'stock': row['stock'] or 0
                    })
                    
                    if len(top_pending_products) >= 5:
                        break
                
                if len(top_pending_products) >= 5:
                    break
        except Exception as e:
            logger.warning(f"Could not load sample verifications for {sm.name}: {e}")
            continue

    logger.info(f"Dashboard: {pending_verifications} total pending verifications across {len(supermarkets_with_storages)} supermarkets")

    # Get notification counts for each storage
    storage_notifications = {}
    for sm in supermarkets:
        for storage in sm.storages.all():
            try:
                with RestockService(storage) as service:
                    cursor = service.db.cursor()

                    # Count negative stock products
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND ps.verified = TRUE
                            AND ps.stock < 0
                    """, (storage.settore,))
                    negative_count = cursor.fetchone()['cnt']

                    # Count unverified products with sales
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND p.purge_flag = FALSE
                            AND ps.verified = FALSE
                            AND ps.sales_sets IS NOT NULL
                            AND jsonb_typeof(ps.sales_sets) = 'array'
                            AND EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements(
                                    jsonb_path_query_array(ps.sales_sets, '$[0 to 6]')
                                ) AS elem
                                WHERE (elem::text)::int > 0
                            )
                    """, (storage.settore,))
                    unverified_sales_count = cursor.fetchone()['cnt']

                    # Count newly added products (bought but not sold)
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND p.purge_flag = FALSE
                            AND ps.verified = FALSE
                            AND ps.sold_last_24 IS NULL
                            AND ps.bought_last_24 IS NOT NULL
                            AND jsonb_typeof(ps.bought_last_24) = 'array'
                            AND EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements(ps.bought_last_24)
                            )
                    """, (storage.settore,))
                    newly_added_count = cursor.fetchone()['cnt']

                    # Count out of stock products
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND p.purge_flag = FALSE
                            AND ps.verified = TRUE
                            AND ps.stock = 0
                            AND p.disponibilita = 'Si'
                    """, (storage.settore,))
                    out_of_stock_count = cursor.fetchone()['cnt']

                    storage_notifications[storage.id] = {
                        'negative': negative_count,
                        'unverified_sales': unverified_sales_count,
                        'newly_added': newly_added_count,
                        'out_of_stock': out_of_stock_count,
                    }
            except Exception as e:
                logger.warning(f"Could not load notifications for storage {storage.name}: {e}")
                storage_notifications[storage.id] = {
                    'negative': 0,
                    'unverified_sales': 0,
                    'newly_added': 0,
                    'out_of_stock': 0,
                }

    # Get unread recipe cost alerts for this user's supermarkets
    recipe_cost_alerts = RecipeCostAlert.objects.filter(
        recipe__supermarket__owner=request.user,
        is_read=False
    ).select_related('recipe', 'recipe__supermarket').order_by('-created_at')[:10]

    unread_alerts_count = RecipeCostAlert.objects.filter(
        recipe__supermarket__owner=request.user,
        is_read=False
    ).count()

    context = {
        'supermarkets': supermarkets,
        'recent_logs': recent_logs,  # ← NOW ONLY ORDERS
        'recent_logs_by_storage': recent_logs_by_storage,  # ← GROUPED BY STORAGE
        'failed_logs': failed_logs,
        'pending_verifications': pending_verifications,
        'top_pending_products': top_pending_products,
        'total_supermarkets': supermarkets.count(),
        'total_storages': sum(s.storages.count() for s in supermarkets),
        'active_schedules': RestockSchedule.objects.filter(
            storage__supermarket__owner=request.user
        ).count(),
        'recipe_cost_alerts': recipe_cost_alerts,
        'unread_alerts_count': unread_alerts_count,
        'storage_notifications': storage_notifications,
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
        return Supermarket.objects.filter(owner=self.request.user).prefetch_related(
            'storages',
            'storages__schedule',
            'storages__restock_logs'
        ).order_by('name')

class SupermarketDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Supermarket
    template_name = 'supermarkets/detail.html'
    context_object_name = 'supermarket'

    def test_func(self):
        return self.get_object().owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['storages'] = self.object.storages.select_related(
            'schedule'  # ForeignKey/OneToOne - use select_related
        ).prefetch_related(
            'blacklists',           # Reverse FK
            'restock_logs',         # Reverse FK
            'blacklists__entries'   # Nested prefetch
        ).order_by('name')
        
        return context


class SupermarketCreateView(LoginRequiredMixin, CreateView):
    model = Supermarket
    fields = ['name', 'username', 'password']
    template_name = 'supermarkets/form.html'

    def form_valid(self, form):
        form.instance.owner = self.request.user
        response = super().form_valid(form)

        # Store client parameters from PAC2000A
        client_json = self.request.POST.get('client_data', '')
        if client_json:
            try:
                client_data = json.loads(client_json)
                self.object.id_cliente = client_data.get('id_cliente')
                self.object.id_azienda = client_data.get('id_azienda')
                self.object.id_marchio = client_data.get('id_marchio')
                self.object.id_clienti_canale = client_data.get('id_clienti_canale')
                self.object.id_clienti_area = client_data.get('id_clienti_area')
                self.object.save(update_fields=[
                    'id_cliente', 'id_azienda', 'id_marchio',
                    'id_clienti_canale', 'id_clienti_area'
                ])
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Could not parse client data: {e}")

        # Create storages from pre-discovered data (submitted as JSON hidden field)
        storages_json = self.request.POST.get('discovered_storages', '')
        if storages_json:
            try:
                storages_data = json.loads(storages_json)
                for s in storages_data:
                    Storage.objects.get_or_create(
                        supermarket=self.object,
                        name=s['name'],
                        defaults={
                            'settore': s.get('settore', ''),
                            'id_cod_mag': s.get('id_cod_mag'),
                        }
                    )
                messages.success(
                    self.request,
                    f"Punto vendita '{self.object.name}' creato con {len(storages_data)} magazzini!"
                )
            except (json.JSONDecodeError, KeyError) as e:
                logger.exception("Error parsing discovered storages")
                messages.warning(
                    self.request,
                    f"Punto vendita creato, ma errore nel salvataggio dei magazzini: {str(e)}"
                )
        else:
            messages.success(
                self.request,
                f"Punto vendita '{self.object.name}' creato. Nessun magazzino sincronizzato."
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

    def post(self, request, *args, **kwargs):
        if 'sync_storages' in request.POST:
            supermarket = self.get_object()
            from .tasks import sync_storages_task
            result = sync_storages_task.apply_async(args=[supermarket.pk])
            return redirect('task-progress', task_id=result.id)
        return super().post(request, *args, **kwargs)

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
def discover_storages_ajax(request):
    """
    AJAX endpoint to discover storages and client parameters from PAC2000A.
    Used by the supermarket creation form to preview storages before saving.
    Two-phase: Finder discovers storages, then WebLister gathers client params.
    """
    import re
    from .scripts.finder import Finder
    from .scripts.web_lister import WebLister
    import tempfile

    username = request.POST.get('username', '').strip()
    password = request.POST.get('password', '').strip()

    if not username or not password:
        return JsonResponse({'error': 'Username e password sono obbligatori.'}, status=400)

    try:
        # Phase 1: Discover storages via Finder
        finder = Finder(username=username, password=password)
        try:
            finder.login()
            storage_tuples = finder.find_storages()
        finally:
            finder.driver.quit()

        results = []
        for name, id_cod_mag in storage_tuples:
            settore = re.sub(r'^[^ ]+\s*-?\s*', '', name)
            results.append({
                'name': name,
                'settore': settore,
                'id_cod_mag': id_cod_mag,
            })

        # Phase 2: Gather client parameters via WebLister
        client_data = {}
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                lister = WebLister(
                    username=username,
                    password=password,
                    storage_name=results[0]['name'] if results else '',
                    download_dir=tmp_dir,
                    headless=True
                )
                try:
                    lister.login()
                    client_data = lister.gather_client_data()
                finally:
                    lister.driver.quit()
        except Exception as e:
            logger.warning(f"Could not gather client data: {e}. "
                           "Client parameters will need to be set manually or via re-sync.")

        return JsonResponse({
            'storages': results,
            'client_data': client_data,
        })

    except Exception as e:
        logger.exception("Error discovering storages")
        return JsonResponse({'error': f'Errore durante la sincronizzazione: {str(e)}'}, status=500)


# ============ Storage Views ============

class StorageDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Storage
    template_name = 'storages/detail.html'
    context_object_name = 'storage'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['recent_logs'] = self.object.restock_logs.select_related(
            'storage__supermarket'
        ).order_by('-started_at')[:20]
        
        context['blacklists'] = self.object.blacklists.prefetch_related(
            'entries'
        ).order_by('name')
        
        try:
            context['schedule'] = self.object.schedule
        except RestockSchedule.DoesNotExist:
            context['schedule'] = None
        
        # ✅ FIXED: Load newly added products with type checking
        from .services import RestockService
        
        try:
            with RestockService(self.object) as service:
                cursor = service.db.cursor()
                
                # ✅ FIXED: Add jsonb_typeof check to prevent "non-array" error
                cursor.execute("""
                    SELECT 
                        p.cod, p.v, p.descrizione, p.pz_x_collo,
                        ps.verified, ps.bought_last_24, ps.sold_last_24
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                        AND p.purge_flag = FALSE
                        AND ps.verified = FALSE
                        AND ps.sold_last_24 IS NULL
                        AND ps.bought_last_24 IS NOT NULL
                        AND jsonb_typeof(ps.bought_last_24) = 'array'
                        AND EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(ps.bought_last_24)
                        )
                    LIMIT 20;
                """, (self.object.settore,))
                
                newly_added = []
                for row in cursor.fetchall():
                    bought = row['bought_last_24'] or []
                    sold = row['sold_last_24'] or []
                    
                    # Check if bought but not sold
                    if bought and (not sold or all(s == 0 for s in sold)):
                        newly_added.append({
                            'cod': row['cod'],
                            'var': row['v'],
                            'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                            'package_size': row['pz_x_collo'] or 12
                        })
                
                context['newly_added_products'] = newly_added
                logger.info(f"Found {len(newly_added)} newly added products needing verification")

                # NEW: Load products with negative stock (anomalies)
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

                # NEW: Load unverified products with recent sales
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.pz_x_collo, ps.stock, ps.sales_sets
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                        AND p.purge_flag = FALSE
                        AND ps.verified = FALSE
                        AND ps.sales_sets IS NOT NULL
                        AND jsonb_typeof(ps.sales_sets) = 'array'
                        AND EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(
                                jsonb_path_query_array(ps.sales_sets, '$[0 to 6]')
                            ) AS elem
                            WHERE (elem::text)::int > 0
                        )
                    ORDER BY p.descrizione
                    LIMIT 50;
                """, (self.object.settore,))

                unverified_sales_products = []
                for row in cursor.fetchall():
                    unverified_sales_products.append({
                        'cod': row['cod'],
                        'var': row['v'],
                        'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'stock': row['stock'],
                        'package_size': row['pz_x_collo'] or 12
                    })

                context['unverified_sales_products'] = unverified_sales_products
                logger.info(f"Found {len(unverified_sales_products)} unverified products with recent sales")

                # Load out of stock products (verified, stock=0, disponibilita='Si')
                cursor.execute("""
                    SELECT
                        p.cod, p.v, p.descrizione, p.pz_x_collo, ps.stock
                    FROM products p
                    JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                        AND p.purge_flag = FALSE
                        AND ps.verified = TRUE
                        AND ps.stock = 0
                        AND p.disponibilita = 'Si'
                    ORDER BY p.descrizione
                    LIMIT 50;
                """, (self.object.settore,))

                out_of_stock_products = []
                for row in cursor.fetchall():
                    out_of_stock_products.append({
                        'cod': row['cod'],
                        'var': row['v'],
                        'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                        'stock': row['stock'],
                        'package_size': row['pz_x_collo'] or 12
                    })

                context['out_of_stock_products'] = out_of_stock_products
                logger.info(f"Found {len(out_of_stock_products)} out of stock products")

        except Exception as e:
            logger.exception("Error loading product anomalies")
            context['newly_added_products'] = []
            context['negative_stock_products'] = []
            context['unverified_sales_products'] = []
            context['out_of_stock_products'] = []

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
        ).select_related(
            'supermarket',  # ForeignKey
            'schedule'      # OneToOne
        ).order_by('supermarket__name', 'name')


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
        storage = get_object_or_404(
            Storage,
            id=self.kwargs.get("storage_id"),
            supermarket__owner=self.request.user
        )
        context["storage"] = storage
        context["supermarket"] = storage.supermarket
        context["day_weights_form"] = DayWeightsForm(instance=storage.supermarket)
        context["day_weights_json"] = json.dumps(storage.supermarket.get_all_day_weights())
        return context

    def form_valid(self, form):
        # Also save supermarket-wide day weights from POST data
        supermarket = self.object.storage.supermarket
        valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        weights_changed = False

        for day in valid_days:
            weight_key = f'{day}_weight'
            if weight_key in self.request.POST:
                try:
                    weight = float(self.request.POST[weight_key])
                    weight = max(0.5, min(2.0, weight))
                    if getattr(supermarket, weight_key) != weight:
                        setattr(supermarket, weight_key, weight)
                        weights_changed = True
                except (ValueError, TypeError):
                    pass

        if weights_changed:
            supermarket.save()

        messages.success(self.request, "Agenda ordini aggiornata!")
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


# ============ Schedule Exceptions API ============

@login_required
def schedule_exceptions_api(request, storage_id):
    """API endpoint for managing schedule exceptions (holidays, custom dates)"""
    storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)
    schedule = get_object_or_404(RestockSchedule, storage=storage)

    if request.method == 'GET':
        # Return all exceptions for this schedule
        exceptions = ScheduleException.objects.filter(schedule=schedule)
        data = {
            'exceptions': [
                {
                    'date': exc.date.isoformat(),
                    'exception_type': exc.exception_type,
                    'delivery_offset': exc.delivery_offset,
                    'skip_sale': exc.skip_sale,
                    'note': exc.note
                }
                for exc in exceptions
            ]
        }
        return JsonResponse(data)

    elif request.method == 'POST':
        # Create or update an exception
        try:
            body = json.loads(request.body)
            date_str = body.get('date')
            exception_type = body.get('exception_type', 'skip')
            delivery_offset = body.get('delivery_offset')
            skip_sale = body.get('skip_sale', False)
            note = body.get('note', '')

            from datetime import datetime
            date = datetime.strptime(date_str, '%Y-%m-%d').date()

            exc, created = ScheduleException.objects.update_or_create(
                schedule=schedule,
                date=date,
                defaults={
                    'exception_type': exception_type,
                    'delivery_offset': delivery_offset if exception_type in ('add', 'modify') else None,
                    'skip_sale': skip_sale,
                    'note': note
                }
            )
            return JsonResponse({'success': True, 'created': created})
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    elif request.method == 'DELETE':
        # Delete an exception
        try:
            body = json.loads(request.body)
            date_str = body.get('date')

            from datetime import datetime
            date = datetime.strptime(date_str, '%Y-%m-%d').date()

            deleted, _ = ScheduleException.objects.filter(
                schedule=schedule,
                date=date
            ).delete()
            return JsonResponse({'success': True, 'deleted': deleted > 0})
        except (json.JSONDecodeError, ValueError) as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    return JsonResponse({'error': 'Method not allowed'}, status=405)


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
        from .tasks import run_restock_for_storage

        result = run_restock_for_storage.apply_async(
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
                with AutomatedRestockService(storage) as service:
                    try:
                        service.retry_from_checkpoint(log, coverage=coverage)
                    except Exception as e:
                        logger.exception(f"Error in background retry for log #{log_id}")           
            thread = threading.Thread(target=run_retry)
            thread.daemon = True
            thread.start()
            
            return JsonResponse({'success': True, 'log_id': log_id})
        else:
            # Synchronous retry
            with AutomatedRestockService(storage) as service:
                updated_log = service.retry_from_checkpoint(log, coverage=coverage)
                
                messages.success(
                    request, 
                    f"Retry successful! Operation completed from checkpoint: {log.get_current_stage_display()}"
                )
                
                return redirect('restock-log-detail', pk=updated_log.id)
    except Exception as e:
        logger.exception(f"Error retrying restock from checkpoint")
        
        if is_ajax:
            return JsonResponse({'success': False, 'message': str(e)}, status=500)
        
        messages.error(request, f"Retry failed: {str(e)}")
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
        context['summary'] = {
            'total_items': 0,
            'total_packages': self.object.total_packages or 0,
            'total_clusters': 0,
            'total_cost': 0,
            'total_new': 0,
            'total_skipped': 0,
            'total_zombie': 0,
            'total_order_skipped': 0,
        }
        
        # ✅ Operations WITHOUT orders (show simple info)
        if operation_type in ['stats_update', 'list_update', 'cluster_assignment', 'product_addition']:
            # These operations don't have order details
            # Just show the basic log info
            logger.info(f"Displaying {operation_type} log #{self.object.id} - no order enrichment needed")
            return context
        
        # ✅ Operations WITH orders/products (full enrichment)
        if operation_type in ['full_restock', 'order_execution', 'verification']:
            # Get all lists from results
            orders = results.get('orders', [])
            new_products = results.get('new_products', [])
            skipped_products = results.get('skipped_products', [])
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
                            CASE
                                WHEN e.sale_start IS NOT NULL
                                AND e.sale_end IS NOT NULL
                                AND CURRENT_DATE BETWEEN e.sale_start AND e.sale_end
                                THEN e.cost_s
                                ELSE e.cost_std
                            END AS cost
                        FROM products p
                        LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
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
                                'total_cost': cost * qty * package_size * rapp,
                                'discount': discount,
                                'on_sale': discount is not None
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
                    enriched_new = self._enrich_product_list(service, new_products, include_pricing=True)
                    enriched_skipped = self._enrich_product_list(service, skipped_products)
                    enriched_zombie = self._enrich_product_list(service, zombie_products)
                    enriched_order_skipped = self._enrich_product_list(service, order_skipped_products)
                    
                    sorted_clusters = dict(sorted(clusters.items(), key=lambda x: x[0]))
                    
                    # Calculate summary
                    summary = {
                        'total_items': len(enriched_orders),
                        'total_packages': sum(int(o.get('qty', 0) or 0) for o in enriched_orders),
                        'total_clusters': len(sorted_clusters),
                        'total_cost': sum(float(o.get('total_cost', 0) or 0) for o in enriched_orders),
                        'total_new': len(enriched_new),
                        'total_skipped': len(enriched_skipped),
                        'total_zombie': len(enriched_zombie),
                        'total_order_skipped': len(enriched_order_skipped),
                    }
                    
                    context['enriched_orders'] = enriched_orders
                    context['clusters'] = sorted_clusters
                    context['summary'] = summary
                    
                    # Add all lists to context
                    context['enriched_new'] = enriched_new
                    context['enriched_skipped'] = enriched_skipped
                    context['enriched_zombie'] = enriched_zombie
                    context['enriched_order_skipped'] = enriched_order_skipped
                    
                    logger.info(
                        f"Context prepared: {len(enriched_orders)} orders, "
                        f"{len(enriched_new)} new, {len(enriched_skipped)} skipped, "
                        f"{len(enriched_zombie)} zombie, {len(enriched_order_skipped)} order-skipped"
                    )
            except Exception as e:
                logger.exception(f"Error enriching orders for log #{self.object.id}")
                # Don't fail completely - just show what we have
                context['error_enriching'] = str(e)
                    
        return context
    
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
                            e.cost_std, e.price_std
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
                        
                        cost_std = row['cost_std'] or 0
                        price_std = row['price_std'] or 0
                        
                        # Calculate per-unit cost and price
                        package_cost = cost_std * package_size
                        # Calculate margin
                        margin_pct = 0
                        if price_std > 0 and cost_std > 0:
                            margin_pct = ((price_std - cost_std) / price_std) * 100
                        
                        product_data.update({
                            'package_size': package_size,
                            'unit_cost': cost_std,
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


# ============ Blacklist Views ============

class BlacklistListView(LoginRequiredMixin, ListView):
    model = Blacklist
    template_name = 'blacklists/list.html'
    context_object_name = 'blacklists'

    def get_queryset(self):
        return Blacklist.objects.filter(
            storage__supermarket__owner=self.request.user
        ).select_related(
            'storage',
            'storage__supermarket'
        ).prefetch_related(
            'entries'
        ).order_by('storage__name', 'name')

class BlacklistDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Blacklist
    template_name = 'blacklists/detail.html'
    context_object_name = 'blacklist'

    def test_func(self):
        return self.get_object().storage.supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        blacklist = self.object

        # Fetch product descriptions from storage database
        entries = list(blacklist.entries.all())
        if entries:
            try:
                from .services import RestockService
                with RestockService(blacklist.storage) as service:
                    cursor = service.db.cursor()
                    # Build query for all product codes
                    codes = [(e.product_code, e.product_var) for e in entries]
                    placeholders = ','.join(['(%s, %s)'] * len(codes))
                    params = [item for pair in codes for item in pair]

                    cursor.execute(f"""
                        SELECT cod, v, descrizione
                        FROM products
                        WHERE (cod, v) IN ({placeholders})
                    """, params)

                    # Create lookup dict
                    descriptions = {(row['cod'], row['v']): row['descrizione'] for row in cursor.fetchall()}

                    # Attach descriptions to entries
                    for entry in entries:
                        entry.description = descriptions.get((entry.product_code, entry.product_var), '-')
            except Exception:
                # If DB query fails, set empty descriptions
                for entry in entries:
                    entry.description = '-'

        context['entries_with_desc'] = entries
        return context


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


# ============ Stock Value Analysis Views ============
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
    from datetime import datetime, timedelta
    from calendar import month_abbr
    
    # Get user's supermarkets
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # ✅ FIX: Auto-select single supermarket
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)
    
    storage_id = request.GET.get('storage_id')
    period = request.GET.get('period', '3')
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
    
    try:
        period_months = int(period)
    except ValueError:
        period_months = 3
    
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
                    fallback_cost = row['cost_std'] or 0.0
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
                                
                                # Calculate for period
                                months_to_include = min(period_months, len(loss_array))
                                period_losses = 0
                                period_value = 0.0
                                
                                for idx in range(months_to_include):
                                    item = loss_array[idx]
                                    
                                    if isinstance(item, list) and len(item) == 2:
                                        qty, cost = item
                                        period_losses += qty
                                        period_value += qty * cost
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
                                            stats[loss_type]['monthly_value'][idx] += qty * cost
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
    
    # Generate month labels (last 12 months)
    today = datetime.now()
    month_labels = []
    for i in range(11, -1, -1):
        month_date = today - timedelta(days=30 * i)
        month_labels.append(month_abbr[month_date.month])
    
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
        'period': period_months,
        'month_labels': json.dumps(month_labels),
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
def promo_products_view(request):
    """
    Display products currently on sale (today BETWEEN sale_start AND sale_end).
    Filtered by storage, showing margins and stock for stocking decisions.
    """
    from datetime import date

    supermarkets = Supermarket.objects.filter(owner=request.user)
    storages = Storage.objects.filter(supermarket__owner=request.user)

    # Get filter parameters
    supermarket_id = request.GET.get('supermarket')
    storage_id = request.GET.get('storage')

    promo_products = []
    scope_description = "Tutti i punti vendita"
    today = date.today()

    # Build list of storages to query
    storages_to_query = []

    if storage_id:
        storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)
        storages_to_query = [storage]
        scope_description = f"{storage.supermarket.name} - {storage.settore}"
    elif supermarket_id:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storages_to_query = list(supermarket.storages.all())
        scope_description = supermarket.name
    else:
        storages_to_query = list(storages)

    for storage in storages_to_query:
        try:
            with RestockService(storage) as service:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT
                        p.cod,
                        p.v,
                        p.descrizione,
                        e.cost_s,
                        e.cost_std,
                        e.price_std,
                        e.sale_start,
                        e.sale_end,
                        ps.stock
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
                    cost_s = float(row['cost_s'] or 0)
                    cost_std = float(row['cost_std'] or 0)
                    price_std = float(row['price_std'] or 0)
                    stock = int(row['stock'] or 0)

                    # Calculate margins (as percentages)
                    margin_std = ((price_std - cost_std) / price_std * 100) if price_std > 0 else 0
                    margin_promo = ((price_std - cost_s) / price_std * 100) if price_std > 0 else 0
                    margin_gain = margin_promo - margin_std  # Extra margin from promo

                    promo_products.append({
                        'cod': row['cod'],
                        'v': row['v'],
                        'descrizione': row['descrizione'],
                        'cost_s': cost_s,
                        'cost_std': cost_std,
                        'price_std': price_std,
                        'margin_std': round(margin_std, 1),
                        'margin_promo': round(margin_promo, 1),
                        'margin_gain': round(margin_gain, 1),
                        'stock': stock,
                        'sale_start': row['sale_start'],
                        'sale_end': row['sale_end'],
                        'storage_id': storage.id,
                        'storage_name': storage.settore,
                        'supermarket_id': storage.supermarket.id,
                        'supermarket_name': storage.supermarket.name,
                    })
        except Exception as e:
            logger.exception(f"Error fetching promo products for storage {storage.id}")
            continue

    # Sort by margin gain (descending) by default
    promo_products.sort(key=lambda x: x['margin_gain'], reverse=True)

    context = {
        'supermarkets': supermarkets,
        'storages': storages,
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
        'scope_description': scope_description,
        'promo_products': promo_products,
        'total_products': len(promo_products),
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
        from .tasks import order_promo_products_task

        result = order_promo_products_task.apply_async(
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
                with RestockService(storage) as service:               
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
    with RestockService(storage) as service:
        pending_purges = service.db.get_purge_pending()    
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
        with RestockService(storage) as service:
            purged = service.db.check_and_purge_flagged()        
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
        
        with RestockService(log.storage) as service:
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

    # Get user's supermarkets for ILIKE search dropdown
    user_supermarkets = Supermarket.objects.filter(owner=request.user)

    if request.method == 'POST':
        form = InventorySearchForm(request.user, request.POST)

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
        else:
            # Form has errors - re-render with errors
            return render(request, 'inventory/search.html', {
                'form': form,
                'user_supermarkets': user_supermarkets
            })
    else:
        form = InventorySearchForm(request.user)

    return render(request, 'inventory/search.html', {
        'form': form,
        'user_supermarkets': user_supermarkets
    })

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
            search_description = f"Product {cod}.{var}"
            
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
                                ps.stock, ps.last_update, ps.verified, ps.minimum_stock
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
                            result['minimum_stock'] = row['minimum_stock'] or 6  
                            results.append(result)
                    except Exception as e:
                        logger.exception(f"Error searching in {sm.name}")

            if not found:
                return redirect('inventory-product-not-found', cod=cod, var=var)

        elif search_type == 'settore_cluster':
            supermarket_id = request.GET.get('supermarket_id')
            settore = request.GET.get('settore')
            cluster_param = request.GET.get('cluster', '')
            clusters = [c.strip() for c in cluster_param.split(',') if c.strip()]

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

            if clusters:
                search_description = f"{supermarket.name} - {settore} - Cluster: {', '.join(clusters)}"
            else:
                search_description = f"{supermarket.name} - {settore} (All Clusters)"

            storage = supermarket.storages.filter(settore=settore).first()

            if not storage:
                messages.warning(request, f"No storage found for settore: {settore}")
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
                                ps.stock, ps.last_update, ps.verified, ps.minimum_stock
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
                                ps.stock, ps.last_update, ps.verified, ps.minimum_stock
                            FROM products p
                            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                            WHERE p.settore = %s AND ps.verified = TRUE
                            ORDER BY p.cluster, p.descrizione
                        """, (settore,))

                    for row in cur.fetchall():
                        result = dict(row)
                        result['supermarket_name'] = supermarket.name
                        result['storage_id'] = storage.id
                        result['minimum_stock'] = row['minimum_stock'] or 6
                        results.append(result)

                except Exception as e:
                    logger.exception(f"Database error in settore search")
                    messages.error(request, f"Database error: {e}")
                    return redirect('inventory-search')
    
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
        'cluster_param': request.GET.get('cluster', '') if search_type == 'settore_cluster' else '',
    }

    return render(request, 'inventory/results.html', context)


@login_required
def cluster_order_preview_view(request):
    """Run the decision maker for specific clusters and render a printable order preview.
    No order is actually placed — Selenium is not used."""
    from .scripts.decision_maker import DecisionMaker

    supermarket_id = request.GET.get('supermarket_id')
    settore = request.GET.get('settore')
    cluster_param = request.GET.get('clusters', '')
    coverage_str = request.GET.get('coverage', '')

    clusters = [c.strip() for c in cluster_param.split(',') if c.strip()]

    try:
        coverage = int(coverage_str)
        if coverage < 1:
            raise ValueError
    except (ValueError, TypeError):
        messages.error(request, "Copertura non valida — inserire un numero intero >= 1")
        return redirect('inventory-search')

    supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
    storage = supermarket.storages.filter(settore=settore).first()
    if not storage:
        messages.error(request, f"Magazzino non trovato per settore: {settore}")
        return redirect('inventory-search')

    orders = []
    try:
        with RestockService(storage) as service:
            blacklist = service.get_blacklist_set()
            dm = DecisionMaker(service.db, service.helper, blacklist_set=blacklist)
            dm.decide_orders_for_settore(settore, coverage)

            if dm.orders_list:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT p.cod, p.v, p.descrizione, p.pz_x_collo, p.rapp, p.cluster, ps.stock
                    FROM products p
                    LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                """, (settore,))
                product_lookup = {(row['cod'], row['v']): dict(row) for row in cur.fetchall()}

                for (cod, var, qty_packages, discount) in dm.orders_list:
                    product = product_lookup.get((cod, var))
                    if not product:
                        continue
                    product_cluster = product.get('cluster') or ''
                    if clusters and product_cluster not in clusters:
                        continue
                    rapp = product.get('rapp') or 1
                    pz_x_collo = product.get('pz_x_collo') or 1
                    package_size = pz_x_collo * rapp
                    orders.append({
                        'cod': cod,
                        'var': var,
                        'descrizione': product.get('descrizione', ''),
                        'cluster': product_cluster,
                        'stock': product.get('stock', 0),
                        'pz_x_collo': pz_x_collo,
                        'rapp': rapp,
                        'package_size': package_size,
                        'qty_packages': qty_packages,
                        'qty_units': qty_packages * package_size,
                        'discount': discount,
                    })
    except Exception as e:
        logger.exception("Error in cluster order preview")
        messages.error(request, f"Errore nel calcolo dell'ordine: {e}")
        return redirect('inventory-search')

    # Sort by cluster then description for readability
    orders.sort(key=lambda o: (o['cluster'], o['descrizione']))

    total_packages = sum(o['qty_packages'] for o in orders)
    total_units = sum(o['qty_units'] for o in orders)

    context = {
        'supermarket': supermarket,
        'settore': settore,
        'clusters': clusters,
        'coverage': coverage,
        'orders': orders,
        'total_packages': total_packages,
        'total_units': total_units,
        'generated_at': timezone.now(),
    }
    return render(request, 'inventory/cluster_order_preview.html', context)


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
def get_clusters_for_settore_view(request, supermarket_id, settore):
    """AJAX endpoint to get clusters for a settore"""
    try:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = supermarket.storages.filter(settore=settore).first()
        
        if not storage:
            logger.warning(f"No storage found for settore: {settore}")
            return JsonResponse({'clusters': []})
        
        with RestockService(storage) as service:
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
    
    except Exception as e:
        logger.exception("Error loading clusters")
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

            if stock > 0:
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

                # Set purge_flag in products table
                try:
                    cursor.execute("ALTER TABLE products ADD COLUMN purge_flag BOOLEAN DEFAULT FALSE")
                    service.db.conn.commit()
                except:
                    pass  # Column already exists

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
                return JsonResponse({'success': True, 'message': result['message']})


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
            with RestockService(storage) as service:
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
            with RestockService(storage) as service:
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
                
                with RestockService(storage) as service:
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
    UPDATED: Now properly displays report from Celery task result.
    Shows comprehensive report with verified, added, and failed products.
    """
    from celery.result import AsyncResult
    from LamApp.celery import app as celery_app
    
    # Try to get from Celery task result first
    task_id = request.GET.get('task_id')
    
    report = None
    
    if task_id:
        task = AsyncResult(task_id, app=celery_app)
        
        if task.ready() and task.successful():
            result = task.result
            
            # ✅ Convert task result to report format
            report = {
                'total_products': result.get('total_products', 0),
                'products_verified': result.get('existing_verified', 0),
                'products_added': result.get('products_added', 0),
                'stock_changes': result.get('stock_changes', []),
                'added_products': result.get('added_products', []),
                'failed_additions': result.get('failed_additions', []),
                'cluster': result.get('cluster'),
                'storage_name': result.get('storage_name'),
            }
        else:
            # Task not ready or failed
            messages.warning(request, "Verification still in progress or failed")
            return redirect('inventory-search')
    else:
        # Fallback to session (for backward compatibility)
        report = request.session.get('verification_report')
    
    if not report:
        messages.warning(request, "No verification report available")
        return redirect('inventory-search')
    
    # Calculate statistics
    total_difference = 0
    total_stock_after = 0
    
    if report.get('stock_changes'):
        total_difference = sum(
            change.get('difference', 0) 
            for change in report['stock_changes']
        )
        total_stock_after = sum(
            change.get('new_stock', 0)
            for change in report['stock_changes']
        )
    
    # Add auto-added products to totals
    if report.get('added_products'):
        total_difference += sum(
            product.get('qty', 0) 
            for product in report['added_products']
        )
        total_stock_after += sum(
            product.get('qty', 0)
            for product in report['added_products']
        )
    
    report['total_difference'] = total_difference
    report['total_stock_after'] = total_stock_after
    
    return render(request, 'inventory/verification_report_unified.html', {
        'report': report
    })


@login_required
@require_POST
def verify_product_ajax_view(request):
    """
    Unified AJAX endpoint for verifying a single product.

    Accepts either:
      - storage_id (direct storage reference)
      - supermarket_id + settore (lookup storage by these)

    Optional parameters:
      - stock: New stock value (required in most cases)
      - cluster: Cluster assignment (optional, 'NONE' to clear)
      - package_size: Package size update (optional)
    """
    try:
        data = json.loads(request.body)

        cod = int(data['cod'])
        var = int(data['var'])
        stock = data.get('stock')  # Can be None for just marking verified
        cluster = data.get('cluster', '').strip().upper() if data.get('cluster') else None
        package_size = data.get('package_size')

        # Resolve storage: either by storage_id or by supermarket_id + settore
        storage_id = data.get('storage_id')
        supermarket_id = data.get('supermarket_id')
        settore = data.get('settore')

        if storage_id:
            storage = get_object_or_404(
                Storage,
                id=storage_id,
                supermarket__owner=request.user
            )
        elif supermarket_id and settore:
            storage = get_object_or_404(
                Storage,
                supermarket_id=supermarket_id,
                settore=settore,
                supermarket__owner=request.user
            )
        else:
            return JsonResponse({
                'success': False,
                'message': 'Must provide either storage_id or supermarket_id + settore'
            }, status=400)

        with RestockService(storage) as service:
            # Update package size if provided
            if package_size is not None:
                cursor = service.db.cursor()
                cursor.execute("""
                    UPDATE products
                    SET pz_x_collo = %s
                    WHERE cod = %s AND v = %s
                """, (int(package_size), cod, var))
                service.db.conn.commit()
                logger.info(f"Updated package size for {cod}.{var} to {package_size}")

            # Handle cluster update (including clearing with 'NONE')
            cluster_to_set = None
            if cluster:
                if cluster == 'NONE':
                    # Clear cluster
                    cursor = service.db.cursor()
                    cursor.execute("""
                        UPDATE products
                        SET cluster = NULL
                        WHERE cod = %s AND v = %s
                    """, (cod, var))
                    service.db.conn.commit()
                else:
                    cluster_to_set = cluster

            # Verify stock (this also marks as verified)
            if stock is not None:
                service.db.verify_stock(cod, var, int(stock), cluster_to_set)
            else:
                service.db.verify_stock(cod, var, new_stock=None, cluster=cluster_to_set)

            message = f'Product {cod}.{var} verified successfully!'
            if package_size:
                message += f' Package size updated to {package_size}.'

            logger.info(
                f"Product verified: {storage.supermarket.name} - {storage.settore} - "
                f"{cod}.{var}" + (f" (package: {package_size})" if package_size else "") +
                (f" (cluster: {cluster})" if cluster else "")
            )

            return JsonResponse({
                'success': True,
                'message': message,
                'cluster_updated': bool(cluster)
            })
    except KeyError as e:
        return JsonResponse({
            'success': False,
            'message': f'Missing required field: {e}'
        }, status=400)
    except Exception as e:
        logger.exception("Error verifying product")
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
    Always generates a redirect_url or message for frontend.
    """
    from celery.result import AsyncResult

    task = AsyncResult(task_id, app=celery_app)

    # Debug logging to diagnose infinite loading issues
    logger.debug(f"Task {task_id}: state={task.state}, ready={task.ready()}, info={task.info}")

    # ✅ FIX: Check both task.ready() AND explicit SUCCESS/FAILURE states
    # This fixes cases where task.ready() returns False incorrectly
    is_complete = task.ready() or task.state in ['SUCCESS', 'FAILURE']

    response_data = {
        'state': task.state,
        'ready': is_complete,  # Use our corrected completion check
        'task_id': task_id,
    }

    if is_complete:
        if task.successful():
            result = task.result
            response_data['success'] = True
            
            # ✅ FIXED: Always ensure we have either redirect_url or message
            if isinstance(result, dict):
                response_data['result'] = result
                
                # Extract message if available
                message = result.get('message', 'Operation completed successfully')
                response_data['message'] = message
                
                # Determine redirect URL based on result content
                if 'log_id' in result:
                    # Restock operation
                    response_data['redirect_url'] = f"/logs/{result['log_id']}/"
                
                elif 'storage_id' in result:
                    # Storage operation (list update, stats update, etc.)
                    response_data['redirect_url'] = f"/storages/{result['storage_id']}/"
                
                elif 'synced' in result:
                    # Storage sync operation
                    response_data['redirect_url'] = f"/supermarkets/{result['supermarket_id']}/edit/"

                elif 'products_added' in result:
                    # Add products operation
                    response_data['redirect_url'] = "/inventory/"
                
                elif 'verified' in result or 'assigned' in result:
                    # Inventory verification or cluster assignment
                    response_data['redirect_url'] = "/inventory/"
                
                else:
                    # Unknown format - default to dashboard
                    response_data['redirect_url'] = "/dashboard/"
            
            else:
                # Non-dict result (shouldn't happen, but handle it)
                response_data['message'] = str(result) if result else 'Operation completed'
                response_data['redirect_url'] = "/dashboard/"
        
        else:
            # Task failed
            response_data['success'] = False
            error_info = task.info
            
            if isinstance(error_info, Exception):
                response_data['error'] = str(error_info)
            elif isinstance(error_info, dict):
                response_data['error'] = error_info.get('exc_message', str(error_info))
            else:
                response_data['error'] = str(error_info) if error_info else 'Unknown error'
    
    else:
        # Task still running - extract progress info
        if isinstance(task.info, dict):
            response_data['progress'] = task.info.get('progress', 0)
            response_data['status_message'] = task.info.get('status', 'Processing...')
        else:
            response_data['progress'] = 0
            response_data['status_message'] = 'Processing...'

        # ✅ FIX: If task has been PENDING for too long, check if result exists anyway
        # This handles edge cases where Celery doesn't update state properly
        if task.state == 'PENDING':
            try:
                # Try to get the result anyway - if it exists, the task is actually done
                result = task.result
                if result is not None:
                    logger.warning(f"Task {task_id} stuck in PENDING but has result. Marking as complete.")
                    response_data['ready'] = True
                    response_data['success'] = True
                    response_data['result'] = result

                    # Extract redirect URL from result
                    if isinstance(result, dict):
                        response_data['message'] = result.get('message', 'Operation completed')

                        if 'log_id' in result:
                            response_data['redirect_url'] = f"/logs/{result['log_id']}/"
                        elif 'storage_id' in result:
                            response_data['redirect_url'] = f"/storages/{result['storage_id']}/"
                        else:
                            response_data['redirect_url'] = "/dashboard/"
                    else:
                        response_data['message'] = 'Operation completed'
                        response_data['redirect_url'] = "/dashboard/"
            except Exception as e:
                logger.debug(f"Task {task_id} is genuinely pending: {e}")

    logger.debug(f"Task {task_id} response: ready={response_data.get('ready')}, state={response_data.get('state')}")
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
    UPDATED: View to edit recorded losses WITHOUT affecting stock.
    Shows both quantity and cost snapshot for each month.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)
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
    
    products_with_losses = []
    
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
                
                # Also get current cost for reference
                query = f"""
                    SELECT 
                        el.cod, el.v,
                        el.broken, el.broken_updated,
                        el.expired, el.expired_updated,
                        el.internal, el.internal_updated,
                        el.shrinkage, el.shrinkage_updated,
                        p.descrizione,
                        p.settore,
                        e.cost_std as current_cost
                    FROM extra_losses el
                    LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                    LEFT JOIN economics e ON el.cod = e.cod AND el.v = e.v
                    {settore_filter}
                    ORDER BY p.descrizione
                """
                
                cursor.execute(query)
                
                for row in cursor.fetchall():
                    cod = row['cod']
                    v = row['v']
                    description = row['descrizione'] or f"Product {cod}.{v}"
                    current_cost = row['current_cost'] or 0.0
                    
                    # Process arrays - convert to format with cost info
                    def process_loss_array(loss_json, fallback_cost):
                        """Convert array to list of {qty, cost, value} dicts"""
                        if not loss_json:
                            return []
                        
                        result = []
                        for item in loss_json:
                            if isinstance(item, list) and len(item) == 2:
                                # New format: [qty, cost]
                                qty, cost = item
                                result.append({
                                    'qty': qty,
                                    'cost': cost,
                                    'value': qty * cost
                                })
                            else:
                                # Old format: just qty
                                qty = item
                                result.append({
                                    'qty': qty,
                                    'cost': fallback_cost,
                                    'value': qty * fallback_cost
                                })
                        return result
                    
                    broken_data = process_loss_array(row['broken'], current_cost)
                    expired_data = process_loss_array(row['expired'], current_cost)
                    internal_data = process_loss_array(row['internal'], current_cost)
                    shrinkage_data = process_loss_array(row['shrinkage'], current_cost)

                    product = {
                        'cod': cod,
                        'var': v,
                        'description': description,
                        'settore': row['settore'],
                        'supermarket_name': sm_data['supermarket'].name,
                        'supermarket_id': sm_data['supermarket'].id,
                        'current_cost': current_cost,
                        'broken': broken_data,
                        'broken_updated': row['broken_updated'],
                        'expired': expired_data,
                        'expired_updated': row['expired_updated'],
                        'internal': internal_data,
                        'internal_updated': row['internal_updated'],
                        'shrinkage': shrinkage_data,
                        'shrinkage_updated': row['shrinkage_updated'],
                    }

                    # Only include if has at least one loss recorded
                    if broken_data or expired_data or internal_data or shrinkage_data:
                        products_with_losses.append(product)
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
    UPDATED: AJAX endpoint to edit a specific loss value.
    Now handles new format: [[qty, cost], [qty, cost], ...]
    Updates extra_losses table WITHOUT affecting stock.
    
    When editing, we preserve the original cost snapshot but allow changing quantity.
    """
    try:
        data = json.loads(request.body)
        
        supermarket_id = int(data['supermarket_id'])
        cod = int(data['cod'])
        var = int(data['var'])
        loss_type = data['loss_type']  # 'broken', 'expired', 'internal', or 'shrinkage'
        month_index = int(data['month_index'])  # 0 = most recent month
        new_value = int(data['new_value'])  # New quantity

        # Validate loss type
        if loss_type not in ['broken', 'expired', 'internal', 'shrinkage']:
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
        
        with RestockService(storage) as service:
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
            
            # Handle both old and new formats
            item = current_array[month_index]
            
            if isinstance(item, list) and len(item) == 2:
                # New format: [qty, cost]
                old_qty = item[0]
                stored_cost = item[1]
                current_array[month_index] = [new_value, stored_cost]  # Keep cost, update qty
            else:
                # Old format: just qty
                old_qty = item
                # Get current cost from economics as fallback
                cursor.execute("""
                    SELECT cost_std FROM economics WHERE cod = %s AND v = %s
                """, (cod, var))
                cost_row = cursor.fetchone()
                current_cost = float(cost_row['cost_std']) if cost_row and cost_row['cost_std'] else 0.0
                
                # Convert to new format with current cost as snapshot
                current_array[month_index] = [new_value, current_cost]
            
            # Calculate stock adjustment needed
            stock_delta = old_qty - new_value  # Positive = add back to stock, negative = remove more
            
            # Update database - ONLY the extra_losses table (no stock adjustment per requirement)
            cursor.execute(f"""
                UPDATE extra_losses
                SET {loss_type} = %s
                WHERE cod = %s AND v = %s
            """, (Json(current_array), cod, var))
            
            service.db.conn.commit()
            
            logger.info(
                f"Loss edited: {supermarket.name} - Product {cod}.{var} - "
                f"{loss_type}[{month_index}]: {old_qty} → {new_value} "
                f"(stock NOT adjusted, cost snapshot preserved)"
            )
            
            return JsonResponse({
                'success': True,
                'message': f'Updated {loss_type} month {month_index}: {old_qty} → {new_value}',
                'old_value': old_qty,
                'new_value': new_value,
                'stock_delta_not_applied': stock_delta
            })          
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
    FIXED: Now properly handles empty minimum_stock value
    """
    try:
        cod = int(request.POST.get('cod'))
        var = int(request.POST.get('var'))
        adjustment_raw = request.POST.get('adjustment', '').strip()
        reason = request.POST.get('reason')
        supermarket_name = request.POST.get('supermarket')
        minimum_stock = request.POST.get('minimum_stock', '').strip()  # ← FIX: strip whitespace
        cluster = request.POST.get('cluster', '').strip().upper()
        
        supermarket = get_object_or_404(Supermarket, name=supermarket_name, owner=request.user)
        storage = supermarket.storages.first()
        
        if not storage:
            return JsonResponse({'success': False, 'message': 'No storage found'}, status=400)
        
        with RestockService(storage) as service:        
            current_stock = service.db.get_stock(cod, var)

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
            
            # Handle stock adjustment with stolen loss type
            loss_type_mapping = {
                'broken': 'broken',
                'expired': 'expired',
                'internal_use': 'internal',
                'stolen': 'stolen',
                'shrinkage': 'shrinkage',
            }
            
            adjustment = None

            if adjustment_raw != '':
                # Reason is mandatory when adjusting stock
                if not reason:
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

            if adjustment is not None and reason in loss_type_mapping and adjustment < 0:
                # This is a loss - record in extra_losses
                loss_type = loss_type_mapping[reason]
                loss_amount = abs(adjustment)
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
                    'loss_amount': loss_amount,
                    'minimum_stock_updated': minimum_stock_updated,
                    'cluster_updated': cluster_updated,
                    'new_cluster': new_cluster_value
                })
            elif adjustment is not None:
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
                    'loss_recorded': False,
                    'minimum_stock_updated': minimum_stock_updated,
                    'cluster_updated': cluster_updated,
                    'new_cluster': new_cluster_value
                })
            else:
                # No stock change, only metadata updated
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
    
@login_required
def upload_ddt_view(request, storage_id):
    """
    Upload DDT (delivery document) to add received stock.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = DDTUploadForm(request.POST, request.FILES)
        
        if form.is_valid():
            pdf_file = request.FILES['pdf_file']
            
            try:
                # Save file temporarily
                temp_dir = Path(settings.BASE_DIR) / 'temp_ddt'
                temp_dir.mkdir(exist_ok=True)
                
                timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
                file_path = temp_dir / f"ddt_{timestamp}_{pdf_file.name}"
                
                with open(file_path, 'wb+') as destination:
                    for chunk in pdf_file.chunks():
                        destination.write(chunk)
                
                # ✅ DISPATCH TO CELERY
                from .tasks import process_ddt_task
                
                result = process_ddt_task.apply_async(
                    args=[storage_id, str(file_path)],
                    retry=True
                )
                
                messages.info(
                    request,
                    f"Processing DDT for {storage.name}. This may take a few minutes."
                )
                
                return redirect('task-progress', task_id=result.id, storage_id=storage_id)
                
            except Exception as e:
                logger.exception("Error saving DDT file")
                messages.error(request, f"Error: {str(e)}")
                return redirect('upload-ddt', storage_id=storage_id)
    else:
        form = DDTUploadForm()
    
    return render(request, 'storages/upload_ddt.html', {
        'storage': storage,
        'form': form
    })

@login_required
def pending_verifications_view(request):
    """
    Show all products that need verification across all supermarkets.
    These are products that have been ordered but not yet verified.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    all_pending = []
    
    # Group by supermarket to minimize DB connections
    for sm in supermarkets:
        if not sm.storages.exists():
            continue
        
        try:
            storage = sm.storages.first()
            
            with RestockService(storage) as service:
                cursor = service.db.cursor()
                
                # Get all settores for this supermarket
                settores = list(sm.storages.values_list('settore', flat=True).distinct())
                
                if not settores:
                    continue
                
                settore_placeholders = ','.join(['%s'] * len(settores))
                
                # ✅ FIXED: Add type check to prevent "non-array" error
                query = f"""
                    SELECT 
                        p.cod, p.v, p.descrizione, p.pz_x_collo, p.settore,
                        ps.stock, ps.bought_last_24,
                        ps.last_update
                    FROM product_stats ps
                    JOIN products p ON ps.cod = p.cod AND ps.v = p.v
                    WHERE ps.verified = FALSE
                    AND p.purge_flag = FALSE
                    AND ps.bought_last_24 IS NOT NULL
                    AND jsonb_typeof(ps.bought_last_24) = 'array'
                    AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(ps.bought_last_24)
                    )
                    AND p.settore IN ({settore_placeholders})
                    ORDER BY ps.last_update DESC
                    LIMIT 20
                """
                
                cursor.execute(query, settores)
                
                for row in cursor.fetchall():
                    bought = row['bought_last_24'] or []
                    sold = []  # They haven't sold any yet
                    
                    # Only include if bought but not sold
                    if bought and (not sold or all(s == 0 for s in sold)):
                        all_pending.append({
                            'supermarket_name': sm.name,
                            'supermarket_id': sm.id,
                            'settore': row['settore'],
                            'cod': row['cod'],
                            'var': row['v'],
                            'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                            'package_size': row['pz_x_collo'] or 12,
                            'stock': row['stock'] or 0,
                            'last_update': row['last_update']
                        })
        except Exception as e:
            logger.exception(f"Error loading pending verifications for {sm.name}")
            continue
    
    # Sort by last_update (most recent first)
    all_pending.sort(key=lambda x: x['last_update'] if x['last_update'] else timezone.now(), reverse=True)
    
    context = {
        'pending_products': all_pending,
        'total_pending': len(all_pending)
    }
    
    return render(request, 'inventory/pending_verifications.html', context)


# ============ Recipe Views ============

class RecipeListView(LoginRequiredMixin, ListView):
    model = Recipe
    template_name = 'recipes/list.html'
    context_object_name = 'recipes'

    def get_queryset(self):
        return Recipe.objects.filter(
            supermarket__owner=self.request.user
        ).select_related(
            'supermarket', 'base_recipe'
        ).prefetch_related(
            'product_items', 'external_items'
        ).order_by('supermarket__name', 'family', 'name')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Group by supermarket, then by family
        grouped = {}
        for recipe in context['recipes']:
            sm_key = (recipe.supermarket.id, recipe.supermarket.name)
            if sm_key not in grouped:
                grouped[sm_key] = {'base': [], 'by_family': {}}

            if recipe.is_base:
                grouped[sm_key]['base'].append(recipe)
            else:
                family = recipe.family or 'Senza famiglia'
                if family not in grouped[sm_key]['by_family']:
                    grouped[sm_key]['by_family'][family] = []
                grouped[sm_key]['by_family'][family].append(recipe)

        context['grouped_recipes'] = grouped
        return context


class RecipeDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = Recipe
    template_name = 'recipes/detail.html'
    context_object_name = 'recipe'

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        recipe = self.object

        # Get product items with fresh data from external DB
        product_items = list(recipe.product_items.all())
        if product_items:
            try:
                # Get any storage from this supermarket to access the DB
                storage = recipe.supermarket.storages.first()
                if storage:
                    with RestockService(storage) as service:
                        cursor = service.db.cursor()
                        codes = [(item.product_code, item.product_var) for item in product_items]
                        placeholders = ','.join(['(%s, %s)'] * len(codes))
                        params = [x for pair in codes for x in pair]

                        cursor.execute(f"""
                            SELECT p.cod, p.v, p.descrizione, e.cost_std
                            FROM products p
                            LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                            WHERE (p.cod, p.v) IN ({placeholders})
                        """, params)

                        data_map = {(r['cod'], r['v']): r for r in cursor.fetchall()}

                        for item in product_items:
                            row = data_map.get((item.product_code, item.product_var))
                            # Convert use_percentage to decimal units for display
                            item.units = item.use_percentage / 100
                            if row:
                                item.live_description = row['descrizione'] or item.cached_description
                                item.live_cost_std = row['cost_std'] or item.cached_cost_std or 0
                                item.live_cost = float(item.live_cost_std) * item.units
                            else:
                                item.live_description = item.cached_description
                                item.live_cost = item.get_cost()
            except Exception as e:
                logger.exception("Error fetching product data for recipe")
                for item in product_items:
                    item.units = item.use_percentage / 100
                    item.live_description = item.cached_description
                    item.live_cost = item.get_cost()

        # Add units to product items that didn't go through the try block
        for item in product_items:
            if not hasattr(item, 'units'):
                item.units = item.use_percentage / 100

        # Get external items and add units attribute
        external_items = list(recipe.external_items.all())
        for item in external_items:
            item.units = item.use_percentage / 100

        context['product_items'] = product_items
        context['external_items'] = external_items

        # Calculate totals
        product_total = sum(getattr(item, 'live_cost', item.get_cost()) for item in product_items)
        external_total = sum(item.get_cost() for item in external_items)
        # Base total includes the multiplier
        base_total = (recipe.base_recipe.get_total_cost() * float(recipe.base_multiplier)) if recipe.base_recipe else 0

        context['product_total'] = product_total
        context['external_total'] = external_total
        context['base_total'] = base_total
        context['total_cost'] = product_total + external_total + base_total
        context['margin_pct'] = recipe.get_margin_percentage()
        context['margin_abs'] = float(recipe.selling_price) - context['total_cost']

        return context


class RecipeDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    model = Recipe
    template_name = 'recipes/confirm_delete.html'
    success_url = reverse_lazy('recipe-list')

    def test_func(self):
        return self.get_object().supermarket.owner == self.request.user

    def delete(self, request, *args, **kwargs):
        messages.success(request, f"Ricetta '{self.get_object().name}' eliminata!")
        return super().delete(request, *args, **kwargs)


@login_required
@require_POST
def dismiss_recipe_cost_alert(request, pk):
    """Mark a single recipe cost alert as read"""
    alert = get_object_or_404(
        RecipeCostAlert,
        pk=pk,
        recipe__supermarket__owner=request.user
    )
    alert.is_read = True
    alert.save(update_fields=['is_read'])

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True})

    messages.success(request, "Notifica archiviata")
    return redirect('dashboard')


@login_required
@require_POST
def dismiss_all_recipe_cost_alerts(request):
    """Mark all recipe cost alerts as read for the current user"""
    updated = RecipeCostAlert.objects.filter(
        recipe__supermarket__owner=request.user,
        is_read=False
    ).update(is_read=True)

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True, 'dismissed': updated})

    messages.success(request, f"{updated} notifiche archiviate")
    return redirect('dashboard')


@login_required
def recipe_create_view(request):
    """Create recipe with AJAX-driven product search"""
    from decimal import Decimal

    if request.method == 'POST':
        supermarket_id = request.POST.get('supermarket')
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)

        # Create recipe
        base_multiplier = Decimal(request.POST.get('base_multiplier') or '1')
        recipe = Recipe.objects.create(
            supermarket=supermarket,
            name=request.POST.get('name'),
            family=request.POST.get('family', ''),
            is_base=request.POST.get('is_base') == 'on',
            base_recipe_id=request.POST.get('base_recipe') or None,
            base_multiplier=base_multiplier,
            selling_price=Decimal(request.POST.get('selling_price') or '0'),
            notes=request.POST.get('notes', '')
        )

        # Process product items from JSON (UI sends decimal units, convert to percentage)
        product_items_json = request.POST.get('product_items', '[]')
        try:
            product_items = json.loads(product_items_json)
            for item in product_items:
                # Convert decimal units to percentage (e.g., 0.5 -> 50, 1.0 -> 100)
                units = float(item.get('units', 1))
                use_percentage = int(units * 100)
                RecipeProductItem.objects.create(
                    recipe=recipe,
                    product_code=item['cod'],
                    product_var=item.get('var', 1),
                    use_percentage=use_percentage,
                    cached_description=item.get('description', ''),
                    cached_cost_std=Decimal(str(item.get('cost_std', 0))) if item.get('cost_std') else None
                )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing product items: {e}")

        # Process external items from JSON (UI sends decimal units, convert to percentage)
        external_items_json = request.POST.get('external_items', '[]')
        try:
            external_items = json.loads(external_items_json)
            for item in external_items:
                # Convert decimal units to percentage
                units = float(item.get('units', 1))
                use_percentage = int(units * 100)
                RecipeExternalItem.objects.create(
                    recipe=recipe,
                    name=item['name'],
                    unit_cost=Decimal(str(item.get('unit_cost', 0))),
                    use_percentage=use_percentage
                )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing external items: {e}")

        messages.success(request, f"Ricetta '{recipe.name}' creata con successo!")
        return redirect('recipe-detail', pk=recipe.pk)

    # GET request
    supermarkets = Supermarket.objects.filter(owner=request.user)
    families = Recipe.objects.filter(
        supermarket__owner=request.user
    ).exclude(family='').values_list('family', flat=True).distinct()
    base_recipes = Recipe.objects.filter(
        supermarket__owner=request.user,
        is_base=True
    ).select_related('supermarket')

    context = {
        'supermarkets': supermarkets,
        'existing_families': list(families),
        'base_recipes': base_recipes,
        'base_recipes_json': json.dumps([
            {'id': r.id, 'name': r.name, 'supermarket_id': r.supermarket_id, 'cost': r.get_total_cost()}
            for r in base_recipes
        ]),
        'is_edit': False
    }
    return render(request, 'recipes/form.html', context)


@login_required
def recipe_update_view(request, pk):
    """Update existing recipe"""
    from decimal import Decimal

    recipe = get_object_or_404(Recipe, pk=pk, supermarket__owner=request.user)

    if request.method == 'POST':
        recipe.name = request.POST.get('name')
        recipe.family = request.POST.get('family', '')
        recipe.is_base = request.POST.get('is_base') == 'on'
        recipe.base_recipe_id = request.POST.get('base_recipe') or None
        recipe.base_multiplier = Decimal(request.POST.get('base_multiplier') or '1')
        recipe.selling_price = Decimal(request.POST.get('selling_price') or '0')
        recipe.notes = request.POST.get('notes', '')
        recipe.save()

        # Replace all items (simpler than diff-based updates)
        recipe.product_items.all().delete()
        recipe.external_items.all().delete()

        # Re-create product items from JSON (UI sends decimal units, convert to percentage)
        product_items_json = request.POST.get('product_items', '[]')
        try:
            product_items = json.loads(product_items_json)
            for item in product_items:
                # Convert decimal units to percentage (e.g., 0.5 -> 50, 1.0 -> 100)
                units = float(item.get('units', 1))
                use_percentage = int(units * 100)
                RecipeProductItem.objects.create(
                    recipe=recipe,
                    product_code=item['cod'],
                    product_var=item.get('var', 1),
                    use_percentage=use_percentage,
                    cached_description=item.get('description', ''),
                    cached_cost_std=Decimal(str(item.get('cost_std', 0))) if item.get('cost_std') else None
                )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing product items: {e}")

        # Re-create external items from JSON (UI sends decimal units, convert to percentage)
        external_items_json = request.POST.get('external_items', '[]')
        try:
            external_items = json.loads(external_items_json)
            for item in external_items:
                # Convert decimal units to percentage
                units = float(item.get('units', 1))
                use_percentage = int(units * 100)
                RecipeExternalItem.objects.create(
                    recipe=recipe,
                    name=item['name'],
                    unit_cost=Decimal(str(item.get('unit_cost', 0))),
                    use_percentage=use_percentage
                )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing external items: {e}")

        messages.success(request, f"Ricetta '{recipe.name}' aggiornata!")
        return redirect('recipe-detail', pk=recipe.pk)

    # GET - prepare edit form with existing data
    supermarkets = Supermarket.objects.filter(owner=request.user)
    families = Recipe.objects.filter(
        supermarket__owner=request.user
    ).exclude(family='').values_list('family', flat=True).distinct()
    base_recipes = Recipe.objects.filter(
        supermarket__owner=request.user,
        is_base=True
    ).exclude(pk=recipe.pk).select_related('supermarket')

    # Prepare existing items as JSON (convert percentage to decimal units for UI)
    existing_product_items = [
        {
            'cod': item.product_code,
            'var': item.product_var,
            'description': item.cached_description,
            'cost_std': float(item.cached_cost_std) if item.cached_cost_std else 0,
            'units': item.use_percentage / 100  # Convert percentage to decimal units
        }
        for item in recipe.product_items.all()
    ]

    existing_external_items = [
        {
            'name': item.name,
            'unit_cost': float(item.unit_cost),
            'units': item.use_percentage / 100  # Convert percentage to decimal units
        }
        for item in recipe.external_items.all()
    ]

    context = {
        'recipe': recipe,
        'supermarkets': supermarkets,
        'existing_families': list(families),
        'base_recipes': base_recipes,
        'base_recipes_json': json.dumps([
            {'id': r.id, 'name': r.name, 'supermarket_id': r.supermarket_id, 'cost': r.get_total_cost()}
            for r in base_recipes
        ]),
        'existing_product_items_json': json.dumps(existing_product_items),
        'existing_external_items_json': json.dumps(existing_external_items),
        'is_edit': True
    }
    return render(request, 'recipes/form.html', context)


@login_required
def recipe_product_search_view(request):
    """AJAX endpoint for searching products by description"""
    query = request.GET.get('q', '').strip()
    supermarket_id = request.GET.get('supermarket_id')

    if len(query) < 3:
        return JsonResponse({'products': [], 'error': 'Minimum 3 characters required'})

    if not supermarket_id:
        return JsonResponse({'products': [], 'error': 'Supermarket ID required'})

    try:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)

        # Get any storage to access the database
        storage = supermarket.storages.first()
        if not storage:
            return JsonResponse({'products': [], 'error': 'No storage found for this supermarket'})

        with RestockService(storage) as service:
            cursor = service.db.cursor()

            # Use ILIKE for case-insensitive search
            cursor.execute("""
                SELECT p.cod, p.v, p.descrizione, e.cost_std
                FROM products p
                LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
                WHERE p.descrizione ILIKE %s
                ORDER BY p.descrizione
                LIMIT 20
            """, [f'%{query}%'])

            products = []
            for row in cursor.fetchall():
                cost_std = float(row['cost_std'] or 0)

                products.append({
                    'cod': row['cod'],
                    'var': row['v'],
                    'description': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                    'cost_std': cost_std
                })

            return JsonResponse({'products': products})

    except Exception as e:
        logger.error(f"Product search error: {e}")
        return JsonResponse({'products': [], 'error': str(e)})


@login_required
def recipe_get_base_items_view(request, pk):
    """Get items from a base recipe (for display when selecting base)"""
    recipe = get_object_or_404(Recipe, pk=pk, supermarket__owner=request.user, is_base=True)

    product_items = [
        {
            'cod': item.product_code,
            'var': item.product_var,
            'description': item.cached_description,
            'use_percentage': item.use_percentage,
            'cost': item.get_cost()
        }
        for item in recipe.product_items.all()
    ]

    external_items = [
        {
            'name': item.name,
            'unit_cost': float(item.unit_cost),
            'use_percentage': item.use_percentage,
            'cost': item.get_cost()
        }
        for item in recipe.external_items.all()
    ]

    return JsonResponse({
        'name': recipe.name,
        'product_items': product_items,
        'external_items': external_items,
        'total_cost': recipe.get_total_cost()
    })