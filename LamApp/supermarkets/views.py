# LamApp/LamApp/supermarkets/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.http import require_POST

from .models import (
    Supermarket, Storage, RestockSchedule, 
    Blacklist, BlacklistEntry, RestockLog
)
from .forms import (
    RestockScheduleForm, BlacklistForm, 
    BlacklistEntryForm, StorageForm
)
from .services import RestockService, StorageService
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
    
    context = {
        'supermarkets': supermarkets,
        'recent_logs': recent_logs,
        'total_supermarkets': supermarkets.count(),
        'total_storages': sum(s.storages.count() for s in supermarkets),
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
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.pk})


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


# ============ Restock Schedule Views ============

class RestockScheduleListView(LoginRequiredMixin, ListView):
    model = Storage
    template_name = "restock_schedule_list.html"
    context_object_name = "storages"
    
    def get_queryset(self):
        return Storage.objects.filter(
            supermarket__owner=self.request.user
        ).select_related('supermarket', 'schedule').order_by('supermarket__name', 'name')


class RestockScheduleView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = RestockSchedule
    form_class = RestockScheduleForm
    template_name = "restock_schedule.html"
    
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

    def get_success_url(self):
        messages.success(self.request, "Schedule updated successfully!")
        return reverse_lazy("restock_schedule_list")


# ============ Restock Operation Views ============

@login_required
def run_restock_view(request, storage_id):
    """Manually trigger a restock check"""
    storage = get_object_or_404(
        Storage, 
        id=storage_id, 
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        try:
            service = RestockService(storage)
            
            # Get coverage from form or use default
            coverage = request.POST.get('coverage')
            if coverage:
                coverage = float(coverage)
            
            log, orders_list = service.run_restock_check(coverage)
            
            messages.success(
                request, 
                f"Restock check completed! {log.products_ordered} products to order, "
                f"{log.total_packages} total packages."
            )
            
            # Ask if user wants to execute the order
            request.session['pending_orders'] = {
                'storage_id': storage_id,
                'log_id': log.id,
                'orders': [[cod, var, qty] for cod, var, qty in orders_list]
            }
            
            return redirect('restock-log-detail', pk=log.id)
            
        except Exception as e:
            logger.exception("Error running restock check")
            messages.error(request, f"Error: {str(e)}")
            return redirect('storage-detail', pk=storage_id)
        finally:
            service.close()
    
    return render(request, 'storages/run_restock.html', {'storage': storage})


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
        context['results'] = self.object.get_results()
        return context


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
    
    def test_func(self):
        return self.get_object().blacklist.storage.supermarket.owner == self.request.user
    
    def get_success_url(self):
        messages.success(self.request, "Blacklist entry removed!")
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.object.blacklist.pk})
