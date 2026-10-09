"""Supermarket CRUD and closure calendar."""

from django.utils import timezone
from datetime import date, timedelta
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import (
    ListView, DetailView, CreateView, UpdateView, DeleteView,
)
from django import forms as django_forms
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
import logging

from ..automation_services import AutomatedRestockService
from ..models import Supermarket, RestockLog

logger = logging.getLogger(__name__)


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
        context['recent_sync_logs'] = self.object.sales_sync_logs.order_by('-created_at')[:5]

        # Today's row is rewritten every 15 min, so a frozen one looks the same as a quiet
        # morning. Surface the age so stock on screen can be trusted for a shelf check
        # mid-day, rather than only before opening.
        last_sync = self.object.last_sales_sync_at
        context['last_sales_sync_at'] = last_sync
        context['sync_age_minutes'] = None
        context['sync_is_stale'] = False
        context['sync_stale_minutes'] = AutomatedRestockService.SYNC_STALE_WARN_MINUTES
        if last_sync:
            age = (timezone.now() - last_sync).total_seconds() / 60
            context['sync_age_minutes'] = int(age)
            # The dispatcher's own threshold, so the page and the order freshness guard
            # never disagree.
            context['sync_is_stale'] = age > AutomatedRestockService.SYNC_STALE_WARN_MINUTES
        context['recent_loss_logs'] = RestockLog.objects.filter(
            storage__supermarket=self.object,
            operation_type='loss_recording',
        ).order_by('-started_at')[:5]

        if self.object.sync_api_token:
            from ..models import SalesSyncLog, is_closure_day
            yesterday = date.today() - timedelta(days=1)
            if is_closure_day(self.object, yesterday):
                context['sync_stale'] = False
                context['sync_zero'] = False
                context['last_sync_date'] = None
            else:
                last_sync = SalesSyncLog.objects.filter(
                    supermarket=self.object
                ).order_by('-sync_date').first()
                context['sync_stale'] = not last_sync or last_sync.sync_date < yesterday
                context['sync_zero'] = (
                    last_sync is not None
                    and last_sync.sync_date >= yesterday
                    and last_sync.applied == 0
                )
                context['last_sync_date'] = last_sync.sync_date if last_sync else None
        else:
            context['sync_stale'] = False
            context['sync_zero'] = False
            context['last_sync_date'] = None

        return context


class SupermarketCreateView(LoginRequiredMixin, CreateView):
    model = Supermarket
    fields = ['name', 'username', 'password', 'store_type']
    template_name = 'supermarkets/form.html'

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        form.fields['name'].label = 'Nome del punto vendita'
        form.fields['username'].label = 'Username Dropzone'
        form.fields['password'].label = 'Password Dropzone'
        form.fields['password'].widget = django_forms.PasswordInput()
        form.fields['store_type'].label = 'Tipo di punto vendita'
        form.fields['store_type'].help_text = 'I punti vendita Rione ricevono solo le promo contrassegnate RIONE.'
        return form

    def form_valid(self, form):
        form.instance.owner = self.request.user
        self.object = form.save()
        from ..tasks import sync_storages_task
        result = sync_storages_task.apply_async(args=[self.object.pk])
        return redirect('task-progress', task_id=result.id)


class SupermarketUpdateView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = Supermarket
    fields = ['name', 'username', 'password', 'store_type']
    template_name = 'supermarkets/form.html'

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        form.fields['name'].label = 'Nome del punto vendita'
        form.fields['username'].label = 'Username Dropzone'
        form.fields['password'].label = 'Password Dropzone'
        form.fields['password'].widget = django_forms.PasswordInput()
        form.fields['password'].required = False
        form.fields['password'].help_text = 'Lascia vuoto per mantenere la password attuale.'
        form.fields['store_type'].label = 'Tipo di punto vendita'
        form.fields['store_type'].help_text = 'I punti vendita Rione ricevono solo le promo contrassegnate RIONE.'
        return form

    def form_valid(self, form):
        if not form.cleaned_data.get('password'):
            form.instance.password = Supermarket.objects.get(pk=form.instance.pk).password
        if 'sync_storages' not in self.request.POST:
            return super().form_valid(form)
        # Save first so the sync uses the credentials just typed
        self.object = form.save()
        from ..tasks import sync_storages_task
        result = sync_storages_task.apply_async(args=[self.object.pk])
        return redirect('task-progress', task_id=result.id)

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
        messages.success(request, f"Punto vendita '{self.get_object().name}' eliminato con successo!")
        return super().delete(request, *args, **kwargs)


@login_required
def closure_calendar_view(request, pk):
    """Calendar UI for managing closure days for a supermarket."""
    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)
    return render(request, 'supermarkets/closures.html', {'supermarket': supermarket})


@login_required
def closure_api_view(request, pk):
    """JSON API for reading and modifying closure days."""
    from ..models import RecurringClosure, OneTimeClosure, RecurringClosureOverride
    import datetime

    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)

    if request.method == 'GET':
        year = int(request.GET.get('year', date.today().year))

        recurring = list(
            supermarket.recurring_closures.values('month', 'day', 'label')
        )
        onetime = list(
            supermarket.onetime_closures.filter(
                date__year=year
            ).values('date', 'label')
        )
        overrides = list(
            supermarket.closure_overrides.filter(
                year=year
            ).values('month', 'day', 'year')
        )

        # Serialize dates
        for entry in onetime:
            entry['date'] = entry['date'].isoformat()

        return JsonResponse({
            'recurring': recurring,
            'onetime': onetime,
            'overrides': overrides,
        })

    if request.method == 'POST':
        try:
            body = json.loads(request.body)
        except (json.JSONDecodeError, ValueError):
            return JsonResponse({'error': 'Invalid JSON'}, status=400)

        action = body.get('action')

        if action == 'add_recurring':
            month, day, label = body.get('month'), body.get('day'), body.get('label', '')
            obj, _ = RecurringClosure.objects.get_or_create(
                supermarket=supermarket, month=month, day=day,
                defaults={'label': label}
            )
            if obj.label != label:
                obj.label = label
                obj.save()
            return JsonResponse({'ok': True})

        elif action == 'remove_recurring':
            RecurringClosure.objects.filter(
                supermarket=supermarket, month=body.get('month'), day=body.get('day')
            ).delete()
            # Also remove any overrides for this month/day
            RecurringClosureOverride.objects.filter(
                supermarket=supermarket, month=body.get('month'), day=body.get('day')
            ).delete()
            return JsonResponse({'ok': True})

        elif action == 'add_onetime':
            date_str = body.get('date')
            label = body.get('label', '')
            try:
                d = datetime.date.fromisoformat(date_str)
            except (ValueError, TypeError):
                return JsonResponse({'error': 'Invalid date'}, status=400)
            obj, _ = OneTimeClosure.objects.get_or_create(
                supermarket=supermarket, date=d,
                defaults={'label': label}
            )
            if obj.label != label:
                obj.label = label
                obj.save()
            return JsonResponse({'ok': True})

        elif action == 'remove_onetime':
            date_str = body.get('date')
            try:
                d = datetime.date.fromisoformat(date_str)
            except (ValueError, TypeError):
                return JsonResponse({'error': 'Invalid date'}, status=400)
            OneTimeClosure.objects.filter(supermarket=supermarket, date=d).delete()
            return JsonResponse({'ok': True})

        elif action == 'add_override':
            RecurringClosureOverride.objects.get_or_create(
                supermarket=supermarket,
                month=body.get('month'),
                day=body.get('day'),
                year=body.get('year'),
            )
            return JsonResponse({'ok': True})

        elif action == 'remove_override':
            RecurringClosureOverride.objects.filter(
                supermarket=supermarket,
                month=body.get('month'),
                day=body.get('day'),
                year=body.get('year'),
            ).delete()
            return JsonResponse({'ok': True})

        return JsonResponse({'error': 'Unknown action'}, status=400)

    return JsonResponse({'error': 'Method not allowed'}, status=405)


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
