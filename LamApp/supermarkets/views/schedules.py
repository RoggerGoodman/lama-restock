"""Restock schedules and schedule exceptions."""

import json
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, UpdateView, DeleteView
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse

from ..models import Storage, RestockSchedule, ScheduleException
from ..forms import RestockScheduleForm, DayWeightsForm


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
        context["intraday_curve_json"] = json.dumps(storage.supermarket.intraday_curve or [])
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
        messages.success(request, "Agenda eliminata con successo!")
        return super().delete(request, *args, **kwargs)


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
            exception_type = body.get('exception_type', 'none')
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
