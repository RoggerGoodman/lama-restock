"""Blacklists and their entries."""

from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, CreateView, DeleteView
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import Storage, Blacklist, BlacklistEntry
from ..forms import BlacklistForm, BlacklistEntryForm
from ..services import RestockService

logger = logging.getLogger(__name__)


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
                from ..services import RestockService
                with RestockService(blacklist.storage) as service:
                    cursor = service.db.cursor()
                    # Build query for all product codes
                    codes = [(e.product_code, e.product_var) for e in entries]
                    placeholders = ','.join(['(%s, %s)'] * len(codes))
                    params = [item for pair in codes for item in pair]

                    cursor.execute(f"""
                        SELECT p.cod, p.v, p.descrizione, p.purge_flag, ps.stock,
                            (
                                SELECT COALESCE(
                                    -- ord 1 is today, still in progress. Counting it would
                                    -- report a day without sales before the day is over.
                                    (SELECT (MIN(t.ord) - 2)::int
                                     FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY AS t(elem, ord)
                                     WHERE t.ord > 1 AND t.elem::numeric != 0),
                                    GREATEST(jsonb_array_length(ps.sales_sets) - 1, 0)
                                )
                            ) AS days_without_sales
                        FROM products p
                        LEFT JOIN product_stats ps ON ps.cod = p.cod AND ps.v = p.v
                        WHERE (p.cod, p.v) IN ({placeholders})
                    """, params)

                    # Create lookup dict
                    info = {(row['cod'], row['v']): row for row in cursor.fetchall()}

                    # Attach descriptions, stock and purge status to entries
                    for entry in entries:
                        row = info.get((entry.product_code, entry.product_var))
                        entry.description = row['descrizione'] if row else '-'
                        entry.stock = row['stock'] if row and row['stock'] is not None else 0
                        entry.purge_flag = bool(row['purge_flag']) if row else False
                        entry.days_without_sales = row['days_without_sales'] if row else None
                        # products row exists but product_stats was already cleared by a purge
                        entry.is_purged = row is not None and row['stock'] is None
                        # no matching products row at all (predates current purge_product() logic)
                        entry.is_missing = row is None
            except Exception:
                # If DB query fails, set empty descriptions
                for entry in entries:
                    entry.description = '-'
                    entry.stock = '-'
                    entry.purge_flag = False
                    entry.days_without_sales = None
                    entry.is_purged = False
                    entry.is_missing = False

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
        messages.success(request, f"Blacklist '{self.get_object().name}' eliminata con successo!")
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


@login_required
@require_POST
def blacklist_entry_reintegrate_view(request, pk):
    """
    AJAX: reintegrate a blacklisted product back into the assortment.
    Removes the BlacklistEntry and clears the product's purge_flag so it
    returns to normal restock consideration (undoing an "In fase di
    eliminazione" flag).
    """
    entry = get_object_or_404(
        BlacklistEntry,
        pk=pk,
        blacklist__storage__supermarket__owner=request.user,
    )
    storage = entry.blacklist.storage
    cod, var = entry.product_code, entry.product_var
    try:
        with RestockService(storage) as service:
            cursor = service.db.cursor()
            cursor.execute(
                "UPDATE products SET purge_flag = FALSE WHERE cod = %s AND v = %s",
                (cod, var),
            )
            service.db.conn.commit()
        entry.delete()
        logger.info(f"Reintegrated {cod}.{var} from blacklist (storage {storage.name})")
        return JsonResponse({'success': True})
    except Exception as e:
        logger.exception("Error reintegrating blacklist entry")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)
