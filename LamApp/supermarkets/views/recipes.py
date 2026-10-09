"""Recipes and recipe cost alerts."""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, DeleteView
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import (
    Supermarket, Recipe, RecipeProductItem, RecipeExternalItem, RecipeCostAlert,
)
from ..services import RestockService

logger = logging.getLogger(__name__)


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
