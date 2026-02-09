# LamApp/supermarkets/models.py
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import json

class Supermarket(models.Model):
    """Main supermarket entity - each user can manage multiple supermarkets"""
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='supermarkets')
    name = models.CharField(max_length=255, unique=True)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)
    # PAC2000A client parameters (discovered via gather_client_data)
    id_cliente = models.IntegerField(null=True, blank=True, help_text="IDCliente from PAC2000A")
    id_azienda = models.IntegerField(null=True, blank=True, help_text="IDAzienda from PAC2000A")
    id_marchio = models.IntegerField(null=True, blank=True, help_text="IDMarchio from PAC2000A")
    id_clienti_canale = models.IntegerField(null=True, blank=True, help_text="IDClientiCanale from PAC2000A")
    id_clienti_area = models.IntegerField(null=True, blank=True, help_text="IDClientiArea from PAC2000A")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Day weights for coverage calculation (1.0 = normal, 0.9 = less traffic, 1.2 = more traffic)
    monday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Monday")
    tuesday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Tuesday")
    wednesday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Wednesday")
    thursday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Thursday")
    friday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Friday")
    saturday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Saturday")
    sunday_weight = models.DecimalField(max_digits=3, decimal_places=2, default=1.0, help_text="Traffic weight for Sunday")

    class Meta:
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.owner.username})"

    def get_day_weight(self, day_index):
        """Get traffic weight for a specific day (0=Monday, 6=Sunday)"""
        weight_fields = [
            'monday_weight', 'tuesday_weight', 'wednesday_weight',
            'thursday_weight', 'friday_weight', 'saturday_weight', 'sunday_weight'
        ]
        return float(getattr(self, weight_fields[day_index]))

    def get_all_day_weights(self):
        """Return dict of all day weights for JSON serialization"""
        return {
            'monday': float(self.monday_weight),
            'tuesday': float(self.tuesday_weight),
            'wednesday': float(self.wednesday_weight),
            'thursday': float(self.thursday_weight),
            'friday': float(self.friday_weight),
            'saturday': float(self.saturday_weight),
            'sunday': float(self.sunday_weight),
        }


class Storage(models.Model):
    """Storage/warehouse within a supermarket"""
    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE, related_name='storages')
    name = models.CharField(max_length=255)
    settore = models.CharField(max_length=255, help_text="Internal settore name from DB")
    id_cod_mag = models.IntegerField(null=True, blank=True, help_text="Warehouse code from PAC2000A (IDCodMag)")
    last_list_update = models.DateTimeField(null=True, blank=True, help_text="Last time product list was updated")
    
    class Meta:
        unique_together = ('supermarket', 'name')
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.supermarket.name})"


WEEKDAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

class RestockSchedule(models.Model):
    """
    Restock schedule with configurable delivery offsets.
    - Each day can be enabled/disabled for ordering
    - Each enabled day has a delivery offset (0=same day, 1=next day, 2=two days later)
    - Coverage is calculated from order day to next delivery day
    """
    storage = models.OneToOneField(Storage, on_delete=models.CASCADE, related_name='schedule')
    
    # Order day flags
    monday = models.BooleanField(default=False, help_text="Order on Monday")
    tuesday = models.BooleanField(default=False, help_text="Order on Tuesday")
    wednesday = models.BooleanField(default=False, help_text="Order on Wednesday")
    thursday = models.BooleanField(default=False, help_text="Order on Thursday")
    friday = models.BooleanField(default=False, help_text="Order on Friday")
    saturday = models.BooleanField(default=False, help_text="Order on Saturday")
    sunday = models.BooleanField(default=False, help_text="Order on Sunday")
    
    # Delivery offsets (0=same day, 1=next day, 2=two days later, etc.)
    monday_delivery_offset = models.IntegerField(
        default=1,
        help_text="Days until delivery after Monday order (0=same day, 1=next day, etc.)"
    )
    tuesday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Tuesday order")
    wednesday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Wednesday order")
    thursday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Thursday order")
    friday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Friday order")
    saturday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Saturday order")
    sunday_delivery_offset = models.IntegerField(default=1, help_text="Days until delivery after Sunday order")

    def get_order_days(self):
        """Returns list of day indices where orders happen (0=Monday, 6=Sunday)"""
        weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        return [i for i, day_name in enumerate(weekday_fields) if getattr(self, day_name)]
    
    def get_delivery_offset(self, order_day_index):
        """Get delivery offset for a specific order day"""
        weekday_fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        offset_field = f"{weekday_fields[order_day_index]}_delivery_offset"
        return getattr(self, offset_field)
    
    def get_delivery_day(self, order_day_index):
        """
        Calculate delivery day index for a given order day.
        Returns day index (0-6), wrapping to next week if needed.
        """
        offset = self.get_delivery_offset(order_day_index)
        return (order_day_index + offset) % 7

    def calculate_coverage_for_day(self, order_day_index):
        """
        Calculate weighted coverage (sum of day weights from order day to next delivery).

        Uses supermarket's day weights instead of just counting days.
        Example: If covering Mon→Thu with weights [0.9, 1.0, 1.0, 1.2] = 4.1 weighted days

        Args:
            order_day_index: 0=Monday, 6=Sunday

        Returns:
            float: Weighted number of days to cover
        """
        order_days = self.get_order_days()

        if not order_days:
            return 0

        if len(order_days) == 1:
            # Only one order per week - default to 9 days weighted
            return self._calculate_weighted_days(order_day_index, 9)

        # Find the next order day after current order_day_index
        next_order_day = None
        for day in order_days:
            if day > order_day_index:
                next_order_day = day
                break

        # If no order day found after current, wrap around to first day of next week
        if next_order_day is None:
            next_order_day = order_days[0] + 7

        # Get delivery offset for the next order
        next_delivery_offset = self.get_delivery_offset(next_order_day % 7)

        # Number of days to cover (unweighted)
        num_days = (next_order_day + next_delivery_offset) - order_day_index + 1

        # Calculate weighted coverage
        return self._calculate_weighted_days(order_day_index, num_days)

    def _calculate_weighted_days(self, start_day_index, num_days):
        """
        Sum the day weights for a period starting from start_day_index.

        Args:
            start_day_index: Starting day (0=Monday, 6=Sunday)
            num_days: Number of days to cover

        Returns:
            float: Sum of weights for the coverage period
        """
        supermarket = self.storage.supermarket
        weighted_sum = 0.0

        for i in range(num_days):
            day_index = (start_day_index + i) % 7
            weighted_sum += supermarket.get_day_weight(day_index)

        return round(weighted_sum, 2)

    def get_week_visual(self):
        """Returns visual data for each day of the week for template rendering"""
        day_names_short = ['Lun', 'Mar', 'Mer', 'Giv', 'Ven', 'Sab', 'Dom']
        order_days = self.get_order_days()

        # Calculate which days have deliveries
        delivery_days = set()
        for order_day in order_days:
            delivery_day = self.get_delivery_day(order_day)
            delivery_days.add(delivery_day)

        result = []
        for i in range(7):
            result.append({
                'short': day_names_short[i],
                'is_order': i in order_days,
                'is_delivery': i in delivery_days,
            })
        return result

    def get_schedule_summary(self):
        """Returns human-readable schedule summary"""
        order_days = self.get_order_days()
        if not order_days:
            return "No orders scheduled"

        day_names = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']

        # Build summary with delivery info
        schedule_parts = []
        for day_idx in order_days:
            offset = self.get_delivery_offset(day_idx)
            delivery_day_idx = (day_idx + offset) % 7

            if offset == 0:
                delivery_text = "same day"
            elif offset == 1:
                delivery_text = f"→{day_names[delivery_day_idx]}"
            else:
                delivery_text = f"→{day_names[delivery_day_idx]}"

            schedule_parts.append(f"{day_names[day_idx]}{delivery_text}")

        # Calculate coverages
        coverages = [self.calculate_coverage_for_day(day) for day in order_days]

        return f"Orders: {', '.join(schedule_parts)} | Coverage: {coverages} days"

    def __str__(self):
        return f"Schedule for {self.storage.name}"


class ScheduleException(models.Model):
    """
    Exception to the standard weekly schedule for specific dates.
    Used for holidays, special events, or custom adjustments.
    """
    EXCEPTION_TYPE_CHOICES = [
        ('skip', 'Skip Order'),  # Don't order on this day
        ('add', 'Add Order'),    # Order on this day even if not in weekly schedule
        ('modify', 'Modify Delivery'),  # Change delivery offset for this day
    ]

    schedule = models.ForeignKey(RestockSchedule, on_delete=models.CASCADE, related_name='exceptions')
    date = models.DateField(help_text="The specific date this exception applies to")
    exception_type = models.CharField(max_length=10, choices=EXCEPTION_TYPE_CHOICES, default='skip')
    delivery_offset = models.IntegerField(
        null=True, blank=True,
        help_text="Custom delivery offset for 'add' or 'modify' types"
    )
    skip_sale = models.BooleanField(default=False, help_text="Skip products on sale for this order day")
    note = models.CharField(max_length=255, blank=True, help_text="Optional note (e.g., 'Natale', 'Ferragosto')")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('schedule', 'date')
        ordering = ['date']
        indexes = [
            models.Index(fields=['schedule', 'date']),
        ]

    def __str__(self):
        return f"{self.get_exception_type_display()} on {self.date} ({self.schedule.storage.name})"


class Blacklist(models.Model):
    """Named blacklist for a storage"""
    storage = models.ForeignKey(Storage, on_delete=models.CASCADE, related_name='blacklists')
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('storage', 'name')
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.storage.name})"


class BlacklistEntry(models.Model):
    """Individual product in a blacklist"""
    blacklist = models.ForeignKey(Blacklist, on_delete=models.CASCADE, related_name='entries')
    product_code = models.IntegerField()
    product_var = models.SmallIntegerField(default=1)

    class Meta:
        unique_together = ('blacklist', 'product_code', 'product_var')
        ordering = ['product_code', 'product_var']

    def __str__(self):
        return f"{self.product_code}.{self.product_var}"


class RestockLog(models.Model):
    """Log of restock operations with checkpoint tracking"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    STAGE_CHOICES = [
        ('pending', 'Pending Start'),
        ('updating_stats', 'Updating Product Stats'),
        ('stats_updated', 'Stats Updated'),
        ('calculating_order', 'Calculating Order'),
        ('order_calculated', 'Order Calculated'),
        ('executing_order', 'Executing Order'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    # NEW: Operation type tracking
    OPERATION_TYPE_CHOICES = [
        ('full_restock', 'Full Restock Order'),
        ('stats_update', 'Statistics Update Only'),
        ('list_update', 'Product List Update'),
        ('order_execution', 'Order Execution Only'),
        ('verification', 'Stock Verification'),
        ('cluster_assignment', 'Cluster Assignment'),
        ('product_addition', 'Product Addition'),
    ]
    
    storage = models.ForeignKey(Storage, on_delete=models.CASCADE, related_name='restock_logs')
    operation_type = models.CharField(
        max_length=20, 
        choices=OPERATION_TYPE_CHOICES, 
        default='full_restock',
        help_text="Type of operation performed"
    )
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Current stage for checkpoint recovery
    current_stage = models.CharField(max_length=30, choices=STAGE_CHOICES, default='pending')
    
    # Stage timestamps
    stats_updated_at = models.DateTimeField(null=True, blank=True)
    order_calculated_at = models.DateTimeField(null=True, blank=True)
    order_executed_at = models.DateTimeField(null=True, blank=True)
    
    # Retry tracking
    retry_count = models.IntegerField(default=0)
    max_retries = models.IntegerField(default=3)
    
    # Statistics
    total_products = models.IntegerField(default=0)
    products_ordered = models.IntegerField(default=0)
    total_packages = models.IntegerField(default=0)
    
    # Detailed results (JSON)
    results = models.TextField(blank=True, help_text="JSON with detailed results")
    error_message = models.TextField(blank=True)
    
    coverage_used = models.DecimalField(max_digits=4, decimal_places=1, null=True)

    # Dismiss failed log warnings from dashboard
    is_dismissed = models.BooleanField(default=False)

    # NEW: Helper methods for operation type display
    def get_operation_icon(self):
        """Return Bootstrap icon class for operation type"""
        icons = {
            'full_restock': 'bi-box-seam',
            'stats_update': 'bi-arrow-repeat',
            'list_update': 'bi-download',
            'order_execution': 'bi-send',
            'verification': 'bi-clipboard-check',
            'cluster_assignment': 'bi-folder',
            'product_addition': 'bi-plus-circle',
        }
        return icons.get(self.operation_type, 'bi-file-text')
    
    def get_operation_color(self):
        """Return Bootstrap color class for operation type"""
        colors = {
            'full_restock': 'primary',
            'stats_update': 'info',
            'list_update': 'warning',
            'order_execution': 'success',
            'verification': 'secondary',
            'cluster_assignment': 'dark',
            'product_addition': 'success',
        }
        return colors.get(self.operation_type, 'secondary')
    
    def get_duration(self):
        """Calculate operation duration"""
        if self.completed_at and self.started_at:
            delta = self.completed_at - self.started_at
            total_seconds = int(delta.total_seconds())
            
            if total_seconds < 60:
                return f"{total_seconds}s"
            elif total_seconds < 3600:
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                return f"{minutes}m {seconds}s"
            else:
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                return f"{hours}h {minutes}m"
        return "—"
    
    def is_stale(self):
        """Check if log status might be stale (processing for >30 min)"""
        if self.status == 'processing':
            from django.utils import timezone
            age = timezone.now() - self.started_at
            return age.total_seconds() > 1800  # 30 minutes
        return False

    def set_results(self, results_dict):
        """Store results as JSON"""
        if results_dict is None:
            self.results = '{}'
        else:
            self.results = json.dumps(results_dict)
    
    def get_results(self):
        """Retrieve results from JSON"""
        if not self.results or self.results.strip() == '':
            return {}
        
        try:
            parsed = json.loads(self.results)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def can_retry(self):
        """Check if this log can be retried"""
        if self.status == 'completed' and self.error_message and 'timeout' in self.error_message.lower():
            return False
        
        return self.retry_count < self.max_retries and self.status == 'failed'
    
    def get_stage_display_info(self):
        """Get human-readable stage info with progress"""
        stage_info = {
            'pending': {'label': 'Pending', 'progress': 0, 'icon': 'clock'},
            'updating_stats': {'label': 'Updating Stats...', 'progress': 10, 'icon': 'download'},
            'stats_updated': {'label': 'Stats Updated', 'progress': 50, 'icon': 'check-circle'},
            'calculating_order': {'label': 'Calculating Order...', 'progress': 60, 'icon': 'calculator'},
            'order_calculated': {'label': 'Order Calculated', 'progress': 70, 'icon': 'check-circle'},
            'executing_order': {'label': 'Placing Order...', 'progress': 80, 'icon': 'send'},
            'completed': {'label': 'Completed', 'progress': 100, 'icon': 'check-circle-fill'},
            'failed': {'label': 'Failed', 'progress': 0, 'icon': 'x-circle'},
        }
        return stage_info.get(self.current_stage, stage_info['pending'])
    
    class Meta:
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['storage', 'status', 'current_stage']),
            models.Index(fields=['operation_type', 'status']),  # NEW: For filtering
        ]

    def __str__(self):
        return f"{self.get_operation_type_display()} - {self.storage.name} - {self.started_at.strftime('%Y-%m-%d %H:%M')}"


class Recipe(models.Model):
    """Recipe with ingredients from products and external items for cost calculation"""
    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE, related_name='recipes')
    name = models.CharField(max_length=255)
    family = models.CharField(max_length=100, blank=True, db_index=True,
        help_text="Optional grouping category (e.g., 'Pizze', 'Panini', 'Dolci')")
    is_base = models.BooleanField(default=False,
        help_text="Flag this as a base recipe that others can build upon")
    base_recipe = models.ForeignKey('self', on_delete=models.SET_NULL,
        null=True, blank=True, related_name='derived_recipes',
        help_text="Optional base recipe to inherit ingredients from")
    base_multiplier = models.DecimalField(max_digits=5, decimal_places=2, default=1,
        help_text="How many units of the base recipe to use (e.g., 0.5 = half, 2 = double)")
    selling_price = models.DecimalField(max_digits=10, decimal_places=2, default=0,
        help_text="Selling price for this recipe")
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('supermarket', 'name')
        ordering = ['family', 'name']
        indexes = [
            models.Index(fields=['supermarket', 'family']),
            models.Index(fields=['supermarket', 'is_base']),
        ]

    def __str__(self):
        return f"{self.name} ({self.supermarket.name})"

    def get_total_cost(self):
        """Calculate total cost from all items including base recipe"""
        product_cost = sum(item.get_cost() for item in self.product_items.all())
        external_cost = sum(item.get_cost() for item in self.external_items.all())
        base_cost = (self.base_recipe.get_total_cost() * float(self.base_multiplier)) if self.base_recipe else 0
        return product_cost + external_cost + base_cost

    def get_margin_percentage(self):
        """Calculate margin percentage"""
        total_cost = self.get_total_cost()
        if self.selling_price > 0 and total_cost > 0:
            return ((float(self.selling_price) - total_cost) / float(self.selling_price)) * 100
        return 0

    def get_margin_absolute(self):
        """Calculate absolute margin"""
        return float(self.selling_price) - self.get_total_cost()


class RecipeProductItem(models.Model):
    """Ingredient from the products table"""
    recipe = models.ForeignKey(Recipe, on_delete=models.CASCADE, related_name='product_items')
    product_code = models.IntegerField()
    product_var = models.SmallIntegerField(default=1)
    use_percentage = models.IntegerField(default=100,
        help_text="100 = 1 unit, 50 = 0.5 units, 150 = 1.5 units")
    cached_description = models.CharField(max_length=255, blank=True)
    cached_cost_std = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)

    class Meta:
        unique_together = ('recipe', 'product_code', 'product_var')
        ordering = ['product_code', 'product_var']

    def __str__(self):
        return f"{self.product_code}.{self.product_var} @ {self.use_percentage}%"

    def get_cost(self):
        """Cost = cost_std * (use_percentage / 100)"""
        if self.cached_cost_std:
            return float(self.cached_cost_std) * (self.use_percentage / 100)
        return 0

    def get_display_name(self):
        return self.cached_description or f"Product {self.product_code}.{self.product_var}"


class RecipeExternalItem(models.Model):
    """Ingredient NOT in the products table (user-defined)"""
    recipe = models.ForeignKey(Recipe, on_delete=models.CASCADE, related_name='external_items')
    name = models.CharField(max_length=255)
    unit_cost = models.DecimalField(max_digits=10, decimal_places=4,
        help_text="Cost per unit of this item")
    use_percentage = models.IntegerField(default=100,
        help_text="100 = 1 unit, 50 = 0.5 units, 150 = 1.5 units")

    class Meta:
        ordering = ['name']

    def __str__(self):
        return f"{self.name} @ {self.use_percentage}%"

    def get_cost(self):
        """Cost = unit_cost * (use_percentage / 100)"""
        return float(self.unit_cost) * (self.use_percentage / 100)


class RecipeCostAlert(models.Model):
    """
    Alert generated when product cost changes affect recipe margins.
    Created during scheduled list updates to notify users of cost changes.
    """
    recipe = models.ForeignKey(Recipe, on_delete=models.CASCADE, related_name='cost_alerts')
    product_code = models.IntegerField()
    product_var = models.SmallIntegerField(default=1)
    product_description = models.CharField(max_length=255, blank=True)
    old_cost = models.DecimalField(max_digits=10, decimal_places=4)
    new_cost = models.DecimalField(max_digits=10, decimal_places=4)
    old_recipe_cost = models.DecimalField(max_digits=10, decimal_places=2,
        help_text="Recipe total cost before the change")
    new_recipe_cost = models.DecimalField(max_digits=10, decimal_places=2,
        help_text="Recipe total cost after the change")
    old_margin_pct = models.DecimalField(max_digits=5, decimal_places=2,
        help_text="Margin percentage before the change")
    new_margin_pct = models.DecimalField(max_digits=5, decimal_places=2,
        help_text="Margin percentage after the change")
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['recipe', 'is_read']),
            models.Index(fields=['is_read', 'created_at']),
        ]

    def __str__(self):
        return f"Cost alert for {self.recipe.name}: {self.product_code}.{self.product_var}"

    @property
    def cost_change_pct(self):
        """Percentage change in ingredient cost"""
        if self.old_cost > 0:
            return ((float(self.new_cost) - float(self.old_cost)) / float(self.old_cost)) * 100
        return 0

    @property
    def margin_change(self):
        """Absolute change in margin percentage"""
        return float(self.new_margin_pct) - float(self.old_margin_pct)

    @property
    def is_cost_increase(self):
        """True if the ingredient cost increased"""
        return self.new_cost > self.old_cost


class StockValueSnapshot(models.Model):
    """
    Snapshot of total stock value for a supermarket.
    Used to track inventory value over time for margin calculations.
    Maximum 36 snapshots stored per supermarket (auto-cleanup of oldest).
    """
    MAX_SNAPSHOTS = 36

    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE, related_name='stock_snapshots')
    created_at = models.DateTimeField(default=timezone.now)
    is_manual = models.BooleanField(default=False, help_text="True if manually triggered, False if automatic monthly")
    total_value = models.DecimalField(max_digits=14, decimal_places=2)
    category_breakdown = models.JSONField(default=list, help_text="List of {name, value, percentage}")

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['supermarket', '-created_at']),
        ]

    def __str__(self):
        snapshot_type = "Manual" if self.is_manual else "Auto"
        return f"{self.supermarket.name} - {snapshot_type} - {self.created_at.strftime('%Y-%m-%d')}"

    @classmethod
    def create_snapshot(cls, supermarket, total_value, category_breakdown, is_manual=False):
        """
        Create a snapshot and enforce the 36-snapshot limit.
        Deletes oldest snapshots if limit exceeded.
        """
        # Create the new snapshot
        snapshot = cls.objects.create(
            supermarket=supermarket,
            total_value=total_value,
            category_breakdown=category_breakdown,
            is_manual=is_manual
        )

        # Enforce limit - delete oldest if over MAX_SNAPSHOTS
        existing_count = cls.objects.filter(supermarket=supermarket).count()
        if existing_count > cls.MAX_SNAPSHOTS:
            # Get IDs of snapshots to keep (newest MAX_SNAPSHOTS)
            keep_ids = cls.objects.filter(
                supermarket=supermarket
            ).order_by('-created_at').values_list('id', flat=True)[:cls.MAX_SNAPSHOTS]

            # Delete the rest
            cls.objects.filter(supermarket=supermarket).exclude(id__in=list(keep_ids)).delete()

        return snapshot