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
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.owner.username})"


class Storage(models.Model):
    """Storage/warehouse within a supermarket"""
    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE, related_name='storages')
    name = models.CharField(max_length=255)
    settore = models.CharField(max_length=255, help_text="Internal settore name from DB")
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
        Calculate coverage (days from order day to next delivery day, inclusive).
        
        Formula: Coverage = (next_order_day + next_delivery_offset) - order_day_index + 1
        
        Example:
        - Order Monday (index=0), delivery Tuesday (offset=1)
        - Next order Friday (index=4), delivery Sunday (offset=2)
        - Coverage = (4 + 2) - 0 + 1 = 7 days
        
        Args:
            order_day_index: 0=Monday, 6=Sunday
            
        Returns:
            int: Number of days to cover
        """
        order_days = self.get_order_days()
        
        if not order_days:
            return 0
        
        if len(order_days) == 1:
            # Only one order per week - default to 9 days
            return 9
        
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
        
        # Coverage = days from current order day to next delivery day (inclusive)
        coverage = (next_order_day + next_delivery_offset) - order_day_index + 1
        
        return coverage

    def get_schedule_summary(self):
        """Returns human-readable schedule summary"""
        order_days = self.get_order_days()
        if not order_days:
            return "No orders scheduled"
        
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
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
    
    storage = models.ForeignKey(Storage, on_delete=models.CASCADE, related_name='restock_logs')
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

    class Meta:
        ordering = ['-started_at']

    def set_results(self, results_dict):
        """Store results as JSON"""
        self.results = json.dumps(results_dict)
    
    def get_results(self):
        """Retrieve results from JSON"""
        if self.results:
            return json.loads(self.results)
        return {}
    
    def can_retry(self):
        """Check if this log can be retried"""
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

    def __str__(self):
        return f"{self.storage.name} - {self.started_at.strftime('%Y-%m-%d %H:%M')}"