from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator
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
    """Storage/warehouse within a supermarket (e.g., RIANO GENERI VARI, SURGELATI, DEPERIBILI)"""
    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE, related_name='storages')
    name = models.CharField(max_length=255)
    settore = models.CharField(max_length=255, help_text="Internal settore name from DB")
    
    class Meta:
        unique_together = ('supermarket', 'name')
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.supermarket.name})"


DAY_CHOICES = [
    ('0', 'Off'),
    ('1', 'Early Morning'),
    ('2', 'Late Afternoon'),
]
WEEKDAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

class RestockSchedule(models.Model):
    """Restock schedule for a specific storage"""
    storage = models.OneToOneField(Storage, on_delete=models.CASCADE, related_name='schedule')
    
    # Day configurations
    monday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    tuesday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    wednesday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    thursday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    friday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    saturday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    sunday = models.CharField(max_length=10, choices=DAY_CHOICES, default='0')
    
    # Time when restock check should run
    restock_time = models.TimeField(default='09:00', help_text="Time to run daily restock check")
    
    # Auto-calculated fields
    restock_intervals = models.CharField(max_length=50, blank=True, editable=False)
    
    # Base coverage (can be overridden dynamically)
    base_coverage = models.DecimalField(
        max_digits=4, 
        decimal_places=1, 
        default=4.5,
        validators=[MinValueValidator(0)],
        help_text="Base coverage in days"
    )

    def get_day_value(self, day_name):
        """Get the value for a specific day"""
        return getattr(self, day_name.lower())
    
    def set_day_value(self, day_name, value):
        """Set the value for a specific day"""
        setattr(self, day_name.lower(), value)

    def calculate_intervals(self):
        """Calculate intervals between restock days"""
        day_fields = [
            self.monday, self.tuesday, self.wednesday, 
            self.thursday, self.friday, self.saturday, self.sunday
        ]
        
        effective_times = []
        for i, value in enumerate(day_fields):
            if value != 'off':
                extra = 0.5 if value == 'late' else 0
                effective_times.append(i + extra)
        
        if len(effective_times) > 1:
            intervals = []
            for i in range(1, len(effective_times)):
                intervals.append(effective_times[i] - effective_times[i - 1])
            intervals.append(7 - effective_times[-1] + effective_times[0])
            return "/".join(map(str, intervals))
        return ""

    def calculate_coverage_for_day(self, order_day):
        """
        Calculate dynamic coverage based on days until next delivery.
        order_day: 0=Monday, 6=Sunday
        """
        day_fields = [
            self.monday, self.tuesday, self.wednesday, 
            self.thursday, self.friday, self.saturday, self.sunday
        ]
        
        # Find next delivery day
        restock_days = [i for i, val in enumerate(day_fields) if val != 'off']
        
        if not restock_days:
            return float(self.base_coverage)
        
        # Find days until next restock
        days_until_next = None
        for day in restock_days:
            if day > order_day:
                days_until_next = day - order_day
                break
        
        if days_until_next is None:
            # Wrap around to next week
            days_until_next = 7 - order_day + restock_days[0]
        
        # Add 1 for the delivery day itself
        return float(days_until_next + 1)

    def save(self, *args, **kwargs):
        self.restock_intervals = self.calculate_intervals()
        super().save(*args, **kwargs)

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
    """Log of restock operations"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    storage = models.ForeignKey(Storage, on_delete=models.CASCADE, related_name='restock_logs')
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
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

    def __str__(self):
        return f"{self.storage.name} - {self.started_at.strftime('%Y-%m-%d %H:%M')}"
