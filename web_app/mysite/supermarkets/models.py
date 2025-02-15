from django.db import models
from django.contrib.auth.models import User

class Supermarket(models.Model):
    owner = models.ForeignKey(User, on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)  # We might encrypt this later

    def __str__(self):
        return f"{self.name} ({self.owner.username})"

class Category(models.Model):
    supermarket = models.ForeignKey(Supermarket, on_delete=models.CASCADE)  # Each supermarket has its own categories
    name = models.CharField(max_length=255)

    class Meta:
        unique_together = ('supermarket', 'name')  # Ensure no duplicate categories for the same supermarket

    def __str__(self):
        return f"{self.name} ({self.supermarket.name})"

DAY_CHOICES = (
    ('off', 'Off'),
    ('early', 'Early'),
    ('late', 'Late'),
)

class RestockSchedule(models.Model):
    category = models.ForeignKey(Category, on_delete=models.CASCADE)  # Linked to Category

    # For each day, the user can choose whether the restock is off, early, or late.
    monday    = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    tuesday   = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    wednesday = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    thursday  = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    friday    = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    saturday  = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')
    sunday    = models.CharField(max_length=10, choices=DAY_CHOICES, default='off')

    # This field stores a string representation of the intervals (calculated automatically).
    restock_intervals = models.CharField(max_length=20, blank=True, editable=False)

    def calculate_intervals(self):
        """
        Compute the intervals between restock days using effective day values.
        If a day is selected as "late", it adds an extra 0.5 to that day's value.
        For example, if Monday is "late" its effective value is 0.5 (instead of 0).
        The method returns a string with the intervals (separated by "/").
        """
        # Create a list of effective day values for the days that are not "off"
        day_fields = [
            self.monday,
            self.tuesday,
            self.wednesday,
            self.thursday,
            self.friday,
            self.saturday,
            self.sunday,
        ]
        effective_times = []
        for i, value in enumerate(day_fields):
            if value != 'off':
                # If the value is "late", add 0.5; if "early", add 0.
                extra = 0.5 if value == 'late' else 0
                effective_times.append(i + extra)
                
        # If more than one day is selected, compute intervals between successive effective times,
        # and account for wrap-around (making sure the cycle sums to 7)
        if len(effective_times) > 1:
            intervals = []
            for i in range(1, len(effective_times)):
                intervals.append(effective_times[i] - effective_times[i - 1])
            # Wrap-around interval: (7 - last effective time) + first effective time
            intervals.append(7 - effective_times[-1] + effective_times[0])
            return "/".join(map(str, intervals))
        return ""

    def save(self, *args, **kwargs):
        self.restock_intervals = self.calculate_intervals()
        super().save(*args, **kwargs)

    def __str__(self):
        # Create a list of day names with their chosen schedule (if not off)
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_values = [self.monday, self.tuesday, self.wednesday, self.thursday, self.friday, self.saturday, self.sunday]
        selected = [
            f"{day} ({'Late' if val=='late' else 'Early'})"
            for day, val in zip(day_names, day_values) if val != 'off'
        ]
        return f"Restock for {self.category}: {', '.join(selected)} (Intervals: {self.restock_intervals})"

class Blacklist(models.Model):
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)  # Add a name field

    def __str__(self):
        return f"{self.name} ({self.category.supermarket.name} - {self.category.name})"
    
class BlacklistEntry(models.Model):
    blacklist = models.ForeignKey(Blacklist, on_delete=models.CASCADE, related_name="entries")
    product_code = models.IntegerField()
    product_var = models.SmallIntegerField()

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)  # Save the new entry

        # Ensure all entries are sorted, but don't trigger another save loop
        self.blacklist.entries.all().order_by("product_code")

    def __str__(self):
        return f"{self.blacklist.name} - {self.product_code} - {self.product_var}"

    class Meta:
        ordering = ["product_code"]  # Ensure ordering is applied automatically