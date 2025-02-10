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

class RestockSchedule(models.Model):
    category = models.ForeignKey(Category, on_delete=models.CASCADE)  # Now linked to Category, not Supermarket

    monday = models.BooleanField(default=False)
    tuesday = models.BooleanField(default=False)
    wednesday = models.BooleanField(default=False)
    thursday = models.BooleanField(default=False)
    friday = models.BooleanField(default=False)
    saturday = models.BooleanField(default=False)
    sunday = models.BooleanField(default=False)

    restock_intervals = models.CharField(max_length=20, blank=True, editable=False)

    def calculate_intervals(self):
        selected_days = []
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

        for i, day in enumerate(days):
            if getattr(self, day):  
                selected_days.append(i)

        if len(selected_days) > 1:
            intervals = [selected_days[i] - selected_days[i - 1] for i in range(1, len(selected_days))]
            intervals.append(7 - selected_days[-1] + selected_days[0])  # Wrap around to the first selected day
            return "/".join(map(str, intervals))
        return ""

    def save(self, *args, **kwargs):
        self.restock_intervals = self.calculate_intervals()
        super().save(*args, **kwargs)

    def __str__(self):
        selected_days = [day.capitalize() for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"] if getattr(self, day)]
        return f"Restock for {self.category}: {', '.join(selected_days)} (Intervals: {self.restock_intervals})"

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