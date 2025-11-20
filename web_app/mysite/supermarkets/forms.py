from django import forms
from .models import RestockSchedule, Blacklist, BlacklistEntry

class RestockScheduleForm(forms.ModelForm):
    class Meta:
        model = RestockSchedule
        # We exclude restock_intervals because it's computed automatically.
        fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

class BlacklistForm(forms.ModelForm):
    class Meta:
        model = Blacklist
        fields = ['category', 'name']  # Adjust the fields as needed

class BlacklistEntryForm(forms.ModelForm):
    class Meta:
        model = BlacklistEntry
        fields = ['product_code', 'product_var']