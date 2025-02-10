from django import forms
from .models import RestockSchedule

class RestockScheduleForm(forms.ModelForm):
    class Meta:
        model = RestockSchedule
        fields = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    def clean(self):
        cleaned_data = super().clean()
        if not any(cleaned_data.values()):
            raise forms.ValidationError("Please select at least one restock day.")
        return cleaned_data
