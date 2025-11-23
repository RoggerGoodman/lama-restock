# LamApp/LamApp/supermarkets/forms.py
from django import forms
from .models import RestockSchedule, Blacklist, BlacklistEntry, Storage


class RestockScheduleForm(forms.ModelForm):
    """Form for editing restock schedules"""
    
    class Meta:
        model = RestockSchedule
        fields = [
            'monday', 'tuesday', 'wednesday', 'thursday', 
            'friday', 'saturday', 'sunday', 
            'restock_time', 'base_coverage'
        ]
        widgets = {
            'restock_time': forms.TimeInput(attrs={'type': 'time'}),
            'base_coverage': forms.NumberInput(attrs={
                'step': '0.5',
                'min': '0',
                'max': '30'
            }),
        }
        help_texts = {
            'monday': 'When to restock on Mondays',
            'tuesday': 'When to restock on Tuesdays',
            'wednesday': 'When to restock on Wednesdays',
            'thursday': 'When to restock on Thursdays',
            'friday': 'When to restock on Fridays',
            'saturday': 'When to restock on Saturdays',
            'sunday': 'When to restock on Sundays',
            'restock_time': 'What time should the restock check run?',
            'base_coverage': 'Default coverage in days (will be calculated dynamically based on schedule)',
        }


class StorageForm(forms.ModelForm):
    """Form for creating/editing storages"""
    
    class Meta:
        model = Storage
        fields = ['name', 'settore']
        widgets = {
            'name': forms.TextInput(attrs={'placeholder': 'e.g., RIANO GENERI VARI'}),
            'settore': forms.TextInput(attrs={'placeholder': 'e.g., RIANO GENERI VARI'}),
        }


class BlacklistForm(forms.ModelForm):
    """Form for creating blacklists"""
    
    class Meta:
        model = Blacklist
        fields = ['storage', 'name', 'description']
        widgets = {
            'name': forms.TextInput(attrs={'placeholder': 'e.g., Seasonal Products'}),
            'description': forms.Textarea(attrs={
                'rows': 3,
                'placeholder': 'Optional description of this blacklist'
            }),
        }


class BlacklistEntryForm(forms.ModelForm):
    """Form for adding products to blacklist"""
    
    class Meta:
        model = BlacklistEntry
        fields = ['product_code', 'product_var']
        widgets = {
            'product_code': forms.NumberInput(attrs={'placeholder': 'Product code'}),
            'product_var': forms.NumberInput(attrs={'placeholder': 'Variant', 'value': 1}),
        }
        
    def clean(self):
        cleaned_data = super().clean()
        code = cleaned_data.get('product_code')
        var = cleaned_data.get('product_var')
        
        # Check for duplicate within the same blacklist (will be set in view)
        if hasattr(self.instance, 'blacklist'):
            existing = BlacklistEntry.objects.filter(
                blacklist=self.instance.blacklist,
                product_code=code,
                product_var=var
            ).exists()
            
            if existing:
                raise forms.ValidationError(
                    f"Product {code}.{var} is already in this blacklist."
                )
        
        return cleaned_data


class ManualRestockForm(forms.Form):
    """Form for manually triggering restock with custom coverage"""
    
    coverage = forms.DecimalField(
        label="Coverage (days)",
        min_value=0,
        max_value=30,
        decimal_places=1,
        required=False,
        help_text="Leave empty to use schedule's calculated coverage",
        widget=forms.NumberInput(attrs={'step': '0.5', 'placeholder': 'Auto'})
    )
