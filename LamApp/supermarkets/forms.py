# LamApp/supermarkets/forms.py
from django import forms
from .models import RestockSchedule, Blacklist, BlacklistEntry, Storage, ListUpdateSchedule


class RestockScheduleForm(forms.ModelForm):
    """
    Simplified form for restock schedules.
    Just checkboxes for which days to order.
    """
    
    class Meta:
        model = RestockSchedule
        fields = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        widgets = {
            'monday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'tuesday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'wednesday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'thursday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'friday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'saturday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'sunday': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }
        help_texts = {
            'monday': 'Check to place orders on Monday (delivery Tuesday)',
            'tuesday': 'Check to place orders on Tuesday (delivery Wednesday)',
            'wednesday': 'Check to place orders on Wednesday (delivery Thursday)',
            'thursday': 'Check to place orders on Thursday (delivery Friday)',
            'friday': 'Check to place orders on Friday (delivery Saturday)',
            'saturday': 'Check to place orders on Saturday (delivery Sunday)',
            'sunday': 'Check to place orders on Sunday (delivery Monday)',
        }


class StorageForm(forms.ModelForm):
    """Form for creating/editing storages"""
    
    class Meta:
        model = Storage
        fields = ['name', 'settore']
        widgets = {
            'name': forms.TextInput(attrs={'placeholder': 'e.g., RIANO GENERI VARI', 'class': 'form-control'}),
            'settore': forms.TextInput(attrs={'placeholder': 'e.g., RIANO GENERI VARI', 'class': 'form-control'}),
        }


class BlacklistForm(forms.ModelForm):
    """Form for creating blacklists"""
    
    class Meta:
        model = Blacklist
        fields = ['storage', 'name', 'description']
        widgets = {
            'storage': forms.Select(attrs={'class': 'form-select'}),
            'name': forms.TextInput(attrs={'placeholder': 'e.g., Seasonal Products', 'class': 'form-control'}),
            'description': forms.Textarea(attrs={
                'rows': 3,
                'placeholder': 'Optional description of this blacklist',
                'class': 'form-control'
            }),
        }


class BlacklistEntryForm(forms.ModelForm):
    """Form for adding products to blacklist"""
    
    class Meta:
        model = BlacklistEntry
        fields = ['product_code', 'product_var']
        widgets = {
            'product_code': forms.NumberInput(attrs={'placeholder': 'Product code', 'class': 'form-control'}),
            'product_var': forms.NumberInput(attrs={'placeholder': 'Variant', 'value': 1, 'class': 'form-control'}),
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
        widget=forms.NumberInput(attrs={'step': '0.5', 'placeholder': 'Auto', 'class': 'form-control'})
    )

class ListUpdateScheduleForm(forms.ModelForm):
    """Form for configuring automatic list updates"""
    
    class Meta:
        model = ListUpdateSchedule
        fields = ['frequency', 'enabled']
        widgets = {
            'frequency': forms.Select(attrs={'class': 'form-select'}),
            'enabled': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }
        help_texts = {
            'frequency': 'How often to download and update product list',
            'enabled': 'Enable automatic updates',
        }


class PromoUploadForm(forms.Form):
    """Form for uploading promo PDF files"""
    
    pdf_file = forms.FileField(
        label="Promo PDF File",
        help_text="Upload the promo PDF file from supplier",
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.pdf'
        })
    )


class StockAdjustmentForm(forms.Form):
    """Form for adjusting stock manually"""
    
    product_code = forms.IntegerField(
        label="Product Code",
        min_value=1,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 12345'
        }),
        help_text="Enter the product code"
    )
    
    product_var = forms.IntegerField(
        label="Product Variant",
        min_value=1,
        initial=1,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'value': '1'
        }),
        help_text="Enter the variant (usually 1)"
    )
    
    adjustment = forms.IntegerField(
        label="Adjustment Amount",
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., -12 or +24'
        }),
        help_text="Positive to add stock, negative to remove stock"
    )
    
    reason = forms.ChoiceField(
        label="Reason for Adjustment",
        choices=[
            ('undelivered', 'Undelivered Package'),
            ('extra_delivery', 'Extra Package Delivered'),
            ('miscount', 'Inventory Miscount'),
            ('damaged', 'Damaged in Transit'),
            ('return', 'Customer Return'),
            ('other', 'Other')
        ],
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    
    notes = forms.CharField(
        label="Notes (Optional)",
        required=False,
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'rows': 3,
            'placeholder': 'Optional: Add any additional details about this adjustment'
        })
    )
    
    def clean(self):
        cleaned_data = super().clean()
        adjustment = cleaned_data.get('adjustment')
        
        if adjustment == 0:
            raise forms.ValidationError("Adjustment cannot be zero")
        
        return cleaned_data


class BulkStockAdjustmentForm(forms.Form):
    """Form for bulk stock adjustments via CSV"""
    
    csv_file = forms.FileField(
        label="CSV File",
        help_text="Upload a CSV with columns: Product Code, Variant, Adjustment, Reason",
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.csv'
        })
    )
    
    reason = forms.ChoiceField(
        label="Default Reason (for all adjustments)",
        choices=[
            ('undelivered', 'Undelivered Package'),
            ('extra_delivery', 'Extra Package Delivered'),
            ('miscount', 'Inventory Miscount'),
            ('damaged', 'Damaged in Transit'),
            ('return', 'Customer Return'),
            ('other', 'Other')
        ],
        widget=forms.Select(attrs={'class': 'form-select'})
    )

class RecordLossesForm(forms.Form):
    """Form for manually uploading and recording losses"""
    
    loss_type = forms.ChoiceField(
        label="Loss Type",
        choices=[
            ('broken', 'ROTTURE (Broken/Damaged)'),
            ('expired', 'SCADUTO (Expired)'),
            ('internal', 'UTILIZZO INTERNO (Internal Use)')
        ],
        widget=forms.Select(attrs={'class': 'form-select'}),
        help_text="Select the type of loss you want to record"
    )
    
    csv_file = forms.FileField(
        label="Loss CSV File",
        help_text="Upload the CSV file with loss data",
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.csv'
        })
    )

class SingleProductVerificationForm(forms.Form):
    """Form for verifying a single product"""
    
    product_code = forms.IntegerField(
        label="Product Code",
        min_value=1,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    
    product_var = forms.IntegerField(
        label="Variant",
        initial=1,
        min_value=1,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    
    stock = forms.IntegerField(
        label="Verified Stock",
        min_value=0,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    
    cluster = forms.CharField(
        label="Cluster (Optional)",
        required=False,
        max_length=100,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )