# LamApp/supermarkets/forms.py
from django import forms
from .models import RestockSchedule, Blacklist, BlacklistEntry

class RestockScheduleForm(forms.ModelForm):
    """
    Form for restock schedules with configurable delivery offsets.
    Each day has:
    - A checkbox to enable/disable ordering
    - A number input for delivery offset (0=same day, 1=next day, etc.)
    """
    
    class Meta:
        model = RestockSchedule
        fields = [
            'monday', 'monday_delivery_offset',
            'tuesday', 'tuesday_delivery_offset',
            'wednesday', 'wednesday_delivery_offset',
            'thursday', 'thursday_delivery_offset',
            'friday', 'friday_delivery_offset',
            'saturday', 'saturday_delivery_offset',
            'sunday', 'sunday_delivery_offset'
        ]
        
        widgets = {
            # Day checkboxes
            'monday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'tuesday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'wednesday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'thursday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'friday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'saturday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            'sunday': forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'updateCoveragePreview()'}),
            
            # Delivery offset inputs
            'monday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'tuesday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'wednesday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'thursday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'friday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'saturday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
            'sunday_delivery_offset': forms.NumberInput(attrs={
                'class': 'form-control form-control-sm',
                'min': '0',
                'max': '6',
                'onchange': 'updateCoveragePreview()'
            }),
        }
        
        help_texts = {
            'monday': 'Enable ordering on Monday',
            'tuesday': 'Enable ordering on Tuesday',
            'wednesday': 'Enable ordering on Wednesday',
            'thursday': 'Enable ordering on Thursday',
            'friday': 'Enable ordering on Friday',
            'saturday': 'Enable ordering on Saturday',
            'sunday': 'Enable ordering on Sunday',
            
            'monday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'tuesday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'wednesday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'thursday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'friday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'saturday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
            'sunday_delivery_offset': '0=same day, 1=next day, 2=two days later, etc.',
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


class RecordLossesForm(forms.Form):
    """Form for manually uploading and recording losses - NOW SUPPORTS PDF"""
    
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
    
    pdf_file = forms.FileField(
        label="Loss PDF File",
        help_text="Upload the PDF file with loss data",
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.pdf'
        })
    )

class AddProductsForm(forms.Form):
    """Form for adding products by code"""
    settore = forms.ChoiceField(
        label="Settore",
        widget=forms.Select(attrs={'class': 'form-select'}),
        help_text="Select the settore for these products"
    )
    
    products = forms.CharField(
        label="Product Codes",
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'rows': 10,
            'placeholder': 'Enter product codes, one per line:\n12345.1\n67890.1\n11111.2'
        }),
        help_text="Enter product codes in format: cod.var (one per line)"
    )
    
    def __init__(self, storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set settore choices based on storage's supermarket
        self.storage = storage
        storages = storage.supermarket.storages.all()
        self.fields['settore'].choices = [(s.settore, s.settore) for s in storages]
        self.fields['settore'].initial = storage.settore
    
    def clean_products(self):
        """Parse and validate product codes"""
        products_text = self.cleaned_data['products']
        products = []
        errors = []
        
        for line_num, line in enumerate(products_text.strip().split('\n'), 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                # Parse cod.var
                if '.' not in line:
                    errors.append(f"Line {line_num}: Missing variant (use format cod.var)")
                    continue
                
                parts = line.split('.')
                if len(parts) != 2:
                    errors.append(f"Line {line_num}: Invalid format (use cod.var)")
                    continue
                
                cod = int(parts[0])
                var = int(parts[1])
                products.append((cod, var))
                
            except ValueError:
                errors.append(f"Line {line_num}: Invalid numbers in '{line}'")
        
        if errors:
            raise forms.ValidationError('\n'.join(errors))
        
        if not products:
            raise forms.ValidationError("No valid product codes found")
        
        return products
    
class PurgeProductsForm(forms.Form):
    """Form for purging products"""
    products = forms.CharField(
        label="Product Codes to Purge",
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'rows': 10,
            'placeholder': 'Enter product codes to purge, one per line:\n12345.1\n67890.1'
        }),
        help_text="Products with stock will be blacklisted and flagged. Products without stock will be deleted immediately."
    )
    
    def clean_products(self):
        """Parse and validate product codes"""
        products_text = self.cleaned_data['products']
        products = []
        errors = []
        
        for line_num, line in enumerate(products_text.strip().split('\n'), 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                if '.' not in line:
                    errors.append(f"Line {line_num}: Missing variant (use format cod.var)")
                    continue
                
                parts = line.split('.')
                if len(parts) != 2:
                    errors.append(f"Line {line_num}: Invalid format (use cod.var)")
                    continue
                
                cod = int(parts[0])
                var = int(parts[1])
                products.append((cod, var))
                
            except ValueError:
                errors.append(f"Line {line_num}: Invalid numbers in '{line}'")
        
        if errors:
            raise forms.ValidationError('\n'.join(errors))
        
        if not products:
            raise forms.ValidationError("No valid product codes found")
        
        return products
    
class InventorySearchForm(forms.Form):
    """Form for searching inventory - FIXED to not validate dynamic choices"""
    
    SEARCH_TYPE_CHOICES = [
        ('cod_var', 'Articolo specifico'),
        ('cod_all', 'Tutte le varianti articolo'),
        ('settore_cluster', 'Magazzino + Cluster'),
    ]
    
    search_type = forms.ChoiceField(
        choices=SEARCH_TYPE_CHOICES,
        widget=forms.RadioSelect(attrs={'class': 'form-check-input'}),
        initial='cod_var'
    )
    
    # Fields for cod_var and cod_all
    product_code = forms.IntegerField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 12345'
        })
    )
    
    product_var = forms.IntegerField(
        required=False,
        initial=1,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'value': '1'
        })
    )
    
    # FIXED: Use CharField instead of ChoiceField for dynamic fields
    # This prevents validation errors on dynamically loaded values
    supermarket = forms.CharField(
        required=False,
        widget=forms.Select(attrs={'class': 'form-select', 'id': 'id_supermarket'})
    )
    
    settore = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control', 
            'id': 'id_settore',
            'readonly': 'readonly',  # Will be set by JavaScript
            'placeholder': 'Select supermarket first'
        })
    )
    
    cluster = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'id_cluster', 
            'readonly': 'readonly',  # Will be set by JavaScript
            'placeholder': 'Select settore first'
        })
    )
    
    def __init__(self, user, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Import here to avoid circular import
        from .models import Supermarket
        
        # Populate supermarket choices
        supermarkets = Supermarket.objects.filter(owner=user)
        
        # Override widget to add choices
        self.fields['supermarket'].widget = forms.Select(
            choices=[('', '-- Seleziona Punto vendita --')] + [
                (str(sm.id), sm.name) for sm in supermarkets
            ],
            attrs={'class': 'form-select', 'id': 'id_supermarket'}
        )
    
    def clean(self):
        cleaned_data = super().clean()
        search_type = cleaned_data.get('search_type')
        
        if search_type in ['cod_var', 'cod_all']:
            if not cleaned_data.get('product_code'):
                raise forms.ValidationError("Product code is required for this search type")
            
            if search_type == 'cod_var' and not cleaned_data.get('product_var'):
                raise forms.ValidationError("Product variant is required for specific product search")
        
        elif search_type == 'settore_cluster':
            if not cleaned_data.get('supermarket'):
                raise forms.ValidationError("Supermarket is required for settore/cluster search")
            if not cleaned_data.get('settore'):
                raise forms.ValidationError("Settore is required for this search type")
        
        return cleaned_data
    
class DDTUploadForm(forms.Form):
    """Form for uploading DDT (delivery document) PDF"""
    
    pdf_file = forms.FileField(
        label="DDT PDF File",
        help_text="Upload the delivery document (DDT) from your supplier",
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.pdf'
        })
    )