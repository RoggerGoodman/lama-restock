"""
sync_views.py — Machine-to-machine API endpoints + onboarding UI for supermarket PC sync.

  POST /api/sync/vensetar-sales/              — daily sold quantities from VENSETAR
  POST /supermarkets/<pk>/generate-sync-token/ — generate/regenerate token (admin UI)
  GET  /api/sync/setup/<token>/bootstrap/     — serve ready-to-run PS1 installer script
  GET  /supermarkets/<pk>/sync-setup/         — onboarding setup page
"""
import json
import logging
import secrets
from datetime import date

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from django.http import JsonResponse
from .models import Blacklist, BlacklistEntry, SalesSyncLog, Storage, Supermarket
from .scripts.DatabaseManager import DatabaseManager
from .scripts.helpers import Helper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data sync endpoint (called by PowerShell on supermarket PC)
# ---------------------------------------------------------------------------

@csrf_exempt
@require_POST
def vensetar_sales_sync_view(request):
    """
    Receive yesterday's sold quantities from the supermarket PC PowerShell script
    and apply them to product_stats.

    Expected JSON body:
    {
        "token":     "<sync_api_token>",
        "sync_date": "YYYY-MM-DD",
        "products": [
            {"cod": 606, "var": 1, "sold": 7},
            ...
        ]
    }
    """
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return HttpResponse('Invalid JSON body', status=400)

    token = data.get('token')
    sync_date_str = data.get('sync_date')
    products_raw = data.get('products')

    if not token or not sync_date_str or not isinstance(products_raw, list):
        return HttpResponse('Missing required fields: token, sync_date, products', status=400)

    try:
        supermarket = Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        logger.warning(f"[VENSETAR SYNC] Rejected request — unknown token (first 8: {token[:8]})")
        return HttpResponse('Invalid token', status=401)

    try:
        sync_date = date.fromisoformat(sync_date_str)
    except ValueError:
        return HttpResponse('Invalid sync_date, expected YYYY-MM-DD', status=400)

    daily_sales = []
    skipped_float = 0
    for entry in products_raw:
        try:
            cod = int(entry['cod'])
            var = int(entry['var'])
            sold_raw = entry['sold']
            # Skip kg-based articles (fractional quantities — system only handles whole units)
            if isinstance(sold_raw, float) and sold_raw != int(sold_raw):
                skipped_float += 1
                continue
            daily_sales.append((cod, var, int(sold_raw)))
        except (KeyError, ValueError, TypeError):
            continue
    if skipped_float:
        logger.info(f"[VENSETAR SYNC] skipped {skipped_float} float-qty products (kg-based, not supported)")

    if not daily_sales:
        return HttpResponse('No valid product entries found', status=400)

    helper = Helper()
    db = DatabaseManager(helper, supermarket_name=supermarket.name)
    try:
        result = db.apply_daily_vensetar_sales(daily_sales, sync_date)
    except Exception:
        logger.exception(f"[VENSETAR SYNC] DB error for supermarket '{supermarket.name}'")
        return HttpResponse('Internal server error', status=500)
    finally:
        db.close()

    # Filter out products already in any blacklist for this supermarket
    blacklisted = set(
        BlacklistEntry.objects.filter(
            blacklist__storage__supermarket=supermarket
        ).values_list('product_code', 'product_var')
    )
    unverified_filtered = [
        p for p in result['unverified_products']
        if (p['cod'], p['v']) not in blacklisted
    ]

    SalesSyncLog.objects.create(
        supermarket=supermarket,
        sync_date=sync_date,
        received=len(daily_sales),
        applied=result['applied'],
        already_synced=result['already_synced'],
        not_in_db=result['not_in_db'],
        unverified_products=unverified_filtered,
    )

    logger.info(
        f"[VENSETAR SYNC] supermarket='{supermarket.name}' "
        f"sync_date={sync_date_str} received={len(daily_sales)} "
        f"applied={result['applied']} unverified={len(unverified_filtered)}"
    )
    return HttpResponse('OK', status=200)


# ---------------------------------------------------------------------------
# Sync log — detail view + actions
# ---------------------------------------------------------------------------

@login_required
def sales_sync_log_detail_view(request, pk):
    """Detail page for a SalesSyncLog — shows stats and unverified products."""
    log = get_object_or_404(SalesSyncLog, pk=pk, supermarket__owner=request.user)
    return render(request, 'supermarkets/sales_sync_log_detail.html', {'log': log})


@login_required
@require_POST
def add_to_non_gestiti_view(request):
    """
    AJAX: add a product to the 'Non gestiti' blacklist for its storage.
    Creates the blacklist if it doesn't exist yet.

    Body (from SalesSyncLog detail): { sync_log_id, cod, var, settore }
    Body (from RestockLog/DDT detail): { storage_id, cod, var }
    """
    try:
        data = json.loads(request.body)
        cod = int(data['cod'])
        var = int(data['var'])
    except (KeyError, ValueError, TypeError):
        return JsonResponse({'success': False, 'message': 'Missing or invalid fields'}, status=400)

    if 'storage_id' in data:
        storage = get_object_or_404(Storage, pk=int(data['storage_id']), supermarket__owner=request.user)
        log_label = f"{storage.supermarket.name} {cod}.{var} (storage_id={storage.pk})"
    else:
        try:
            sync_log_id = int(data['sync_log_id'])
            settore = data['settore']
        except (KeyError, ValueError, TypeError):
            return JsonResponse({'success': False, 'message': 'Missing or invalid fields'}, status=400)
        sync_log = get_object_or_404(SalesSyncLog, pk=sync_log_id, supermarket__owner=request.user)
        storage = get_object_or_404(Storage, supermarket=sync_log.supermarket, settore=settore)
        log_label = f"{sync_log.supermarket.name} {cod}.{var} (settore={settore})"

    blacklist, _ = Blacklist.objects.get_or_create(
        storage=storage,
        name='Non gestiti',
        defaults={'description': 'Prodotti venduti ma non gestiti nel sistema di riordino'}
    )
    _, created = BlacklistEntry.objects.get_or_create(
        blacklist=blacklist,
        product_code=cod,
        product_var=var,
    )

    logger.info(f"[NON GESTITI] {'Added' if created else 'Already in'} blacklist: {log_label}")
    return JsonResponse({'success': True, 'already_existed': not created})


# ---------------------------------------------------------------------------
# Admin UI — token generation + setup page
# ---------------------------------------------------------------------------

@login_required
@require_POST
def generate_sync_token_view(request, pk):
    """Generate (or regenerate) the sync API token for a supermarket."""
    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)
    supermarket.sync_api_token = secrets.token_urlsafe(32)
    supermarket.save(update_fields=['sync_api_token'])
    return redirect('sync-setup', pk=pk)


@login_required
def sync_setup_view(request, pk):
    """Setup page: shows the token status and the one-liner install command."""
    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)

    # Build the one-liner the client will paste into PowerShell (Admin)
    oneliner = None
    if supermarket.sync_api_token:
        bootstrap_url = request.build_absolute_uri(
            f'/api/sync/setup/{supermarket.sync_api_token}/bootstrap/'
        )
        oneliner = (
            f'powershell -ExecutionPolicy Bypass -Command '
            f'"[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; '
            f'irm \'{bootstrap_url}\' | iex"'
        )

    return render(request, 'supermarkets/sync_setup.html', {
        'supermarket': supermarket,
        'oneliner': oneliner,
    })


# ---------------------------------------------------------------------------
# Bootstrap endpoint — serves the ready-to-run PS1 installer
# ---------------------------------------------------------------------------

def sync_bootstrap_view(request, token):
    """
    Returns a PowerShell script (plain text) that installs sync_sales.ps1
    and registers the Windows Scheduled Task on the supermarket PC.

    The token in the URL IS the auth — anyone who has it can re-download
    the script, but they still need the token to actually sync data.
    """
    try:
        supermarket = Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        return HttpResponse('Not found', status=404)

    server_url = request.build_absolute_uri('/api/sync/vensetar-sales/')

    script = _build_bootstrap_script(token=token, server_url=server_url)
    return HttpResponse(script, content_type='text/plain; charset=utf-8')


def _build_bootstrap_script(token: str, server_url: str) -> str:
    """
    Returns the full PowerShell bootstrap script as a string,
    with token and server_url already substituted.
    """
    sync_script_content = _build_sync_script(token=token, server_url=server_url)
    # Escape single quotes inside the here-string for PowerShell safety
    sync_script_escaped = sync_script_content.replace("'", "''")

    return f"""# LamApp Sales Sync — Bootstrap Installer
# Run as Administrator in PowerShell.
# Downloads and installs the daily sales sync script + Scheduled Task.

$ScriptDir  = "C:\\LamApp"
$ScriptPath = "$ScriptDir\\sync_sales.ps1"
$TaskName   = "LamApp Sales Sync"

Write-Host "Installing LamApp Sales Sync..."

# 1. Create directory
if (-not (Test-Path $ScriptDir)) {{
    New-Item -ItemType Directory -Path $ScriptDir | Out-Null
    Write-Host "  Created $ScriptDir"
}}

# 2. Write sync script (token and server URL already embedded)
@'
{sync_script_escaped}
'@ | Out-File -FilePath $ScriptPath -Encoding UTF8 -Force
Write-Host "  Wrote $ScriptPath"

# 3. Register Scheduled Task (daily at 06:00, runs as SYSTEM)
$TaskCmd = "powershell.exe"
$TaskArgs = "-ExecutionPolicy Bypass -NonInteractive -WindowStyle Hidden -File `"$ScriptPath`""
schtasks /create /tn "$TaskName" /tr "$TaskCmd $TaskArgs" /sc daily /st 06:00 /ru SYSTEM /f | Out-Null
Write-Host "  Scheduled task '$TaskName' registered (daily 06:00)"

Write-Host ""
Write-Host "Setup complete. The sync will run every morning at 06:00."
"""


def _build_sync_script(token: str, server_url: str) -> str:
    """Returns the daily sync_sales.ps1 content with credentials substituted."""
    return f"""# sync_sales.ps1 — LamApp daily sales sync
# Auto-generated. Do not edit manually.

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ServerUrl = '{server_url}'
$Token     = '{token}'

$Yesterday   = (Get-Date).AddDays(-1).Date
$DotNetDow   = [int]$Yesterday.DayOfWeek   # 0=Sun, 1=Mon ... 6=Sat
$VensDay     = if ($DotNetDow -eq 0) {{ 7 }} else {{ $DotNetDow }}

$WeekMonday    = $Yesterday.AddDays(-($VensDay - 1))
$WeekMondayStr = $WeekMonday.ToString("yyyyMMdd")
$YesterdayStr  = $Yesterday.ToString("yyyy-MM-dd")

Write-Host "Syncing sales for $YesterdayStr (day $VensDay, week $WeekMondayStr)"

$Query = @"
SELECT
    Cod_Articolo      AS cod,
    Variante_Articolo AS var,
    SUM(Quantita_vendita_$VensDay) AS sold
FROM VENSETAR
WHERE Data_vendita_dal_ = '$WeekMondayStr'
GROUP BY Cod_Articolo, Variante_Articolo
"@

try {{
    $Rows = Invoke-Sqlcmd -ServerInstance "localhost" -Database "Statistiche" -Query $Query -ErrorAction Stop
}} catch {{
    Write-Host "ERROR: VENSETAR query failed: $_"
    exit 1
}}

if (-not $Rows -or $Rows.Count -eq 0) {{
    Write-Host "WARNING: No rows for week $WeekMondayStr. Nothing to sync."
    exit 0
}}

Write-Host "Sending $($Rows.Count) products to server..."

$Products = @($Rows | ForEach-Object {{
    @{{ cod = [int]$_.cod; var = [int]$_.var; sold = [int]$_.sold }}
}})

$Payload = @{{
    token     = $Token
    sync_date = $YesterdayStr
    products  = $Products
}} | ConvertTo-Json -Depth 3 -Compress

try {{
    Invoke-RestMethod -Uri $ServerUrl -Method POST -Body $Payload -ContentType "application/json" -ErrorAction Stop
    Write-Host "Sync OK."
}} catch {{
    Write-Host "ERROR: POST failed: $_"
    exit 1
}}
"""
