"""
sync_views.py — Machine-to-machine API endpoints + onboarding UI for supermarket PC sync.

  POST /api/sync/realtime-sales/               — today's running sold totals (Everest till log)
  POST /api/sync/intraday-curve/               — measured per-weekday hourly sales shape
  POST /supermarkets/<pk>/generate-sync-token/ — generate/regenerate token (admin UI)
  GET  /api/sync/setup/<token>/bootstrap-rt/   — serve ready-to-run PS1 installer script
  GET  /supermarkets/<pk>/sync-setup/          — onboarding setup page
"""
import hashlib
import json
import logging
import secrets
from datetime import date

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.utils import timezone
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from django.http import JsonResponse
from .models import Blacklist, BlacklistEntry, SalesSyncLog, Storage, Supermarket
from .scripts.DatabaseManager import DatabaseManager
from .logging_context import enter_supermarket_log, exit_supermarket_log

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data sync endpoint (called by PowerShell on supermarket PC)
# ---------------------------------------------------------------------------


@csrf_exempt
@require_POST
def realtime_sales_sync_view(request):
    """
    Receive the day's running sold totals from the Everest till log and apply them.

    Expected JSON body:
    {
        "token":     "<sync_api_token>",
        "sync_date": "YYYY-MM-DD",
        "mode":      "absolute",
        "products":  [{"cod": 606, "var": 1, "sold": 7, "shelf_life": 360}, ...]
    }

    `sold` is the day's total SO FAR, not an increment; the server books the difference.
    An empty product list is valid — the first run of a new day rolls the previous closed.

    One SalesSyncLog row per supermarket per day, rewritten on each run.
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

    if data.get('mode') != 'absolute':
        # Guards against an older delta-style client being pointed here by mistake,
        # which would double-book every run.
        return HttpResponse("Unsupported mode, expected 'absolute'", status=400)

    try:
        supermarket = Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        logger.warning(f"[RT SYNC] Rejected request — unknown token (first 8: {token[:8]})")
        return HttpResponse('Invalid token', status=401)

    _log_ctx = enter_supermarket_log(supermarket.name)
    try:
        try:
            sync_date = date.fromisoformat(sync_date_str)
        except ValueError:
            return HttpResponse('Invalid sync_date, expected YYYY-MM-DD', status=400)

        totals = []
        shelf_life_map = {}
        skipped_float = 0
        for entry in products_raw:
            try:
                cod = int(entry['cod'])
                var = int(entry['var'])
                sold_raw = entry['sold']
                # Everest reports whole units only (weight-sold lines are filtered out
                # store-side by N0_UOM_CODE), but stay defensive: the rest of the system
                # cannot represent fractional sales.
                if isinstance(sold_raw, float) and sold_raw != int(sold_raw):
                    skipped_float += 1
                    continue
                totals.append((cod, var, int(sold_raw)))
                sl = entry.get('shelf_life')
                if sl is not None:
                    shelf_life_map[(cod, var)] = int(sl)
            except (KeyError, ValueError, TypeError):
                continue

        if skipped_float:
            logger.info(f"[RT SYNC] skipped {skipped_float} fractional-qty products")

        db = DatabaseManager(supermarket_name=supermarket.name)
        try:
            result = db.apply_realtime_sales(totals, sync_date, shelf_life_map=shelf_life_map)
        except Exception:
            logger.exception(f"[RT SYNC] DB error for supermarket '{supermarket.name}'")
            return HttpResponse('Internal server error', status=500)
        finally:
            db.close()

        blacklisted = set(
            BlacklistEntry.objects.filter(
                blacklist__storage__supermarket=supermarket
            ).values_list('product_code', 'product_var')
        )
        unverified_filtered = [
            p for p in result['unverified_products']
            if (p['cod'], p['v']) not in blacklisted
        ]

        supermarket.last_sales_sync_at = timezone.now()
        supermarket.save(update_fields=['last_sales_sync_at'])

        # The payload is cumulative, so the latest run already describes the whole day —
        # overwrite rather than accumulate.
        SalesSyncLog.objects.update_or_create(
            supermarket=supermarket,
            sync_date=sync_date,
            defaults={
                'received': len(totals),
                'applied': len(totals) - result['not_in_db'],
                'already_synced': result['unchanged'],
                'not_in_db': result['not_in_db'],
                'unverified_products': unverified_filtered,
            },
        )

        logger.info(
            f"[RT SYNC] supermarket='{supermarket.name}' date={sync_date_str} "
            f"received={len(totals)} changed={result['applied']} "
            f"units={result['units_applied']} rolled_over={result['rolled_over']} "
            f"unverified={len(unverified_filtered)}"
        )
        return JsonResponse({
            'ok': True,
            'received': len(totals),
            'changed': result['applied'],
            'units': result['units_applied'],
            'rolled_over': result['rolled_over'],
            'send_curve': _curve_is_stale(supermarket),
        })
    finally:
        exit_supermarket_log(_log_ctx)


CURVE_MAX_AGE_HOURS = 20


def _curve_is_stale(supermarket) -> bool:
    """True when the store should recompute and ship its intraday curve."""
    if not supermarket.intraday_curve or not supermarket.intraday_curve_updated_at:
        return True
    age = timezone.now() - supermarket.intraday_curve_updated_at
    return age.total_seconds() > CURVE_MAX_AGE_HOURS * 3600


@csrf_exempt
@require_POST
def intraday_curve_sync_view(request):
    """
    Receive the store's measured intraday sales curve.

    Expected JSON body:
    {
        "token": "<sync_api_token>",
        "curve": [[h0..h23] x 7]     # [weekday][hour], Monday first
    }

    Separate from the sales endpoint: a curve-only POST carries no products and would
    otherwise rewrite the day's SalesSyncLog row with zero counts.
    """
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return HttpResponse('Invalid JSON body', status=400)

    token = data.get('token')
    curve = data.get('curve')

    if not token or not isinstance(curve, list):
        return HttpResponse('Missing required fields: token, curve', status=400)

    try:
        supermarket = Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        logger.warning(f"[CURVE SYNC] Rejected request — unknown token (first 8: {token[:8]})")
        return HttpResponse('Invalid token', status=401)

    # Validate hard: a malformed curve silently distorts every coverage figure.
    if len(curve) != 7:
        return HttpResponse('curve must have 7 weekday rows', status=400)
    clean = []
    for row in curve:
        if not isinstance(row, list) or len(row) != 24:
            return HttpResponse('each weekday row must have 24 hours', status=400)
        try:
            vals = [float(x) for x in row]
        except (TypeError, ValueError):
            return HttpResponse('curve values must be numeric', status=400)
        if any(v < 0 for v in vals):
            return HttpResponse('curve values must be non-negative', status=400)
        clean.append(vals)

    if sum(sum(r) for r in clean) <= 0:
        return HttpResponse('curve is empty', status=400)

    supermarket.intraday_curve = clean
    supermarket.intraday_curve_updated_at = timezone.now()
    supermarket.save(update_fields=['intraday_curve', 'intraday_curve_updated_at'])

    covered = sum(1 for r in clean if sum(r) > 0)
    logger.info(f"[CURVE SYNC] supermarket='{supermarket.name}' weekdays_with_data={covered}/7")
    return JsonResponse({'ok': True, 'weekdays_with_data': covered})


@login_required
def history_import_view(request, pk):
    """
    Seed a supermarket's sales history from a dump generated on its own PC.

    GET shows the one-liner and what the upload would overwrite; POST applies the file.
    """
    supermarket = get_object_or_404(Supermarket, pk=pk, owner=request.user)

    # A supermarket whose schema was never created has no product_stats to count, and a
    # malformed sales_sets would make jsonb_array_length raise. Neither should turn an
    # onboarding page into a 500.
    state = {'products': 0, 'with_history': 0, 'by_settore': [], 'schema_ready': False}
    try:
        db = DatabaseManager(supermarket_name=supermarket.name)
        try:
            cur = db.cursor()
            cur.execute("""
                SELECT COUNT(*) AS products,
                       COUNT(*) FILTER (
                           WHERE jsonb_typeof(sales_sets) = 'array'
                             AND jsonb_array_length(sales_sets) > 1
                       ) AS with_history
                FROM product_stats
            """)
            state.update(dict(cur.fetchone()))
            cur.execute("SELECT settore, COUNT(*) AS n FROM products GROUP BY settore ORDER BY settore")
            state['by_settore'] = [dict(r) for r in cur.fetchall()]
            state['schema_ready'] = True
        finally:
            db.close()
    except Exception:
        logger.warning(f"[HISTORY] catalogue not ready for '{supermarket.name}'", exc_info=True)

    storages = list(supermarket.storages.all().order_by('settore', 'name'))
    counts = {r['settore']: r['n'] for r in state['by_settore']}
    for s in storages:
        s.product_count = counts.get(s.settore, 0)

    result = None
    error = None
    queued_lists = None

    if request.method == 'POST' and request.POST.get('action') == 'download_lists':
        from .tasks import manual_list_update_task
        chosen = request.POST.getlist('storages')
        wanted = [s for s in storages if str(s.id) in chosen]
        for s in wanted:
            manual_list_update_task.apply_async(args=[s.id])
        queued_lists = [s.name for s in wanted]
        if not queued_lists:
            error = "Nessun settore selezionato."
        else:
            logger.info(
                f"[HISTORY] queued list update for {queued_lists} "
                f"({supermarket.name}) by user={request.user}"
            )

    elif request.method == 'POST':
        upload = request.FILES.get('history_file')
        if not upload:
            error = "Nessun file selezionato."
        else:
            try:
                payload = json.loads(upload.read().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = None
                error = "Il file non e' un JSON valido."

            if payload is not None:
                generated = payload.get('generated_at')
                today = date.today().isoformat()
                if generated != today:
                    # Every index is relative to the dump's own date, so a file from
                    # another day would shift all 84 values silently.
                    error = (f"Il file e' del {generated}, oggi e' {today}. "
                             f"Rigenera il file sul PC del punto vendita.")
                elif not isinstance(payload.get('products'), list):
                    error = "Il file non contiene l'elenco prodotti."
                else:
                    entries = []
                    for p in payload['products']:
                        try:
                            entries.append((
                                int(p['cod']), int(p['var']),
                                [float(x) for x in p['m']],
                                [float(x) for x in p['d']],
                            ))
                        except (KeyError, TypeError, ValueError):
                            continue

                    if not entries:
                        error = "Nessun prodotto valido nel file."
                    else:
                        db = DatabaseManager(supermarket_name=supermarket.name)
                        try:
                            result = db.apply_history_backfill(
                                entries,
                                include_current=supermarket.last_sales_sync_at is None,
                                history_date=date.today(),
                            )
                        except Exception:
                            logger.exception(f"[HISTORY] failed for '{supermarket.name}'")
                            error = "Errore durante l'importazione. Controlla i log."
                        finally:
                            db.close()

                        if result:
                            logger.info(
                                f"[HISTORY] supermarket='{supermarket.name}' "
                                f"applied={result['applied']} by user={request.user}"
                            )

    script_url = request.build_absolute_uri(
        f'/api/sync/setup/{supermarket.sync_api_token}/history-script/'
    ) if supermarket.sync_api_token else None

    oneliner = (
        f'powershell -ExecutionPolicy Bypass -Command '
        f'"[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; '
        f'irm \'{script_url}\' | iex"'
    ) if script_url else None

    return render(request, 'supermarkets/history_import.html', {
        'supermarket': supermarket,
        'oneliner': oneliner,
        'state': state,
        'storages': storages,
        'queued_lists': queued_lists,
        'result': result,
        'error': error,
    })


def history_script_view(request, token):
    """Serves the PowerShell dump script (plain text) for the history backfill."""
    try:
        Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        return HttpResponse('Not found', status=404)
    return HttpResponse(_build_history_export_script(),
                        content_type='text/plain; charset=utf-8')



def _build_history_export_script() -> str:
    """
    PowerShell that dumps 24 monthly + 60 daily figures per product to a local JSON file.

    No network and no token: it only reads the store's own databases and writes a file the
    operator then uploads. That keeps the backfill independent of firewall whitelisting,
    which is usually the slowest part of onboarding.

    Monthly comes from VEMEART (the only source reaching 24 months, and the one table that
    keeps pieces and kg in separate columns). Daily prefers the Everest till log so the
    seeded days are computed by exactly the query the live sync uses; VENGART is the
    fallback where Everest is absent.
    """
    return r"""# LamApp - export storico vendite
# Auto-generated. Do not edit manually. ASCII only.

$OutFile = "C:\LamApp\history_export.json"
$Server  = "localhost"
if (-not (Test-Path "C:\LamApp")) { New-Item -ItemType Directory -Path "C:\LamApp" | Out-Null }

$hasEverest = (Invoke-Sqlcmd -ServerInstance $Server -Database "master" -Query `
    "SET NOCOUNT ON; SELECT COUNT(*) AS n FROM sys.databases WHERE name='everest'").n -gt 0

$MonthlyQuery = @"
SET NOCOUNT ON;
DECLARE @today date = CAST(GETDATE() AS DATE);
DECLARE @cur date = DATEFROMPARTS(YEAR(@today), MONTH(@today), 1);
WITH m AS (
    SELECT v.codice_articolo AS cod, v.variante_articolo AS var,
           DATEFROMPARTS(v.anno_di_esercizio, x.mese, 1) AS mstart, x.pz
    FROM VEMEART v
    CROSS APPLY (VALUES
        (1,v.qta_vend__pezzi_gennaio),(2,v.qta_vend__pezzi_febbraio),(3,v.qta_vend__pezzi_marzo),
        (4,v.qta_vend__pezzi_aprile),(5,v.qta_vend__pezzi_maggio),(6,v.qta_vend__pezzi_giugno),
        (7,v.qta_vend__pezzi_luglio),(8,v.qta_vend__pezzi_agosto),(9,v.qta_vend__pezzi_settembre),
        (10,v.qta_vend__pezzi_ottobre),(11,v.qta_vend__pezzi_novembre),(12,v.qta_vend__pezzi_dicembre)
    ) AS x(mese, pz)
    WHERE v.punto_vendita = 1
      AND v.anno_di_esercizio >= YEAR(DATEADD(month,-23,@today))
)
SELECT cod, var, DATEDIFF(month, mstart, @cur) AS idx, SUM(pz) AS pz
FROM m WHERE DATEDIFF(month, mstart, @cur) BETWEEN 0 AND 23
GROUP BY cod, var, DATEDIFF(month, mstart, @cur)
HAVING SUM(pz) <> 0;
"@

$DailyEverest = @"
SET NOCOUNT ON;
DECLARE @today date = CAST(GETDATE() AS DATE);
SELECT DISTINCT TRY_CAST(LTRIM(RTRIM(cod_est)) AS BIGINT) AS ean_n, cod_art AS cod, var_art AS v
INTO #map FROM CassaAna.dbo.CodEan
WHERE segn = 1 AND TRY_CAST(LTRIM(RTRIM(cod_est)) AS BIGINT) IS NOT NULL;
CREATE INDEX ix_map ON #map(ean_n);
SELECT m.cod AS cod, m.v AS var,
       DATEDIFF(day, CAST(li.DT_TIME_STAMP AS DATE), @today) AS idx,
       SUM(CASE WHEN li.BL_RETURN=1 THEN -li.N0_QUANTITY ELSE li.N0_QUANTITY END) AS pz
FROM everest.dbo.RDB_LOG_ITEM li
JOIN #map m ON m.ean_n = TRY_CAST(LTRIM(RTRIM(li.SZ_ITEM_REF_NO)) AS BIGINT)
WHERE CAST(li.DT_TIME_STAMP AS DATE) BETWEEN DATEADD(day,-59,@today) AND @today
  AND li.BL_VOIDED = 0 AND li.BL_MGR_VOIDED = 0 AND li.N0_UOM_CODE <> 2
GROUP BY m.cod, m.v, DATEDIFF(day, CAST(li.DT_TIME_STAMP AS DATE), @today)
HAVING SUM(CASE WHEN li.BL_RETURN=1 THEN -li.N0_QUANTITY ELSE li.N0_QUANTITY END) <> 0;
"@

$DailyVengart = @"
SET NOCOUNT ON;
DECLARE @today date = CAST(GETDATE() AS DATE);
SELECT cod__articolo AS cod, variante_articolo AS var,
       DATEDIFF(day, CONVERT(date, data_vendita, 112), @today) AS idx,
       SUM(qt__venduta_pezzi) AS pz
FROM VENGART
WHERE punto_vendita = 1
  AND CONVERT(date, data_vendita, 112) BETWEEN DATEADD(day,-59,@today) AND @today
GROUP BY cod__articolo, variante_articolo,
         DATEDIFF(day, CONVERT(date, data_vendita, 112), @today)
HAVING SUM(qt__venduta_pezzi) <> 0;
"@

Write-Host "Estrazione mensile in corso..."
$mrows = Invoke-Sqlcmd -ServerInstance $Server -Database "Essepiu" -Query $MonthlyQuery -QueryTimeout 600
Write-Host "  $($mrows.Count) righe mensili"

Write-Host "Estrazione giornaliera in corso..."
if ($hasEverest) {
    $drows = Invoke-Sqlcmd -ServerInstance $Server -Database "Essepiu" -Query $DailyEverest -QueryTimeout 600
    $dailySource = "everest"
} else {
    $drows = Invoke-Sqlcmd -ServerInstance $Server -Database "Essepiu" -Query $DailyVengart -QueryTimeout 600
    $dailySource = "vengart"
}
Write-Host "  $($drows.Count) righe giornaliere (fonte: $dailySource)"

$acc = @{}
foreach ($r in $mrows) {
    $k = "$($r.cod).$($r.var)"
    if (-not $acc.ContainsKey($k)) { $acc[$k] = @{ cod=[int]$r.cod; var=[int]$r.var; m=@(0)*24; d=@(0)*60 } }
    $acc[$k].m[[int]$r.idx] = [double]$r.pz
}
foreach ($r in $drows) {
    $k = "$($r.cod).$($r.var)"
    if (-not $acc.ContainsKey($k)) { $acc[$k] = @{ cod=[int]$r.cod; var=[int]$r.var; m=@(0)*24; d=@(0)*60 } }
    $acc[$k].d[[int]$r.idx] = [double]$r.pz
}

# Built by hand: ConvertTo-Json is very slow at this size and renders typed arrays
# as {value,Count} objects rather than plain JSON arrays.
$sb = New-Object System.Text.StringBuilder
[void]$sb.Append('{"generated_at":"' + (Get-Date -Format 'yyyy-MM-dd') + '"')
[void]$sb.Append(',"daily_source":"' + $dailySource + '","products":[')
$first = $true
foreach ($p in $acc.Values) {
    if (-not $first) { [void]$sb.Append(',') }
    $first = $false
    [void]$sb.Append('{"cod":' + $p.cod + ',"var":' + $p.var)
    [void]$sb.Append(',"m":[' + ($p.m -join ',') + ']')
    [void]$sb.Append(',"d":[' + ($p.d -join ',') + ']}')
}
[void]$sb.Append(']}')
[IO.File]::WriteAllText($OutFile, $sb.ToString(), [Text.Encoding]::UTF8)

Write-Host ""
Write-Host "Fatto: $OutFile ($([math]::Round((Get-Item $OutFile).Length/1MB,1)) MB, $($acc.Count) prodotti)"
Write-Host "Caricare il file sul sito, nella pagina Importa storico."
"""

# ---------------------------------------------------------------------------
# Sync log — detail view + actions
# ---------------------------------------------------------------------------

@login_required
def sales_sync_log_detail_view(request, pk):
    """Detail page for a SalesSyncLog — shows stats and unverified products."""
    log = get_object_or_404(SalesSyncLog, pk=pk, supermarket__owner=request.user)

    unverified = log.unverified_products or []
    if unverified:
        db = DatabaseManager(supermarket_name=log.supermarket.name)
        try:
            placeholders = ','.join(['(%s,%s)'] * len(unverified))
            values = [x for p in unverified for x in (p['cod'], p['v'])]
            cur = db.cursor()
            cur.execute(
                f"SELECT cod, v, descrizione, settore FROM products WHERE (cod, v) IN ({placeholders})",
                values,
            )
            info = {(r['cod'], r['v']): r for r in cur.fetchall()}
        finally:
            db.close()
        unverified = [
            {
                'cod': p['cod'], 'v': p['v'],
                'descrizione': info.get((p['cod'], p['v']), {}).get('descrizione', '—'),
                'settore':     info.get((p['cod'], p['v']), {}).get('settore', '—'),
            }
            for p in unverified
        ]

    return render(request, 'supermarkets/sales_sync_log_detail.html', {
        'log': log,
        'unverified_products': unverified,
    })


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
            f'/api/sync/setup/{supermarket.sync_api_token}/bootstrap-rt/'
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



def sync_bootstrap_realtime_view(request, token):
    """
    Returns a PowerShell script (plain text) that installs sync_sales_rt.ps1 and
    registers its Scheduled Task on the supermarket PC.

    Only for stores running Everest. The installer checks for the database and
    refuses rather than silently installing a script that would never find data.
    """
    try:
        supermarket = Supermarket.objects.get(sync_api_token=token)
    except Supermarket.DoesNotExist:
        return HttpResponse('Not found', status=404)

    server_url = request.build_absolute_uri('/api/sync/realtime-sales/')
    curve_url = request.build_absolute_uri('/api/sync/intraday-curve/')

    script = _build_realtime_bootstrap_script(
        token=token, server_url=server_url, curve_url=curve_url
    )
    return HttpResponse(script, content_type='text/plain; charset=utf-8')


def _build_realtime_bootstrap_script(token: str, server_url: str, curve_url: str) -> str:
    """
    Installer for sync_sales_rt.ps1 + a Scheduled Task every 30 min, from ~08:30 to ~21:30.

    Not overnight: the store's own ERP jobs run in the small hours and the tills are off.

    The start minute is offset 0-29 by a hash of the token so stores spread across the
    half hour instead of all hitting the server on :00 and :30. Derived from the token
    rather than random so a reinstall lands on the same slot.
    """
    offset = int(hashlib.sha256(token.encode()).hexdigest()[:8], 16) % 30
    start_time = f"08:{30 + offset:02d}"

    sync_script_content = _build_realtime_sync_script(
        token=token, server_url=server_url, curve_url=curve_url
    )

    # Everything emitted below must stay pure ASCII. These scripts land on legacy
    # Windows boxes with unpredictable codepages, and a UTF-8 em dash read back as
    # CP1252 ends in 0x94 = U+201D, which PowerShell parses as a string delimiter.
    return f"""# LamApp Real-Time Sales Sync - Bootstrap Installer
# Run as Administrator in PowerShell.

$ScriptDir  = "C:\\LamApp"
$ScriptPath = "$ScriptDir\\sync_sales_rt.ps1"
$TaskName   = "LamApp Sales Sync RT"

Write-Host "Installing LamApp Real-Time Sales Sync..."

# 1. Refuse early if this store does not run Everest
try {{
    $dbCheck = Invoke-Sqlcmd -ServerInstance "localhost" -Database "master" -ErrorAction Stop `
        -Query "SET NOCOUNT ON; SELECT COUNT(*) AS n FROM sys.databases WHERE name = 'everest'"
}} catch {{
    Write-Host "ERROR: cannot reach SQL Server on localhost: $_"
    exit 1
}}
if ($dbCheck.n -eq 0) {{
    Write-Host "ERROR: no 'everest' database on this machine."
    Write-Host "This store is not on NCR Everest, so there is nothing for this sync to read."
    exit 1
}}

# 2. Create directory
if (-not (Test-Path $ScriptDir)) {{
    New-Item -ItemType Directory -Path $ScriptDir | Out-Null
    Write-Host "  Created $ScriptDir"
}}

# 3. Write sync script (token and server URL already embedded)
@'
{sync_script_content}
'@ | Out-File -FilePath $ScriptPath -Encoding UTF8 -Force
Write-Host "  Wrote $ScriptPath"

# 4. Register Scheduled Task - every 30 min from {start_time}, for 13h
$TaskCmd  = "powershell.exe"
$TaskArgs = "-ExecutionPolicy Bypass -NonInteractive -WindowStyle Hidden -File `"$ScriptPath`""
$action   = New-ScheduledTaskAction -Execute $TaskCmd -Argument $TaskArgs
# -Daily and -RepetitionInterval are different parameter sets, so combining them always
# fails with AmbiguousParameterSet. Build a throwaway -Once trigger purely to borrow its
# Repetition object and graft it onto the daily one.
$trigger = New-ScheduledTaskTrigger -Daily -At "{start_time}"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "{start_time}" `
                          -RepetitionInterval (New-TimeSpan -Minutes 30) `
                          -RepetitionDuration (New-TimeSpan -Hours 13)).Repetition
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
                -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
                -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {{
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false | Out-Null
}}
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal | Out-Null

# Verify rather than assume: an unconditional success message once hid a failed
# registration, leaving a store that looked installed and never synced again.
$registered = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $registered) {{
    Write-Host "ERROR: task registration FAILED - the sync will NOT run automatically."
    exit 1
}}
if (-not $registered.Triggers[0].Repetition.Interval) {{
    Write-Host "WARNING: task registered but without a repetition interval - it will run"
    Write-Host "         only once a day at {start_time} instead of every 30 minutes."
}}
Write-Host "  Scheduled task '$TaskName' registered (every 30 min from {start_time}, user: $env:USERNAME)"

Write-Host ""
Write-Host "Setup complete. Running once now to verify..."
& powershell.exe -ExecutionPolicy Bypass -NonInteractive -File $ScriptPath
"""


def _build_realtime_sync_script(token: str, server_url: str, curve_url: str) -> str:
    """
    Returns sync_sales_rt.ps1: today's running sales totals from the Everest till log.

    Sends ABSOLUTE totals, not increments — the server subtracts what it already applied,
    so runs are idempotent and the store PC keeps no state.

    Maps via CassaAna.CodEan (what the till resolves against) filtered to segn = 1;
    without that filter a barcode resolves to several articles and quantities multiply.
    N0_UOM_CODE = 2 is weight-sold goods, excluded since we only handle whole units.
    """
    return f"""# sync_sales_rt.ps1 - LamApp real-time sales sync
# Auto-generated. Do not edit manually. ASCII only (see bootstrap builder).

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ServerUrl = '{server_url}'
$CurveUrl  = '{curve_url}'
$Token     = '{token}'

# The date is pinned here and passed into the query, so the rows and the payload can
# never straddle midnight between the two.
$Today    = (Get-Date).Date
$TodayStr = $Today.ToString("yyyy-MM-dd")

$Query = @"
SET NOCOUNT ON;
DECLARE @today date = '$TodayStr';

SELECT DISTINCT TRY_CAST(LTRIM(RTRIM(cod_est)) AS BIGINT) AS ean_n,
       cod_art AS cod, var_art AS v
INTO #map
FROM CassaAna.dbo.CodEan
WHERE segn = 1 AND TRY_CAST(LTRIM(RTRIM(cod_est)) AS BIGINT) IS NOT NULL;
CREATE INDEX ix_map ON #map(ean_n);

SELECT m.cod AS cod, m.v AS var,
       SUM(CASE WHEN li.BL_RETURN = 1 THEN -li.N0_QUANTITY ELSE li.N0_QUANTITY END) AS sold,
       MAX(a.durata_max_articolo) AS shelf_life
FROM everest.dbo.RDB_LOG_ITEM li
JOIN #map m ON m.ean_n = TRY_CAST(LTRIM(RTRIM(li.SZ_ITEM_REF_NO)) AS BIGINT)
LEFT JOIN Essepiu.dbo.ARTICOLI a
       ON a.cod__articolo = m.cod AND a.variante_articolo = m.v
WHERE CAST(li.DT_TIME_STAMP AS DATE) = @today
  AND li.BL_VOIDED = 0
  AND li.BL_MGR_VOIDED = 0
  AND li.N0_UOM_CODE <> 2
GROUP BY m.cod, m.v
HAVING SUM(CASE WHEN li.BL_RETURN = 1 THEN -li.N0_QUANTITY ELSE li.N0_QUANTITY END) <> 0;
"@

try {{
    $Rows = Invoke-Sqlcmd -ServerInstance "localhost" -Database "Essepiu" -Query $Query -QueryTimeout 120 -ErrorAction Stop
}} catch {{
    Write-Host "ERROR: Everest query failed: $_"
    exit 1
}}

$Products = @($Rows | ForEach-Object {{
    $sl = $null
    if ($_.shelf_life -ne $null -and $_.shelf_life -isnot [System.DBNull]) {{
        $sl = [int]$_.shelf_life
    }}
    @{{ cod = [int]$_.cod; var = [int]$_.var; sold = [int]$_.sold; shelf_life = $sl }}
}})

# Posted even when empty: the 08:30 run is the first of the day and carries little or
# nothing, but it is what tells the server to close the previous day and open a new slot.
$Payload = @{{
    token     = $Token
    sync_date = $TodayStr
    mode      = 'absolute'
    products  = $Products
}} | ConvertTo-Json -Depth 3 -Compress

Write-Host "Sending $($Products.Count) products for $TodayStr..."

try {{
    $resp = Invoke-RestMethod -Uri $ServerUrl -Method POST -Body $Payload -ContentType "application/json" -ErrorAction Stop
    Write-Host "Sync OK."
}} catch {{
    Write-Host "ERROR: POST failed: $_"
    exit 1
}}

# Server-driven so a missed run just gets asked again, with no clock rule or local state.
if (-not $resp.send_curve) {{ exit 0 }}

Write-Host "Server requested intraday curve, computing..."

# 84 days = 12 samples per weekday. Per weekday, not blended: Sunday sells ~71% of its
# units before 14:00 against ~50% on weekdays.
$CurveQuery = @"
SET NOCOUNT ON;
SELECT ((DATEPART(dw, li.DT_TIME_STAMP) + @@DATEFIRST - 2) % 7) AS dow,
       DATEPART(hour, li.DT_TIME_STAMP) AS hr,
       SUM(li.N0_QUANTITY) AS units
FROM everest.dbo.RDB_LOG_ITEM li
WHERE li.DT_TIME_STAMP >= DATEADD(day, -84, CAST(GETDATE() AS DATE))
  AND li.BL_VOIDED = 0
  AND li.BL_MGR_VOIDED = 0
  AND li.N0_UOM_CODE <> 2
GROUP BY ((DATEPART(dw, li.DT_TIME_STAMP) + @@DATEFIRST - 2) % 7),
         DATEPART(hour, li.DT_TIME_STAMP);
"@

try {{
    $CurveRows = Invoke-Sqlcmd -ServerInstance "localhost" -Database "Essepiu" -Query $CurveQuery -QueryTimeout 300 -ErrorAction Stop
}} catch {{
    Write-Host "WARNING: curve query failed, skipping: $_"
    exit 0
}}

# Plain @() arrays, NOT New-Object 'double[]'. PowerShell 5.1's ConvertTo-Json renders a
# strongly-typed array as {{"value":[...],"Count":24}} rather than a bare JSON array, which
# the server rejects outright - so the curve would never arrive and nothing would say why.
$Curve = @()
for ($d = 0; $d -lt 7; $d++) {{ $Curve += ,(@(0.0) * 24) }}
foreach ($row in $CurveRows) {{
    $Curve[[int]$row.dow][[int]$row.hr] = [double]$row.units
}}

$CurvePayload = @{{ token = $Token; curve = $Curve }} | ConvertTo-Json -Depth 5 -Compress

try {{
    Invoke-RestMethod -Uri $CurveUrl -Method POST -Body $CurvePayload -ContentType "application/json" -ErrorAction Stop | Out-Null
    Write-Host "Curve sent."
}} catch {{
    Write-Host "WARNING: curve POST failed: $_"
}}
"""

