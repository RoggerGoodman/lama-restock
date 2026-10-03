# LamApp/supermarkets/credit_note_views.py
"""
"Note di accredito": credit notes imported from Dropzone wait here until a
human checks them. Approving takes each line's qty off stock.
"""
import logging
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from .models import CreditNote
from .scripts.DatabaseManager import DatabaseManager

logger = logging.getLogger(__name__)

RECENT_DECIDED_DAYS = 30


@login_required
def credit_note_list_view(request):
    notes = (
        CreditNote.objects
        .filter(storage__supermarket__owner=request.user)
        .select_related('storage', 'storage__supermarket')
        .prefetch_related('lines')
    )
    pending = notes.filter(status=CreditNote.STATUS_PENDING).order_by('doc_date', 'id')
    recent = notes.exclude(status=CreditNote.STATUS_PENDING).filter(
        decided_at__gte=timezone.now() - timedelta(days=RECENT_DECIDED_DAYS)
    ).order_by('-decided_at')
    return render(request, 'credit_notes/list.html', {
        'pending_notes': pending,
        'recent_notes': recent,
        'recent_days': RECENT_DECIDED_DAYS,
    })


def _current_stock(note, lines):
    pairs = list({(l.cod, l.v) for l in lines})
    if not pairs:
        return {}
    db = DatabaseManager(supermarket_name=note.storage.supermarket.name)
    try:
        cur = db.cursor()
        placeholders = ','.join(['(%s,%s)'] * len(pairs))
        cur.execute(
            f"SELECT cod, v, stock FROM product_stats WHERE (cod, v) IN ({placeholders})",
            [x for pair in pairs for x in pair],
        )
        return {(r['cod'], r['v']): r['stock'] for r in cur.fetchall()}
    except Exception:
        logger.exception(f"Could not read stock for credit note {note.pk}")
        return {}
    finally:
        db.close()


def _apply_edits(note, post):
    """qty_<id> sets a line's qty; remove_<id> or a qty of 0 drops the line."""
    for line in list(note.lines.all()):
        if post.get(f'remove_{line.id}'):
            line.delete()
            continue
        raw = post.get(f'qty_{line.id}')
        if raw is None:
            continue
        try:
            qty = int(raw)
        except ValueError:
            continue
        if qty <= 0:
            line.delete()
        elif qty != line.qty:
            line.qty = qty
            line.save(update_fields=['qty'])


@login_required
def credit_note_detail_view(request, pk):
    note = get_object_or_404(
        CreditNote.objects.select_related('storage', 'storage__supermarket'),
        pk=pk, storage__supermarket__owner=request.user,
    )

    if request.method == 'POST':
        action = request.POST.get('action')
        with transaction.atomic():
            # Locked so a double click can't deduct the same note twice
            note = CreditNote.objects.select_for_update().select_related(
                'storage', 'storage__supermarket'
            ).get(pk=note.pk)
            if note.status != CreditNote.STATUS_PENDING:
                messages.warning(request, "Questa nota è già stata gestita.")
                return redirect('credit-note-detail', pk=note.pk)

            if action == 'reject':
                note.status = CreditNote.STATUS_REJECTED
                note.decided_at = timezone.now()
                note.decided_by = request.user
                note.save(update_fields=['status', 'decided_at', 'decided_by'])
                messages.info(request, f"Nota di accredito {note.number} scartata: la giacenza non è stata modificata.")
                return redirect('credit-note-list')

            _apply_edits(note, request.POST)

            if action == 'approve':
                lines = list(note.lines.all())
                if lines:
                    db = DatabaseManager(supermarket_name=note.storage.supermarket.name)
                    try:
                        db.deduct_credit_note([(l.cod, l.v, l.qty) for l in lines])
                    finally:
                        db.close()
                note.status = CreditNote.STATUS_APPROVED
                note.decided_at = timezone.now()
                note.decided_by = request.user
                note.save(update_fields=['status', 'decided_at', 'decided_by'])
                logger.info(f"[NAC] {note.doc_key} ({note.storage.name}) approved by {request.user}: "
                            f"{[(l.cod, l.v, l.qty) for l in lines]}")
                messages.success(request, f"Nota di accredito {note.number} applicata: {len(lines)} articoli scalati dalla giacenza.")
                return redirect('credit-note-list')

        messages.success(request, "Modifiche salvate.")
        return redirect('credit-note-detail', pk=note.pk)

    lines = list(note.lines.all())
    stock = _current_stock(note, lines) if note.status == CreditNote.STATUS_PENDING else {}
    for line in lines:
        line.current_stock = stock.get((line.cod, line.v))
    return render(request, 'credit_notes/detail.html', {
        'note': note,
        'lines': lines,
        'is_pending': note.status == CreditNote.STATUS_PENDING,
    })
