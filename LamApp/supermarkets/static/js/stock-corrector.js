// static/js/stock-corrector.js
// Inline stock corrector shared by every stock-correction screen: the real shelf
// count plus a mandatory reason. Pages call
// openStockCorrector(container, initialValue, submit), where submit(newStock, reason)
// returns a promise; the editor closes when it resolves.
(function () {
    'use strict';

    // Values must match DatabaseManager.CORRECTION_REASONS
    const REASONS = [
        ['expired', 'Scaduto'],
        ['broken', 'Rotto/Danneggiato'],
        ['internal_use', 'Uso interno'],
        ['stolen', 'Furti'],
        ['shrinkage', 'Differenze inventariali'],
        ['other', 'Altro'],
    ];

    window.openStockCorrector = function (container, initialValue, submit) {
        if (!container || container.querySelector('.stock-editor')) return;
        const ed = document.createElement('div');
        ed.className = 'stock-editor d-flex flex-wrap gap-1 align-items-center mt-1';
        ed.innerHTML =
            '<input type="number" class="form-control form-control-sm se-stock" min="0" style="width:60px;">' +
            '<select class="form-select form-select-sm se-reason" style="width:auto;">' +
            '<option value="" selected disabled>Motivo…</option>' +
            REASONS.map(function (r) { return '<option value="' + r[0] + '">' + r[1] + '</option>'; }).join('') +
            '</select>' +
            '<button type="button" class="btn btn-sm btn-success se-save"><i class="bi bi-check-lg"></i></button>' +
            '<button type="button" class="btn btn-sm btn-outline-secondary se-cancel"><i class="bi bi-x-lg"></i></button>';
        container.appendChild(ed);
        const stockInput = ed.querySelector('.se-stock');
        stockInput.value = initialValue;
        stockInput.focus();
        stockInput.select();
        ed.querySelector('.se-cancel').addEventListener('click', function () { ed.remove(); });
        ed.querySelector('.se-save').addEventListener('click', function () {
            const n = parseInt(stockInput.value);
            const reason = ed.querySelector('.se-reason').value;
            if (isNaN(n) || n < 0) { alert('Valore non valido'); return; }
            if (!reason) { alert('Seleziona un motivo'); return; }
            const btn = this;
            btn.disabled = true;
            Promise.resolve(submit(n, reason))
                .then(function () { ed.remove(); })
                .catch(function (err) {
                    btn.disabled = false;
                    alert(err && err.message ? err.message : err);
                });
        });
    };
})();
