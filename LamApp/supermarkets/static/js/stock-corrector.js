// static/js/stock-corrector.js
// Stock-correction dialog shared by every stock-correction screen: the real shelf
// count plus a mandatory reason. Built for phones, where most corrections happen:
// a modal clear of any table scroll, large tap targets, numeric keypad.
// Pages call openStockCorrector({ title, currentStock, initialValue, submit }), where
// submit(newStock, reason) returns a promise; the dialog closes when it resolves.
(function () {
    'use strict';

    const MODAL_ID = 'stockCorrectorModal';
    // Values must match DatabaseManager.CORRECTION_REASONS
    const REASONS = [
        ['expired', 'Scaduto'],
        ['broken', 'Rotto/Danneggiato'],
        ['internal_use', 'Uso interno'],
        ['stolen', 'Furti'],
        ['shrinkage', 'Differenze inventariali'],
        ['other', 'Altro'],
    ];

    let submitFn = null;

    function buildModal() {
        let el = document.getElementById(MODAL_ID);
        if (el) return el;
        el = document.createElement('div');
        el.id = MODAL_ID;
        // No fade: the modal shows synchronously, so focusing the field stays part of the tap
        el.className = 'modal';
        el.tabIndex = -1;
        el.innerHTML =
            '<div class="modal-dialog">' +
            '<div class="modal-content">' +
            '<div class="modal-header py-2">' +
            '<h6 class="modal-title sc-title mb-0" style="overflow-wrap:anywhere;"></h6>' +
            '<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Chiudi"></button>' +
            '</div>' +
            '<div class="modal-body">' +
            '<label class="form-label fw-semibold mb-1" for="scStock">Giacenza reale</label>' +
            '<input id="scStock" type="number" inputmode="numeric" pattern="[0-9]*" min="0" enterkeyhint="done"' +
            ' class="form-control form-control-lg text-center sc-stock">' +
            '<div class="small text-muted mt-1">Attuale: <span class="sc-current"></span></div>' +
            '<div class="fw-semibold mt-3 mb-1">Motivo</div>' +
            '<div class="row g-2">' +
            REASONS.map(function (r) {
                return '<div class="col-6">' +
                    '<input type="radio" class="btn-check" name="scReason" id="scReason-' + r[0] + '" value="' + r[0] + '" autocomplete="off">' +
                    '<label class="btn btn-outline-primary w-100 h-100 py-2 d-flex align-items-center justify-content-center" for="scReason-' + r[0] + '">' + r[1] + '</label>' +
                    '</div>';
            }).join('') +
            '</div>' +
            '<div class="text-danger small mt-2 sc-error"></div>' +
            '</div>' +
            '<div class="modal-footer">' +
            '<button type="button" class="btn btn-outline-secondary btn-lg" data-bs-dismiss="modal">Annulla</button>' +
            '<button type="button" class="btn btn-success btn-lg flex-grow-1 sc-save">Salva</button>' +
            '</div>' +
            '</div>' +
            '</div>';
        document.body.appendChild(el);

        const input = el.querySelector('.sc-stock');
        input.addEventListener('focus', function () { input.select(); });
        input.addEventListener('keydown', function (e) {
            if (e.key !== 'Enter') return;
            e.preventDefault();
            // Enter closes the keypad, which may be covering the reasons
            if (el.querySelector('input[name="scReason"]:checked')) save(); else input.blur();
        });
        el.querySelector('.sc-save').addEventListener('click', save);
        return el;
    }

    function save() {
        const el = document.getElementById(MODAL_ID);
        const err = el.querySelector('.sc-error');
        const btn = el.querySelector('.sc-save');
        const n = parseInt(el.querySelector('.sc-stock').value);
        const checked = el.querySelector('input[name="scReason"]:checked');
        if (isNaN(n) || n < 0) { err.textContent = 'Inserisci una giacenza valida'; return; }
        if (!checked) { err.textContent = 'Seleziona un motivo'; return; }
        if (btn.disabled) return;
        err.textContent = '';
        btn.disabled = true;
        btn.textContent = 'Salvataggio…';
        Promise.resolve(submitFn(n, checked.value))
            .then(function () { bootstrap.Modal.getOrCreateInstance(el).hide(); })
            .catch(function (e) { err.textContent = (e && e.message) ? e.message : String(e); })
            .finally(function () { btn.disabled = false; btn.textContent = 'Salva'; });
    }

    window.openStockCorrector = function (opts) {
        const el = buildModal();
        submitFn = opts.submit;
        el.querySelector('.sc-title').textContent = opts.title || '';
        el.querySelector('.sc-current').textContent = opts.currentStock;
        el.querySelector('.sc-stock').value = opts.initialValue;
        el.querySelectorAll('input[name="scReason"]').forEach(function (r) { r.checked = false; });
        el.querySelector('.sc-error').textContent = '';
        bootstrap.Modal.getOrCreateInstance(el).show();
        // On touch screens the keypad would cover the reasons; the user taps the field instead
        if (!window.matchMedia('(pointer: coarse)').matches) el.querySelector('.sc-stock').focus();
    };
})();
