// static/js/ean-scanner.js
// Shared native-BarcodeDetector EAN scanner: a fixed viewfinder frame that turns
// green when a code locks, plus tap-to-confirm. Used by inventory search and order
// review. The overlay markup is injected here so pages only include this file and
// call initEanScanner({ buttonId, onConfirm }).
//
// Why a fixed frame instead of a box drawn on the barcode: drawing a tracking box
// needs a <canvas> overlay, which rendered invisibly on some devices. A fixed DOM
// frame is guaranteed to show and needs no coordinate math; detection stays on the
// <video>, which is reliable.
(function () {
    'use strict';

    const OVERLAY_ID = 'eanScanOverlay';
    const IDLE_BORDER = 'rgba(255,255,255,0.9)';
    const LOCK_BORDER = '#22c55e';
    // A code must read identically this many frames before it locks, which filters
    // out the transient misreads that fired wrong codes.
    const STABLE_FRAMES = 3;

    // Built once and reused; shared safely because only one scan runs at a time.
    function buildOverlay() {
        let overlay = document.getElementById(OVERLAY_ID);
        if (overlay) return overlay;
        overlay = document.createElement('div');
        overlay.id = OVERLAY_ID;
        overlay.style.cssText = 'display:none; position:fixed; inset:0; z-index:2000; background:#000;';
        overlay.innerHTML =
            '<video class="ean-scan-video" playsinline muted style="position:absolute; inset:0; width:100%; height:100%; object-fit:cover;"></video>' +
            // Centered viewfinder; the huge box-shadow dims everything outside it.
            '<div class="ean-scan-reticle" style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); ' +
                'width:78%; max-width:420px; height:42%; max-height:220px; border:3px solid ' + IDLE_BORDER + '; ' +
                'border-radius:14px; box-shadow:0 0 0 100vmax rgba(0,0,0,0.35); ' +
                'transition:border-color 0.12s; pointer-events:none;"></div>' +
            '<div style="position:absolute; top:0; left:0; right:0; padding:12px; display:flex; justify-content:space-between; align-items:center; background:rgba(0,0,0,0.4);">' +
                '<span class="text-white small ean-scan-status">Inquadra il codice a barre…</span>' +
                '<button type="button" class="btn btn-sm btn-light ean-scan-close"><i class="bi bi-x-lg"></i> Chiudi</button>' +
            '</div>' +
            '<div class="ean-scan-hint" style="position:absolute; bottom:0; left:0; right:0; padding:16px; text-align:center; color:#fff; background:rgba(0,0,0,0.4); font-size:0.95rem;">Inquadra il codice a barre nel riquadro</div>';
        document.body.appendChild(overlay);
        return overlay;
    }

    // options: { buttonId: string, onConfirm: function(code) }
    // The button stays hidden where the camera or BarcodeDetector is unavailable.
    window.initEanScanner = function (options) {
        options = options || {};
        const scanBtn = document.getElementById(options.buttonId);
        const onConfirm = options.onConfirm;
        if (!scanBtn || typeof onConfirm !== 'function') return;

        const supported = ('BarcodeDetector' in window)
            && !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
        if (!supported) return;

        const overlay = buildOverlay();
        const video = overlay.querySelector('.ean-scan-video');
        const reticle = overlay.querySelector('.ean-scan-reticle');
        const scanStatus = overlay.querySelector('.ean-scan-status');
        const scanHint = overlay.querySelector('.ean-scan-hint');
        const scanCloseBtn = overlay.querySelector('.ean-scan-close');
        const detector = new BarcodeDetector({ formats: ['ean_13', 'ean_8', 'upc_a', 'upc_e'] });

        let stream = null;
        let scanning = false;
        let lastCode = null;     // most recent decoded value (acquiring)
        let stableCount = 0;     // consecutive frames matching lastCode
        let pendingCode = null;  // value that is locked and ready to confirm

        function setIdle() {
            reticle.style.borderColor = IDLE_BORDER;
            scanHint.textContent = 'Inquadra il codice a barre nel riquadro';
        }
        function setLocked(code) {
            pendingCode = code;
            reticle.style.borderColor = LOCK_BORDER;
            scanHint.textContent = 'Tocca lo schermo per confermare: ' + code;
        }

        function resetState() {
            lastCode = null;
            stableCount = 0;
            pendingCode = null;
        }

        function stopScan() {
            scanning = false;
            resetState();
            if (stream) {
                stream.getTracks().forEach(t => t.stop());
                stream = null;
            }
            overlay.style.display = 'none';
        }

        function confirmPending() {
            if (!pendingCode) return;   // nothing locked on yet: ignore the tap
            const code = pendingCode;
            stopScan();
            onConfirm(code);
        }

        // Detection runs on the <video> (reliable). The lock is sticky: once a code
        // turns the frame green it stays green until the user taps or a DIFFERENT
        // code stabilises, so a dropped frame never makes it flicker.
        async function scanLoop() {
            if (!scanning) return;
            try {
                const codes = await detector.detect(video);
                if (codes.length > 0 && codes[0].rawValue) {
                    const code = codes[0].rawValue.trim();
                    if (code === pendingCode) {
                        // already locked on this one: keep it
                    } else if (code === lastCode) {
                        if (++stableCount >= STABLE_FRAMES) setLocked(code);
                    } else {
                        lastCode = code;
                        stableCount = 1;
                    }
                }
                // No code this frame: keep any existing lock (avoids flicker).
            } catch (err) {
                // Transient decode errors are normal between good frames
            }
            requestAnimationFrame(scanLoop);
        }

        async function startScan() {
            try {
                resetState();
                setIdle();
                scanStatus.textContent = 'Inquadra il codice a barre…';
                overlay.style.display = 'block';
                stream = await navigator.mediaDevices.getUserMedia({
                    video: { facingMode: 'environment' }
                });
                video.srcObject = stream;
                await video.play();
                scanning = true;
                requestAnimationFrame(scanLoop);
            } catch (err) {
                stopScan();
                alert('Impossibile accedere alla fotocamera: ' + err.message);
            }
        }

        scanBtn.style.display = '';
        scanBtn.addEventListener('click', startScan);
        scanCloseBtn.addEventListener('click', stopScan);
        // Tap anywhere on the overlay confirms the locked-on code (but not the
        // close button, which has its own handler).
        overlay.addEventListener('click', function (e) {
            if (e.target === scanCloseBtn || scanCloseBtn.contains(e.target)) return;
            confirmPending();
        });
    };
})();
