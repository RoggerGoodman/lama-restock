// static/js/ean-scanner.js
// Shared native-BarcodeDetector EAN scanner: green-box targeting + tap-to-confirm.
// Used by inventory search and order review. The overlay markup is injected here
// so pages only include this file and call initEanScanner({ buttonId, onConfirm }).
(function () {
    'use strict';

    const OVERLAY_ID = 'eanScanOverlay';
    // A code must read identically this many frames before it is tappable,
    // which filters out the transient misreads that fired wrong codes.
    const STABLE_FRAMES = 3;

    // Built once and reused; shared safely because only one scan runs at a time.
    function buildOverlay() {
        let overlay = document.getElementById(OVERLAY_ID);
        if (overlay) return overlay;
        overlay = document.createElement('div');
        overlay.id = OVERLAY_ID;
        overlay.style.cssText = 'display:none; position:fixed; inset:0; z-index:2000; background:#000;';
        // Video and canvas share the same box + object-fit:cover, so barcode
        // coordinates map 1:1 onto the canvas with no manual scaling.
        overlay.innerHTML =
            '<video class="ean-scan-video" playsinline muted style="position:absolute; inset:0; width:100%; height:100%; object-fit:cover;"></video>' +
            '<canvas class="ean-scan-canvas" style="position:absolute; inset:0; width:100%; height:100%; object-fit:cover; pointer-events:none;"></canvas>' +
            '<div style="position:absolute; top:0; left:0; right:0; padding:12px; display:flex; justify-content:space-between; align-items:center; background:rgba(0,0,0,0.4);">' +
                '<span class="text-white small ean-scan-status">Inquadra il codice a barre…</span>' +
                '<button type="button" class="btn btn-sm btn-light ean-scan-close"><i class="bi bi-x-lg"></i> Chiudi</button>' +
            '</div>' +
            '<div class="ean-scan-hint" style="position:absolute; bottom:0; left:0; right:0; padding:16px; text-align:center; color:#fff; background:rgba(0,0,0,0.4); font-size:0.95rem;">Inquadra il codice a barre</div>';
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
        const canvas = overlay.querySelector('.ean-scan-canvas');
        const scanStatus = overlay.querySelector('.ean-scan-status');
        const scanHint = overlay.querySelector('.ean-scan-hint');
        const scanCloseBtn = overlay.querySelector('.ean-scan-close');
        const ctx = canvas.getContext('2d');
        const detector = new BarcodeDetector({ formats: ['ean_13', 'ean_8', 'upc_a', 'upc_e'] });

        let stream = null;
        let scanning = false;
        let lastCode = null;     // most recent decoded value
        let stableCount = 0;     // consecutive frames matching lastCode
        let pendingCode = null;  // value that is stable and ready to confirm

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

        // Canvas bitmap matches the camera frame and shares object-fit:cover with
        // the video, so boxes draw in native coordinates with no scaling/offset.
        function drawBox(box, ready) {
            ctx.strokeStyle = ready ? '#22c55e' : 'rgba(255,255,255,0.7)';
            ctx.lineWidth = Math.max(canvas.width, canvas.height) * 0.008;
            ctx.strokeRect(box.x, box.y, box.width, box.height);
        }

        async function scanLoop() {
            if (!scanning) return;
            if (canvas.width !== video.videoWidth || canvas.height !== video.videoHeight) {
                canvas.width = video.videoWidth;
                canvas.height = video.videoHeight;
            }
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            try {
                const codes = await detector.detect(video);
                if (codes.length > 0 && codes[0].rawValue) {
                    const code = codes[0].rawValue.trim();
                    if (code === lastCode) {
                        stableCount++;
                    } else {
                        lastCode = code;
                        stableCount = 1;
                    }
                    const ready = stableCount >= STABLE_FRAMES;
                    if (ready) {
                        pendingCode = code;
                        scanHint.textContent = 'Tocca lo schermo per confermare: ' + code;
                    }
                    drawBox(codes[0].boundingBox, ready);
                } else {
                    // Lost the code: require it to re-stabilise before confirming
                    resetState();
                    scanHint.textContent = 'Inquadra il codice a barre';
                }
            } catch (err) {
                // Transient decode errors are normal between good frames
            }
            requestAnimationFrame(scanLoop);
        }

        async function startScan() {
            try {
                resetState();
                scanStatus.textContent = 'Inquadra il codice a barre…';
                scanHint.textContent = 'Inquadra il codice a barre';
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
