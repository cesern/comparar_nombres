// static/app.js — extraido de templates/index.html (Fase 3B)
        let currentExcelB64 = null;

        // ── Tabla de detalle: estado y paginación (cliente) ──
        const PAGE_SIZE = 50;
        const COMPARE_TIMEOUT_MS = 120000;
        let detailResults = [];
        let detailFilter = 'all';
        let detailQuery = '';
        let detailPage = 1;
        let compareAbort = null;

        function showToast(msg, type = 'error') {
            const wrap = document.getElementById('toastWrap');
            if (!wrap) return;
            const el = document.createElement('div');
            el.className = 'toast toast-' + type;
            const icon = document.createElement('i');
            icon.className = type === 'error' ? 'fas fa-circle-exclamation' : 'fas fa-circle-info';
            const span = document.createElement('span');
            span.textContent = msg;
            el.append(icon, span);
            wrap.appendChild(el);
            setTimeout(() => { el.classList.add('out'); setTimeout(() => el.remove(), 350); }, 5000);
        }

        function updateSubmitState() {
            const ok = document.getElementById('file1').files.length > 0 &&
                       document.getElementById('file2').files.length > 0;
            document.getElementById('submitBtn').disabled = !ok;
        }

        function numberToExcelCol(idx) {
            // 0->A ... 25->Z, 26->AA, 27->AB ...
            let s = '';
            let n = idx + 1;
            while (n > 0) {
                const m = (n - 1) % 26;
                s = String.fromCharCode(65 + m) + s;
                n = Math.floor((n - 1) / 26);
            }
            return s;
        }

        function filteredResults() {
            const q = detailQuery.trim().toLowerCase();
            return detailResults.filter(r => {
                if (detailFilter === 'match' && r['Resultado'] !== 'COINCIDENCIA') return false;
                if (detailFilter === 'miss' && r['Resultado'] === 'COINCIDENCIA') return false;
                if (q && !(String(r['Nombre Archivo 1']).toLowerCase().includes(q) ||
                           String(r['Mejor Coincidencia Archivo 2']).toLowerCase().includes(q))) return false;
                return true;
            });
        }

        function scoreClass(v) {
            if (v >= 95) return 'score-pill score-hi';
            if (v >= 80) return 'score-pill score-mid';
            return 'score-pill score-lo';
        }

        function renderDetail() {
            const body = document.getElementById('resultsBody');
            const rows = filteredResults();
            const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
            if (detailPage > totalPages) detailPage = totalPages;
            const start = (detailPage - 1) * PAGE_SIZE;
            const page = rows.slice(start, start + PAGE_SIZE);

            body.innerHTML = '';
            page.forEach(r => {
                const tr = document.createElement('tr');
                const td1 = document.createElement('td'); td1.textContent = r['Nombre Archivo 1'];
                const td2 = document.createElement('td'); td2.textContent = r['Mejor Coincidencia Archivo 2'];
                const td3 = document.createElement('td');
                const pill = document.createElement('span');
                pill.className = scoreClass(Number(r['Similitud (%)']));
                pill.textContent = r['Similitud (%)'] + '%';
                td3.appendChild(pill);
                const td4 = document.createElement('td');
                const badge = document.createElement('span');
                const ok = r['Resultado'] === 'COINCIDENCIA';
                badge.className = ok ? 'res-badge res-ok' : 'res-badge res-miss';
                badge.textContent = r['Resultado'];
                td4.appendChild(badge);
                tr.append(td1, td2, td3, td4);
                body.appendChild(tr);
            });

            document.getElementById('c-all').textContent = `(${detailResults.length})`;
            document.getElementById('c-match').textContent =
                `(${detailResults.filter(r => r['Resultado'] === 'COINCIDENCIA').length})`;
            document.getElementById('c-miss').textContent =
                `(${detailResults.filter(r => r['Resultado'] !== 'COINCIDENCIA').length})`;

            document.getElementById('pagerInfo').textContent = rows.length
                ? `Mostrando ${start + 1}–${start + page.length} de ${rows.length}`
                : 'Sin resultados para este filtro';
            document.getElementById('prevBtn').disabled = detailPage <= 1;
            document.getElementById('nextBtn').disabled = detailPage >= totalPages;

            document.querySelectorAll('.tab-btn').forEach(b =>
                b.classList.toggle('active', b.dataset.filter === detailFilter));
        }

        function setDetailData(results) {
            detailResults = results || [];
            detailFilter = 'all';
            detailQuery = '';
            detailPage = 1;
            document.getElementById('detailSearch').value = '';
            renderDetail();
        }

        function setFilter(f) { detailFilter = f; detailPage = 1; renderDetail(); }
        function onSearch(q) { detailQuery = q; detailPage = 1; renderDetail(); }
        function changePage(d) { detailPage += d; renderDetail(); }

        function resetDetail() {
            detailResults = [];
            detailFilter = 'all';
            detailQuery = '';
            detailPage = 1;
            document.getElementById('detailSearch').value = '';
            document.getElementById('resultsBody').innerHTML = '';
            ['c-all', 'c-match', 'c-miss'].forEach(id => { document.getElementById(id).textContent = ''; });
            document.getElementById('pagerInfo').textContent = '';
            document.querySelectorAll('.tab-btn').forEach(b =>
                b.classList.toggle('active', b.dataset.filter === 'all'));
        }

        function openModal() { document.getElementById('modalOverlay').classList.add('open'); }
        function closeModal() { document.getElementById('modalOverlay').classList.remove('open'); }
        function handleOverlayClick(e) { if (e.target === document.getElementById('modalOverlay')) closeModal(); }
        document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

        async function onFileSelected(n) {
            const input = document.getElementById(`file${n}`);
            const nameEl = document.getElementById(`name${n}`);
            const dz = document.getElementById(`dz${n}`);
            const selWrap = document.getElementById(`selWrap${n}`);
            const sheetSel = document.getElementById(`sheetSelect${n}`);
            const colSel = document.getElementById(`colSelect${n}`);
            const isEm = n === 2;

            if (!input.files.length) return;
            const file = input.files[0];

            nameEl.textContent = file.name;
            dz.classList.add(isEm ? 'ready-emerald' : 'ready-indigo');

            colSel.innerHTML = '';
            sheetSel.innerHTML = '<option>Cargando hojas…</option>';
            selWrap.classList.add('visible');

            try {
                const fd = new FormData();
                fd.append('file', file);
                const res = await fetch('/sheets', { method: 'POST', body: fd });
                if (!res.ok) throw new Error();
                const data = await res.json();
                sheetSel.innerHTML = '';
                data.sheets.forEach(s => {
                    const opt = document.createElement('option');
                    opt.value = s; opt.textContent = s;
                    sheetSel.appendChild(opt);
                });
                await loadColumns(n);
            } catch {
                sheetSel.innerHTML = '<option value="">Error al leer hojas</option>';
            } finally {
                updateSubmitState();
            }
        }

        async function onSheetSelected(n) { await loadColumns(n); }

        async function loadColumns(n) {
            const fileInput = document.getElementById(`file${n}`);
            const sheetSel = document.getElementById(`sheetSelect${n}`);
            const colSel = document.getElementById(`colSelect${n}`);
            if (!fileInput.files.length) return;
            colSel.innerHTML = '<option>Cargando columnas…</option>';
            try {
                const fd = new FormData();
                fd.append('file', fileInput.files[0]);
                if (sheetSel.value) fd.append('sheet_name', sheetSel.value);
                const res = await fetch('/headers', { method: 'POST', body: fd });
                if (!res.ok) throw new Error();
                const data = await res.json();
                colSel.innerHTML = '';
                data.columns.forEach((col, idx) => {
                    const opt = document.createElement('option');
                    opt.value = col;
                    opt.textContent = `${numberToExcelCol(idx)} — ${col}`;
                    colSel.appendChild(opt);
                });
            } catch {
                colSel.innerHTML = '<option value="">Error al leer columnas</option>';
            }
        }

        document.getElementById('compareForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const file1 = document.getElementById('file1').files[0];
            const file2 = document.getElementById('file2').files[0];
            if (!file1 || !file2) { showToast('Por favor selecciona ambos archivos.', 'info'); return; }

            const submitBtn = document.getElementById('submitBtn');
            const loadingArea = document.getElementById('loadingArea');
            const resultsArea = document.getElementById('resultsArea');
            const emptyState = document.getElementById('emptyState');

            if (compareAbort) compareAbort.abort();
            compareAbort = new AbortController();
            const timer = setTimeout(() => compareAbort.abort(), COMPARE_TIMEOUT_MS);

            submitBtn.disabled = true;
            loadingArea.style.display = 'block';
            resultsArea.style.display = 'none';
            emptyState.style.display = 'none';

            const formData = new FormData();
            formData.append('file1', file1);
            formData.append('file2', file2);
            formData.append('col1_name', document.getElementById('colSelect1').value || '');
            formData.append('col2_name', document.getElementById('colSelect2').value || '');
            formData.append('sheet1_name', document.getElementById('sheetSelect1').value || '');
            formData.append('sheet2_name', document.getElementById('sheetSelect2').value || '');
            formData.append('threshold', document.getElementById('threshold').value || '85');

            try {
                const response = await fetch('/compare', { method: 'POST', body: formData, signal: compareAbort.signal });
                if (!response.ok) {
                    let msg = 'Error interno al procesar los archivos';
                    try { const e = await response.json(); if (e.detail) msg = e.detail; } catch (_) {}
                    throw new Error(msg);
                }
                const data = await response.json();
                currentExcelB64 = data.excel_b64;

                const s = data.stats;
                document.getElementById('st-total1').textContent = s.total_file1.toLocaleString();
                document.getElementById('st-total2').textContent = s.total_file2.toLocaleString();
                document.getElementById('st-matches').textContent = s.matches.toLocaleString();
                document.getElementById('st-notfound').textContent = s.not_found.toLocaleString();
                document.getElementById('st-rate').textContent = s.match_rate + '%';
                document.getElementById('st-dupes').textContent = s.duplicates_file2.toLocaleString();
                document.getElementById('st-bar').style.width = '0%';
                setDetailData(data.results);

                resultsArea.style.display = 'flex';
                setTimeout(() => { document.getElementById('st-bar').style.width = s.match_rate + '%'; }, 80);

            } catch (err) {
                if (err && err.name === 'AbortError') {
                    showToast('La comparación superó el tiempo límite (120 s). Prueba con archivos más pequeños.');
                } else {
                    showToast('Hubo un error: ' + err.message);
                }
                emptyState.style.display = 'block';
            } finally {
                clearTimeout(timer);
                updateSubmitState();
                loadingArea.style.display = 'none';
            }
        });

        document.getElementById('downloadBtn').addEventListener('click', () => {
            if (!currentExcelB64) return;
            const link = document.createElement('a');
            link.href = 'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,' + currentExcelB64;
            link.download = 'Resultados_Comparacion.xlsx';
            document.body.appendChild(link); link.click(); document.body.removeChild(link);
        });

        function resetForm() {
            [1, 2].forEach(n => {
                document.getElementById(`file${n}`).value = '';
                document.getElementById(`name${n}`).textContent = 'Haz clic para seleccionar';
                const dz = document.getElementById(`dz${n}`);
                dz.classList.remove('ready-indigo', 'ready-emerald');
                dz.classList.add('file-card');
                document.getElementById(`selWrap${n}`).classList.remove('visible');
                document.getElementById(`sheetSelect${n}`).innerHTML = '';
                document.getElementById(`colSelect${n}`).innerHTML = '';
            });
            document.getElementById('resultsArea').style.display = 'none';
            document.getElementById('loadingArea').style.display = 'none';
            document.getElementById('emptyState').style.display = 'block';
            updateSubmitState();
            ['st-total1', 'st-total2', 'st-matches', 'st-notfound', 'st-rate', 'st-dupes'].forEach(id => {
                document.getElementById(id).textContent = '—';
            });
            document.getElementById('st-bar').style.width = '0%';
            currentExcelB64 = null;
            resetDetail();
            document.getElementById('threshold').value = '85';
            document.getElementById('thresholdVal').textContent = '85';
            document.getElementById('modalThreshold').textContent = '85';
        }

        updateSubmitState();
    
