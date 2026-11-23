// static/app.js — extraido de templates/index.html (Fase 3B)
        let currentExcelB64 = null;
        let currentDownloadUrl = null;

        // ── Tabla de detalle: estado y paginación (cliente) ──
        const PAGE_SIZE = 50;
        let detailResults = [];
        let detailFilter = 'all';
        let detailQuery = '';
        let detailPage = 1;
        let compareAbort = null;

        function makeIcon(name, extraClass = '') {
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.setAttribute('class', extraClass ? `ic ${extraClass}` : 'ic');
            svg.setAttribute('aria-hidden', 'true');
            const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
            use.setAttribute('href', `#ic-${name}`);
            svg.appendChild(use);
            return svg;
        }

        function showToast(msg, type = 'error') {
            const wrap = document.getElementById('toastWrap');
            if (!wrap) return;
            const el = document.createElement('div');
            el.className = 'toast toast-' + type;
            const icon = makeIcon(type === 'error' ? 'circle-exclamation' : 'circle-info');
            const span = document.createElement('span');
            span.textContent = msg;
            el.append(icon, span);
            wrap.appendChild(el);
            setTimeout(() => { el.classList.add('out'); setTimeout(() => el.remove(), 350); }, 5000);
        }

        function updateSubmitState() {
            const ok = document.getElementById('file1').files.length > 0 &&
                       document.getElementById('file2').files.length > 0 &&
                       document.getElementById('colSelect1').value !== '' &&
                       document.getElementById('colSelect2').value !== '';
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
                const ok = r['Resultado'] === 'COINCIDENCIA';
                tr.className = ok ? 'row-ok' : 'row-miss';
                const td1 = document.createElement('td'); td1.textContent = r['Nombre Archivo 1'];
                const td2 = document.createElement('td'); td2.textContent = r['Mejor Coincidencia Archivo 2'];
                const td3 = document.createElement('td');
                const pill = document.createElement('span');
                pill.className = scoreClass(Number(r['Similitud (%)']));
                pill.textContent = r['Similitud (%)'] + '%';
                td3.appendChild(pill);
                const td4 = document.createElement('td');
                const badge = document.createElement('span');
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
            document.getElementById('dupWarn').style.display = 'none';
            ['c-all', 'c-match', 'c-miss'].forEach(id => { document.getElementById(id).textContent = ''; });
            document.getElementById('pagerInfo').textContent = '';
            document.querySelectorAll('.tab-btn').forEach(b =>
                b.classList.toggle('active', b.dataset.filter === 'all'));
        }

        let lastFocusedEl = null;

        function openModal() {
            lastFocusedEl = document.activeElement;
            const overlay = document.getElementById('modalOverlay');
            overlay.classList.add('open');
            const closeBtn = overlay.querySelector('.modal-close');
            if (closeBtn) closeBtn.focus();
        }

        function closeModal() {
            const overlay = document.getElementById('modalOverlay');
            if (!overlay.classList.contains('open')) return;
            overlay.classList.remove('open');
            if (lastFocusedEl && typeof lastFocusedEl.focus === 'function') {
                lastFocusedEl.focus();
                lastFocusedEl = null;
            }
        }

        function handleOverlayClick(e) { if (e.target === document.getElementById('modalOverlay')) closeModal(); }

        document.addEventListener('keydown', e => {
            const overlay = document.getElementById('modalOverlay');
            if (!overlay.classList.contains('open')) return;
            if (e.key === 'Escape') { closeModal(); return; }
            if (e.key === 'Tab') {
                const focusables = overlay.querySelectorAll('button, [href], input, select, [tabindex]:not([tabindex="-1"])');
                if (!focusables.length) return;
                const first = focusables[0];
                const last = focusables[focusables.length - 1];
                if (e.shiftKey && document.activeElement === first) {
                    e.preventDefault(); last.focus();
                } else if (!e.shiftKey && document.activeElement === last) {
                    e.preventDefault(); first.focus();
                }
            }
        });

        function populateColumns(colSel, columns) {
            colSel.innerHTML = '';
            columns.forEach((col, idx) => {
                const opt = document.createElement('option');
                opt.value = col;
                opt.textContent = `${numberToExcelCol(idx)} — ${col}`;
                colSel.appendChild(opt);
            });
        }

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
                if (Array.isArray(data.columns) && data.columns.length > 0) {
                    populateColumns(colSel, data.columns);
                } else {
                    await loadColumns(n);
                }
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
                populateColumns(colSel, data.columns);
            } catch {
                colSel.innerHTML = '<option value="">Error al leer columnas</option>';
            } finally {
                updateSubmitState();
            }
        }

        // ── Envío async con barra de progreso ──
        let activeJob = null;
        let elapsedTimer = null;
        let pollCounts = '';

        function setLoadingMode() {
            document.getElementById('loadingTrack').classList.add('determinate');
            document.getElementById('loadingSub').style.display = 'flex';
            document.getElementById('loadingPct').textContent = '';
            document.getElementById('loadingFill').style.transform = 'scaleX(0)';
            document.getElementById('loadingMsg').textContent = 'Procesando registros…';
            document.getElementById('loadingCounts').textContent = '';
            pollCounts = '';
        }

        function stopLoading() {
            if (elapsedTimer) { clearInterval(elapsedTimer); elapsedTimer = null; }
            document.getElementById('loadingArea').style.display = 'none';
            updateSubmitState();
        }

        function cancelActiveJobSilent() {
            if (!activeJob) return;
            const j = activeJob;
            activeJob = null;
            fetch(`/progress/${j}`, { method: 'DELETE' }).catch(() => {});
        }

        function showResults(data) {
            const resultsArea = document.getElementById('resultsArea');
            currentExcelB64 = data.excel_b64 || null;
            currentDownloadUrl = data.download_url || null;

            const s = data.stats;
            document.getElementById('st-total1').textContent = s.total_file1.toLocaleString();
            document.getElementById('st-total2').textContent = s.total_file2.toLocaleString();
            document.getElementById('st-matches').textContent = s.matches.toLocaleString();
            document.getElementById('st-notfound').textContent = s.not_found.toLocaleString();
            document.getElementById('st-rate').textContent = s.match_rate + '%';
            document.getElementById('st-dupes').textContent = s.duplicates_file2.toLocaleString();
            document.getElementById('st-bar').style.transform = 'scaleX(0)';
            setDetailData(data.results);

            const dupWarn = document.getElementById('dupWarn');
            const d1 = s.duplicates_file1 || 0;
            const d2 = s.duplicates_file2 || 0;
            if (d1 > 0 || d2 > 0) {
                const parts = [];
                if (d2 > 0) parts.push(`${d2} en Archivo 2`);
                if (d1 > 0) parts.push(`${d1} en Archivo 1`);
                document.getElementById('dupWarnText').textContent =
                    `Duplicados detectados (${parts.join(' · ')}). Revisa la hoja correspondiente en el Excel.`;
                dupWarn.style.display = 'flex';
            } else {
                dupWarn.style.display = 'none';
            }

            resultsArea.style.display = 'flex';
            requestAnimationFrame(() => {
                document.getElementById('st-bar').style.transform = `scaleX(${s.match_rate / 100})`;
            });
        }

        async function readError(res) {
            let msg = 'Error interno al procesar los archivos';
            try { const e = await res.json(); if (e.detail) msg = e.detail; } catch (_) {}
            return new Error(msg);
        }

        const STAGE_MSG = { encolado: 'En cola…', comparando: 'Comparando nombres…', generando: 'Generando Excel…' };

        async function runAsync(formData) {
            const started = Date.now();
            const startRes = await fetch('/start', { method: 'POST', body: formData, signal: compareAbort.signal });
            if (!startRes.ok) throw await readError(startRes);
            const { job_id } = await startRes.json();
            activeJob = job_id;
            setLoadingMode();

            elapsedTimer = setInterval(() => {
                if (activeJob !== job_id) { clearInterval(elapsedTimer); elapsedTimer = null; return; }
                const base = Math.round((Date.now() - started) / 1000) + ' s';
                document.getElementById('loadingCounts').textContent = pollCounts ? `${base} · ${pollCounts}` : base;
            }, 500);

            let pollErrors = 0;
            while (activeJob === job_id) {
                await new Promise(r => setTimeout(r, 600));
                if (activeJob !== job_id) return;
                let p;
                try {
                    const pr = await fetch(`/progress/${job_id}`, { signal: compareAbort ? compareAbort.signal : undefined });
                    if (!pr.ok) {
                        const err = await readError(pr);
                        err.noRetry = pr.status >= 400 && pr.status < 500;
                        throw err;
                    }
                    p = await pr.json();
                    pollErrors = 0;
                } catch (err) {
                    if (err && err.name === 'AbortError' && (!compareAbort || activeJob !== job_id)) return;
                    if (err && err.noRetry) throw err;
                    pollErrors += 1;
                    if (pollErrors <= 3) continue;
                    throw err;
                }
                if (p.status === 'done') {
                    activeJob = null;
                    showResults(p.result);
                    return;
                }
                if (p.status === 'error') throw new Error(p.error || 'Error en el job');
                if (p.status === 'cancelled') {
                    activeJob = null;
                    showToast('Comparación cancelada.', 'info');
                    document.getElementById('emptyState').style.display = 'block';
                    return;
                }
                const pct = p.total > 0 ? Math.round(p.processed / p.total * 100) : 0;
                document.getElementById('loadingFill').style.transform = `scaleX(${pct / 100})`;
                document.getElementById('loadingPct').textContent = pct + '%';
                document.getElementById('loadingMsg').textContent = STAGE_MSG[p.stage] || 'Procesando…';
                pollCounts = `${Number(p.processed).toLocaleString()} / ${Number(p.total).toLocaleString()}`;
            }
        }

        document.getElementById('cancelBtn').addEventListener('click', async () => {
            if (!activeJob) return;
            const j = activeJob;
            activeJob = null;
            try { await fetch(`/progress/${j}`, { method: 'DELETE' }); } catch (_) {}
            showToast('Comparación cancelada.', 'info');
            document.getElementById('emptyState').style.display = 'block';
            stopLoading();
        });

        document.getElementById('compareForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const file1 = document.getElementById('file1').files[0];
            const file2 = document.getElementById('file2').files[0];
            if (!file1 || !file2) { showToast('Por favor selecciona ambos archivos.', 'info'); return; }

            const submitBtn = document.getElementById('submitBtn');
            const loadingArea = document.getElementById('loadingArea');
            const resultsArea = document.getElementById('resultsArea');
            const emptyState = document.getElementById('emptyState');

            cancelActiveJobSilent();
            if (compareAbort) compareAbort.abort();
            compareAbort = new AbortController();

            submitBtn.disabled = true;
            setLoadingMode();
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
                await runAsync(formData);
            } catch (err) {
                if (err && err.name === 'AbortError') {
                    if (!compareAbort) return;
                    showToast('Comparación interrumpida.', 'info');
                } else {
                    showToast('Hubo un error: ' + err.message);
                }
                emptyState.style.display = 'block';
            } finally {
                stopLoading();
            }
        });

        document.getElementById('downloadBtn').addEventListener('click', () => {
            if (currentDownloadUrl) {
                const link = document.createElement('a');
                link.href = currentDownloadUrl;
                link.download = 'Resultados_Comparacion.xlsx';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                return;
            }
            if (!currentExcelB64) return;
            const bin = atob(currentExcelB64);
            const bytes = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
            const blob = new Blob([bytes], {
                type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'Resultados_Comparacion.xlsx';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            setTimeout(() => URL.revokeObjectURL(url), 1000);
        });

        function resetForm() {
            cancelActiveJobSilent();
            if (compareAbort) { compareAbort.abort(); compareAbort = null; }
            [1, 2].forEach(n => {
                document.getElementById(`file${n}`).value = '';
                document.getElementById(`name${n}`).textContent = 'Haz clic o arrastra el archivo';
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
            document.getElementById('st-bar').style.transform = 'scaleX(0)';
            currentExcelB64 = null;
            currentDownloadUrl = null;
            resetDetail();
            document.getElementById('threshold').value = '85';
            document.getElementById('thresholdVal').textContent = '85';
            document.getElementById('modalThreshold').textContent = '85';
        }

        updateSubmitState();

        // ── Drag & drop real en las tarjetas de archivo ──
        [1, 2].forEach(n => {
            const dz = document.getElementById(`dz${n}`);
            const input = document.getElementById(`file${n}`);
            ['dragenter', 'dragover'].forEach(ev => dz.addEventListener(ev, e => {
                e.preventDefault();
                dz.classList.add('dragover');
            }));
            ['dragleave', 'drop'].forEach(ev => dz.addEventListener(ev, e => {
                e.preventDefault();
                dz.classList.remove('dragover');
            }));
            dz.addEventListener('drop', e => {
                if (e.dataTransfer.files.length) {
                    input.files = e.dataTransfer.files;
                    onFileSelected(n);
                }
            });
        });

        // ── Dropdown propio accesible (reemplaza el popup nativo del SO) ──
        function closeAllDd() {
            document.querySelectorAll('.dd.open').forEach(d => {
                d.classList.remove('open');
                d.querySelector('.dd-btn').setAttribute('aria-expanded', 'false');
            });
        }

        function splitLetter(text) {
            const m = String(text).match(/^([A-Z]+)\s*[—–-]\s*(.*)$/);
            return m ? { letter: m[1], name: m[2] } : { letter: '', name: String(text) };
        }

        function buildDropdown(cfg) {
            const native = document.getElementById(cfg.select);
            if (!native) return;
            native.style.display = 'none';

            const wrap = document.createElement('div');
            wrap.className = 'dd' + (cfg.em ? ' em' : '');
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'dd-btn';
            btn.setAttribute('aria-haspopup', 'listbox');
            btn.setAttribute('aria-expanded', 'false');
            const lbl = document.querySelector(`label[for="${cfg.select}"]`);
            if (lbl && lbl.id) btn.setAttribute('aria-labelledby', lbl.id);
            const letter = document.createElement('span');
            letter.className = 'dd-letter';
            letter.style.display = 'none';
            const val = document.createElement('span');
            val.className = 'dd-value placeholder';
            val.textContent = cfg.placeholder;
            const chev = makeIcon('chevron-down', 'dd-chevron');
            btn.append(letter, val, chev);
            const list = document.createElement('div');
            list.className = 'dd-list';
            list.setAttribute('role', 'listbox');
            wrap.append(btn, list);
            native.parentNode.insertBefore(wrap, native);

            let hi = -1;

            function paint() {
                const o = native.options[native.selectedIndex];
                if (o && o.value !== '') {
                    const parts = splitLetter(o.textContent);
                    letter.style.display = parts.letter ? '' : 'none';
                    letter.textContent = parts.letter;
                    val.textContent = parts.name || o.textContent;
                    val.classList.remove('placeholder');
                } else if (o) {
                    letter.style.display = 'none';
                    val.textContent = o.textContent;
                    val.classList.remove('placeholder');
                } else {
                    letter.style.display = 'none';
                    val.textContent = cfg.placeholder;
                    val.classList.add('placeholder');
                }
                Array.from(list.children).forEach((b, i) => {
                    const opt = native.options[i];
                    b.setAttribute('aria-selected', opt && opt.selected ? 'true' : 'false');
                });
            }

            function clearHi() {
                Array.from(list.children).forEach(b => b.classList.remove('highlight'));
            }

            function setHi(i) {
                hi = i;
                clearHi();
                const b = list.children[i];
                if (b) {
                    b.classList.add('highlight');
                    b.scrollIntoView({ block: 'nearest' });
                }
            }

            function open(instant = false) {
                closeAllDd();
                wrap.classList.toggle('instant', instant);
                wrap.classList.add('open');
                btn.setAttribute('aria-expanded', 'true');
                const idx = Math.max(0, native.selectedIndex);
                setHi(idx);
                const b = list.children[idx];
                if (b) b.focus();
            }

            function close() {
                wrap.classList.remove('open', 'instant');
                btn.setAttribute('aria-expanded', 'false');
                hi = -1;
                clearHi();
            }

            function choose(i) {
                native.selectedIndex = i;
                close();
                btn.focus();
                paint();
                if (cfg.kind === 'sheet') onSheetSelected(cfg.n);
            }

            function refresh() {
                list.innerHTML = '';
                Array.from(native.options).forEach((o, i) => {
                    const parts = splitLetter(o.textContent);
                    const b = document.createElement('button');
                    b.type = 'button';
                    b.className = 'dd-opt';
                    b.setAttribute('role', 'option');
                    b.setAttribute('aria-selected', o.selected ? 'true' : 'false');
                    const ls = document.createElement('span');
                    ls.className = 'dd-letter';
                    ls.textContent = parts.letter;
                    if (!parts.letter) ls.style.display = 'none';
                    const nm = document.createElement('span');
                    nm.className = 'dd-opt-name';
                    nm.textContent = parts.name;
                    const ck = makeIcon('check', 'dd-check');
                    b.append(ls, nm, ck);
                    b.addEventListener('click', () => choose(i));
                    b.addEventListener('mousemove', () => setHi(i));
                    list.appendChild(b);
                });
                paint();
            }

            btn.addEventListener('click', () => {
                if (wrap.classList.contains('open')) { close(); } else { open(false); }
            });
            btn.addEventListener('keydown', e => {
                if (['ArrowDown', 'ArrowUp', 'Enter', ' '].includes(e.key)) {
                    e.preventDefault();
                    open(true);
                }
            });
            list.addEventListener('keydown', e => {
                const count = list.children.length;
                if (e.key === 'ArrowDown') { e.preventDefault(); setHi(Math.min(count - 1, hi + 1)); }
                else if (e.key === 'ArrowUp') { e.preventDefault(); setHi(Math.max(0, hi - 1)); }
                else if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); if (hi >= 0) choose(hi); }
                else if (e.key === 'Escape') { e.preventDefault(); close(); btn.focus(); }
                else if (e.key === 'Tab') { close(); }
            });

            new MutationObserver(refresh).observe(native, { childList: true });
            refresh();
        }

        [
            { select: 'sheetSelect1', kind: 'sheet', n: 1, em: false, placeholder: 'Elige hoja…' },
            { select: 'colSelect1', kind: 'col', n: 1, em: false, placeholder: 'Elige columna…' },
            { select: 'sheetSelect2', kind: 'sheet', n: 2, em: true, placeholder: 'Elige hoja…' },
            { select: 'colSelect2', kind: 'col', n: 2, em: true, placeholder: 'Elige columna…' },
        ].forEach(buildDropdown);
        document.addEventListener('click', e => {
            if (!e.target.closest('.dd')) closeAllDd();
        });
    
