const App = (() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);
  const content = () => $('#content');

  // Numeric FieldKind values from pkg/types.go — FieldOptions.Kind is encoded
  // as a plain integer over the wire (no string enum on the Go side).
  const FIELD_KINDS = [
    ['0', 'keyword'], ['1', 'text'], ['2', 'int'],
    ['3', 'float'], ['4', 'bool'], ['5', 'time'], ['6', 'vector'],
  ];

  function apiBase() {
    return ($('#api-base').value || '').replace(/\/+$/, '');
  }

  async function api(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(apiBase() + path, opts);
    const text = await res.text();
    if (!res.ok) throw new Error(`${res.status} ${text}`);
    try { return JSON.parse(text); } catch { return text; }
  }

  async function fetchIndexes() {
    try {
      const data = await api('GET', '/v1/indexes');
      return Array.isArray(data) ? data : [];
    } catch { return []; }
  }

  function toast(msg, ok = true) {
    const el = document.createElement('div');
    el.className = `toast ${ok ? 'toast-ok' : 'toast-err'}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3500);
  }

  function json(v) {
    return `<pre class="json">${escHtml(JSON.stringify(v, null, 2))}</pre>`;
  }

  function escHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function fmtNum(n) {
    if (n === undefined || n === null) return '—';
    return Number(n).toLocaleString();
  }

  function loading() {
    return '<div class="empty-state"><div class="spinner"></div><p class="mt-2">Loading…</p></div>';
  }

  function empty(msg) {
    return `<div class="empty-state"><p>${escHtml(msg)}</p></div>`;
  }

  function field(name, opts = {}) {
    const typ = opts.type || 'text';
    const lbl = opts.label || name;
    const ph = opts.placeholder || '';
    const val = opts.default || '';
    return `
      <div class="space-y-1">
        <label class="text-xs font-medium text-gray-400">${escHtml(lbl)}</label>
        <input id="${name}" type="${typ}" name="${name}" value="${escHtml(val)}" placeholder="${escHtml(ph)}" />
      </div>`;
  }

  function indexSelect(inputId) {
    return `<select id="${inputId}"><option value="">Loading…</option></select>`;
  }

  async function populateIndexSelect(inputId, selected) {
    const list = await fetchIndexes();
    const el = $(`#${inputId}`);
    if (!el) return;
    el.innerHTML = list.length
      ? list.map(i => `<option value="${escHtml(i.id)}" ${i.id === selected ? 'selected' : ''}>${escHtml(i.id)}</option>`).join('')
      : '<option value="">— no indexes —</option>';
  }

  let currentTab = 'search';

  function switchTab(tab) {
    currentTab = tab;
    $$('.nav-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    const titles = { search: 'Search', indexes: 'Indexes', integrations: 'Integrations' };
    $('#page-title').textContent = titles[tab] || tab;
    render();
  }

  async function render() {
    content().innerHTML = loading();
    try {
      if (currentTab === 'search') await renderSearch();
      else if (currentTab === 'indexes') await renderIndexes();
      else if (currentTab === 'integrations') await renderIntegrations();
    } catch (e) {
      content().innerHTML = empty('Error: ' + e.message);
    }
  }

  // ── Search ─────────────────────────────────────────────────────────────────
  // GET  /v1/indexes/{id}/lookup?k=v&...    quick key=value lookup
  // POST /v1/indexes/{id}/search            full query DSL (WireQuery)

  async function renderSearch() {
    content().innerHTML = `
      <form class="search-bar" onsubmit="event.preventDefault(); App.doLookup()">
        <div class="search-index">${indexSelect('lookup-index')}</div>
        <input id="lookup-query" type="search" autocomplete="off" placeholder="field=value &amp; status=active" aria-label="Search filters" />
        <input id="lookup-limit" type="number" min="1" max="500" value="25" aria-label="Result limit" />
        <button class="btn btn-primary" type="submit">Search</button>
      </form>
      <div id="search-fields" class="field-list"></div>
      <div id="search-results" class="mt-4">
        <div class="empty-state"><p>Select an index and enter filters.</p></div>
      </div>
    `;
    await populateIndexSelect('lookup-index');
    $('#lookup-index').addEventListener('change', loadSearchFields);
    await loadSearchFields();
  }

  async function loadSearchFields() {
    const id = $('#lookup-index') && $('#lookup-index').value;
    const host = $('#search-fields');
    if (!id || !host) return;
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/schema`);
      host.innerHTML = Object.keys(data.fields || {}).map(name =>
        `<button type="button" class="field-chip" onclick="App.addSearchField(decodeURIComponent('${encodeURIComponent(name)}'))">${escHtml(name)}</button>`).join('');
    } catch { host.innerHTML = ''; }
  }

  function addSearchField(name) {
    const input = $('#lookup-query');
    const separator = input.value.trim() ? '&' : '';
    input.value += `${separator}${name}=`;
    input.focus();
  }

  function fmtCell(v) {
    if (v === undefined || v === null || v === '') return '<span class="text-gray-600">—</span>';
    if (typeof v === 'object') return escHtml(JSON.stringify(v));
    return escHtml(String(v));
  }

  function renderHits(hits) {
    if (!hits || !hits.length) return empty('No results.');

    // One column per field actually present across the returned docs, in
    // first-seen order, so the table shows real data instead of a raw blob.
    const fieldOrder = [];
    const seen = new Set();
    let anyDoc = false;
    for (const h of hits) {
      if (!h.doc) continue;
      anyDoc = true;
      for (const k of Object.keys(h.doc)) {
        if (k === 'id') continue;
        if (!seen.has(k)) { seen.add(k); fieldOrder.push(k); }
      }
    }

    let html = `<div class="card"><div class="card-header">${hits.length} result(s)</div>`;
    if (!anyDoc) {
      html += `<p class="text-xs text-gray-500 mb-2">This index doesn't store full documents (created with DisableSource), so only ID and score are available.</p>`;
    }
    html += `<div class="overflow-x-auto"><table><thead><tr><th>ID</th><th>Score</th>${fieldOrder.map(f => `<th>${escHtml(f)}</th>`).join('')}</tr></thead><tbody>`;
    for (const h of hits) {
      html += `<tr>
        <td class="font-mono text-sm">${escHtml(h.id)}</td>
        <td>${h.score !== undefined ? h.score.toFixed(4) : '—'}</td>
        ${fieldOrder.map(f => `<td class="text-sm max-w-xs truncate" title="${h.doc && h.doc[f] !== undefined ? escHtml(String(h.doc[f])) : ''}">${fmtCell(h.doc && h.doc[f])}</td>`).join('')}
      </tr>`;
    }
    html += '</tbody></table></div></div>';
    return html;
  }

  async function doLookup() {
    const id = $('#lookup-index').value.trim();
    if (!id) { toast('Select an index first.', false); return; }
    const limit = parseInt($('#lookup-limit').value) || 25;
    const raw = $('#lookup-query').value.trim().replace(/^\?/, '');
    const params = new URLSearchParams();
    for (const part of raw.split('&')) {
      if (!part.trim()) continue;
      const split = part.indexOf('=');
      if (split < 1) { toast(`Invalid filter: ${part}`, false); return; }
      params.append(part.slice(0, split).trim(), part.slice(split + 1).trim());
    }
    params.set('limit', String(limit));
    const res = $('#search-results');
    res.innerHTML = loading();
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/lookup?${params.toString()}`);
      res.innerHTML = `<div class="result-meta"><span>${fmtNum(data.total)} results</span><span>${fmtNum(data.latency_ns)} ns</span></div>${renderHits(data.hits)}`;
    } catch (e) {
      res.innerHTML = `<div class="card text-red-400 text-sm">Lookup error: ${escHtml(e.message)}</div>`;
    }
  }

  // ── Indexes ────────────────────────────────────────────────────────────────
  // GET/POST/DELETE /v1/indexes, /v1/indexes/{id}/stats, reload, reload-async,
  // freeze, persist, snapshot, docs/{id}, generations, validate, repair, compact

  let schemaFieldRows = 0;

  async function renderIndexes() {
    const list = await fetchIndexes();
    let totalDocs = 0;
    for (const mi of list) totalDocs += (mi.stats && mi.stats.docs) || 0;

    let html = `
      <div class="grid-3 mb-6">
        <div class="card"><div class="card-header">Indexes</div><div class="text-3xl font-bold">${fmtNum(list.length)}</div></div>
        <div class="card"><div class="card-header">Total Documents</div><div class="text-3xl font-bold">${fmtNum(totalDocs)}</div></div>
        <div class="card"><div class="card-header">Server</div><div id="server-status" class="text-sm text-green-400 mt-1">checking…</div></div>
      </div>

      <div class="card max-w-2xl mb-6">
        <div class="card-header">Create Index</div>
        <div class="space-y-3 mt-2">
          ${field('new-index-id', { label: 'Index ID', placeholder: 'e.g. claims, members, products' })}
          <div class="space-y-1">
            <label class="text-xs font-medium text-gray-400">Schema</label>
            <select id="new-index-schema">
              <option value="record">Built-in record schema (term + group_id + date_key + partition_id)</option>
              <option value="custom">Custom fields</option>
            </select>
          </div>
          <div id="custom-schema-rows" class="hidden space-y-2"></div>
          <button id="add-field-btn" class="btn btn-secondary text-xs hidden" onclick="App.addSchemaFieldRow()">+ Add field</button>
          <button class="btn btn-primary" onclick="App.createIndex()">Create</button>
        </div>
      </div>

      <div class="space-y-4">`;

    if (!list.length) {
      html += empty('No indexes yet. Create one above.');
    } else {
      for (const mi of list) {
        const st = mi.stats || {};
        const lat = mi.latency || {};
        html += `
          <div class="card">
            <div class="flex items-center justify-between mb-3">
              <div class="flex items-center gap-2">
                <span class="font-semibold">${escHtml(mi.id)}</span>
                ${mi.reloading ? '<span class="badge badge-yellow">Reloading</span>' : ''}
                <span class="text-xs text-gray-500">gen ${fmtNum(mi.generation)}</span>
              </div>
              <div class="flex gap-2 flex-wrap justify-end">
                <button class="btn btn-primary text-xs" onclick="App.reloadIndex('${escHtml(mi.id)}')">Reload</button>
                <button class="btn btn-secondary text-xs" onclick="App.freezeIndex('${escHtml(mi.id)}')">Freeze</button>
                <button class="btn btn-secondary text-xs" onclick="App.persistIndex('${escHtml(mi.id)}')">Persist</button>
                <button class="btn btn-secondary text-xs" onclick="App.toggleDetail('${escHtml(mi.id)}')">Detail</button>
                <button class="btn btn-danger text-xs" onclick="App.deleteIndex('${escHtml(mi.id)}')">Delete</button>
              </div>
            </div>
            <div class="grid grid-cols-4 gap-3 text-sm">
              <div class="text-gray-500">Docs: <span class="text-gray-200">${fmtNum(st.docs)}</span></div>
              <div class="text-gray-500">Fields: <span class="text-gray-200">${fmtNum(st.fields)}</span></div>
              <div class="text-gray-500">Terms: <span class="text-gray-200">${fmtNum(st.terms)}</span></div>
              <div class="text-gray-500">Rows/s: <span class="text-gray-200">${lat.last_rows_per_second ? fmtNum(Math.round(lat.last_rows_per_second)) : '—'}</span></div>
            </div>
            ${lat.last_error ? `<div class="text-xs text-red-400 mt-2">Last error: ${escHtml(lat.last_error)}</div>` : ''}
            <div id="detail-${escHtml(mi.id)}" class="mt-4 hidden"></div>
          </div>`;
      }
    }
    html += '</div>';
    content().innerHTML = html;

    $('#new-index-schema').addEventListener('change', (e) => {
      const custom = e.target.value === 'custom';
      $('#custom-schema-rows').classList.toggle('hidden', !custom);
      $('#add-field-btn').classList.toggle('hidden', !custom);
      if (custom && !$('#custom-schema-rows').children.length) addSchemaFieldRow();
    });

    api('GET', '/health').then(() => {
      const el = $('#server-status');
      if (el) { el.textContent = 'Connected'; el.className = 'text-sm text-green-400 mt-1'; }
    }).catch(() => {
      const el = $('#server-status');
      if (el) { el.textContent = 'Unreachable'; el.className = 'text-sm text-red-400 mt-1'; }
    });
  }

  function addSchemaFieldRow() {
    schemaFieldRows++;
    const i = schemaFieldRows;
    const row = document.createElement('div');
    row.className = 'schema-row grid grid-cols-6 gap-2 items-end';
    row.innerHTML = `
      <input name="fname-${i}" placeholder="field name" class="col-span-2" />
      <select name="fkind-${i}" class="col-span-1">
        ${FIELD_KINDS.map(([v, l]) => `<option value="${v}">${l}</option>`).join('')}
      </select>
      <label class="text-xs flex items-center gap-1"><input type="checkbox" name="findexed-${i}" /> indexed</label>
      <label class="text-xs flex items-center gap-1"><input type="checkbox" name="flookup-${i}" checked /> lookup</label>
      <label class="text-xs flex items-center gap-1"><input type="checkbox" name="fprefix-${i}" /> prefix</label>`;
    $('#custom-schema-rows').appendChild(row);
  }

  function collectSchemaFields() {
    const fields = {};
    for (const row of $$('.schema-row')) {
      const name = row.querySelector('[name^="fname-"]').value.trim();
      if (!name) continue;
      const kind = parseInt(row.querySelector('[name^="fkind-"]').value, 10);
      const indexed = row.querySelector('[name^="findexed-"]').checked;
      const lookup = row.querySelector('[name^="flookup-"]').checked;
      const prefix = row.querySelector('[name^="fprefix-"]').checked;
      fields[name] = { kind, indexed, lookup, prefix, lowercase: true };
      if (prefix) { fields[name].min_prefix = 3; fields[name].max_prefix = 5; }
    }
    return fields;
  }

  async function createIndex() {
    const id = $('#new-index-id').value.trim();
    if (!id) { toast('Index ID required.', false); return; }
    const schema = $('#new-index-schema').value;
    const body = { id };
    if (schema === 'record') {
      body.schema = 'record';
    } else {
      const fields = collectSchemaFields();
      if (!Object.keys(fields).length) { toast('Add at least one field.', false); return; }
      body.config = { schema: { fields } };
    }
    try {
      await api('POST', '/v1/indexes', body);
      toast('Created index ' + id);
      schemaFieldRows = 0;
      render();
    } catch (e) { toast('Create failed: ' + e.message, false); }
  }

  async function reloadIndex(id) {
    toast('Reloading ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload`);
      toast('Reloaded ' + id);
      render();
    } catch (e) { toast('Reload failed: ' + e.message, false); }
  }

  async function freezeIndex(id) {
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/freeze`);
      toast('Frozen ' + id);
    } catch (e) { toast('Freeze failed: ' + e.message, false); }
  }

  async function persistIndex(id) {
    try {
      const man = await api('POST', `/v1/indexes/${encodeURIComponent(id)}/persist`);
      toast(`Persisted ${id} (gen ${man.generation})`);
    } catch (e) { toast('Persist failed: ' + e.message, false); }
  }

  async function deleteIndex(id) {
    if (!confirm(`Delete index "${id}"? This cannot be undone.`)) return;
    try {
      await api('DELETE', '/v1/indexes', { id });
      toast('Deleted ' + id);
      render();
    } catch (e) { toast('Delete failed: ' + e.message, false); }
  }

  async function toggleDetail(id) {
    const el = $(`#detail-${id}`);
    if (!el) return;
    if (!el.classList.contains('hidden')) { el.classList.add('hidden'); return; }
    el.classList.remove('hidden');
    el.innerHTML = loading();
    let stats = {};
    try { stats = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/stats`); } catch (e) {}
    el.innerHTML = `
      <div class="border-t border-gray-800 pt-3 mt-1 space-y-3">
        ${json(stats)}
        <div class="grid grid-cols-2 gap-4">
          <div>
            <div class="text-xs font-medium text-gray-400 mb-1">Upsert Document</div>
            <div class="flex gap-2">
              <input id="doc-id-${id}" placeholder="doc id" class="w-32" />
              <input id="doc-json-${id}" placeholder='{"term":"foo"}' class="flex-1" />
              <button class="btn btn-primary text-xs" onclick="App.upsertDoc('${id}')">Save</button>
            </div>
          </div>
          <div>
            <div class="text-xs font-medium text-gray-400 mb-1">Delete Document</div>
            <div class="flex gap-2">
              <input id="del-doc-id-${id}" placeholder="doc id" class="flex-1" />
              <button class="btn btn-danger text-xs" onclick="App.deleteDoc('${id}')">Delete</button>
            </div>
          </div>
        </div>
        <div>
          <div class="text-xs font-medium text-gray-400 mb-1">Maintenance (persisted indexes)</div>
          <div class="flex gap-2 flex-wrap">
            <button class="btn btn-secondary text-xs" onclick="App.listGenerations('${id}')">Generations</button>
            <button class="btn btn-secondary text-xs" onclick="App.validateIndex('${id}')">Validate</button>
            <button class="btn btn-secondary text-xs" onclick="App.repairIndex('${id}')">Repair</button>
            <button class="btn btn-secondary text-xs" onclick="App.compactIndex('${id}')">Compact (keep 2)</button>
          </div>
          <div id="maint-${id}" class="mt-2"></div>
        </div>
      </div>`;
  }

  async function upsertDoc(id) {
    const docId = $(`#doc-id-${id}`).value.trim();
    const raw = $(`#doc-json-${id}`).value.trim();
    if (!docId || !raw) { toast('Doc id and JSON required.', false); return; }
    try {
      const doc = JSON.parse(raw);
      await api('PUT', `/v1/indexes/${encodeURIComponent(id)}/docs/${encodeURIComponent(docId)}`, doc);
      toast('Upserted ' + docId);
    } catch (e) { toast('Upsert failed: ' + e.message, false); }
  }

  async function deleteDoc(id) {
    const docId = $(`#del-doc-id-${id}`).value.trim();
    if (!docId) { toast('Doc id required.', false); return; }
    try {
      await api('DELETE', `/v1/indexes/${encodeURIComponent(id)}/docs/${encodeURIComponent(docId)}`);
      toast('Deleted ' + docId);
    } catch (e) { toast('Delete failed: ' + e.message, false); }
  }

  async function listGenerations(id) {
    const el = $(`#maint-${id}`);
    el.innerHTML = loading();
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/generations`);
      el.innerHTML = json(data.generations || data);
    } catch (e) { el.innerHTML = `<p class="text-red-400 text-sm">${escHtml(e.message)}</p>`; }
  }

  async function validateIndex(id) {
    const el = $(`#maint-${id}`);
    el.innerHTML = loading();
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/validate`);
      el.innerHTML = json(data);
    } catch (e) { el.innerHTML = `<p class="text-red-400 text-sm">${escHtml(e.message)}</p>`; }
  }

  async function repairIndex(id) {
    const el = $(`#maint-${id}`);
    el.innerHTML = loading();
    try {
      const data = await api('POST', `/v1/indexes/${encodeURIComponent(id)}/repair`);
      el.innerHTML = json(data);
    } catch (e) { el.innerHTML = `<p class="text-red-400 text-sm">${escHtml(e.message)}</p>`; }
  }

  async function compactIndex(id) {
    const el = $(`#maint-${id}`);
    el.innerHTML = loading();
    try {
      const data = await api('POST', `/v1/indexes/${encodeURIComponent(id)}/compact`, { keep_last: 2 });
      el.innerHTML = json(data);
    } catch (e) { el.innerHTML = `<p class="text-red-400 text-sm">${escHtml(e.message)}</p>`; }
  }

  // ── Integrations ───────────────────────────────────────────────────────────
  // POST /v1/indexes/{id}/reload-sql, /reload-table, /reload — attach a data
  // source to an already-created index.

  async function renderIntegrations() {
    content().innerHTML = `
      <div class="space-y-6">
        <div class="card max-w-3xl">
          <div class="card-header">SQL Query</div>
          <p class="text-xs text-gray-500 mb-3">POST /reload-sql — run one SQL query and load every row.</p>
          <div class="space-y-3">
            <div>${indexSelect('sql-index')}</div>
            <div class="grid grid-cols-2 gap-3">
              ${field('sql-driver', { label: 'Driver', placeholder: 'postgres / mysql / sqlite3' })}
              ${field('sql-id-col', { label: 'ID Column', default: 'id' })}
            </div>
            ${field('sql-dsn', { label: 'DSN', placeholder: 'host=localhost user=postgres password=… dbname=mydb' })}
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">Query</label>
              <textarea id="sql-query" rows="3" class="font-mono text-sm" placeholder="SELECT id, name, category, created_at FROM my_table WHERE active = true"></textarea>
            </div>
            <div class="space-y-1">
              <div class="flex items-center justify-between">
                <label class="text-xs font-medium text-gray-400">Column mappings (one per line: column field kind)</label>
                <button class="btn btn-secondary text-xs" onclick="App.detectColumns('sql')">Detect from query</button>
              </div>
              <textarea id="sql-cols" rows="4" class="font-mono text-sm" placeholder="Click &quot;Detect from query&quot;, or type manually: name term keyword"></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadSQLQuery()">Load</button>
          </div>
        </div>

        <div class="card max-w-3xl">
          <div class="card-header">SQL Table (paged)</div>
          <p class="text-xs text-gray-500 mb-3">POST /reload-table — keyset-paginated full table scan, safe for very large tables.</p>
          <div class="space-y-3">
            <div>${indexSelect('tbl-index')}</div>
            <div class="grid grid-cols-2 gap-3">
              ${field('tbl-driver', { label: 'Driver', placeholder: 'postgres / mysql / sqlite3' })}
              ${field('tbl-name', { label: 'Table', placeholder: 'my_table' })}
            </div>
            ${field('tbl-dsn', { label: 'DSN', placeholder: 'host=localhost user=postgres password=… dbname=mydb' })}
            <div class="grid grid-cols-2 gap-3">
              ${field('tbl-id-col', { label: 'ID Column', default: 'id' })}
              ${field('tbl-page-size', { label: 'Page Size', type: 'number', default: '10000' })}
            </div>
            ${field('tbl-where', { label: 'Where (optional, raw SQL)', placeholder: 'active = true' })}
            <div class="space-y-1">
              <div class="flex items-center justify-between">
                <label class="text-xs font-medium text-gray-400">Column mappings (one per line: column field kind)</label>
                <button class="btn btn-secondary text-xs" onclick="App.detectColumns('tbl')">Detect from table</button>
              </div>
              <textarea id="tbl-cols" rows="4" class="font-mono text-sm" placeholder="Click &quot;Detect from table&quot;, or type manually: name term keyword"></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadSQLTable()">Load</button>
          </div>
        </div>

        <div class="card max-w-3xl">
          <div class="card-header">Inline JSON Records</div>
          <p class="text-xs text-gray-500 mb-3">PUT /docs/{id} per record — upserts each object into the index (each needs an "id" field).</p>
          <div class="space-y-3">
            <div>${indexSelect('json-index')}</div>
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">Records</label>
              <textarea id="json-data" rows="6" class="font-mono text-sm" placeholder='[{"id":"1","term":"foo","group_id":"1","date_key":"2026-01-01","partition_id":"1"}]'></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadJSONRecords()">Load</button>
          </div>
        </div>
      </div>
      <div id="integration-result" class="mt-4"></div>`;
    await Promise.all([
      populateIndexSelect('sql-index'),
      populateIndexSelect('tbl-index'),
      populateIndexSelect('json-index'),
    ]);
  }

  function parseColumns(text) {
    return text.trim().split('\n').filter(Boolean).map(line => {
      const parts = line.trim().split(/\s+/);
      return { column: parts[0], field: parts[1] || parts[0], kind: parts[2] || 'keyword' };
    });
  }

  function showIntegrationResult(promise, doneMsg) {
    const res = $('#integration-result');
    res.innerHTML = loading();
    return promise.then(data => {
      toast(doneMsg);
      res.innerHTML = json(data);
    }).catch(e => {
      toast('Load failed: ' + e.message, false);
      res.innerHTML = `<div class="card text-red-400 text-sm">${escHtml(e.message)}</div>`;
    });
  }

  // detectColumns calls the read-only /v1/infer-columns endpoint (no index is
  // created or modified) and fills the "column mappings" textarea from the
  // result, so Load can never be submitted with an empty columns list.
  async function detectColumns(prefix) {
    const driver = $(`#${prefix}-driver`).value.trim();
    const dsn = $(`#${prefix}-dsn`).value.trim();
    if (!driver || !dsn) { toast('Driver and DSN required.', false); return; }
    const body = {
      driver, dsn,
      id_column: $(`#${prefix}-id-col`).value.trim() || 'id',
    };
    if (prefix === 'sql') {
      const query = $('#sql-query').value.trim();
      if (!query) { toast('Query required.', false); return; }
      body.source = 'sql_query';
      body.query = query;
    } else {
      const table = $('#tbl-name').value.trim();
      if (!table) { toast('Table required.', false); return; }
      body.source = 'sql_table';
      body.table = table;
      const where = $('#tbl-where').value.trim();
      if (where) body.where = where;
    }
    try {
      const data = await api('POST', '/v1/infer-columns', body);
      const cols = data.columns || [];
      if (!cols.length) { toast('No columns detected.', false); return; }
      $(`#${prefix}-cols`).value = cols.map(c => `${c.column} ${c.field} ${c.kind}`).join('\n');
      toast(`Detected ${cols.length} column(s).`);
    } catch (e) {
      toast('Detect failed: ' + e.message, false);
    }
  }

  function loadSQLQuery() {
    const id = $('#sql-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const driver = $('#sql-driver').value.trim();
    const dsn = $('#sql-dsn').value.trim();
    const query = $('#sql-query').value.trim();
    if (!driver || !dsn || !query) { toast('Driver, DSN, and query required.', false); return; }
    const columns = parseColumns($('#sql-cols').value);
    if (!columns.length) { toast('No column mappings — click "Detect from query" first (or add mappings manually).', false); return; }
    const body = {
      driver, dsn, query,
      id_column: $('#sql-id-col').value.trim() || 'id',
      columns,
    };
    showIntegrationResult(
      api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-sql`, body),
      'Data loaded for ' + id);
  }

  function loadSQLTable() {
    const id = $('#tbl-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const driver = $('#tbl-driver').value.trim();
    const dsn = $('#tbl-dsn').value.trim();
    const table = $('#tbl-name').value.trim();
    if (!driver || !dsn || !table) { toast('Driver, DSN, and table required.', false); return; }
    const columns = parseColumns($('#tbl-cols').value);
    if (!columns.length) { toast('No column mappings — click "Detect from table" first (or add mappings manually).', false); return; }
    const body = {
      driver, dsn, table,
      where: $('#tbl-where').value.trim(),
      id_column: $('#tbl-id-col').value.trim() || 'id',
      page_size: parseInt($('#tbl-page-size').value) || 10000,
      select_columns: columns.map(c => c.column),
      columns,
    };
    showIntegrationResult(
      api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-table`, body),
      'Data loaded for ' + id);
  }

  async function loadJSONRecords() {
    const id = $('#json-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const raw = $('#json-data').value.trim();
    if (!raw) { toast('Provide JSON records.', false); return; }
    let records;
    try { records = JSON.parse(raw); } catch (e) { toast('Invalid JSON: ' + e.message, false); return; }
    if (!Array.isArray(records)) records = [records];
    // There is no bulk "load raw JSON" endpoint on the server, so each record
    // is upserted individually via the per-document endpoint.
    const res = $('#integration-result');
    res.innerHTML = loading();
    let ok = 0;
    const errors = [];
    for (const r of records) {
      const docId = r.id !== undefined ? String(r.id) : '';
      if (!docId) { errors.push('record missing "id" field: ' + JSON.stringify(r)); continue; }
      try {
        await api('PUT', `/v1/indexes/${encodeURIComponent(id)}/docs/${encodeURIComponent(docId)}`, r);
        ok++;
      } catch (e) {
        errors.push(`${docId}: ${e.message}`);
      }
    }
    toast(`Loaded ${ok}/${records.length} record(s) for ${id}`, errors.length === 0);
    res.innerHTML = json({ loaded: ok, total: records.length, errors });
  }

  function refresh() { render(); }

  function init() {
    $$('.nav-btn').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
    render();
  }

  document.addEventListener('DOMContentLoaded', init);

  // Wraps every onclick-invoked handler so a mismatched/missing DOM element
  // (e.g. a stale cached app.js served against a newer index.html) surfaces
  // as a toast instead of a silent uncaught exception that makes buttons
  // appear to do nothing.
  function safe(fn) {
    return function (...args) {
      try {
        const r = fn.apply(this, args);
        if (r && typeof r.catch === 'function') {
          r.catch(e => toast('Unexpected error: ' + e.message, false));
        }
        return r;
      } catch (e) {
        toast('Unexpected error: ' + e.message, false);
      }
    };
  }

  const handlers = {
    refresh, render,
    doLookup, addSearchField,
    addSchemaFieldRow, createIndex, reloadIndex, freezeIndex, persistIndex, deleteIndex,
    toggleDetail, upsertDoc, deleteDoc, listGenerations, validateIndex, repairIndex, compactIndex,
    detectColumns, loadSQLQuery, loadSQLTable, loadJSONRecords,
  };
  for (const k of Object.keys(handlers)) handlers[k] = safe(handlers[k]);
  return handlers;
})();
