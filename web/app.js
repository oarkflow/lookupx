const App = (() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);
  const content = () => $('#content');

  function apiBase() {
    return ($('#api-base').value || '').replace(/\/+$/, '');
  }

  async function api(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const url = apiBase() + path;
    const res = await fetch(url, opts);
    const text = await res.text();
    if (!res.ok) throw new Error(`${res.status} ${text}`);
    try { return JSON.parse(text); } catch { return text; }
  }

  async function fetchIndexIDs() {
    try {
      const data = await api('GET', '/v1/indexes');
      if (Array.isArray(data)) return data.map(i => i.id || i);
      if (data && Array.isArray(data.indexes)) return data.indexes.map(i => i.id || i);
      return [];
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

  function badge(text, color) {
    return `<span class="badge badge-${color}">${escHtml(text)}</span>`;
  }

  function loading() {
    return '<div class="empty-state"><div class="spinner"></div><p class="mt-2">Loading…</p></div>';
  }

  function empty(msg) {
    return `<div class="empty-state"><p>${escHtml(msg)}</p></div>`;
  }

  function fieldInput(name, value, opts = {}) {
    const typ = opts.type || 'text';
    const lbl = opts.label || name;
    const ph = opts.placeholder || '';
    const cls = opts.class || '';
    const val = value !== undefined ? value : (opts.default || '');
    return `
      <div class="space-y-1">
        <label class="text-xs font-medium text-gray-400">${escHtml(lbl)}</label>
        <input type="${typ}" name="${name}" value="${escHtml(String(val))}" placeholder="${escHtml(ph)}" class="${cls}" />
      </div>`;
  }

  function indexSelect(inputId, selected) {
    return `<select id="${inputId}" class="w-full"><option value="">Loading…</option></select>`;
  }

  async function populateIndexSelect(inputId, selected) {
    const ids = await fetchIndexIDs();
    const el = $(`#${inputId}`);
    if (!el) return;
    el.innerHTML = ids.length
      ? ids.map(id => `<option value="${escHtml(id)}" ${id === selected ? 'selected' : ''}>${escHtml(id)}</option>`).join('')
      : '<option value="">— no indexes —</option>';
  }

  let currentTab = 'dashboard';

  function switchTab(tab) {
    currentTab = tab;
    $$('.nav-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    const titles = {
      dashboard: 'Dashboard', indexes: 'Indexes', search: 'Search',
      config: 'Configuration', datasources: 'Datasources', bulk: 'Bulk Operations', generations: 'Generations'
    };
    $('#page-title').textContent = titles[tab] || tab;
    render();
  }

  async function render() {
    content().innerHTML = loading();
    try {
      switch (currentTab) {
        case 'dashboard': await renderDashboard(); break;
        case 'indexes': await renderIndexes(); break;
        case 'search': await renderSearch(); break;
        case 'config': await renderConfig(); break;
        case 'datasources': await renderDatasources(); break;
        case 'bulk': await renderBulk(); break;
        case 'generations': await renderGenerations(); break;
      }
    } catch (e) {
      content().innerHTML = empty('Error: ' + e.message);
    }
  }

  // ── Dashboard ──────────────────────────────────────────────────────────────

  async function renderDashboard() {
    let indexes = [];
    try { indexes = await api('GET', '/v1/indexes'); } catch { content().innerHTML = empty('Cannot reach server. Check API base URL.'); return; }
    const list = Array.isArray(indexes) ? indexes : (indexes.indexes || []);
    if (!list.length) {
      content().innerHTML = `
        <div class="empty-state">
          <p class="text-lg mb-2">No indexes registered</p>
          <p class="text-sm text-gray-600 mb-4">Create one from the Configuration tab, or use the API:</p>
          <pre class="json text-left inline-block">POST /v1/indexes
{
  "id": "my_index",
  "schema": "record"
}</pre>
        </div>`;
      return;
    }

    const cards = [];
    let totalDocs = 0;
    for (const item of list) {
      const id = item.id || item;
      try {
        const st = await api('GET', `/v1/indexes/${id}/stats`);
        totalDocs += (st.stats ? st.stats.docs : st.docs) || 0;
        cards.push({ id, ...st });
      } catch {
        cards.push({ id, error: true });
      }
    }

    let html = `
      <div class="grid-3 mb-6">
        <div class="card"><div class="card-header">Total Indexes</div><div class="text-3xl font-bold">${fmtNum(list.length)}</div></div>
        <div class="card"><div class="card-header">Total Documents</div><div class="text-3xl font-bold">${fmtNum(totalDocs)}</div></div>
        <div class="card"><div class="card-header">Server</div><div class="text-sm text-green-400 mt-1">Connected</div></div>
      </div>
      <div class="grid-2">`;

    for (const c of cards) {
      if (c.error) {
        html += `<div class="card"><div class="card-header">${escHtml(c.id)}</div><p class="text-red-400 text-sm">Failed to load stats</p></div>`;
        continue;
      }
      const s = c.stats || {};
      const frozen = s.frozen ? badge('Frozen', 'green') : badge('Live', 'yellow');
      html += `
        <div class="card cursor-pointer hover:border-brand-600 transition" onclick="App.showIndexDetail('${escHtml(c.id)}')">
          <div class="card-header flex items-center justify-between">
            <span>${escHtml(c.id)}</span>${frozen}
          </div>
          <div class="grid grid-cols-2 gap-2 text-sm mt-2">
            <div><span class="text-gray-500">Docs:</span> ${fmtNum(s.docs)}</div>
            <div><span class="text-gray-500">Fields:</span> ${fmtNum(s.fields)}</div>
            <div><span class="text-gray-500">Partitions:</span> ${fmtNum(s.partitions)}</div>
            <div><span class="text-gray-500">Generation:</span> ${fmtNum(c.generation)}</div>
          </div>
        </div>`;
    }
    html += '</div>';
    content().innerHTML = html;
  }

  // ── Indexes ────────────────────────────────────────────────────────────────

  async function renderIndexes() {
    let indexes = [];
    try { indexes = await api('GET', '/v1/indexes'); } catch { content().innerHTML = empty('Cannot reach server.'); return; }
    const list = Array.isArray(indexes) ? indexes : (indexes.indexes || []);
    if (!list.length) { content().innerHTML = empty('No indexes. Create one from the Configuration tab.'); return; }

    let html = `<div class="space-y-4">`;
    for (const item of list) {
      const id = item.id || item;
      let s = {};
      try { s = await api('GET', `/v1/indexes/${id}/stats`); } catch {}
      const st = s.stats || {};
      const frozen = st.frozen ? badge('Frozen', 'green') : badge('Live', 'yellow');
      html += `
        <div class="card">
          <div class="flex items-center justify-between mb-3">
            <div class="flex items-center gap-2">
              <span class="font-semibold">${escHtml(id)}</span>${frozen}
              <span class="text-xs text-gray-500">gen ${s.generation || '?'}</span>
            </div>
            <div class="flex gap-2">
              <button class="btn btn-primary text-xs" onclick="App.reloadIndex('${escHtml(id)}')">Reload</button>
              <button class="btn btn-secondary text-xs" onclick="App.reloadSQL('${escHtml(id)}')">Reload SQL</button>
              <button class="btn btn-secondary text-xs" onclick="App.reloadTable('${escHtml(id)}')">Reload Table</button>
              <button class="btn btn-secondary text-xs" onclick="App.showIndexDetail('${escHtml(id)}')">Detail</button>
              <button class="btn btn-danger text-xs" onclick="App.deleteIndex('${escHtml(id)}')">Delete</button>
            </div>
          </div>
          <div class="grid grid-cols-4 gap-3 text-sm">
            <div class="text-gray-500">Docs: <span class="text-gray-200">${fmtNum(st.docs)}</span></div>
            <div class="text-gray-500">Fields: <span class="text-gray-200">${fmtNum(st.fields)}</span></div>
            <div class="text-gray-500">Partitions: <span class="text-gray-200">${fmtNum(st.partitions)}</span></div>
            <div class="text-gray-500">Rows/s: <span class="text-gray-200">${s.latency ? fmtNum(Math.round(s.latency.last_rows_per_second)) : '—'}</span></div>
          </div>
        </div>`;
    }
    html += '</div>';
    content().innerHTML = html;
  }

  async function showIndexDetail(id) {
    content().innerHTML = loading();
    try {
      const st = await api('GET', `/v1/indexes/${id}/stats`);
      let html = `
        <div class="mb-4"><button class="btn btn-secondary text-xs" onclick="App.render()">← Back</button></div>
        <div class="card">
          <div class="card-header">${escHtml(id)} — Full Stats</div>
          ${json(st)}
        </div>`;
      try {
        const plan = await api('GET', `/v1/indexes/${id}/plan`);
        html += `<div class="card mt-4"><div class="card-header">Query Plan</div>${json(plan)}</div>`;
      } catch {}
      content().innerHTML = html;
    } catch (e) {
      content().innerHTML = empty('Error: ' + e.message);
    }
  }

  async function reloadIndex(id) {
    toast('Reloading ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${id}/reload`);
      toast('Reloaded ' + id);
      render();
    } catch (e) { toast('Reload failed: ' + e.message, false); }
  }

  async function reloadSQL(id) {
    toast('Reload SQL ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${id}/reload-sql`);
      toast('Reloaded SQL ' + id);
      render();
    } catch (e) { toast('SQL reload failed: ' + e.message, false); }
  }

  async function reloadTable(id) {
    toast('Reload table ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${id}/reload-table`);
      toast('Reloaded table ' + id);
      render();
    } catch (e) { toast('Table reload failed: ' + e.message, false); }
  }

  async function deleteIndex(id) {
    if (!confirm(`Delete index "${id}"? This cannot be undone.`)) return;
    try {
      await api('DELETE', '/v1/indexes', { id });
      toast('Deleted ' + id);
      render();
    } catch (e) { toast('Delete failed: ' + e.message, false); }
  }

  // ── Search ─────────────────────────────────────────────────────────────────

  async function renderSearch() {
    content().innerHTML = `
      <div class="card max-w-3xl">
        <div class="card-header">Search / Lookup</div>
        <div class="space-y-4 mt-3">
          <div>${indexSelect('search-index')}</div>
          ${fieldInput('search-limit', '25', { label: 'Limit', type: 'number' })}
          <div class="space-y-1">
            <label class="text-xs font-medium text-gray-400">Query (key=value pairs, one per line)</label>
            <textarea id="search-query" rows="4" class="font-mono text-sm" placeholder="term=foo&#10;group_id=1&#10;date_key=2026-01-01"></textarea>
          </div>
          <button class="btn btn-primary" onclick="App.doSearch()">Search</button>
        </div>
      </div>
      <div id="search-results" class="mt-4"></div>`;
    await populateIndexSelect('search-index');
  }

  async function doSearch() {
    const id = $('#search-index').value.trim();
    if (!id) { toast('Select an index first.', false); return; }
    const limit = parseInt($('#search-limit').value) || 25;
    const raw = $('#search-query').value.trim();
    const params = raw.split('\n').filter(Boolean).join('&');
    const res = $('#search-results');
    res.innerHTML = loading();
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/lookup?${params}&limit=${limit}`);
      const hits = data.hits || data || [];
      if (!hits.length) { res.innerHTML = empty('No results.'); return; }
      let html = `<div class="card"><div class="card-header">${hits.length} result(s)</div><div class="overflow-x-auto"><table>
        <thead><tr><th>ID</th><th>Score</th><th>Data</th></tr></thead><tbody>`;
      for (const h of hits) {
        html += `<tr>
          <td class="font-mono text-sm">${escHtml(h.id)}</td>
          <td>${h.score ? h.score.toFixed(4) : '—'}</td>
          <td class="text-xs text-gray-400 max-w-md truncate">${escHtml(JSON.stringify(h.data || h))}</td>
        </tr>`;
      }
      html += '</tbody></table></div></div>';
      res.innerHTML = html;
    } catch (e) {
      res.innerHTML = `<div class="card text-red-400 text-sm">Search error: ${escHtml(e.message)}</div>`;
    }
  }

  // ── Configuration ──────────────────────────────────────────────────────────

  async function renderConfig() {
    content().innerHTML = `
      <div class="space-y-6">
        <!-- Create new index -->
        <div class="card max-w-3xl">
          <div class="card-header">Create New Index</div>
          <div class="space-y-4 mt-3">
            ${fieldInput('cfg-new-id', '', { label: 'Index ID', placeholder: 'e.g. claims, members, products' })}
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">Schema</label>
              <select id="cfg-schema" class="w-full">
                <option value="record">Record (term + group_id + date_key + partition_id)</option>
                <option value="custom">Custom (provide fields below)</option>
              </select>
            </div>
            <div id="cfg-custom-fields" class="hidden space-y-1">
              <label class="text-xs font-medium text-gray-400">Custom Schema Fields (one per line: name kind)</label>
              <textarea id="cfg-fields-text" rows="4" class="font-mono text-sm" placeholder="term keyword&#10;group_id keyword&#10;date_key keyword&#10;partition_id keyword"></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.createIndex()">Create Index</button>
          </div>
        </div>

        <!-- Add SQL data source to existing index -->
        <div class="card max-w-3xl">
          <div class="card-header">Load Data: SQL Query</div>
          <div class="space-y-3 mt-3">
            <div>${indexSelect('cfg-sql-index')}</div>
            ${fieldInput('cfg-sql-driver', '', { label: 'Driver', placeholder: 'pgx / mysql / sqlite' })}
            ${fieldInput('cfg-sql-dsn', '', { label: 'DSN', placeholder: 'host=localhost user=postgres password=… dbname=mydb' })}
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">SQL Query</label>
              <textarea id="cfg-sql-query" rows="3" class="font-mono text-sm" placeholder="SELECT id, name, category, created_at FROM my_table WHERE active = true"></textarea>
            </div>
            ${fieldInput('cfg-sql-id', 'id', { label: 'ID Column' })}
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">Column Mappings (one per line: column field kind)</label>
              <textarea id="cfg-sql-cols" rows="4" class="font-mono text-sm" placeholder="name term keyword&#10;category group_id keyword&#10;created_at date_key keyword"></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadSQLQuery()">Load Data</button>
          </div>
        </div>

        <!-- Add SQL table (paged) data source -->
        <div class="card max-w-3xl">
          <div class="card-header">Load Data: SQL Table (Paged)</div>
          <div class="space-y-3 mt-3">
            <div>${indexSelect('cfg-tbl-index')}</div>
            ${fieldInput('cfg-tbl-driver', '', { label: 'Driver', placeholder: 'pgx / mysql / sqlite' })}
            ${fieldInput('cfg-tbl-dsn', '', { label: 'DSN', placeholder: 'host=localhost user=postgres password=… dbname=mydb' })}
            ${fieldInput('cfg-tbl-name', '', { label: 'Table Name', placeholder: 'my_table' })}
            ${fieldInput('cfg-tbl-id', 'id', { label: 'ID Column' })}
            ${fieldInput('cfg-tbl-page', '10000', { label: 'Page Size', type: 'number' })}
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">Column Mappings (one per line: column field kind)</label>
              <textarea id="cfg-tbl-cols" rows="4" class="font-mono text-sm" placeholder="name term keyword&#10;category group_id keyword&#10;created_at date_key keyword"></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadSQLTable()">Load Data</button>
          </div>
        </div>

        <!-- Add inline JSON data -->
        <div class="card max-w-3xl">
          <div class="card-header">Load Data: Inline JSON Records</div>
          <div class="space-y-3 mt-3">
            <div>${indexSelect('cfg-json-index')}</div>
            <div class="space-y-1">
              <label class="text-xs font-medium text-gray-400">JSON Records Array</label>
              <textarea id="cfg-json-data" rows="6" class="font-mono text-sm" placeholder='[{"id":"1","term":"foo","group_id":"1","date_key":"2026-01-01","partition_id":"1"}]'></textarea>
            </div>
            <button class="btn btn-primary" onclick="App.loadJSONRecords()">Load Records</button>
          </div>
        </div>
      </div>`;

    document.getElementById('cfg-schema').addEventListener('change', (e) => {
      document.getElementById('cfg-custom-fields').classList.toggle('hidden', e.target.value !== 'custom');
    });
    await Promise.all([
      populateIndexSelect('cfg-sql-index'),
      populateIndexSelect('cfg-tbl-index'),
      populateIndexSelect('cfg-json-index'),
    ]);
  }

  function parseColumns(text) {
    return text.trim().split('\n').filter(Boolean).map(line => {
      const parts = line.trim().split(/\s+/);
      return { column: parts[0], field: parts[1] || parts[0], kind: parts[2] || 'keyword' };
    });
  }

  async function createIndex() {
    const id = $('#cfg-new-id').value.trim();
    if (!id) { toast('Index ID required.', false); return; }
    const schema = $('#cfg-schema').value;
    const body = { id };
    if (schema === 'record') {
      body.schema = 'record';
    } else {
      const raw = $('#cfg-fields-text').value.trim();
      if (!raw) { toast('Provide schema fields.', false); return; }
      const fields = raw.split('\n').filter(Boolean).map(line => {
        const [name, kind] = line.split(/\s+/);
        return { name, kind: kind || 'keyword' };
      });
      body.config = { schema: { fields } };
    }
    try {
      await api('POST', '/v1/indexes', body);
      toast('Created index ' + id);
      render();
    } catch (e) { toast('Create failed: ' + e.message, false); }
  }

  async function loadSQLQuery() {
    const id = $('#cfg-sql-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const driver = $('#cfg-sql-driver').value.trim();
    const dsn = $('#cfg-sql-dsn').value.trim();
    const query = $('#cfg-sql-query').value.trim();
    const idCol = $('#cfg-sql-id').value.trim();
    if (!driver || !dsn || !query) { toast('Driver, DSN, and query required.', false); return; }
    const columns = parseColumns($('#cfg-sql-cols').value);
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-sql`, {
        driver, dsn, query, id_column: idCol, columns,
      });
      toast('Data loaded for ' + id);
    } catch (e) { toast('Load failed: ' + e.message, false); }
  }

  async function loadSQLTable() {
    const id = $('#cfg-tbl-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const driver = $('#cfg-tbl-driver').value.trim();
    const dsn = $('#cfg-tbl-dsn').value.trim();
    const table = $('#cfg-tbl-name').value.trim();
    const idCol = $('#cfg-tbl-id').value.trim();
    const pageSize = parseInt($('#cfg-tbl-page').value) || 10000;
    if (!driver || !dsn || !table) { toast('Driver, DSN, and table required.', false); return; }
    const columns = parseColumns($('#cfg-tbl-cols').value);
    const selectColumns = columns.map(c => c.column);
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-table`, {
        driver, dsn, table, id_column: idCol, page_size: pageSize, select_columns: selectColumns, columns,
      });
      toast('Data loaded for ' + id);
    } catch (e) { toast('Load failed: ' + e.message, false); }
  }

  async function loadJSONRecords() {
    const id = $('#cfg-json-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const raw = $('#cfg-json-data').value.trim();
    if (!raw) { toast('Provide JSON records.', false); return; }
    try {
      const records = JSON.parse(raw);
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload`, { records });
      toast('Records loaded for ' + id);
    } catch (e) { toast('Load failed: ' + e.message, false); }
  }

  // ── Datasources ────────────────────────────────────────────────────────────

  async function renderDatasources() {
    content().innerHTML = `
      <div class="card max-w-4xl">
        <div class="card-header">Index Info</div>
        <div class="space-y-3 mt-3">
          <div>${indexSelect('ds-index')}</div>
          <button class="btn btn-primary" onclick="App.inspectDatasource()">Inspect</button>
        </div>
        <div id="ds-result" class="mt-4"></div>
      </div>`;
    await populateIndexSelect('ds-index');
  }

  async function inspectDatasource() {
    const id = $('#ds-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const res = $('#ds-result');
    res.innerHTML = loading();
    try {
      const st = await api('GET', `/v1/indexes/${id}/stats`);
      res.innerHTML = json(st);
    } catch (e) { res.innerHTML = `<p class="text-red-400 text-sm">${escHtml(e.message)}</p>`; }
  }

  // ── Bulk Operations ────────────────────────────────────────────────────────

  async function renderBulk() {
    content().innerHTML = `
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div class="card">
          <div class="card-header">Bulk Reload</div>
          <p class="text-sm text-gray-500 mt-1">Reload all indexes from their configured sources.</p>
          <button class="btn btn-primary mt-3" onclick="App.bulkReload()">Reload All</button>
          <div id="bulk-reload-result" class="mt-3 text-sm"></div>
        </div>
        <div class="card">
          <div class="card-header">Freeze Index</div>
          <div class="space-y-3 mt-2">
            <div>${indexSelect('freeze-index')}</div>
            <button class="btn btn-primary" onclick="App.freezeIndex()">Freeze</button>
          </div>
        </div>
        <div class="card">
          <div class="card-header">Plan Deployment</div>
          <div class="space-y-3 mt-2">
            <div>${indexSelect('plan-index')}</div>
            ${fieldInput('plan-rows', '1000000000', { label: 'Row Count', type: 'number' })}
            <button class="btn btn-primary" onclick="App.planDeployment()">Generate Plan</button>
          </div>
          <div id="plan-result" class="mt-3"></div>
        </div>
        <div class="card">
          <div class="card-header">Compact Generations</div>
          <div class="space-y-3 mt-2">
            <div>${indexSelect('compact-index')}</div>
            ${fieldInput('compact-keep', '2', { label: 'Keep Last N', type: 'number' })}
            <button class="btn btn-danger" onclick="App.compactGenerations()">Compact</button>
          </div>
        </div>
        <div class="card">
          <div class="card-header">Validate Index</div>
          <div class="space-y-3 mt-2">
            <div>${indexSelect('val-index')}</div>
            <button class="btn btn-secondary" onclick="App.validateIndex()">Validate</button>
          </div>
          <div id="validate-result" class="mt-3"></div>
        </div>
        <div class="card">
          <div class="card-header">Repair Index</div>
          <div class="space-y-3 mt-2">
            <div>${indexSelect('repair-index')}</div>
            <button class="btn btn-secondary" onclick="App.repairIndex()">Repair</button>
          </div>
        </div>
      </div>`;
    await Promise.all([
      populateIndexSelect('freeze-index'),
      populateIndexSelect('plan-index'),
      populateIndexSelect('compact-index'),
      populateIndexSelect('val-index'),
      populateIndexSelect('repair-index'),
    ]);
  }

  async function bulkReload() {
    const res = $('#bulk-reload-result');
    res.innerHTML = '<div class="spinner"></div>';
    try {
      await api('POST', '/v1/bulk/reload');
      res.innerHTML = '<span class="text-green-400">All indexes reloaded.</span>';
    } catch (e) { res.innerHTML = `<span class="text-red-400">${escHtml(e.message)}</span>`; }
  }

  async function planDeployment() {
    const id = $('#plan-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const rows = parseInt($('#plan-rows').value) || 1e9;
    const res = $('#plan-result');
    res.innerHTML = loading();
    try {
      const plan = await api('POST', '/v1/plan', { index_id: id, rows });
      res.innerHTML = json(plan);
    } catch (e) { res.innerHTML = `<span class="text-red-400 text-sm">${escHtml(e.message)}</span>`; }
  }

  async function freezeIndex() {
    const id = $('#freeze-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    toast('Freezing ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/freeze`);
      toast('Frozen ' + id);
    } catch (e) { toast('Freeze failed: ' + e.message, false); }
  }

  async function compactGenerations() {
    const id = $('#compact-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const keep = parseInt($('#compact-keep').value) || 2;
    toast('Compacting ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/compact`, { keep_last: keep });
      toast('Compacted ' + id);
    } catch (e) { toast('Compact failed: ' + e.message, false); }
  }

  async function validateIndex() {
    const id = $('#val-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    const res = $('#validate-result');
    res.innerHTML = loading();
    try {
      const v = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/validate`);
      res.innerHTML = json(v);
    } catch (e) { res.innerHTML = `<span class="text-red-400 text-sm">${escHtml(e.message)}</span>`; }
  }

  async function repairIndex() {
    const id = $('#repair-index').value.trim();
    if (!id) { toast('Select an index.', false); return; }
    toast('Repairing ' + id + '…');
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/repair`);
      toast('Repaired ' + id);
    } catch (e) { toast('Repair failed: ' + e.message, false); }
  }

  // ── Generations ────────────────────────────────────────────────────────────

  async function renderGenerations() {
    let indexes = [];
    try { indexes = await api('GET', '/v1/indexes'); } catch { content().innerHTML = empty('Cannot reach server.'); return; }
    const list = Array.isArray(indexes) ? indexes : (indexes.indexes || []);
    if (!list.length) { content().innerHTML = empty('No indexes.'); return; }

    let html = '<div class="space-y-4">';
    for (const item of list) {
      const id = item.id || item;
      let gens = [];
      try { gens = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/generations`); } catch {}
      const arr = Array.isArray(gens) ? gens : (gens.generations || []);
      html += `<div class="card">
        <div class="card-header">${escHtml(id)}</div>`;
      if (!arr.length) {
        html += '<p class="text-sm text-gray-500 mt-2">No generations.</p>';
      } else {
        html += `<table class="mt-2"><thead><tr><th>Generation</th><th>Docs</th><th>Path</th><th>Actions</th></tr></thead><tbody>`;
        for (const g of arr) {
          html += `<tr>
            <td>${fmtNum(g.generation)}</td>
            <td>${fmtNum(g.docs)}</td>
            <td class="text-xs text-gray-400 font-mono">${escHtml(g.path || '—')}</td>
            <td><button class="btn btn-secondary text-xs" onclick="App.restoreGeneration('${escHtml(id)}', ${g.generation})">Restore</button></td>
          </tr>`;
        }
        html += '</tbody></table>';
      }
      html += '</div>';
    }
    html += '</div>';
    content().innerHTML = html;
  }

  async function restoreGeneration(id, gen) {
    toast(`Restoring ${id} gen ${gen}…`);
    try {
      await api('POST', `/v1/indexes/${encodeURIComponent(id)}/restore`, { generation: gen });
      toast('Restored generation ' + gen);
      render();
    } catch (e) { toast('Restore failed: ' + e.message, false); }
  }

  function refresh() { render(); }

  function init() {
    $$('.nav-btn').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
    render();
  }

  document.addEventListener('DOMContentLoaded', init);

  return {
    refresh, render, showIndexDetail, reloadIndex, reloadSQL, reloadTable, deleteIndex,
    doSearch, createIndex, loadSQLQuery, loadSQLTable, loadJSONRecords, inspectDatasource,
    bulkReload, planDeployment, freezeIndex, compactGenerations, validateIndex, repairIndex,
    restoreGeneration
  };
})();
