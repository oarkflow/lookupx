const App = (() => {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);
  const content = () => $('#content');
  let csrfToken = '';
  let adminSetupRequired = false;

  // Numeric FieldKind values from pkg/types.go — FieldOptions.Kind is encoded
  // as a plain integer over the wire (no string enum on the Go side).
  const FIELD_KINDS = [
    ['0', 'keyword'], ['1', 'text'], ['2', 'int'],
    ['3', 'float'], ['4', 'bool'], ['5', 'time'], ['6', 'vector'],
  ];

  function apiBase() {
    const raw = ($('#api-base').value || '').trim();
    if (!raw) return '';
    const target = new URL(raw, window.location.origin);
    if (target.origin !== window.location.origin || target.username || target.password || target.search || target.hash) {
      throw new Error('The console API endpoint must use this page’s origin.');
    }
    return target.pathname.replace(/\/+$/, '');
  }

  async function api(method, path, body) {
    const opts = { method, headers: {} };
    opts.credentials = 'same-origin';
    if (!['GET', 'HEAD', 'OPTIONS'].includes(method.toUpperCase()) && csrfToken) opts.headers['X-CSRF-Token'] = csrfToken;
    if (body !== undefined) {
      if (body instanceof FormData) {
        opts.body = body;
      } else {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
      }
    }
    let res = await fetch(apiBase() + path, opts);
    if (res.status === 401 && !path.startsWith('/auth/')) {
      showAuthGate('Your admin session expired. Sign in again.');
    }
    const text = await res.text();
    if (!res.ok) throw new Error(`${res.status} ${text}`);
    try { return JSON.parse(text); } catch { return text; }
  }

  function showAuthGate(message = '') {
    document.body.classList.remove('auth-ready');
    $('#auth-error').textContent = message;
  }

  function setAuthMode(setup) {
    adminSetupRequired = setup;
    const form = $('#auth-form');
    form.querySelectorAll('label, input, button[type="submit"]').forEach(el => { el.hidden = false; el.disabled = false; });
    form.action = setup ? '/auth/setup' : '/auth/login';
    form.method = 'post';
    form.querySelector('h1').textContent = setup ? 'Set up LookupX admin' : 'LookupX admin';
    form.querySelector('p').textContent = setup
      ? 'Create the first administrator account. This cannot be changed here later.'
      : 'Sign in to open the console.';
    $('#auth-password').autocomplete = setup ? 'new-password' : 'current-password';
    $('#auth-password').minLength = setup ? 12 : 1;
    form.querySelector('button[type="submit"]').textContent = setup ? 'Create admin account' : 'Sign in';
  }

  function setAuthUnavailable(message) {
    setAuthMode(false);
    const form = $('#auth-form');
    form.querySelector('h1').textContent = 'Admin status unavailable';
    form.querySelector('p').textContent = 'LookupX could not determine whether setup is complete.';
    form.querySelectorAll('label, input, button[type="submit"]').forEach(el => { el.hidden = true; el.disabled = true; });
    $('#auth-error').textContent = message;
  }

  async function submitLogin(event) {
    event.preventDefault();
    const error = $('#auth-error');
    error.textContent = '';
    const username = $('#auth-username').value;
    const passwordInput = $('#auth-password');
    const password = passwordInput.value;
    try {
      let opts = { method: 'POST', credentials: 'same-origin', headers: {} };
      let endpoint = '/auth/login';
      if (adminSetupRequired) {
        if (password.length < 12) { error.textContent = 'Use a password with at least 12 characters.'; return; }
        endpoint = '/auth/setup';
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify({ username, password });
      } else {
        const bytes = new TextEncoder().encode(`${username}:${password}`);
        const credentials = btoa(Array.from(bytes, byte => String.fromCharCode(byte)).join(''));
        opts.headers.Authorization = `Basic ${credentials}`;
      }
      const res = await fetch(apiBase() + endpoint, opts);
      if (!res.ok) {
        const detail = await res.text();
        error.textContent = res.status === 401 ? 'Invalid username or password.' : `Sign in failed (${res.status}): ${detail}`;
        return;
      }
      const data = await res.json();
      csrfToken = data.csrf_token || '';
      if (!csrfToken) { error.textContent = 'Sign in did not establish a session.'; return; }
      passwordInput.value = '';
      document.body.classList.add('auth-ready');
      render();
    } catch (err) {
      error.textContent = `Sign in failed: ${err.message}`;
    }
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

  function indexNameField(inputId) {
    return `<div class="form-field"><label for="${inputId}">Index name</label><input id="${inputId}" type="text" list="existing-index-names" placeholder="Choose an index or enter a new name" autocomplete="off" /></div>`;
  }

  async function populateIndexSelect(inputId, selected) {
    const list = await fetchIndexes();
    const el = $(`#${inputId}`);
    if (!el) return;
    el.innerHTML = list.length
      ? list.map(i => `<option value="${escHtml(i.id)}" ${i.id === selected ? 'selected' : ''}>${escHtml(i.id)}</option>`).join('')
      : '<option value="">— no indexes —</option>';
  }

  async function populateIndexNames() {
    const list = await fetchIndexes();
    const datalist = $('#existing-index-names');
    if (datalist) datalist.innerHTML = list.map(i => `<option value="${escHtml(i.id)}"></option>`).join('');
  }

  let currentTab = 'search';
  let activeIntegrationSource = 'sql-query';

  function switchTab(tab) {
    if (tab === 'admin') {
      window.location.assign('/admin.html');
      return;
    }
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

  let searchSchema = {};
  let lookupRules = [];
  let filterRowSeq = 0;
  let columnFilterValues = {};

  async function renderSearch() {
    content().innerHTML = `
      <div class="page-heading">
        <div><div class="eyebrow">QUERY WORKSPACE</div><h1>Search records</h1><p>Run a lookup across an index and inspect matching documents.</p></div>
        <div class="heading-note">API <code>/v1/indexes/{id}/lookup</code></div>
      </div>
      <section class="search-panel" aria-label="Search controls">
        <form class="search-bar" onsubmit="event.preventDefault(); App.doLookup()">
          <div class="search-control search-index"><label class="control-label" for="lookup-index">INDEX</label>${indexSelect('lookup-index')}</div>
          <div class="search-control term-control"><label class="control-label" for="lookup-term">SEARCH TERM</label><div class="term-input-row"><input id="lookup-term" type="search" placeholder="Search text" aria-label="Full-text search term" /><select id="lookup-term-field" aria-label="Search term field"><option value="">All searchable fields</option></select></div></div>
          <label class="search-fuzzy"><input id="lookup-fuzzy" type="checkbox" /> Fuzzy</label>
          <label class="search-fuzzy"><input id="lookup-verbose" type="checkbox" /> Verbose</label>
          <label class="search-fuzzy"><input id="lookup-profile" type="checkbox" /> Profile</label>
          <div class="search-control limit-control"><label class="control-label" for="lookup-limit">LIMIT</label><input id="lookup-limit" type="number" min="1" max="500" value="25" aria-label="Result limit" /></div>
          <button class="btn btn-primary" type="submit">Search</button>
        </form>
        <div class="filter-toolbar"><span class="filter-toolbar-label">Refine with field filters</span><button class="btn btn-secondary" type="button" onclick="App.addFilter()">+ Add filter</button></div>
        <div id="filter-rows" class="filter-rows"></div>
        <div class="filter-toolbar"><span class="filter-toolbar-label">Field values (exact match or configured rule)</span><button class="btn btn-secondary" type="button" onclick="App.addFieldValue()">+ Add field value</button></div>
        <datalist id="field-value-name-options"></datalist>
        <div id="field-value-rows" class="filter-rows"></div>
      </section>
      <div id="search-results"><div class="empty-state"><p>Select an index and enter filters.</p></div></div>
    `;
    await populateIndexSelect('lookup-index');
    $('#lookup-index').addEventListener('change', loadSearchFields);
    await loadSearchFields();
  }

  async function loadSearchFields() {
    const id = $('#lookup-index') && $('#lookup-index').value;
    if (!id) return;
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/schema`);
      searchSchema = data.fields || {};
      lookupRules = Array.isArray(data.lookup_rules) ? data.lookup_rules : [];
      const fieldNameOptions = [...Object.keys(searchSchema), ...lookupRules.map(rule => rule.parameter)];
      $('#field-value-name-options').innerHTML = [...new Set(fieldNameOptions)].sort((a, b) => a.localeCompare(b))
        .map(name => `<option value="${escHtml(name)}"></option>`).join('');
      const termField = $('#lookup-term-field');
      if (termField) {
        const hasConfiguredTermFields = Object.values(searchSchema).some(info => info.term_search);
        const searchable = Object.entries(searchSchema)
          .filter(([, info]) => hasConfiguredTermFields
            ? info.term_search
            : [0, 1, 4].includes(Number(info.kind)))
          .map(([name]) => name)
          .sort((a, b) => a.localeCompare(b));
        termField.innerHTML = '<option value="">All searchable fields</option>' + searchable
          .map(name => `<option value="${escHtml(name)}">${escHtml(name)}</option>`).join('');
      }
      columnFilterValues = {};
      const rows = $('#filter-rows');
      rows.innerHTML = '';
      addFilter();
      $('#field-value-rows').innerHTML = '';
      addFieldValue();
    } catch { searchSchema = {}; }
  }

  function operatorsForField(name) {
    const kind = Number((searchSchema[name] || {}).kind);
    if ([2, 3, 5].includes(kind)) return [
      ['eq', 'Equal'], ['ne', 'Not equal'], ['not_zero', 'Not zero'],
      ['gt', 'Greater than'], ['gte', 'Greater or equal'],
      ['lt', 'Less than'], ['lte', 'Less or equal'], ['between', 'Between'],
      ['in', 'In list'], ['not_in', 'Not in list'], ['exists', 'Has value'], ['missing', 'Has no value'],
    ];
    return [
      ['eq', 'Equal'], ['ne', 'Not equal'], ['contains', 'Contains'],
      ['not_contains', 'Does not contain'], ['starts_with', 'Starts with'],
      ['ends_with', 'Ends with'], ['fuzzy', 'Similar to'], ['in', 'In list'],
      ['not_in', 'Not in list'], ['exists', 'Has value'], ['missing', 'Has no value'],
    ];
  }

  function addFilter(selectedField = '') {
    const host = $('#filter-rows');
    const fields = Object.keys(searchSchema);
    if (!host || !fields.length) return;
    const id = ++filterRowSeq;
    const fieldName = selectedField || fields[0];
    host.insertAdjacentHTML('beforeend', `
      <div class="filter-row" data-filter-id="${id}">
        <select class="filter-field" aria-label="Filter field" onchange="App.updateFilterOperators(${id})">
          ${fields.map(name => `<option value="${escHtml(name)}" ${name === fieldName ? 'selected' : ''}>${escHtml(name)}</option>`).join('')}
        </select>
        <select class="filter-operator" aria-label="Filter operator"></select>
        <div class="filter-value-pair">
          <input class="filter-value" type="text" autocomplete="off" placeholder="Value" aria-label="Filter value" />
          <input class="filter-value-2" type="text" autocomplete="off" placeholder="Maximum" aria-label="Filter value 2" style="display:none" />
        </div>
        <button class="filter-remove" type="button" title="Remove filter" aria-label="Remove filter" onclick="App.removeFilter(${id})">×</button>
      </div>`);
    updateFilterOperators(id);
  }

  function updateFilterOperators(id) {
    const row = document.querySelector(`[data-filter-id="${id}"]`);
    if (!row) return;
    const fieldName = row.querySelector('.filter-field').value;
    const op = row.querySelector('.filter-operator');
    op.innerHTML = operatorsForField(fieldName).map(([value, label]) => `<option value="${value}">${label}</option>`).join('');
    op.onchange = () => updateFilterValueState(id);
    updateFilterValueState(id);
  }

  function fieldKind(name) {
    const field = searchSchema[name] || {};
    return field.kind_name || Number(field.kind);
  }

  function temporalInputType(kind) {
    if (kind === 'date') return 'date';
    if (kind === 'datetime') return 'datetime-local';
    if (kind === 'time') return 'time';
    return 'text'; // datetimez keeps its explicit offset (RFC3339).
  }

  let fieldValueRowSeq = 0;
  function addFieldValue() {
    const host = $('#field-value-rows');
    if (!host) return;
    const rowID = ++fieldValueRowSeq;
    host.insertAdjacentHTML('beforeend', `
      <div class="field-value-row" data-field-value-id="${rowID}">
        <input class="field-value-name" type="text" list="field-value-name-options" placeholder="Field name" aria-label="Field name" onchange="App.updateFieldValueType(this)" onkeydown="if(event.key==='Enter'){event.preventDefault();App.doLookup()}" />
        <input class="field-value-value" type="text" placeholder="Value" aria-label="Field value" onkeydown="if(event.key==='Enter'){event.preventDefault();App.doLookup()}" />
        <button class="filter-remove" type="button" title="Remove field value" aria-label="Remove field value" onclick="App.removeFieldValue(${rowID})">×</button>
      </div>`);
  }

  function removeFieldValue(rowID) {
    const row = document.querySelector(`[data-field-value-id="${rowID}"]`);
    if (row) row.remove();
  }

  function updateFieldValueType(nameInput) {
    const value = nameInput.closest('.field-value-row')?.querySelector('.field-value-value');
    if (!value) return;
    const name = nameInput.value.trim();
    const kind = fieldKind(name);
    const temporal = ['date', 'datetime', 'time', 'datetimez'].includes(kind);
    value.type = temporal ? temporalInputType(kind) : [2, 3].includes(Number(kind)) ? 'number' : 'text';
    value.placeholder = kind === 'datetimez' ? 'RFC3339, e.g. 2015-01-01T00:00:00Z' : 'Value';
  }

  function updateFilterValueState(id) {
    const row = document.querySelector(`[data-filter-id="${id}"]`);
    if (!row) return;
    const op = row.querySelector('.filter-operator').value;
    const fieldName = row.querySelector('.filter-field').value;
    const kind = fieldKind(fieldName);
    const input = row.querySelector('.filter-value');
    const input2 = row.querySelector('.filter-value-2');
    const noValue = ['exists', 'missing', 'not_zero'].includes(op);
    const isBetween = op === 'between';
    const timeType = temporalInputType(kind);
    const isTime = ['date', 'datetime', 'time', 'datetimez'].includes(kind);
    const isNumeric = [2, 3, 5].includes(Number(kind));

    if (noValue) {
      input.disabled = true;
      input.value = '';
      input2.style.display = 'none';
      input2.disabled = true;
      input2.value = '';
      row.style.gridTemplateColumns = '';
    } else if (isBetween) {
      input.disabled = false;
      input2.disabled = false;
      input2.style.display = '';
      input.placeholder = 'Minimum';
      input2.placeholder = 'Maximum';
      input.type = isTime ? timeType : isNumeric ? 'number' : 'text';
      input2.type = isTime ? timeType : isNumeric ? 'number' : 'text';
      row.style.gridTemplateColumns = '';
    } else {
      input.disabled = false;
      input2.disabled = true;
      input2.style.display = 'none';
      input2.value = '';
      row.style.gridTemplateColumns = '';
      input.placeholder = ['in', 'not_in'].includes(op) ? 'Value 1, value 2' : 'Value';
      input.type = isTime ? timeType : isNumeric ? 'number' : 'text';
      if (kind === 'datetimez') input.placeholder = 'RFC3339, e.g. 2015-01-01T00:00:00Z';
    }
  }

  function removeFilter(id) {
    const row = document.querySelector(`[data-filter-id="${id}"]`);
    if (row) row.remove();
  }

  function fmtCell(v) {
    if (v === undefined || v === null || v === '') return '<span class="text-gray-600">—</span>';
    if (typeof v === 'object') return escHtml(JSON.stringify(v));
    return escHtml(String(v));
  }

  function renderHits(hits) {
    if (!hits || !hits.length) return empty('No results.');
    const verbose = hits.some(h => h && Object.prototype.hasOwnProperty.call(h, 'doc'));
    const recordOf = h => h && h.doc && typeof h.doc === 'object' ? h.doc : h;

    // One column per document field actually present, including a database
    // field named "id". The hit's internal index ID is not a document field
    // and is intentionally never rendered.
    const fieldOrder = Object.keys(searchSchema);
    const seen = new Set(fieldOrder);
    let anyDoc = false;
    for (const h of hits) {
      const record = recordOf(h);
      if (!record || typeof record !== 'object') continue;
      anyDoc = true;
      for (const k of Object.keys(record)) {
        if (!seen.has(k)) { seen.add(k); fieldOrder.push(k); }
      }
    }

    let html = `<div class="card results-card"><div class="card-header">Matching documents <span class="results-count">${hits.length}</span></div>`;
    if (!anyDoc) {
      html += `<p class="text-xs text-gray-500 mb-2">This index doesn't store full documents, so the table can only show relevance scores.</p>`;
    }
    html += `<div class="table-wrap"><table><thead>
      <tr>${verbose ? '<th>Score</th>' : ''}${fieldOrder.map(f => `<th>${escHtml(f)}</th>`).join('')}</tr>
      <tr class="column-filter-row">${verbose ? '<th></th>' : ''}${fieldOrder.map(f => {
        const kind = fieldKind(f);
        const numeric = [2, 3].includes(Number(kind));
        const temporal = ['date', 'datetime', 'time', 'datetimez'].includes(kind) || Number(kind) === 5;
        const type = temporal ? temporalInputType(kind === 'datetimez' || kind === 'datetime' || kind === 'date' || kind === 'time' ? kind : 'datetimez') : numeric ? 'number' : 'search';
        const operator = numeric || Number(kind) === 4 || temporal ? 'eq' : 'contains';
        const value = columnFilterValues[f] || '';
        return `<th><input class="column-filter" data-column="${escHtml(f)}" data-operator="${operator}" type="${type}" value="${escHtml(value)}" placeholder="Filter…" aria-label="Filter ${escHtml(f)}" onkeydown="if(event.key==='Enter'){event.preventDefault();App.doLookup()}" /></th>`;
      }).join('')}</tr>
    </thead><tbody>`;
    for (const h of hits) {
      const record = recordOf(h);
      html += `<tr>
        ${verbose ? `<td>${h.score !== undefined ? h.score.toFixed(4) : '—'}</td>` : ''}
        ${fieldOrder.map(f => `<td class="text-sm max-w-xs truncate" title="${record && record[f] !== undefined ? escHtml(String(record[f])) : ''}">${fmtCell(record && record[f])}</td>`).join('')}
      </tr>`;
    }
    html += '</tbody></table></div></div>';
    return html;
  }

  async function doLookup() {
    const id = $('#lookup-index').value.trim();
    if (!id) { toast('Select an index first.', false); return; }
    const limit = parseInt($('#lookup-limit').value) || 25;
    const params = new URLSearchParams();
	const term = $('#lookup-term').value.trim();
	if (term) {
	  params.set('term', term);
	  const termField = $('#lookup-term-field').value;
	  if (termField) params.set('term_field', termField);
	  if ($('#lookup-fuzzy').checked) params.set('fuzzy', 'true');
	}
	if ($('#lookup-verbose').checked) params.set('verbose', 'true');
	if ($('#lookup-profile').checked) params.set('profile', 'true');
    for (const row of $$('.filter-row')) {
      const fieldName = row.querySelector('.filter-field').value;
      const operator = row.querySelector('.filter-operator').value;
      const input = row.querySelector('.filter-value');
      const input2 = row.querySelector('.filter-value-2');
      let value;
      if (operator === 'between') {
        const v1 = input.value.trim();
        const v2 = input2.value.trim();
        if (!v1 && !v2) continue;
        if (!v1 || !v2) { toast(`Both values required for ${fieldName} between`, false); input.focus(); return; }
        value = `${v1},${v2}`;
      } else {
        value = input.disabled ? '' : input.value.trim();
      }
      if (operator !== 'between' && !input.disabled && !value) continue;
      params.append(`${fieldName}__${operator}`, value);
    }
    for (const row of $$('.field-value-row')) {
      const fieldName = row.querySelector('.field-value-name').value.trim();
      const value = row.querySelector('.field-value-value').value.trim();
      if (!fieldName || !value) continue;
      params.append(fieldName, value);
    }
    for (const input of $$('.column-filter')) {
      const fieldName = input.dataset.column;
      const value = input.value.trim();
      columnFilterValues[fieldName] = value;
      if (!value) continue;
      params.append(`${fieldName}__${input.dataset.operator}`, value);
    }
    params.set('limit', String(limit));
    const res = $('#search-results');
    res.innerHTML = loading();
    try {
      const data = await api('GET', `/v1/indexes/${encodeURIComponent(id)}/lookup?${params.toString()}`);
	  const usage = data.resources || {};
	  const meta = [];
	  if (data.total !== undefined) meta.push(`${fmtNum(data.total)} total`);
	  if (data.latency_ns !== undefined) meta.push(`${fmtNum(data.latency_ns)} ns`);
	  if (usage.cpu_seconds !== undefined) meta.push(`${(usage.cpu_seconds * 1000).toFixed(2)} ms CPU`);
	  if (usage.total_alloc_bytes !== undefined) meta.push(`${(usage.total_alloc_bytes / 1048576).toFixed(2)} MB allocated`);
	  if (usage.heap_bytes !== undefined) meta.push(`${(usage.heap_bytes / 1048576).toFixed(1)} MB heap`);
	  const metaHTML = meta.length ? `<div class="result-meta"><span>${meta.join(' · ')}</span></div>` : '';
	  res.innerHTML = `${metaHTML}${renderHits(data.hits)}`;
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
      <div class="page-heading">
        <div><div class="eyebrow">INDEX MANAGEMENT</div><h1>Indexes</h1><p>Review index health, manage lifecycle actions, and inspect stored data.</p></div>
        <div class="heading-note">${list.length ? `${fmtNum(list.length)} active index${list.length === 1 ? '' : 'es'}` : 'No indexes configured'}</div>
      </div>
      <div class="grid-3 mb-6">
        <div class="card metric-card"><div><div class="metric-label">Indexes</div><div class="metric-value">${fmtNum(list.length)}</div></div><span class="metric-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><ellipse cx="12" cy="5" rx="8" ry="3"></ellipse><path d="M4 5v7c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12v7c0 1.7 3.6 3 8 3s8-1.3 8-3v-7"></path></svg></span></div>
        <div class="card metric-card"><div><div class="metric-label">Total documents</div><div class="metric-value">${fmtNum(totalDocs)}</div></div><span class="metric-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3h7l5 5v13H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Z"></path><path d="M14 3v6h6M9 14h6M9 17h6"></path></svg></span></div>
        <div class="card metric-card"><div><div class="metric-label">Server status</div><div id="server-status" class="metric-value server-pending">Checking</div></div><span class="metric-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3a9 9 0 1 0 9 9"></path><path d="M12 7v5l3 2M16 3h5v5"></path></svg></span></div>
      </div>

      <div class="card create-index-card mb-6">
        <div class="section-heading"><div><div class="card-header">Create an index</div><p>Choose a starting schema. You can connect a data source after creation.</p></div></div>
        <div class="create-index-form">
          ${field('new-index-id', { label: 'Index ID', placeholder: 'e.g. claims, members, products' })}
          <div class="space-y-1">
            <label class="text-xs font-medium text-gray-400">Schema</label>
            <select id="new-index-schema">
              <option value="record">Built-in record schema</option>
              <option value="custom">Custom fields</option>
            </select>
            <p class="schema-help">term · group_id · date_key · partition_id</p>
          </div>
          <div id="custom-schema-rows" class="hidden space-y-2"></div>
          <div class="create-index-actions"><button id="add-field-btn" class="btn btn-secondary text-xs hidden" onclick="App.addSchemaFieldRow()">+ Add field</button>
          <button class="btn btn-primary" onclick="App.createIndex()">Create index</button></div>
        </div>
      </div>

      <div class="index-list-heading"><h2>Available indexes</h2><span>${fmtNum(list.length)} total</span></div><div class="space-y-4">`;

    if (!list.length) {
      html += empty('No indexes yet. Create one above.');
    } else {
      for (const mi of list) {
        const st = mi.stats || {};
        const lat = mi.latency || {};
        const progress = mi.progress || {};
        html += `
          <div class="card index-card">
            <div class="index-card-top">
              <div class="index-identity">
                <span class="index-name">${escHtml(mi.id)}</span>
                <span data-index-reloading="${escHtml(mi.id)}" class="badge badge-yellow${mi.reloading ? '' : ' hidden'}">Indexing</span>
                <span class="index-generation">Generation ${fmtNum(mi.generation)}</span>
              </div>
              <div class="index-actions">
                <button class="btn btn-secondary" onclick="App.reloadIndex('${escHtml(mi.id)}')">Reload</button>
                <button class="btn btn-secondary" onclick="App.freezeIndex('${escHtml(mi.id)}')">Freeze</button>
                <button class="btn btn-secondary" onclick="App.persistIndex('${escHtml(mi.id)}')">Persist</button>
                <button class="btn btn-secondary" onclick="App.toggleDetail('${escHtml(mi.id)}')">Details</button>
                <button class="btn btn-danger" onclick="App.deleteIndex('${escHtml(mi.id)}')">Delete</button>
              </div>
            </div>
            <div class="index-stats">
              <div class="index-stat">DOCUMENTS<strong>${fmtNum(st.docs)}</strong></div>
              <div class="index-stat">FIELDS<strong>${fmtNum(st.fields)}</strong></div>
              <div class="index-stat">TERMS<strong>${fmtNum(st.terms)}</strong></div>
              <div class="index-stat">LAST INGEST RATE<strong>${lat.last_rows_per_second ? `${fmtNum(Math.round(lat.last_rows_per_second))} rows/s` : '—'}</strong></div>
            </div>
            <div data-index-progress="${escHtml(mi.id)}" class="index-progress-line${mi.reloading ? '' : ' hidden'}">${indexProgressText(mi, progress)}</div>
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
      if (el) { el.textContent = 'Connected'; el.className = 'metric-value server-connected'; }
    }).catch(() => {
      const el = $('#server-status');
      if (el) { el.textContent = 'Unreachable'; el.className = 'metric-value server-unreachable'; }
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
  // POST /v1/indexes/{id}/reload-sql, /reload-table, /reload-file — infer a
  // schema and create the named index when needed, or replace an existing one.

  const integrationSources = [
    { id: 'sql-query', title: 'SQL query', detail: 'Import results from a query', icon: 'SQL' },
    { id: 'sql-table', title: 'SQL table', detail: 'Paged table scan', icon: 'DB' },
    { id: 'file', title: 'File upload', detail: 'CSV, TSV, XLSX, JSONL', icon: 'FILE' },
    { id: 'json', title: 'JSON data', detail: 'Paste an object or array', icon: '{}' },
  ];

  function integrationFormMarkup(source) {
    if (source === 'sql-query') return `
      <div class="integration-source-heading"><div><div class="eyebrow">DATABASE</div><h2>Import from a SQL query</h2><p>Run a query and stream its result rows into the index.</p></div></div>
      <div class="integration-form">
        ${indexNameField('sql-index')}
        <div class="form-grid">
          <div class="space-y-1"><label for="sql-driver">Database driver</label><select id="sql-driver"><option value="postgres">PostgreSQL</option><option value="mysql">MySQL</option><option value="sqlite3">SQLite</option></select></div>
          ${field('sql-id-col', { label: 'Primary key field (optional)', placeholder: 'Leave blank to generate row IDs' })}
        </div>
        ${field('sql-dsn', { label: 'Connection string', placeholder: 'host=localhost user=postgres dbname=mydb' })}
        <div class="form-field"><label for="sql-query">SQL query</label><textarea id="sql-query" rows="4" class="font-mono" placeholder="SELECT id, name, category FROM products"></textarea></div>
        <div class="upload-note">Fields and types are inferred from up to 200 sample rows. A blank primary key generates row IDs.</div>
        <div class="integration-actions"><button class="btn btn-primary" onclick="App.loadSQLQuery()">Import query results</button></div>
      </div>`;

    if (source === 'sql-table') return `
      <div class="integration-source-heading"><div><div class="eyebrow">DATABASE</div><h2>Import from a SQL table</h2><p>Scan a table in pages to keep memory use bounded.</p></div></div>
      <div class="integration-form">
        ${indexNameField('tbl-index')}
        <div class="form-grid">
          <div class="space-y-1"><label for="tbl-driver">Database driver</label><select id="tbl-driver"><option value="postgres">PostgreSQL</option><option value="mysql">MySQL</option><option value="sqlite3">SQLite</option></select></div>
          ${field('tbl-name', { label: 'Table name', placeholder: 'products' })}
        </div>
        ${field('tbl-dsn', { label: 'Connection string', placeholder: 'host=localhost user=postgres dbname=mydb' })}
        <div class="form-grid">
          ${field('tbl-id-col', { label: 'Primary key field (optional)', placeholder: 'Leave blank to generate row IDs' })}
          ${field('tbl-page-size', { label: 'Rows per page', type: 'number', default: '10000' })}
        </div>
        ${field('tbl-where', { label: 'Filter (optional)', placeholder: 'active = true' })}
        <div class="upload-note">A selected key uses keyset paging. Without one, rows are read in offset pages; keep the table stable during import.</div>
        <div class="integration-actions"><button class="btn btn-primary" onclick="App.loadSQLTable()">Import table</button></div>
      </div>`;

    if (source === 'file') return `
      <div class="integration-source-heading"><div><div class="eyebrow">FILE</div><h2>Import a file</h2><p>Upload a tabular file or structured JSON document.</p></div></div>
      <div class="integration-form">
        ${indexNameField('file-index')}
        <div class="form-grid">
          <div class="space-y-1"><label for="file-format">File format</label><select id="file-format"><option value="auto">Detect from filename</option><option value="csv">CSV</option><option value="tsv">Tab-separated text</option><option value="xlsx">Excel workbook (.xlsx)</option><option value="json">JSON object or array</option><option value="jsonl">JSON Lines</option></select></div>
          ${field('file-id-field', { label: 'Primary key field (optional)', placeholder: 'Leave blank to generate row IDs' })}
        </div>
        <div class="form-grid">
          <div class="form-field"><label for="index-file">Choose file</label><input id="index-file" type="file" accept=".csv,.tsv,.xlsx,.json,.jsonl,.ndjson" /></div>
          ${field('xlsx-sheet', { label: 'Worksheet (optional)', placeholder: 'Uses the first worksheet' })}
        </div>
        <div class="upload-note">Headers and value types are inferred from a sample. Uploads can be up to 1 GiB.</div>
        <div class="integration-actions"><button id="upload-file-btn" class="btn btn-primary" onclick="App.loadFile()">Import file</button></div>
      </div>`;

    return `
      <div class="integration-source-heading"><div><div class="eyebrow">JSON</div><h2>Import JSON data</h2><p>Paste one object or an array of objects for bulk indexing.</p></div></div>
      <div class="integration-form">
        ${indexNameField('json-index')}
        ${field('json-id-field', { label: 'Primary key field (optional)', placeholder: 'Leave blank to generate row IDs' })}
        <div class="form-field"><label for="json-data">JSON object or array</label><textarea id="json-data" rows="8" class="font-mono" placeholder='[{"id":"1","name":"Example"}]'></textarea></div>
        <div class="upload-note">Fields and types are inferred from the JSON data. Imports replace the selected index generation.</div>
        <div class="integration-actions"><button class="btn btn-primary" onclick="App.loadJSONRecords()">Import JSON</button></div>
      </div>`;
  }

  function chooseIntegrationSource(source) {
    if (!integrationSources.some(item => item.id === source)) return;
    activeIntegrationSource = source;
    $$('.source-option').forEach(button => {
      const active = button.dataset.source === source;
      button.classList.toggle('active', active);
      button.setAttribute('aria-pressed', String(active));
    });
    const panel = $('#integration-source-panel');
    if (panel) panel.innerHTML = integrationFormMarkup(source);
    const result = $('#integration-result');
    if (result) result.innerHTML = '';
  }

  async function renderIntegrations() {
    content().innerHTML = `
      <div class="page-heading">
        <div><div class="eyebrow">DATA SOURCES</div><h1>Index data</h1><p>Choose a source, name the index, and import your data.</p></div>
        <div class="heading-note">Imports replace the current index generation.</div>
      </div>
      <div class="integration-workspace">
        <section class="source-picker" aria-label="Choose a data source">
          <div class="source-picker-heading"><strong>Data source</strong><span>Choose one to configure</span></div>
          <div class="source-options">${integrationSources.map(item => `
            <button type="button" class="source-option${item.id === activeIntegrationSource ? ' active' : ''}" data-source="${item.id}" aria-pressed="${item.id === activeIntegrationSource}" onclick="App.chooseIntegrationSource('${item.id}')">
              <span class="source-option-icon" aria-hidden="true">${item.icon}</span><span class="source-option-copy"><strong>${item.title}</strong><small>${item.detail}</small></span><span class="source-option-chevron" aria-hidden="true">›</span>
            </button>`).join('')}</div>
        </section>
        <section id="integration-source-panel" class="integration-source-panel"></section>
      </div>
      <datalist id="existing-index-names"></datalist>
      <div id="integration-result" class="mt-4"></div>`;
    chooseIntegrationSource(activeIntegrationSource);
    await populateIndexNames();
  }

  async function loadFile() {
    const id = $('#file-index').value.trim();
    if (!id) { toast('Enter or select an index name.', false); return; }
    const fileInput = $('#index-file');
    const file = fileInput.files && fileInput.files[0];
    if (!file) { toast('Choose a file to index.', false); fileInput.focus(); return; }
    const form = new FormData();
    form.append('file', file);
    form.append('format', $('#file-format').value);
    form.append('id_field', $('#file-id-field').value.trim());
    form.append('sheet', $('#xlsx-sheet').value.trim());
    const button = $('#upload-file-btn');
    button.disabled = true;
    button.textContent = 'Uploading and indexing…';
    try {
      const data = await api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-file`, form);
      toast(`Indexed ${fmtNum(data.stats && data.stats.indexed)} rows into ${id}`);
      const result = $('#integration-result');
      result.innerHTML = json(data);
    } catch (e) {
      toast('File import failed: ' + e.message, false);
      $('#integration-result').innerHTML = `<div class="card text-red-400 text-sm">${escHtml(e.message)}</div>`;
    } finally {
      button.disabled = false;
      button.textContent = 'Import file';
    }
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

  function loadSQLQuery() {
    const id = $('#sql-index').value.trim();
    if (!id) { toast('Enter or select an index name.', false); return; }
    const driver = $('#sql-driver').value.trim();
    const dsn = $('#sql-dsn').value.trim();
    const query = $('#sql-query').value.trim();
    if (!driver || !dsn || !query) { toast('Driver, DSN, and query required.', false); return; }
    const body = {
      driver, dsn, query,
      id_column: $('#sql-id-col').value.trim(),
    };
    showIntegrationResult(
      api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-sql`, body),
      'Data loaded for ' + id);
  }

  function loadSQLTable() {
    const id = $('#tbl-index').value.trim();
    if (!id) { toast('Enter or select an index name.', false); return; }
    const driver = $('#tbl-driver').value.trim();
    const dsn = $('#tbl-dsn').value.trim();
    const table = $('#tbl-name').value.trim();
    if (!driver || !dsn || !table) { toast('Driver, DSN, and table required.', false); return; }
    const body = {
      driver, dsn, table,
      where: $('#tbl-where').value.trim(),
      id_column: $('#tbl-id-col').value.trim(),
      page_size: parseInt($('#tbl-page-size').value) || 10000,
    };
    showIntegrationResult(
      api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-table`, body),
      'Data loaded for ' + id);
  }

  async function loadJSONRecords() {
    const id = $('#json-index').value.trim();
    if (!id) { toast('Enter or select an index name.', false); return; }
    const raw = $('#json-data').value.trim();
    if (!raw) { toast('Provide JSON records.', false); return; }
    try { JSON.parse(raw); } catch (e) { toast('Invalid JSON: ' + e.message, false); return; }
    const form = new FormData();
    form.append('file', new Blob([raw], { type: 'application/json' }), 'records.json');
    form.append('format', 'json');
    form.append('id_field', $('#json-id-field').value.trim());
    return showIntegrationResult(
      api('POST', `/v1/indexes/${encodeURIComponent(id)}/reload-file`, form),
      `Indexed JSON data into ${id}`);
  }

  function refresh() { render(); }

  function indexProgressText(mi, progress = {}) {
    if (!mi.reloading) return '';
    const started = Date.parse(progress.started_at || '');
    const elapsedSeconds = Number.isFinite(started) ? Math.max(0, (Date.now() - started) / 1000) : 0;
    const rate = elapsedSeconds > 0 ? Math.round((progress.seen || 0) / elapsedSeconds) : 0;
    const elapsed = Math.floor(elapsedSeconds);
    const clock = `${String(Math.floor(elapsed / 3600)).padStart(2, '0')}:${String(Math.floor((elapsed % 3600) / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}`;
    return `INDEXING · ${fmtNum(progress.seen || 0)} rows seen · ${fmtNum(progress.indexed || 0)} indexed · ${fmtNum(progress.skipped || 0)} skipped · ${fmtNum(rate)} rows/s · ${clock} elapsed`;
  }

  async function refreshIndexProgress() {
    const nodes = [...document.querySelectorAll('[data-index-progress]')];
    if (!nodes.length) return;
    const list = await fetchIndexes();
    const byId = new Map(list.map(mi => [mi.id, mi]));
    for (const node of nodes) {
      const mi = byId.get(node.dataset.indexProgress);
      if (!mi) continue;
      node.textContent = indexProgressText(mi, mi.progress || {});
      node.classList.toggle('hidden', !mi.reloading);
      const badge = document.querySelectorAll('[data-index-reloading]');
      for (const item of badge) {
        if (item.dataset.indexReloading === mi.id) item.classList.toggle('hidden', !mi.reloading);
      }
    }
  }

  async function logout() {
    try {
      await api('POST', '/auth/logout');
      csrfToken = '';
      showAuthGate();
      $('#auth-password').value = '';
    } catch (err) {
      toast(`Sign out failed: ${err.message}`, false);
    }
  }

  async function init() {
    $$('.nav-btn').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
    $('#auth-form').addEventListener('submit', submitLogin);
    window.setInterval(() => {
      if (currentTab === 'indexes' && document.body.classList.contains('auth-ready')) refreshIndexProgress();
    }, 2000);
    try {
      const res = await fetch(apiBase() + '/auth/session', { credentials: 'same-origin' });
      if (res.ok) {
        const data = await res.json();
        csrfToken = data.csrf_token || '';
        if (data.authenticated && csrfToken) {
          setAuthMode(false);
          document.body.classList.add('auth-ready');
          render();
          return;
        }
        if (typeof data.setup_required !== 'boolean') {
          setAuthUnavailable('The server could not determine whether admin setup is complete. Restart the current LookupX server and reload.');
          return;
        }
        setAuthMode(Boolean(data.setup_required));
        showAuthGate();
        return;
      }
      setAuthUnavailable(`Could not determine admin setup status (HTTP ${res.status}). Restart the current LookupX server and reload.`);
    } catch (err) {
      setAuthUnavailable(`Could not determine admin setup status: ${err.message}`);
    }
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
    refresh, render, logout,
    doLookup, addFilter, updateFilterOperators, removeFilter, addFieldValue, removeFieldValue, updateFieldValueType,
    addSchemaFieldRow, createIndex, reloadIndex, freezeIndex, persistIndex, deleteIndex,
    toggleDetail, upsertDoc, deleteDoc, listGenerations, validateIndex, repairIndex, compactIndex,
    loadSQLQuery, loadSQLTable, loadJSONRecords, loadFile, chooseIntegrationSource,
  };
  for (const k of Object.keys(handlers)) handlers[k] = safe(handlers[k]);
  return handlers;
})();
