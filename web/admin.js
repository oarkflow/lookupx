(() => {
  const $ = (selector) => document.querySelector(selector);
  let csrf = '';
  let bclEditor = null;
  let config = { content: '', path: '' };
  const HELP = {
    addr: 'Network address where the HTTP server listens. Use 127.0.0.1 for local-only or 0.0.0.0 to accept connections on all interfaces; use firewall rules and TLS at the public edge.',
    data_dir: 'Directory for index data and the admin auth registry. Keep it on persistent storage and restrict filesystem permissions.',
    api_keys: 'Static API keys accepted by the API. Prefer managed API users and rotatable keys from the secured admin routes.',
    datasource: 'Connects an index to a named data source. Click the reference to jump to its datasource definition.',
    driver: 'Registered database driver name, such as mysql, postgres, or sqlite.',
    dsn: 'Database connection expression. Use env("NAME") to keep credentials outside the BCL file.',
    query: 'SQL source. Use query { string "SELECT …" } for inline SQL or query { file "queries/name.sql" } for a UTF-8 SQL file relative to the BCL directory.',
    file: 'Path to a SQL query file, relative to the directory containing lookupx.bcl. Select it here to edit its SELECT statement.',
    string: 'Inline SQL SELECT statement. Keep it read-only and return the fields configured for this datasource.',
    schema: 'Named schema preset used to define the fields indexed for each document.',
    schema_field_set: 'Reusable collection of schema_field definitions. Indexes can include it by reference, then add or override fields locally.',
    lookup_rule_set: 'Reusable collection of lookup rules. Follow the reference to inspect each parameter, target field, operator, and grouping field.',
    column_set: 'Reusable datasource column mappings. Each column maps a SQL result name to an indexed field and optional normalization.',
    index_defaults: 'Reusable index defaults. Values from this block are inherited by indexes that reference it.',
    bulk_settings: 'Reusable ingestion settings. Index bulk blocks can inherit a batch size and override checkpoint settings.',
    schema_field: 'A field mapping and search behavior. indexed controls matching, stored controls returned values, lookup enables exact field filters, term_search includes the field in general text search.',
    lookup_rule: 'Maps a query parameter to a field operation. target_field is the stored/indexed field, operator controls comparison, and group_by enables latest-per-group rules.',
    bulk: 'Controls streaming ingestion. batch_size is rows per write; checkpoint settings allow large imports to resume safely.',
    batch_size: 'Number of rows processed per ingestion batch. Larger values can improve throughput while using more memory.',
    checkpoint_every: 'Persist import progress after this many rows. A smaller interval resumes closer to the last successful write.',
    checkpoint_path: 'Checkpoint file path for resumable indexing. Keep the directory writable by the LookupX process.',
    resume: 'Resume a prior import from its checkpoint when available.',
    initial_capacity: 'Initial in-memory index capacity. This is a sizing hint and grows as needed.',
    id_column: 'Unique source column used to identify records. SQL query results must include this column.',
    seq_column: 'Monotonic ordering column for keyset-paginated scans. Needed for efficient large-table reads.',
    column: 'Maps a datasource result column to an index field and optional type normalization.',
    target_field: 'Destination field in the index for the selected source column or lookup parameter.',
    operator: 'Filter behavior such as eq, contains, range, latest_gte, or latest_lte. latest operations require group_by.',
    group_by: 'Field used to select one latest matching record per group, commonly a code or entity key.',
    indexed: 'Build an index structure for this field so it can be filtered or searched. Disable for values that are returned only.',
    stored: 'Keep the field value with each record so search results can return it.',
    stored_only: 'Keep the value for display without building a search index for it.',
    lookup: 'Expose this field to exact-match lookup filters in the browser and HTTP lookup endpoint.',
    term_search: 'Include this field when the user searches across all searchable fields.',
    normalized: 'Normalize incoming values before indexing so matching follows the configured field kind.',
    lowercase: 'Lowercase values for case-insensitive matching.',
    prefix: 'Enable prefix matching on this field.',
    sortable: 'Allow this field to be used for result sorting.',
    facetable: 'Allow aggregation of result counts by this field.',
    index_alias: 'Use this field as the indexed source for another schema field.',
    stored_alias: 'Return the value from another stored field under this field name.',
    kind: 'Field type. keyword is exact text, text is tokenized, int/float are numeric, bool is boolean, and vector is an embedding. Temporal kinds: date (YYYY-MM-DD), datetime (local-looking timestamp stored as UTC), time (HH:MM:SS since midnight), datetimez (RFC3339 with timezone).',
    env: 'Reads a secret or setting from the process environment or .env file. Prefer env() for database credentials and session secrets.',
    params: 'Named datasource parameters used to substitute values in datasource configuration.',
    headers: 'HTTP headers sent when reading an HTTP datasource.',
    table: 'Database table to scan for a sql_table datasource. Use a trusted identifier from the configured database.',
    view: 'Database view to scan for a sql_view datasource. A view can expose only the columns LookupX needs.',
    query_file: 'Legacy SQL-file setting. Prefer query { file "queries/name.sql" }; the file is relative to the BCL directory and must contain a SELECT.',
    page_param: 'HTTP query parameter used for page numbering when reading a paginated endpoint.',
    limit_param: 'Maximum rows requested per HTTP page when the source supports a limit parameter.',
    id_field: 'JSON property used as a unique record ID for file ingestion.',
    timeout_seconds: 'Maximum time allowed for an HTTP datasource request.',
    page_size: 'Rows fetched per page for a paginated source.',
    order_column: 'Stable source column used to order a paginated scan.',
    where: 'Optional SQL predicate used by table or view datasources. Keep it static and trusted.',
    persistent: 'Persist this index state on disk when disk storage is enabled.',
    store_source: 'Retain the raw source document with each indexed record.',
    version: 'Configuration format version declared by the root BCL block.',
    const: 'Reusable BCL value. Click its usage to jump back here.',
  };

  async function api(method, path, body) {
    const options = { method, credentials: 'same-origin', headers: {} };
    if (method !== 'GET' && method !== 'HEAD' && csrf) options.headers['X-CSRF-Token'] = csrf;
    if (body !== undefined) {
      options.headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify(body);
    }
    const response = await fetch(path, options);
    const text = await response.text();
    if (!response.ok) throw new Error(`${response.status}: ${text}`);
    try { return JSON.parse(text); } catch { return text; }
  }
  function value() { return bclEditor ? bclEditor.getValue() : $('#bcl-source').value; }
  function setBCL(text) { if (bclEditor) bclEditor.setValue(text); else $('#bcl-source').value = text; }
  function toast(message, error = false) {
    const item = document.createElement('div'); item.className = `toast${error ? ' error' : ''}`; item.textContent = message;
    document.body.appendChild(item); setTimeout(() => item.remove(), 4000);
  }
  function lineAt(line) {
    if (bclEditor) { bclEditor.setCursor({ line, ch: 0 }); bclEditor.scrollIntoView({ line, ch: 0 }, 120); bclEditor.focus(); }
    else { const area = $('#bcl-source'); const starts = value().split('\n').slice(0, line).join('\n').length; area.focus(); area.setSelectionRange(starts, starts); area.scrollTop = Math.max(0, (line - 5) * 18); }
  }
  function plainText(line) { return line.replace(/\/\/.*$/, '').replace(/#.*$/, '').trim(); }
  function scanBCL(source) {
    const lines = source.split('\n'); const nodes = []; const stack = [];
    let queryBlock = null;
    let quote = ''; let tripleString = false; let escaped = false; let blockComment = false;
    const closeCount = (line) => {
      let count = 0;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (blockComment) { if (ch === '*' && line[i + 1] === '/') { blockComment = false; i++; } continue; }
        if (tripleString) { if (line.startsWith('"""', i)) { tripleString = false; i += 2; } continue; }
        if (escaped) { escaped = false; continue; }
        if (quote && ch === '\\') { escaped = true; continue; }
        if (quote) { if (ch === quote) quote = ''; continue; }
        if (ch === '/' && line[i + 1] === '*') { blockComment = true; i++; continue; }
        if (ch === '"' && line.startsWith('"""', i)) { quote = ''; tripleString = true; i += 2; continue; }
        if (ch === '"' || ch === "'" || ch === '`') { quote = ch; continue; }
        if (ch === '/' && line[i + 1] === '/') break;
        if (ch === '#') break;
        if (ch === '}') count++;
      }
      return count;
    };
    for (let line = 0; line < lines.length; line++) {
      const raw = lines[line]; const text = tripleString || blockComment ? '' : plainText(raw);
      const header = text.match(/^([A-Za-z_][\w-]*)\s*(?:"([^"]+)"|'([^']+)'|([\w.-]+))?\s*\{/);
      if (header) {
        const kind = header[1]; const id = header[2] || header[3] || header[4] || '';
        const node = { kind, id, label: id ? `${kind} "${id}"` : kind, line, depth: stack.length, parent: stack[stack.length - 1] || null, endLine: line };
        nodes.push(node); if (node.parent) node.parent.children = [...(node.parent.children || []), node];
        if (kind.toLowerCase() === 'query') queryBlock = { parent: node.parent, line };
        stack.push(node);
        if (kind.toLowerCase() === 'query') {
          const inline = text.slice(header[0].length).replace(/\s*}\s*$/, '').trim();
          const property = inline.match(/^(file|string)\s+("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')$/i);
          if (property) {
            const child = { kind: property[1], id: property[2].slice(1, -1), label: `${property[1]}  ${property[2]}`, line, depth: stack.length, parent: node, assignment: true, raw: `${property[1]} ${property[2]}` };
            if (property[1].toLowerCase() === 'file') child.sqlFile = child.id;
            else child.queryString = true;
            nodes.push(child); node.children = [...(node.children || []), child];
          }
        }
      } else if (text && !text.startsWith('}') && !text.startsWith('//')) {
        const reference = text.match(/^&([A-Za-z_][\w-]*)\.([\w.-]+)\s*$/);
        if (reference) {
          const rawReference = `&${reference[1]}.${reference[2]}`;
          const node = { kind: 'reference', id: `${reference[1]}.${reference[2]}`, label: `↗ ${rawReference}`, line, depth: stack.length, parent: stack[stack.length - 1] || null, assignment: true, reference: true, raw: rawReference };
          nodes.push(node); if (node.parent) node.parent.children = [...(node.parent.children || []), node];
        } else {
          const field = text.match(/^(&?[A-Za-z_][\w.-]*)\s+(.*)$/);
          if (field) {
            const constant = text.match(/^const\s+([A-Za-z_][\w]*)\s*=\s*(.*)$/);
            const name = constant ? 'const' : field[1].replace(/^&/, '');
            const id = constant ? constant[1] : field[2].replace(/^['"]|['"]$/g, '');
            const shortValue = field[2].length > 64 ? `${field[2].slice(0, 61)}…` : field[2];
            const displayValue = name.toLowerCase() === 'query' ? 'inline SQL' : shortValue;
            const node = { kind: name, id, label: constant ? `const ${id}` : `${name}  ${displayValue}`, line, depth: stack.length, parent: stack[stack.length - 1] || null, assignment: true, raw: text };
            nodes.push(node); if (node.parent) node.parent.children = [...(node.parent.children || []), node];
            if (queryBlock && stack.some((item) => item.kind.toLowerCase() === 'query')) {
              const qfile = text.match(/^file\s+["']([^"']+)["']/i);
              const qstring = text.match(/^string\s+["'](.*)["']$/i);
              if (qfile) node.sqlFile = qfile[1];
              if (qstring) node.queryString = true;
            }
            if (name.toLowerCase() === 'query_file') {
              const qfile = text.match(/^query_file\s+["']([^"']+\.sql)["']/i);
              if (qfile) node.sqlFile = qfile[1];
            }
            if (name.toLowerCase() === 'query' && /^query\s+["']/.test(text)) node.queryString = true;
          }
        }
      }
      const closes = closeCount(raw);
      for (let i = 0; i < closes && stack.length; i++) {
        const closed = stack.pop(); closed.endLine = line;
        if (closed.kind.toLowerCase() === 'query') queryBlock = null;
      }
    }
    return nodes;
  }
  function describeNode(node, all, clickedSummary = false) {
    const map = $('#bcl-tree');
    map.querySelectorAll('.active').forEach(item => item.classList.remove('active'));
    const mapItem = map.querySelector(`[data-line="${node.line}"]`);
    if (mapItem) {
      for (let parent = mapItem.parentElement; parent; parent = parent.parentElement) {
        if (parent.tagName === 'DETAILS' && !(clickedSummary && mapItem.tagName === 'SUMMARY' && parent === mapItem.parentElement)) parent.open = true;
      }
      mapItem.classList.add('active');
      mapItem.scrollIntoView({ block: 'nearest' });
    }
    const key = node.kind.toLowerCase();
    const referenceKind = node.raw && node.raw.match(/^&([\w.-]+)\./)?.[1]?.split('.')[0].toLowerCase();
    $('#help-title').textContent = node.label;
    const help = key === 'kind' && node.parent && node.parent.kind.toLowerCase() === 'datasource'
      ? 'Datasource adapter: sql_query runs a SELECT statement or SQL file; sql_table and sql_view provide table/view scans; csv, jsonl, http, and sql_file load other formats.'
      : HELP[key] || HELP[referenceKind];
    $('#help-text').textContent = help || (node.assignment
      ? `Configuration value for ${key}. Click its enclosing block to understand the section, or follow the reference link below.`
      : `The ${node.kind} block groups related LookupX configuration. Review its children in the map and select a field for its explanation.`);
    const links = $('#help-links'); links.replaceChildren();
    const refs = [];
    if (node.assignment && node.raw) {
      const ds = node.raw.match(/^datasource\s+["']?([^"'\s]+)["']?/i);
      const ref = node.raw.match(/^&([\w.-]+)/);
      if (ds) refs.push({ label: `Datasource ${ds[1]}`, target: all.find(n => n.kind === 'datasource' && n.id === ds[1]) });
      const schema = node.raw.match(/^schema\s+["']?([\w.-]+)/i);
      if (schema) refs.push({ label: `Schema defaults ${schema[1]}`, target: all.find(n => n.kind === 'index_defaults' && n.id === schema[1]) });
      if (ref) {
        const separator = ref[1].indexOf('.');
        const kind = separator < 0 ? ref[1] : ref[1].slice(0, separator);
        const id = separator < 0 ? '' : ref[1].slice(separator + 1);
        refs.push({ label: `Reference ${ref[1]}`, target: all.find(n => n.kind === kind && n.id === id) || all.find(n => n.id === id) });
      }
      const constant = node.raw.match(/\b([A-Z][A-Z0-9_]+)\s*$/);
      if (constant) refs.push({ label: `Constant ${constant[1]}`, target: all.find(n => n.kind === 'const' && n.id === constant[1]) });
    }
    refs.filter(r => r.target).forEach((ref) => {
      const link = document.createElement('a'); link.className = 'help-link'; link.textContent = ref.label;
      link.onclick = () => { lineAt(ref.target.line); describeNode(ref.target, all); }; links.appendChild(link);
    });
    renderSelection(node, all);
  }
  function renderSelection(node, all) {
    const title = $('#selection-title'); const summary = $('#selection-summary'); const fields = $('#selection-fields');
    title.textContent = node.label;
    summary.textContent = node.assignment ? 'Configuration value and source location.' : 'Settings inside this configuration block. Select a setting to jump to its source.';
    fields.replaceChildren();
    if (node.assignment) {
      appendSetting(fields, node, all);
      return;
    }
    const children = node.children || [];
    if (!children.length) { const empty=document.createElement('p');empty.className='muted';empty.textContent='This block has no settings.';fields.append(empty);return; }
    children.forEach(child => {
      if (child.assignment) appendSetting(fields, child, all);
      else {
        const nested=document.createElement('button');nested.className='selection-nested';nested.type='button';nested.textContent=`${child.kind}${child.id?` · ${child.id}`:''}  ›`;
        nested.onclick=()=>{lineAt(child.line);describeNode(child,all);};fields.append(nested);
      }
    });
  }
  function appendSetting(host, node, all) {
    const rawKey = node.raw ? (node.raw.match(/^&?[\w.-]+/) || ['setting'])[0] : node.kind;
    const reference = rawKey.startsWith('&');
    const key = reference ? 'Reference' : rawKey;
    const card=document.createElement('button');card.type='button';card.className='setting-card';card.title='Jump to this setting in the BCL editor';
    const name=document.createElement('strong');name.textContent=key;
    const val=document.createElement('code');val.textContent=node.raw ? (reference ? rawKey : node.raw.slice(rawKey.length).trim()) : node.label;
    const referenceKind=reference ? rawKey.slice(1).split('.')[0].toLowerCase() : '';
    const hint=document.createElement('span');hint.textContent=(reference && HELP[referenceKind]) || HELP[key.toLowerCase()] || 'Select to edit this value in the complete BCL editor.';
    card.append(name,val,hint);card.onclick=()=>{lineAt(node.line);describeNode(node,all);};host.append(card);
  }
  function renderTree() {
    const all = scanBCL(value()); const root = $('#bcl-tree'); root.replaceChildren();
    const makeNode = (node) => {
      if (node.assignment) {
        const row=document.createElement('button');row.type='button';row.dataset.line=String(node.line);row.className=`tree-row tree-leaf${node.sqlFile||node.reference?' reference':''}`;row.title=node.reference?'Follow BCL reference':HELP[node.kind.toLowerCase()]||'';row.textContent=node.label;
        row.onclick=()=>{lineAt(node.line);describeNode(node,all);if(node.sqlFile)window.location.assign(`/admin-queries.html?path=${encodeURIComponent(node.sqlFile)}`);};return row;
      }
      const details=document.createElement('details');details.className='tree-group';
      const summary=document.createElement('summary');summary.dataset.line=String(node.line);summary.className='tree-row';summary.title=HELP[node.kind.toLowerCase()]||'';
      const type=document.createElement('span');type.className='tree-type';type.textContent=node.kind;
      const label=document.createElement('span');label.className='tree-label';label.textContent=node.id||node.kind;
      summary.append(type,label);summary.onclick=()=>{describeNode(node,all,true);};details.append(summary);
      const children=document.createElement('div');children.className='tree-children';
      (node.children||[]).forEach(child=>children.append(makeNode(child)));
      if(children.childElementCount)details.append(children);
      else details.classList.add('empty');
      return details;
    };
    const topLevel=all.filter(node=>!node.parent);
    topLevel.forEach(node=>root.append(makeNode(node)));
    $('#bcl-dirty').textContent = value() === config.content ? '' : 'Unsaved edits';
  }
  function installEditor(id, mode) {
    const area = $(id); if (!window.CodeMirror || !CodeMirror.fromTextArea) return null;
    try {
      if (mode === 'bcl' && CodeMirror.defineSimpleMode && !(CodeMirror.modes && CodeMirror.modes.lookupxBCL)) {
        CodeMirror.defineSimpleMode('lookupxBCL', { start: [
          { regex: /#.*/, token: 'comment' }, { regex: /\/\/.*/, token: 'comment' },
          { regex: /"([^"\\]|\\.)*"/, token: 'string' }, { regex: /\b(env|true|false|null)\b/, token: 'atom' },
          { regex: /\b(index|datasource|schema_field|lookup_rule|column|bulk|const|query)\b/, token: 'keyword' },
          { regex: /&[\w.-]+/, token: 'variable-2' }, { regex: /\b\d+\b/, token: 'number' },
        ] });
      }
      const editor = CodeMirror.fromTextArea(area, { mode: mode === 'bcl' ? 'lookupxBCL' : 'text/x-sql', theme: 'material-darker', lineNumbers: true, lineWrapping: mode === 'sql', indentUnit: 2, tabSize: 2 });
      editor.on('change', () => { if (mode === 'bcl') renderTree(); });
      return editor;
    } catch (error) { console.warn('Code editor unavailable; using textarea.', error); return null; }
  }
  function findBlock(kind, id, nodes = scanBCL(value())) {
    return nodes.find(n => n.kind.toLowerCase() === kind.toLowerCase() && n.id === id && !n.assignment);
  }
  function referencedBlocks(owner, targetKind, nodes, visited = new Set()) {
    if (!owner || visited.has(owner)) return [];
    visited.add(owner);
    const found = [];
    for (const child of owner.children || []) {
      if (!child.assignment || !child.raw) continue;
      const ref = child.raw.match(/^&([A-Za-z_][\w-]*)\.([\w.-]+)/);
      if (!ref) continue;
      const [, kind, id] = ref;
      if (kind.toLowerCase() === targetKind.toLowerCase()) {
        const block = nodes.find(item => !item.assignment && item.kind.toLowerCase() === targetKind.toLowerCase() && item.id === id);
        if (block && !found.includes(block)) found.push(block);
      } else if (kind.toLowerCase() === 'index_defaults') {
        const defaults = nodes.find(item => !item.assignment && item.kind.toLowerCase() === 'index_defaults' && item.id === id);
        referencedBlocks(defaults, targetKind, nodes, visited).forEach(block => { if (!found.includes(block)) found.push(block); });
      }
    }
    return found;
  }
  function effectiveDefinitions(indexBlock, setKind, itemKind, nodes) {
    const sets = referencedBlocks(indexBlock, setKind, nodes);
    const result = new Map();
    sets.forEach(set => (set.children || []).filter(item => !item.assignment && item.kind.toLowerCase() === itemKind).forEach(item => {
      if (item.id) result.set(itemKind === 'lookup_rule' ? item.id.toLowerCase() : item.id, item);
    }));
    (indexBlock && indexBlock.children || []).filter(item => !item.assignment && item.kind.toLowerCase() === itemKind).forEach(item => {
      if (item.id) result.set(itemKind === 'lookup_rule' ? item.id.toLowerCase() : item.id, item);
    });
    return { sets, items: [...result.values()] };
  }
  function referencedDefaultBlocks(indexBlock, nodes) {
    const defaults = [];
    for (const child of indexBlock && indexBlock.children || []) {
      const ref = child.assignment && child.raw && child.raw.match(/^&index_defaults\.([\w.-]+)/);
      if (!ref) continue;
      const block = nodes.find(item => !item.assignment && item.kind.toLowerCase() === 'index_defaults' && item.id === ref[1]);
      if (block && !defaults.includes(block)) defaults.push(block);
    }
    return defaults;
  }
  function showIndexCards(indexes) {
    const nodes = scanBCL(value()); const host = $('#index-cards'); host.replaceChildren();
    if (!indexes.length) { host.innerHTML = '<p class="muted">No indexes are defined in this BCL.</p>'; return; }
    indexes.forEach((index) => {
      const block = findBlock('index', index.id, nodes); const source = block && nodes.find(n => n.parent === block && n.kind.toLowerCase() === 'datasource');
      const datasource = source && source.raw.match(/^datasource\s+["']?([^"'\s]+)["']?/i)?.[1] || index.datasource_id || 'Not assigned';
      const dsBlock = findBlock('datasource', datasource, nodes); const kindNode = dsBlock && nodes.find(n => n.parent === dsBlock && n.kind.toLowerCase() === 'kind');
      const defaults=referencedDefaultBlocks(block,nodes);
      const schemaNode = block && nodes.find(n => n.parent === block && n.kind.toLowerCase() === 'schema') || defaults.flatMap(item=>item.children||[]).find(n=>n.kind.toLowerCase()==='schema');
      const queryNode = dsBlock && nodes.find(n => n.kind.toLowerCase() === 'query' && n.parent === dsBlock);
      const queryFileNode = nodes.find(n => n.sqlFile && (queryNode ? n.parent === queryNode : n.parent === dsBlock));
      const queryStringNode = nodes.find(n => n.queryString && (queryNode ? n.parent === queryNode : n.parent === dsBlock));
      const card = document.createElement('article'); card.className = 'index-card';
      const title = document.createElement('h3'); title.textContent = index.id;
      const dl = document.createElement('dl');
      [["Datasource", datasource], ["Adapter", kindNode ? kindNode.raw.replace(/^kind\s+/, '') : index.kind || 'See datasource'], ["Schema preset", schemaNode ? schemaNode.raw.replace(/^schema\s+/, '') : 'Custom or built in'], ["Query source", queryFileNode ? `SQL file: ${queryFileNode.sqlFile}` : queryStringNode ? 'Inline SELECT statement' : 'See datasource configuration']].forEach(([k,v]) => { const dt=document.createElement('dt');dt.textContent=k;const dd=document.createElement('dd');dd.textContent=v;dl.append(dt,dd); });
      card.append(title, dl);
      [["Open index block", block], ["Open datasource", dsBlock]].forEach(([label,target]) => { if (!target) return; const button=document.createElement('button');button.className='button';button.textContent=label;button.onclick=()=>{lineAt(target.line);describeNode(target,nodes);};card.append(button); });
      defaults.forEach(item=>{const button=document.createElement('button');button.className='button';button.textContent=`Open defaults · ${item.id}`;button.title=HELP.index_defaults;button.onclick=()=>{lineAt(item.line);describeNode(item,nodes);};card.append(button);});
      if (queryFileNode) { const button=document.createElement('button');button.className='button';button.textContent='Edit SQL query file';button.onclick=()=>window.location.assign(`/admin-queries.html?path=${encodeURIComponent(queryFileNode.sqlFile)}`);card.append(button); }
      else if (queryNode) { const button=document.createElement('button');button.className='button';button.textContent='Open query settings';button.onclick=()=>{lineAt(queryNode.line);describeNode(queryNode,nodes);};card.append(button); }
      const fieldDefinitions=effectiveDefinitions(block,'schema_field_set','schema_field',nodes);
      const ruleDefinitions=effectiveDefinitions(block,'lookup_rule_set','lookup_rule',nodes);
      const fields=fieldDefinitions.items;
      const rules=ruleDefinitions.items;
      const bulk=nodes.find(node=>node.parent===block&&node.kind.toLowerCase()==='bulk'&&!node.assignment);
      const summary=document.createElement('div');summary.className='index-details';
      const appendDefinitions=(label, definitions, helpText, helpKey) => {
        const group=document.createElement('section');group.className='index-definition-group';
        const heading=document.createElement('strong');heading.textContent=`${label} (${definitions.items.length})`;group.append(heading);
        if(definitions.sets.length){const sources=document.createElement('p');sources.className='index-definition-sources';sources.textContent=`Inherited from ${definitions.sets.map(set=>set.id).join(', ')}. Local definitions override matching names.`;group.append(sources);}
        else {const note=document.createElement('p');note.className='index-definition-sources';note.textContent=helpText;group.append(note);}
        if(definitions.items.length){const list=document.createElement('details');list.className='index-browse';const caption=document.createElement('summary');caption.textContent=`Browse ${label.toLowerCase()}`;list.append(caption);definitions.items.forEach(item=>{const button=document.createElement('button');button.type='button';button.className='index-item-link';button.textContent=`${item.id||`unnamed ${label.toLowerCase().slice(0,-1)}`}${item.parent===block?' · local':' · inherited'}`;button.title=HELP[helpKey];button.onclick=()=>{lineAt(item.line);describeNode(item,nodes);};list.append(button);});group.append(list);}
        if(definitions.sets.length){const links=document.createElement('div');links.className='index-set-links';definitions.sets.forEach(set=>{const button=document.createElement('button');button.type='button';button.className='index-set-link';button.textContent=`Open ${set.kind} · ${set.id}`;button.title=HELP[set.kind.toLowerCase()];button.onclick=()=>{lineAt(set.line);describeNode(set,nodes);};links.append(button);});group.append(links);}
        summary.append(group);
      };
      appendDefinitions('Schema fields',fieldDefinitions,'Fields resolved from the index schema and reusable field sets appear here.', 'schema_field');
      appendDefinitions('Lookup rules',ruleDefinitions,'Configured query parameters and their target fields appear here.', 'lookup_rule');
      if(bulk){const button=document.createElement('button');button.className='button';button.textContent='View bulk settings';button.title=HELP.bulk;button.onclick=()=>{lineAt(bulk.line);describeNode(bulk,nodes);};summary.append(button);}
      card.append(summary);
      host.append(card);
    });
  }
  async function load() {
    const session = await fetch('/auth/session', { credentials: 'same-origin' });
    if (!session.ok) throw new Error(`Admin session check failed (${session.status})`);
    const auth = await session.json(); csrf = auth.csrf_token || '';
    if (!auth.authenticated || !csrf) { window.location.replace('/'); return; }
    document.body.classList.add('admin-ready');
    $('.topbar').hidden = false; $('main').hidden = false; $('#admin-gate').hidden = true;
    const [bcl, indexes, users, keys] = await Promise.all([api('GET','/admin/api/config'),api('GET','/admin/api/indexes'),api('GET','/admin/api/users'),api('GET','/admin/api/keys')]);
    config = bcl; $('#config-path').textContent = bcl.path; setBCL(bcl.content);
    bclEditor = installEditor('#bcl-source','bcl');
    renderTree(); showIndexCards(indexes); renderAccess(users, keys); $('#save-state').textContent='Configuration loaded';
    const requestedFile = new URLSearchParams(window.location.search).get('file');
    if (requestedFile) {
      const nodes = scanBCL(value());
      const reference = nodes.find(node => node.sqlFile === requestedFile);
      if (reference) { lineAt(reference.line); describeNode(reference, nodes); }
    }
  }
  function prop(object, lower, upper) { return object[lower] !== undefined ? object[lower] : object[upper]; }
  function renderAccess(users, keys) {
    const usersHost=$('#users-list'), keysHost=$('#keys-list'), userSelect=$('#key-user');
    usersHost.replaceChildren(); keysHost.replaceChildren(); userSelect.replaceChildren();
    const enabled=users.filter(user=>user.enabled);
    enabled.forEach(user=>{const option=document.createElement('option');option.value=user.id;option.textContent=`${user.name} (${user.id})`;userSelect.append(option);});
    if(!enabled.length){const option=document.createElement('option');option.value='';option.textContent='Create an enabled user first';userSelect.append(option);}
    const userTable=document.createElement('table');userTable.className='access-table';userTable.innerHTML='<thead><tr><th>User</th><th>Scopes</th><th>Status</th><th></th></tr></thead>';
    const userBody=document.createElement('tbody');
    users.forEach(user=>{const row=document.createElement('tr');const action=user.enabled?`<button class="button" data-disable-user="${escapeAttr(user.id)}">Disable</button>`:'';row.innerHTML=`<td>${escapeHTML(user.name)}<br><span class="muted">${escapeHTML(user.id)}</span></td><td>${escapeHTML((user.scopes||[]).join(', '))}</td><td>${user.enabled?'Enabled':'Disabled'}</td><td>${action}</td>`;userBody.append(row);});
    userTable.append(userBody);usersHost.append(userTable);
    const keyTable=document.createElement('table');keyTable.className='access-table';keyTable.innerHTML='<thead><tr><th>Key</th><th>User</th><th>Expiry</th><th>State</th><th></th></tr></thead>';
    const keyBody=document.createElement('tbody');
    keys.forEach(key=>{const id=prop(key,'id','ID'),name=prop(key,'name','Name'),owner=prop(key,'subject_id','SubjectID'),expires=prop(key,'expires_at','ExpiresAt'),revoked=prop(key,'revoked','Revoked');const date=expires&& !String(expires).startsWith('0001-')?new Date(expires).toLocaleDateString():'Never';const row=document.createElement('tr');row.innerHTML=`<td>${escapeHTML(name)}<br><span class="muted">${escapeHTML(id)}</span></td><td>${escapeHTML(owner)}</td><td>${escapeHTML(date)}</td><td>${revoked?'Revoked':'Active'}</td><td>${revoked?'':`<button class="button" data-rotate-key="${escapeAttr(id)}">Rotate</button><button class="button" data-revoke-key="${escapeAttr(id)}">Revoke</button>`}</td>`;keyBody.append(row);});
    keyTable.append(keyBody);keysHost.append(keyTable);
    usersHost.querySelectorAll('[data-disable-user]').forEach(button=>button.onclick=async()=>{if(!confirm('Disable this user and revoke all their API keys?'))return;try{await api('DELETE',`/admin/api/users/${encodeURIComponent(button.dataset.disableUser)}`);await reloadAccess();}catch(e){toast(e.message,true);}});
    keysHost.querySelectorAll('[data-revoke-key]').forEach(button=>button.onclick=async()=>{if(!confirm('Revoke this API key?'))return;try{await api('DELETE',`/admin/api/keys/${encodeURIComponent(button.dataset.revokeKey)}`);await reloadAccess();}catch(e){toast(e.message,true);}});
    keysHost.querySelectorAll('[data-rotate-key]').forEach(button=>button.onclick=async()=>{if(!confirm('Rotate this API key? The current key will stop working.'))return;try{const result=await api('POST',`/admin/api/keys/${encodeURIComponent(button.dataset.rotateKey)}/rotate`,{});showGeneratedKey(result.key);await reloadAccess();}catch(e){toast(e.message,true);}});
  }
  async function reloadAccess(){const [users,keys]=await Promise.all([api('GET','/admin/api/users'),api('GET','/admin/api/keys')]);renderAccess(users,keys);}
  function escapeHTML(text){return String(text??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
  function escapeAttr(text){return escapeHTML(text);}
  function showGeneratedKey(key){const output=$('#generated-key');output.textContent=`Copy this API key now; it will not be shown again:\n${key}`;output.hidden=false;output.scrollIntoView({behavior:'smooth',block:'center'});}
  $('#validate-bcl').onclick = async () => { $('#save-state').textContent='Validating…'; try { await api('POST','/admin/api/config/validate',{content:value()}); $('#save-state').textContent='BCL is valid'; toast('Configuration validation succeeded'); } catch(e){$('#save-state').textContent=e.message;toast(e.message,true);} };
  $('#save-bcl').onclick = async () => { $('#save-state').textContent='Saving…'; try { await api('PUT','/admin/api/config',{content:value()}); config.content=value(); $('#save-state').textContent='Saved; restart required'; renderTree(); toast('BCL saved. Restart the server to apply it.'); } catch(e){$('#save-state').textContent=e.message;toast(e.message,true);} };
  $('#user-form').onsubmit = async (event) => {event.preventDefault();const scopes=$('#user-scope').value==='read'?['lookup:read']:['lookup:read','lookup:write'];try{await api('POST','/admin/api/users',{id:$('#user-id').value,name:$('#user-name').value,scopes});event.target.reset();await reloadAccess();toast('API user created');}catch(e){toast(e.message,true);}};
  $('#key-form').onsubmit = async (event) => {event.preventDefault();const date=$('#key-expiry').value;try{const result=await api('POST','/admin/api/keys',{user_id:$('#key-user').value,name:$('#key-name').value,expires_at:date?new Date(`${date}T23:59:59Z`).toISOString():'',allowed_ips:$('#key-ips').value.split(',').map(v=>v.trim()).filter(Boolean)});event.target.reset();showGeneratedKey(result.key);await reloadAccess();toast('API key created');}catch(e){toast(e.message,true);}};
  $('#logout').onclick = async () => { try{await api('POST','/auth/logout');window.location.replace('/');}catch(e){toast(e.message,true);} };
  $('#bcl-source').addEventListener('input',renderTree);
  window.addEventListener('beforeunload', event => {
    if (value() === config.content) return;
    event.preventDefault();
    event.returnValue = '';
  });
  load().catch((error)=>{
    if (document.body.classList.contains('admin-ready')) {
      $('#save-state').textContent = error.message;
      toast(error.message, true);
      return;
    }
    const gate=$('#admin-gate');gate.replaceChildren();
    const message=document.createElement('p');message.textContent=`Could not verify the administrator session: ${error.message}`;
    const link=document.createElement('a');link.href='/';link.textContent='Return to sign in';
    gate.append(message,link);
  });
})();
