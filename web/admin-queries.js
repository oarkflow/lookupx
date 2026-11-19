(() => {
  const $ = selector => document.querySelector(selector);
  let csrf = '';
  let editor = null;
  let selectedFile = '';
  let currentDir = '';
  let dirty = false;
  let settingContent = false;

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

  function setContent(content) {
    settingContent = true;
    if (editor) editor.setValue(content); else $('#sql-source').value = content;
    settingContent = false;
    dirty = false;
    $('#sql-state').textContent = '';
  }

  function content() { return editor ? editor.getValue() : $('#sql-source').value; }

  function notify(message, error = false) {
    const item = document.createElement('div');
    item.className = `toast${error ? ' error' : ''}`;
    item.textContent = message;
    document.body.append(item);
    setTimeout(() => item.remove(), 4500);
  }

  function confirmDiscard() {
    return !dirty || window.confirm('Discard unsaved SQL changes?');
  }

  async function listFiles(dir = '') {
    currentDir = dir;
    $('#file-breadcrumb').textContent = `/${dir}`;
    const result = await api('GET', `/admin/api/sql-files${dir ? `?dir=${encodeURIComponent(dir)}` : ''}`);
    const host = $('#file-list');
    host.replaceChildren();
    if (dir) {
      const up = document.createElement('button');
      up.className = 'file-entry';
      up.textContent = '↰ Parent folder';
      up.onclick = () => { if (confirmDiscard()) listFiles(dir.split('/').slice(0, -1).join('/')).catch(e => notify(e.message, true)); };
      host.append(up);
    }
    if (!result.entries.length) {
      const empty = document.createElement('p');
      empty.className = 'muted';
      empty.textContent = 'No SQL files or folders here yet.';
      host.append(empty);
    }
    result.entries.forEach(entry => {
      const button = document.createElement('button');
      button.className = `file-entry${entry.is_dir ? ' directory' : ''}${entry.path === selectedFile ? ' active' : ''}`;
      button.textContent = `${entry.is_dir ? '▸' : '▤'}  ${entry.name}`;
      button.onclick = async () => {
        if (!confirmDiscard()) return;
        if (entry.is_dir) {
          try { await listFiles(entry.path); } catch (error) { notify(error.message, true); }
        } else {
          await openFile(entry.path, false);
        }
      };
      host.append(button);
    });
  }

  async function openFile(path, fromReference) {
    try {
      const result = await api('GET', `/admin/api/sql-files?path=${encodeURIComponent(path)}`);
      selectedFile = result.path;
      setContent(result.content);
      $('#sql-title').textContent = selectedFile;
      $('#sql-hint').textContent = fromReference
        ? 'Opened from a query.file reference in the active BCL. Saving updates this SQL file.'
        : 'Edit the SELECT statement, then save the SQL file.';
      $('#save-sql').disabled = false;
      $('#open-query-ref').hidden = !fromReference;
      $('#open-query-ref').onclick = () => window.location.assign(`/admin.html?file=${encodeURIComponent(selectedFile)}`);
      await listFiles(currentDir);
      if (editor) editor.focus();
      $('#sql-title').scrollIntoView({ behavior: 'smooth', block: 'center' });
    } catch (error) {
      notify(error.message, true);
    }
  }

  async function saveFile() {
    if (!selectedFile) return;
    $('#sql-state').textContent = 'Validating and saving…';
    try {
      const result = await api('PUT', '/admin/api/sql-files', { path: selectedFile, content: content() });
      dirty = false;
      $('#sql-state').textContent = 'Saved';
      $('#query-state').textContent = `Saved ${result.path}`;
      await listFiles(currentDir);
      notify(`Saved ${result.path}`);
    } catch (error) {
      $('#sql-state').textContent = error.message;
      notify(error.message, true);
    }
  }

  function mountEditor() {
    if (!window.CodeMirror || !CodeMirror.fromTextArea) return;
    try {
      editor = CodeMirror.fromTextArea($('#sql-source'), {
        mode: 'text/x-sql', theme: 'material-darker', lineNumbers: true,
        lineWrapping: true, indentUnit: 2, tabSize: 2,
      });
      editor.on('change', () => {
        if (settingContent) return;
        dirty = true;
        $('#sql-state').textContent = 'Unsaved changes';
      });
    } catch (error) {
      console.warn('SQL editor unavailable; using textarea.', error);
    }
  }

  async function load() {
    const response = await fetch('/auth/session', { credentials: 'same-origin' });
    if (!response.ok) throw new Error(`Admin session check failed (${response.status})`);
    const session = await response.json();
    csrf = session.csrf_token || '';
    if (!session.authenticated || !csrf) { window.location.replace('/'); return; }
    document.body.classList.add('admin-ready');
    $('.topbar').hidden = false;
    $('main').hidden = false;
    $('#query-gate').hidden = true;
    const config = await api('GET', '/admin/api/config');
    $('#query-config-path').textContent = config.path;
    mountEditor();
    await listFiles();
    const requestedFile = new URLSearchParams(window.location.search).get('path');
    if (requestedFile) await openFile(requestedFile, true);
    $('#query-state').textContent = 'Query library ready';
  }

  $('#save-sql').onclick = saveFile;
  $('#sql-source').addEventListener('input', () => {
    if (editor || settingContent) return;
    dirty = true;
    $('#sql-state').textContent = 'Unsaved changes';
  });
  $('#new-folder').onclick = async () => {
    const name = window.prompt('Folder name (relative to the current SQL folder):');
    if (!name) return;
    const path = [currentDir, name].filter(Boolean).join('/');
    try {
      await api('POST', '/admin/api/sql-folders', { path });
      await listFiles(currentDir);
      notify(`Created ${path}`);
    } catch (error) { notify(error.message, true); }
  };
  $('#new-sql').onclick = async () => {
    let name = window.prompt('SQL filename (for example queries/cpt.sql):');
    if (!name) return;
    if (!name.toLowerCase().endsWith('.sql')) name += '.sql';
    const path = name.includes('/') ? name : [currentDir, name].filter(Boolean).join('/');
    if (selectedFile && !confirmDiscard()) return;
    try {
      await api('PUT', '/admin/api/sql-files', { path, content: 'SELECT\n  -- Add the columns required by this datasource\nFROM your_table\n' });
      await openFile(path, false);
      notify(`Created ${path}`);
    } catch (error) { notify(error.message, true); }
  };
  window.addEventListener('beforeunload', event => {
    if (!dirty) return;
    event.preventDefault();
    event.returnValue = '';
  });
  load().catch(error => {
    if (document.body.classList.contains('admin-ready')) {
      $('#query-state').textContent = error.message;
      notify(error.message, true);
      return;
    }
    const gate = $('#query-gate');
    gate.replaceChildren();
    const message = document.createElement('p');
    message.textContent = `Could not verify the administrator session: ${error.message}`;
    const link = document.createElement('a');
    link.href = '/';
    link.textContent = 'Return to sign in';
    gate.append(message, link);
  });
})();
