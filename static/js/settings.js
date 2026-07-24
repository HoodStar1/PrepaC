(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;
  const form = document.getElementById('settingsForm');
  const providerBox = document.getElementById('postingProvidersBuilder');
  const destinationBox = document.getElementById('shareDestinationsBuilder');
  const providersEditable = providerBox?.dataset.editable === 'true';
  const destinationsEditable = destinationBox?.dataset.editable === 'true';
  const categoryOptions = P.parseJSONData('settingsCategoryOptionsData', []);
  let providers = P.parseJSONData('postingProvidersData', []);
  let destinations = P.parseJSONData('shareDestinationsData', []);

  const slug = (value, fallback) => String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || fallback;
  const normalizeProvider = (value = {}, index = 0) => ({ id: slug(value.id || value.name, `provider${index + 1}`), name: String(value.name || `Provider ${index + 1}`), enabled: value.enabled !== false, host: String(value.host || ''), port: String(value.port || '563'), ssl: value.ssl !== false, username: String(value.username || ''), password: '', password_configured: Boolean(value.password_configured), password_source: String(value.password_source || 'unset'), clear_password: Boolean(value.clear_password), connections: String(value.connections || '25'), max_connections: String(value.max_connections || value.connections || '25'), account_group: String(value.account_group || ''), priority_up_to_gb: index ? String(value.priority_up_to_gb || '0') : '0' });
  const normalizeDestination = (value = {}, index = 0) => ({ id: slug(value.id || value.name, `destination${index + 1}`), name: String(value.name || 'New destination'), enabled: value.enabled !== false, mode: String(value.mode || 'both'), base_url: String(value.base_url || ''), api_key: '', api_key_configured: Boolean(value.api_key_configured), clear_api_key: Boolean(value.clear_api_key), include_nfo: value.include_nfo !== false, include_mediainfo: value.include_mediainfo !== false, includemeta: value.includemeta !== false, basic_auth: Boolean(value.basic_auth), username: String(value.username || ''), password: '', password_configured: Boolean(value.password_configured), clear_password: Boolean(value.clear_password), categories_cache: Array.isArray(value.categories_cache) ? value.categories_cache : [], category_overrides: value.category_overrides && typeof value.category_overrides === 'object' ? { ...value.category_overrides } : {} });
  providers = (Array.isArray(providers) ? providers : []).map(normalizeProvider);
  destinations = (Array.isArray(destinations) ? destinations : []).map(normalizeDestination);

  const markDirty = () => { const status = document.getElementById('settingsDirtyStatus'); if (status) status.textContent = 'Unsaved changes.'; };
  const sync = () => {
    document.getElementById('postingProvidersJson').value = JSON.stringify(providers.map(normalizeProvider));
    document.getElementById('shareDestinationsJson').value = JSON.stringify(destinations.map(normalizeDestination));
  };
  const label = (text, control, hint) => {
    const node = P.el('label'); node.append(P.el('span', { text })); node.append(control); if (hint) node.append(P.el('small', { className: 'muted', text: hint })); return node;
  };
  const secretField = (text, control, hint) => {
    const node = P.el('div', { className: 'field' }); node.append(P.el('span', { className: 'field-label', text }), control); if (hint) node.append(P.el('small', { className: 'muted', text: hint })); return node;
  };
  const input = (type, value, onInput, attributes = {}) => {
    const node = P.el('input', { type, value: value ?? '', ...attributes });
    node.addEventListener('input', () => { onInput(type === 'number' ? node.value : node.value); sync(); markDirty(); });
    return node;
  };
  const checkbox = (text, checked, onChange, disabled = false) => {
    const control = P.el('input', { type: 'checkbox', checked, disabled });
    control.addEventListener('change', () => { onChange(control.checked); sync(); markDirty(); });
    return P.el('label', { className: 'checkbox' }, [control, P.el('span', { text })]);
  };
  const select = (options, value, onChange, disabled = false) => {
    const node = P.el('select', { disabled });
    options.forEach(([optionValue, optionLabel]) => node.append(P.el('option', { value: optionValue, text: optionLabel, selected: String(optionValue) === String(value) })));
    node.addEventListener('change', () => { onChange(node.value); sync(); markDirty(); });
    return node;
  };
  const secretControl = (state, key, configuredKey, clearKey, disabled, accessibleName) => {
    const wrapper = P.el('span', { className: 'secret-field' });
    const control = P.el('input', { type: 'password', value: '', placeholder: state[configuredKey] ? 'Configured — leave blank to keep' : 'Not configured', autocomplete: 'new-password', disabled, 'aria-label': accessibleName });
    control.addEventListener('input', () => { state[key] = control.value; state[clearKey] = false; if (control.value) state[configuredKey] = true; sync(); markDirty(); });
    const clear = P.button('Clear', { className: 'danger small', disabled, ariaLabel: `Clear ${accessibleName}` });
    clear.addEventListener('click', () => { state[key] = ''; state[configuredKey] = false; state[clearKey] = true; control.value = ''; control.placeholder = 'Will be cleared when saved'; sync(); markDirty(); });
    wrapper.append(control, clear);
    return wrapper;
  };
  const reorderButtons = (list, index, render, editable) => {
    const actions = P.el('div', { className: 'actions' });
    const up = P.button('Move up', { className: 'secondary small', disabled: !editable || index === 0 });
    const down = P.button('Move down', { className: 'secondary small', disabled: !editable || index === list.length - 1 });
    up.addEventListener('click', () => { [list[index - 1], list[index]] = [list[index], list[index - 1]]; render(); markDirty(); });
    down.addEventListener('click', () => { [list[index + 1], list[index]] = [list[index], list[index + 1]]; render(); markDirty(); });
    actions.append(up, down); return actions;
  };

  const renderProviders = () => {
    if (!providerBox) return;
    if (!providers.length) { P.state(providerBox, 'empty', 'No posting providers', providersEditable ? 'Add a provider to configure posting.' : 'Providers are managed externally.'); sync(); return; }
    const fragment = document.createDocumentFragment();
    providers.forEach((provider, index) => {
      const card = P.el('article', { className: 'job' });
      const actions = reorderButtons(providers, index, renderProviders, providersEditable);
      const clone = P.button('Clone', { className: 'secondary small', disabled: !providersEditable });
      const remove = P.button('Remove', { className: 'danger small', disabled: !providersEditable });
      clone.addEventListener('click', () => { const copy = normalizeProvider({ ...provider, name: `${provider.name} copy`, id: '' }, providers.length); copy.password_configured = false; providers.splice(index + 1, 0, copy); renderProviders(); markDirty(); });
      remove.addEventListener('click', () => { providers.splice(index, 1); renderProviders(); markDirty(); });
      actions.append(clone, remove);
      card.append(P.el('div', { className: 'job-header' }, [P.el('div', {}, [P.el('span', { className: 'eyebrow', text: `Provider ${index + 1}` }), P.el('h3', { text: provider.name })]), actions]));
      const grid = P.el('div', { className: 'form-grid' });
      grid.append(checkbox('Enabled', provider.enabled, (value) => { provider.enabled = value; }, !providersEditable));
      grid.append(label('Display name', input('text', provider.name, (value) => { provider.name = value; }, { disabled: !providersEditable })));
      grid.append(label('ID', input('text', provider.id, (value) => { provider.id = slug(value, `provider${index + 1}`); }, { disabled: !providersEditable })));
      grid.append(checkbox('Use SSL', provider.ssl, (value) => { provider.ssl = value; }, !providersEditable));
      grid.append(label('Host', input('text', provider.host, (value) => { provider.host = value; }, { disabled: !providersEditable })));
      grid.append(label('Port', input('text', provider.port, (value) => { provider.port = value; }, { disabled: !providersEditable })));
      grid.append(label('Username', input('text', provider.username, (value) => { provider.username = value; }, { disabled: !providersEditable, autocomplete: 'off' })));
      grid.append(secretField(`Password (${provider.password_configured ? 'configured' : 'not configured'})`, secretControl(provider, 'password', 'password_configured', 'clear_password', !providersEditable, `${provider.name} password`), `Source: ${provider.password_source || 'unset'}`));
      grid.append(label('Upload connections', input('number', provider.connections, (value) => { provider.connections = value; }, { disabled: !providersEditable, min: 1 })));
      grid.append(label('Account maximum connections', input('number', provider.max_connections, (value) => { provider.max_connections = value; }, { disabled: !providersEditable, min: 1 })));
      grid.append(label('Account group', input('text', provider.account_group, (value) => { provider.account_group = value; }, { disabled: !providersEditable }), 'Use the same value for entries sharing an upstream account limit.'));
      grid.append(label('Prioritize jobs up to (GB)', input('number', index ? provider.priority_up_to_gb : '0', (value) => { provider.priority_up_to_gb = value; }, { disabled: !providersEditable || index === 0, min: 0, step: '.1' })));
      card.append(grid); fragment.append(card);
    });
    providerBox.replaceChildren(fragment); sync();
  };

  const categoryOverrideFields = (destination, index, editable) => {
    const grid = P.el('div', { className: 'form-grid' });
    categoryOptions.forEach((category) => {
      const options = [['', 'Automatic category match'], ...(destination.categories_cache || []).map((item) => [String(item.id || ''), `${item.id || ''} — ${item.label || ''}`])];
      grid.append(label(category.label || category.value, select(options, destination.category_overrides?.[category.value] || '', (value) => { if (value) destination.category_overrides[category.value] = value; else delete destination.category_overrides[category.value]; }, !editable)));
    });
    return grid;
  };

  const renderDestinations = () => {
    if (!destinationBox) return;
    if (!destinations.length) { P.state(destinationBox, 'empty', 'No share destinations', destinationsEditable ? 'Add a destination to enable Share.' : 'Destinations are managed externally.'); sync(); return; }
    const fragment = document.createDocumentFragment();
    destinations.forEach((destination, index) => {
      const card = P.el('article', { className: 'job' });
      const actions = reorderButtons(destinations, index, renderDestinations, destinationsEditable);
      const clone = P.button('Clone', { className: 'secondary small', disabled: !destinationsEditable });
      const test = P.button('Test saved connection', { className: 'secondary small', disabled: !destination.id });
      const remove = P.button('Remove', { className: 'danger small', disabled: !destinationsEditable });
      clone.addEventListener('click', () => { const copy = normalizeDestination({ ...destination, name: `${destination.name} copy`, id: '', api_key_configured: false, password_configured: false }, destinations.length); destinations.splice(index + 1, 0, copy); renderDestinations(); markDirty(); });
      test.addEventListener('click', async () => {
        const output = document.getElementById('shareCapsResult'); P.setBusy(test, true, 'Testing…'); output.textContent = `Testing saved destination ${destination.name || destination.id}…`;
        try { const data = await P.requestJSON('/api/share/destination/test', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ destination_id: destination.id }) }); destination.categories_cache = data.categories || []; output.textContent = `Connection succeeded. ${destination.categories_cache.length} categories loaded.`; renderDestinations(); }
        catch (error) { output.textContent = `Connection test failed: ${error.message}`; P.setBusy(test, false); }
      });
      remove.addEventListener('click', () => { destinations.splice(index, 1); renderDestinations(); markDirty(); });
      actions.append(clone, test, remove);
      card.append(P.el('div', { className: 'job-header' }, [P.el('div', {}, [P.el('span', { className: 'eyebrow', text: `Destination ${index + 1}` }), P.el('h3', { text: destination.name })]), actions]));
      const grid = P.el('div', { className: 'form-grid' });
      grid.append(checkbox('Enabled', destination.enabled, (value) => { destination.enabled = value; }, !destinationsEditable));
      grid.append(label('Display name', input('text', destination.name, (value) => { destination.name = value; }, { disabled: !destinationsEditable })));
      grid.append(label('ID', input('text', destination.id, (value) => { destination.id = slug(value, `destination${index + 1}`); }, { disabled: !destinationsEditable })));
      grid.append(label('Base URL (before /api)', input('text', destination.base_url, (value) => { destination.base_url = value; }, { disabled: !destinationsEditable, placeholder: 'https://indexer.example' })));
      grid.append(secretField(`API key (${destination.api_key_configured ? 'configured' : 'not configured'})`, secretControl(destination, 'api_key', 'api_key_configured', 'clear_api_key', !destinationsEditable, `${destination.name} API key`)));
      grid.append(label('Mode', select([['manual', 'Manual'], ['auto', 'Automatic'], ['both', 'Manual and automatic']], destination.mode, (value) => { destination.mode = value; }, !destinationsEditable)));
      grid.append(checkbox('Attach generated NFO', destination.include_nfo, (value) => { destination.include_nfo = value; }, !destinationsEditable));
      grid.append(checkbox('Attach MediaInfo XML', destination.include_mediainfo, (value) => { destination.include_mediainfo = value; }, !destinationsEditable));
      grid.append(checkbox('Send includemeta=true', destination.includemeta, (value) => { destination.includemeta = value; }, !destinationsEditable));
      grid.append(checkbox('Use basic authentication', destination.basic_auth, (value) => { destination.basic_auth = value; }, !destinationsEditable));
      grid.append(label('Basic-auth username', input('text', destination.username, (value) => { destination.username = value; }, { disabled: !destinationsEditable, autocomplete: 'off' })));
      grid.append(secretField(`Basic-auth password (${destination.password_configured ? 'configured' : 'not configured'})`, secretControl(destination, 'password', 'password_configured', 'clear_password', !destinationsEditable, `${destination.name} basic-auth password`)));
      card.append(grid);
      const overrides = P.el('details'); overrides.append(P.el('summary', { text: `Category overrides (${Object.values(destination.category_overrides || {}).filter(Boolean).length})` }), categoryOverrideFields(destination, index, destinationsEditable)); card.append(overrides);
      const cached = P.el('details'); cached.append(P.el('summary', { text: `Cached categories (${destination.categories_cache.length})` }));
      if (destination.categories_cache.length) cached.append(P.el('div', { className: 'row' }, destination.categories_cache.map((item) => P.el('code', { text: `${item.id || ''} — ${item.label || ''}` })))); else cached.append(P.el('p', { className: 'muted', text: 'No cached categories yet.' }));
      card.append(cached); fragment.append(card);
    });
    destinationBox.replaceChildren(fragment); sync();
  };

  document.getElementById('addPostingProviderBtn')?.addEventListener('click', () => { providers.push(normalizeProvider({}, providers.length)); renderProviders(); markDirty(); });
  document.getElementById('addShareDestinationBtn')?.addEventListener('click', () => { destinations.push(normalizeDestination({}, destinations.length)); renderDestinations(); markDirty(); });
  document.getElementById('refreshShareCapsSettingsBtn')?.addEventListener('click', async (event) => {
    const button = event.currentTarget; const output = document.getElementById('shareCapsResult'); P.setBusy(button, true, 'Refreshing…'); output.textContent = 'Refreshing saved destination categories…';
    try { const data = await P.requestJSON('/api/share/caps/refresh', { method: 'POST' }); output.textContent = `Refresh completed for ${(data.results || []).length} destination(s).`; }
    catch (error) { output.textContent = `Refresh failed: ${error.message}`; }
    finally { P.setBusy(button, false); }
  });

  document.getElementById('loadPlexServersBtn')?.addEventListener('click', async (event) => {
    const button = event.currentTarget; const box = document.getElementById('serversBox'); P.setBusy(button, true, 'Loading…'); P.state(box, 'loading', 'Loading Plex servers');
    try {
      const data = await P.requestJSON('/api/plex/servers');
      if (!(data.servers || []).length) { P.state(box, 'empty', 'No Plex servers found'); return; }
      const fragment = document.createDocumentFragment();
      data.servers.forEach((server) => {
        const card = P.el('article', { className: 'job' }); card.append(P.el('h3', { text: server.name || 'Plex server' }));
        const connections = P.el('div', { className: 'stack' });
        (server.connections || []).forEach((connection) => {
          const use = P.button('Use this URL', { className: 'secondary small' });
          use.addEventListener('click', async () => {
            P.setBusy(use, true, 'Saving…');
            try { const result = await P.requestJSON('/api/plex/server/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ server_url: connection.uri, choice_token: connection.choice_token }) }); const fieldNode = form.querySelector('input[name="plex_url"]'); if (fieldNode) fieldNode.value = result.plex_url || connection.uri || ''; markDirty(); P.toast('Plex URL selected. Save settings to keep other changes.', 'success'); }
            catch (error) { P.toast(error.message, 'error'); P.setBusy(use, false); }
          });
          connections.append(P.el('div', { className: 'row' }, [P.el('code', { text: connection.uri || '' }), use]));
        });
        card.append(connections); fragment.append(card);
      });
      box.replaceChildren(fragment);
    } catch (error) { P.state(box, 'error', 'Could not load Plex servers', error.message); }
    finally { P.setBusy(button, false); }
  });

  document.getElementById('checkUpdatesBtn')?.addEventListener('click', async (event) => {
    const button = event.currentTarget; const output = document.getElementById('updateCheckResult'); P.setBusy(button, true, 'Checking…'); output.textContent = 'Checking for updates…';
    const data = await window.prepacCheckForUpdates();
    output.textContent = !data ? 'Update check failed.' : data.update_available ? `Current: ${data.current_version}\nLatest: ${data.latest_version}\nRelease: ${data.release_url || ''}\nAsset: ${data.asset_name || data.asset_url || ''}` : `PrepaC is up to date. Current: ${data.current_version}`;
    P.setBusy(button, false);
  });

  document.querySelectorAll('[data-secret-toggle]').forEach((button) => button.addEventListener('click', () => {
    const fieldNode = button.parentElement?.querySelector('input'); if (!fieldNode) return; const reveal = fieldNode.type === 'password'; fieldNode.type = reveal ? 'text' : 'password'; button.textContent = reveal ? 'Hide' : 'Show'; button.setAttribute('aria-pressed', String(reveal));
  }));
  document.getElementById('settingsSearch')?.addEventListener('input', (event) => {
    const query = event.currentTarget.value.toLowerCase().trim();
    document.querySelectorAll('.settings-section').forEach((section) => { section.hidden = Boolean(query && !section.textContent.toLowerCase().includes(query)); });
  });
  form.addEventListener('input', markDirty);
  form.addEventListener('change', markDirty);
  form.addEventListener('submit', sync);
  renderProviders(); renderDestinations(); sync();
})();
