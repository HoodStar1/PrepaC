(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;

  const resultsBox = document.getElementById('resultsBox');
  const resultCount = document.getElementById('cleanResultCount');
  const statusNode = document.getElementById('cleanStatus');
  const scanButton = document.getElementById('cleanScanBtn');
  const processButton = document.getElementById('deleteBtn');
  const selectAll = document.getElementById('selectAllClean');
  const confirmInput = document.getElementById('confirm');
  let candidates = [];
  let candidateByKey = new Map();
  let previewScope = null;
  const selectedKeys = new Set();

  const candidateKey = (item) => String(item.candidate_id || '');

  const filteredCandidates = () => {
    const query = document.getElementById('search').value.toLowerCase().trim();
    const sort = document.getElementById('sortBy').value;
    const list = candidates.filter((item) => !query || String(item.title || '').toLowerCase().includes(query) || String(item.target_path || '').toLowerCase().includes(query));
    if (sort === 'size_desc') list.sort((a, b) => (Number(b.size_bytes) || 0) - (Number(a.size_bytes) || 0));
    else if (sort === 'size_asc') list.sort((a, b) => (Number(a.size_bytes) || 0) - (Number(b.size_bytes) || 0));
    else list.sort((a, b) => String(a.title || '').localeCompare(String(b.title || '')));
    return list;
  };

  const syncControls = () => {
    const visible = filteredCandidates().map(candidateKey);
    selectAll.checked = visible.length > 0 && visible.every((key) => selectedKeys.has(key));
    selectAll.indeterminate = visible.some((key) => selectedKeys.has(key)) && !selectAll.checked;
    processButton.disabled = selectedKeys.size === 0 || confirmInput.value !== 'DELETE';
    resultCount.textContent = candidates.length ? `${visible.length} of ${candidates.length} candidates shown · ${selectedKeys.size} selected` : 'No candidates found.';
  };

  const updateSeasonContext = () => {
    const groups = new Map();
    resultsBox.querySelectorAll('[data-candidate-key]').forEach((card) => {
      const item = candidateByKey.get(card.dataset.candidateKey);
      if (!item) return;
      const details = item.details || {};
      if (item.media_type !== 'tv' || !details.season_parent_show_path || !Array.isArray(details.season_folder_names_in_show)) return;
      if (!groups.has(details.season_parent_show_path)) groups.set(details.season_parent_show_path, { all: new Set(details.season_folder_names_in_show), cards: [] });
      groups.get(details.season_parent_show_path).cards.push({ card, item, selected: selectedKeys.has(candidateKey(item)) });
    });
    resultsBox.querySelectorAll('.clean-extra-note').forEach((note) => note.replaceChildren());
    groups.forEach((group) => {
      const names = new Set(group.cards.filter((entry) => entry.selected).map((entry) => String(entry.item.target_path || '').split(/[\\/]/).pop()));
      const allSelected = group.all.size && Array.from(group.all).every((name) => names.has(name));
      if (!allSelected) return;
      group.cards.filter((entry) => entry.selected).forEach((entry) => {
        entry.card.querySelector('.clean-extra-note')?.append(P.el('strong', { text: 'All seasons selected. ' }), 'The parent show folder will also be processed.');
      });
    });
  };

  const candidateCard = (item) => {
    const key = candidateKey(item);
    const card = P.el('article', { className: 'job', dataset: { candidateKey: key } });
    const checkbox = P.el('input', { type: 'checkbox', className: 'cand', checked: selectedKeys.has(key), 'aria-label': `Select ${item.title || item.target_path || 'candidate'}` });
    checkbox.addEventListener('change', () => {
      if (checkbox.checked) selectedKeys.add(key); else selectedKeys.delete(key);
      updateSeasonContext();
      syncControls();
    });
    card.append(P.el('label', { className: 'checkbox' }, [checkbox, P.el('span', { text: 'Select candidate' })]));
    card.append(P.safeImage(item.poster_url, item.title || '', 'clean-poster'));
    card.append(P.el('h3', { text: item.title || 'Untitled candidate' }));
    card.append(P.paragraph('Type', item.media_type || 'unknown'));
    card.append(P.paragraph('Reason', item.reason || ''));
    card.append(P.paragraph('Target kind', item.target_kind || ''));
    card.append(P.paragraph('Target path', item.target_path || '', true));
    card.append(P.paragraph('Estimated logical size', P.formatMB(item.size_bytes)));
    const note = P.el('p', { className: 'clean-extra-note' });
    if (item.details?.will_also_remove_show_folder) note.append(P.el('strong', { text: 'Additional action. ' }), 'The parent show folder will also be processed.');
    card.append(note, P.jsonDetails('Storage breakdown', item.breakdown || []), P.jsonDetails('Candidate details', item.details || {}));
    return card;
  };

  const render = () => {
    const visible = filteredCandidates();
    if (!visible.length) {
      P.state(resultsBox, 'empty', candidates.length ? 'No matching candidates' : 'No candidates found', candidates.length ? 'Adjust the result filter to see more items.' : 'Nothing matched the current scan.');
      syncControls();
      return;
    }
    const fragment = document.createDocumentFragment();
    visible.forEach((item) => fragment.append(candidateCard(item)));
    resultsBox.replaceChildren(fragment);
    updateSeasonContext();
    syncControls();
  };

  const runScan = async () => {
    P.setBusy(scanButton, true, 'Scanning…');
    P.state(resultsBox, 'loading', 'Scanning media', 'This can take a moment for large libraries.');
    statusNode.textContent = '';
    try {
      const reason = document.getElementById('reason').value;
      const type = document.getElementById('type').value;
      const data = await P.requestJSON(`/api/clean/preview?reason=${encodeURIComponent(reason)}&type=${encodeURIComponent(type)}`);
      candidates = Array.isArray(data.results) ? data.results : [];
      candidateByKey = new Map(candidates.map((item) => [candidateKey(item), item]));
      previewScope = { reason, type };
      selectedKeys.clear();
      render();
    } catch (error) {
      candidates = [];
      candidateByKey.clear();
      previewScope = null;
      selectedKeys.clear();
      P.state(resultsBox, 'error', 'Scan failed', error.message);
      resultCount.textContent = 'The scan could not be completed.';
    } finally {
      P.setBusy(scanButton, false);
      syncControls();
    }
  };

  const processSelected = async () => {
    const candidateIds = Array.from(selectedKeys).filter((key) => candidateByKey.has(key));
    if (!candidateIds.length || !previewScope || confirmInput.value !== 'DELETE') return;
    const dryRun = document.getElementById('dryRun').checked;
    const useRecycleBin = document.getElementById('useRecycleBin').checked;
    P.setBusy(processButton, true, dryRun ? 'Running preview…' : 'Processing…');
    statusNode.textContent = dryRun ? 'Dry run enabled: no files will be changed.' : 'Processing the reviewed cleanup request…';
    try {
      const data = await P.requestJSON('/api/clean/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          confirmation: confirmInput.value,
          candidate_ids: candidateIds,
          filter_reason: previewScope.reason,
          filter_type: previewScope.type,
          dry_run: dryRun,
          use_recycle_bin: useRecycleBin
        })
      });
      const refreshed = data.plex_refresh?.refreshed?.length || 0;
      sessionStorage.setItem('prepac_clean_result', JSON.stringify({ processed_count: (data.results || []).length, refreshed_count: refreshed, results: data.results || [] }));
      window.location.href = '/clean/result';
    } catch (error) {
      statusNode.textContent = `Clean failed: ${error.message}`;
      P.toast(`Clean failed: ${error.message}`, 'error');
    } finally {
      P.setBusy(processButton, false);
      syncControls();
    }
  };

  scanButton.addEventListener('click', runScan);
  processButton.addEventListener('click', processSelected);
  document.getElementById('search').addEventListener('input', render);
  document.getElementById('sortBy').addEventListener('change', render);
  confirmInput.addEventListener('input', syncControls);
  selectAll.addEventListener('change', () => {
    filteredCandidates().forEach((item) => {
      const key = candidateKey(item);
      if (selectAll.checked) selectedKeys.add(key); else selectedKeys.delete(key);
    });
    render();
  });
  syncControls();
})();
