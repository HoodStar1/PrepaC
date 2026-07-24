(() => {
  'use strict';
  const P = window.PrepaC;
  const summaryBox = document.getElementById('summaryBox');
  const resultBox = document.getElementById('resultBox');
  if (!P || !summaryBox || !resultBox) return;

  const diskDetails = (title, items, valueKey, verb) => {
    const details = P.el('details', { open: true });
    details.append(P.el('summary', { text: title }));
    if (!items?.length) details.append(P.el('p', { className: 'muted', text: 'No disk information.' }));
    else items.forEach((item) => details.append(P.el('div', {}, [P.el('code', { text: item.mount_path || '' }), ` — ${verb || ''}${P.formatGB(item[valueKey])}`])));
    return details;
  };

  const raw = sessionStorage.getItem('prepac_clean_result');
  if (!raw) {
    P.state(summaryBox, 'empty', 'No result in this tab', 'Run a clean action or open Clean History for saved records.');
    P.state(resultBox, 'empty', 'No per-item result', 'Results are held in this browser tab only.');
    return;
  }

  let data;
  try { data = JSON.parse(raw); }
  catch (_error) {
    P.state(summaryBox, 'error', 'Result could not be read', 'The saved browser data is invalid.');
    P.state(resultBox, 'error', 'No per-item result', 'Return to Clean and run a new review.');
    return;
  }

  const results = Array.isArray(data.results) ? data.results : [];
  const total = (field, fallback) => results.reduce((sum, item) => sum + (Number(item[field] ?? item[fallback]) || 0), 0);
  const stats = [
    ['Processed items', data.processed_count || results.length],
    ['Plex libraries refreshed', data.refreshed_count || 0],
    ['Logical size selected', P.formatGB(total('logical_size_bytes', 'size_bytes'))],
    ['Allocated before', P.formatGB(total('allocated_size_bytes'))],
    ['Actual space freed', P.formatGB(total('actual_freed_bytes'))]
  ];
  const grid = P.el('div', { className: 'stat-grid' });
  stats.forEach(([label, value]) => grid.append(P.el('div', { className: 'stat' }, [P.el('span', { className: 'stat-label', text: label }), P.el('div', { className: 'stat-value', text: value })])));
  summaryBox.replaceChildren(grid);

  if (!results.length) {
    P.state(resultBox, 'empty', 'No item results', 'The operation did not return per-item details.');
    return;
  }

  const fragment = document.createDocumentFragment();
  results.forEach((item) => {
    const card = P.el('article', { className: 'job' });
    card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: item.target_path || 'Unknown target' }), P.badge(item.success ? 'done' : 'failed')]));
    const meta = P.el('div', { className: 'job-meta' });
    [['Media type', item.media_type || ''], ['Reason', item.reason || ''], ['Logical size', P.formatGB(item.logical_size_bytes || item.size_bytes)], ['Allocated before', P.formatGB(item.allocated_size_bytes)], ['Actually freed', P.formatGB(item.actual_freed_bytes)]].forEach(([label, value]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), value])));
    card.append(meta, P.paragraph('Message', item.message || ''));
    card.append(diskDetails('Disk free space before', item.disk_free_before, 'free_bytes', ''), diskDetails('Disk free space after', item.disk_free_after, 'free_bytes', ''), diskDetails('Disk free space delta', item.disk_free_delta, 'freed_bytes', 'Freed '), P.jsonDetails('Full result data', item));
    fragment.append(card);
  });
  resultBox.replaceChildren(fragment);
})();
