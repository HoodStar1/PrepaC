(() => {
  'use strict';
  const P = window.PrepaC;
  const box = document.getElementById('runningOverview');
  if (!P || !box) return;

  const render = (items) => {
    if (!items.length) {
      P.state(box, 'empty', 'No active jobs', 'Queued and running work will appear here.');
      return;
    }
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
      const card = P.el('article', { className: 'job' });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: item.kind || 'Workflow job' }), P.badge(item.status || 'running')]));
      const meta = P.el('div', { className: 'job-meta' });
      const values = [['Item', item.title || ''], ['Phase', item.phase || ''], ['Progress', item.percent == null ? '—' : `${item.percent}%`]];
      values.forEach(([label, value], index) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), index === 0 ? P.el('code', { text: value }) : value])));
      card.append(meta);
      if (item.percent != null) card.append(P.progress(item.percent, `${item.kind || 'Job'} progress`));
      card.append(P.el('p', { className: 'muted', text: item.message || '' }));
      fragment.append(card);
    });
    box.replaceChildren(fragment);
  };

  let stream = null;
  const start = () => {
    stream?.close();
    stream = new EventSource('/api/dashboard/running/stream');
    stream.onmessage = (event) => {
      try { render((JSON.parse(event.data || '{}').running || [])); }
      catch (_error) { P.state(box, 'error', 'Live update failed', 'The next update will retry automatically.'); }
    };
    if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
  };

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) stream?.close();
    else start();
  });
  start();
})();
