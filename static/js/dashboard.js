(() => {
  'use strict';
  const P = window.PrepaC;
  const box = document.getElementById('runningOverview');
  if (!P || !box) return;
  const statValues = Array.from(document.querySelectorAll('[data-dashboard-stat]'));
  const statsStatus = document.getElementById('dashboardStatsStatus');
  const statsRefreshIntervalMs = 15000;
  const statsRequestTimeoutMs = 8000;

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
  let statsTimer = null;
  let statsRefreshInFlight = false;
  let statsAbortController = null;
  let statsRequestGeneration = 0;

  const setStatsStatus = (isCurrent, message) => {
    if (!statsStatus) return;
    statsStatus.textContent = message;
    statsStatus.classList.toggle('success', isCurrent);
    statsStatus.classList.toggle('warning', !isCurrent);
  };

  const markStatsStale = () => {
    statValues.forEach((node) => {
      node.dataset.dashboardStale = 'true';
      node.title = 'The latest dashboard statistics could not be loaded.';
    });
    setStatsStatus(false, 'Totals refresh failed');
  };

  const renderStats = (data) => {
    const display = data?.display;
    if (!display || typeof display !== 'object') throw new Error('Dashboard totals are missing.');
    const missingKeys = statValues
      .map((node) => node.dataset.dashboardStat || '')
      .filter((key) => !Object.prototype.hasOwnProperty.call(display, key));
    if (missingKeys.length) throw new Error('Dashboard totals are incomplete.');
    statValues.forEach((node) => {
      const key = node.dataset.dashboardStat || '';
      node.textContent = String(display[key]);
      node.removeAttribute('data-dashboard-stale');
      node.removeAttribute('title');
    });
    setStatsStatus(true, 'Totals current');
  };

  const refreshStats = async () => {
    if (statsRefreshInFlight || document.hidden) return;
    statsRefreshInFlight = true;
    const generation = ++statsRequestGeneration;
    const controller = new AbortController();
    statsAbortController = controller;
    const timeoutId = window.setTimeout(() => controller.abort(), statsRequestTimeoutMs);
    try {
      const data = await P.requestJSON('/api/dashboard/stats', {
        cache: 'no-store',
        signal: controller.signal,
      });
      if (generation !== statsRequestGeneration) return;
      renderStats(data);
    } catch (_error) {
      if (generation !== statsRequestGeneration) return;
      markStatsStale();
    } finally {
      window.clearTimeout(timeoutId);
      if (generation === statsRequestGeneration) {
        statsAbortController = null;
        statsRefreshInFlight = false;
      }
    }
  };

  const start = () => {
    stream?.close();
    stream = new EventSource('/api/dashboard/running/stream');
    stream.onmessage = (event) => {
      try { render((JSON.parse(event.data || '{}').running || [])); }
      catch (_error) { P.state(box, 'error', 'Live update failed', 'The next update will retry automatically.'); }
    };
    if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
  };

  const startStats = () => {
    if (statsTimer != null) window.clearInterval(statsTimer);
    void refreshStats();
    statsTimer = window.setInterval(() => { void refreshStats(); }, statsRefreshIntervalMs);
  };

  const stopStats = () => {
    if (statsTimer != null) window.clearInterval(statsTimer);
    statsTimer = null;
    statsRequestGeneration += 1;
    statsAbortController?.abort();
    statsAbortController = null;
    statsRefreshInFlight = false;
  };

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      stream?.close();
      stopStats();
    } else {
      start();
      startStats();
    }
  });
  window.addEventListener('pagehide', () => {
    stream?.close();
    stopStats();
  });
  window.addEventListener('pageshow', (event) => {
    if (!event.persisted) return;
    start();
    startStats();
  });
  start();
  startStats();
})();
