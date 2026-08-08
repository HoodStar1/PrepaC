(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;
  const selectedShows = new Map();
  const selectedMovies = new Map();
  let activeShow = null;
  let queueItems = [];
  let previewData = [];
  let queueBusy = false;
  const queueSubmissionErrors = new Map();

  const queueBox = document.getElementById('queueBox');
  const jobsBox = document.getElementById('prepareJobs');
  const previewButton = document.getElementById('previewBtn');
  const clearButton = document.getElementById('clearQueueBtn');
  const startButton = document.getElementById('startBtn');
  const previewStatus = document.getElementById('previewStatus');

  const setTab = (tab, focusQuery = true) => {
    const tv = tab === 'tv';
    const tvButton = document.getElementById('tabTvBtn');
    const movieButton = document.getElementById('tabMovieBtn');
    document.getElementById('tab-tv').hidden = !tv;
    document.getElementById('tab-movie').hidden = tv;
    tvButton.setAttribute('aria-selected', String(tv));
    movieButton.setAttribute('aria-selected', String(!tv));
    tvButton.tabIndex = tv ? 0 : -1;
    movieButton.tabIndex = tv ? -1 : 0;
    if (focusQuery) document.getElementById(tv ? 'tvQuery' : 'movieQuery').focus();
  };

  const updateSummaries = () => {
    const showNames = Array.from(selectedShows.entries()).filter(([, data]) => data.seasons.size).map(([name]) => name);
    const seasonCount = Array.from(selectedShows.values()).reduce((sum, data) => sum + data.seasons.size, 0);
    const movieNames = Array.from(selectedMovies.keys());
    document.getElementById('selectedShowsLabel').textContent = showNames.length ? showNames.join(', ') : 'None';
    document.getElementById('selectedSeasonsLabel').textContent = seasonCount ? `${seasonCount} selected` : 'None';
    document.getElementById('selectedMoviesLabel').textContent = movieNames.length ? movieNames.join(', ') : 'None';
  };

  const syncQueueButtons = () => {
    const previewReady = queueItems.length > 0
      && previewData.length === queueItems.length
      && previewData.every((item, index) => (
        queueItems[index]
        && item
        && item.ok !== false
        && typeof item.preview_token === 'string'
        && item.preview_token.length > 0
      ));
    previewButton.disabled = queueBusy || queueItems.length === 0;
    clearButton.disabled = queueBusy || queueItems.length === 0;
    startButton.disabled = queueBusy || !previewReady;
    document.getElementById('bracketOverride').disabled = queueBusy;
    document.getElementById('queueCount').textContent = `${queueItems.length} item${queueItems.length === 1 ? '' : 's'}`;
  };

  const queueCard = (item, index) => {
    const preview = previewData[index];
    const card = P.el('article', { className: 'job queue-job' });
    card.append(P.el('div', { className: 'queue-media' }, P.safeImage(item.poster_url, item.label || '', 'queue-poster small')));
    const content = P.el('div', { className: 'queue-content' });
    const remove = P.button('Remove', { className: 'mini-remove', disabled: queueBusy });
    remove.addEventListener('click', () => removeQueueItem(index));
    content.append(P.el('div', { className: 'queue-header-row' }, [P.el('strong', { text: item.label || 'Queue item' }), remove]));
    const queueError = queueSubmissionErrors.get(item);
    if (queueError) content.append(P.el('div', { className: 'flash error', text: queueError }));
    if (!preview) content.append(P.el('p', { className: 'muted', text: 'Preview not built yet.' }));
    else if (preview.ok === false) content.append(P.el('div', { className: 'flash error', text: preview.error || 'Preview failed.' }));
    else {
      const important = P.el('div', { className: 'queue-important' });
      [['Chosen bracket', preview.chosen_bracket || ''], ['Destination folder', preview.dest_folder || ''], ['Destination path', preview.dest_path || ''], ['Detected tags', JSON.stringify(preview.detected_tags?.detected_tags || [])]].forEach(([label, value]) => important.append(P.el('div', {}, [P.el('strong', { text: label }), P.el('code', { text: value })])));
      const visiblePreview = { ...preview };
      delete visiblePreview.preview_token;
      content.append(important, P.jsonDetails('Full preview', visiblePreview));
    }
    card.append(content);
    return card;
  };

  const renderQueue = () => {
    if (!queueItems.length) P.state(queueBox, 'empty', 'Queue is empty', 'Select one or more TV seasons or movies.');
    else {
      const fragment = document.createDocumentFragment();
      queueItems.forEach((item, index) => fragment.append(queueCard(item, index)));
      queueBox.replaceChildren(fragment);
    }
    syncQueueButtons();
  };

  function removeQueueItem(index) {
    if (queueBusy) return;
    const removed = queueItems[index];
    queueSubmissionErrors.delete(removed);
    queueItems.splice(index, 1);
    previewData = [];
    if (removed?.kind === 'tv') {
      selectedShows.get(removed.show_name)?.seasons.delete(removed.season_name);
      if (activeShow === removed.show_name) renderSeasons(activeShow);
    } else if (removed?.kind === 'movie') selectedMovies.delete(removed.movie_name);
    updateSummaries();
    renderMovieSelectionStates();
    renderShowSelectionStates();
    renderQueue();
    previewStatus.textContent = 'Queue changed. Build a new preview before starting.';
  }

  const clearQueue = () => {
    if (queueBusy) return;
    queueItems = [];
    previewData = [];
    queueSubmissionErrors.clear();
    selectedShows.forEach((data) => data.seasons.clear());
    selectedMovies.clear();
    renderSeasons(activeShow);
    renderMovieSelectionStates();
    renderShowSelectionStates();
    updateSummaries();
    renderQueue();
    previewStatus.textContent = 'Idle.';
  };

  const renderShowSelectionStates = () => {
    document.querySelectorAll('#tvShows [data-show-name]').forEach((button) => {
      const data = selectedShows.get(button.dataset.showName);
      button.classList.toggle('selected-item', Boolean(data?.seasons.size));
      button.classList.toggle('active-item', activeShow === button.dataset.showName);
    });
  };

  const renderMovieSelectionStates = () => {
    document.querySelectorAll('#movieResults [data-movie-name]').forEach((button) => button.classList.toggle('selected-item', selectedMovies.has(button.dataset.movieName)));
  };

  const renderSeasons = (showName) => {
    const list = document.getElementById('tvSeasons');
    document.getElementById('activeShowLabel').textContent = showName || 'no selected show';
    const data = selectedShows.get(showName);
    if (!showName || !data) { list.replaceChildren(P.el('li', { className: 'empty-state', text: 'Choose a show' })); return; }
    if (!data.loadedSeasons.length) { list.replaceChildren(P.el('li', { className: 'empty-state', text: 'No seasons found' })); return; }
    const fragment = document.createDocumentFragment();
    data.loadedSeasons.forEach((season) => {
      const button = P.button(season, { className: `linkbtn ${data.seasons.has(season) ? 'selected-item' : ''}` });
      button.setAttribute('aria-pressed', String(data.seasons.has(season)));
      button.addEventListener('click', () => {
        if (queueBusy) return;
        const selected = data.seasons.has(season);
        if (selected) {
          data.seasons.delete(season);
          queueItems = queueItems.filter((item) => !(item.kind === 'tv' && item.show_name === showName && item.season_name === season));
        } else {
          data.seasons.add(season);
          if (!queueItems.some((item) => item.kind === 'tv' && item.show_name === showName && item.season_name === season)) queueItems.push({ kind: 'tv', show_name: showName, season_name: season, label: `${showName} / ${season}`, poster_url: data.poster_url || '' });
        }
        previewData = [];
        renderSeasons(showName);
        renderShowSelectionStates();
        updateSummaries();
        renderQueue();
        previewStatus.textContent = 'Queue changed. Build a preview before starting.';
      });
      fragment.append(P.el('li', {}, button));
    });
    list.replaceChildren(fragment);
  };

  const loadSeasons = async (showName) => {
    const status = document.getElementById('tvStatus');
    status.textContent = 'Loading seasons…';
    try {
      const data = await P.requestJSON(`/api/prepare/tv/seasons?show=${encodeURIComponent(showName)}`);
      const show = selectedShows.get(showName);
      if (show) show.loadedSeasons = data.results || [];
      status.textContent = show?.loadedSeasons.length ? 'Choose one or more seasons.' : 'No seasons found.';
      renderSeasons(showName);
    } catch (error) { status.textContent = `Could not load seasons: ${error.message}`; }
  };

  const searchTV = async () => {
    const query = document.getElementById('tvQuery').value;
    const status = document.getElementById('tvStatus');
    const list = document.getElementById('tvShows');
    P.state(list, 'loading', 'Searching TV folders');
    status.textContent = 'Searching…';
    try {
      const results = (await P.requestJSON(`/api/prepare/tv/search?q=${encodeURIComponent(query)}`)).results || [];
      if (!results.length) { P.state(list, 'empty', 'No matching shows'); status.textContent = 'No results found.'; return; }
      const fragment = document.createDocumentFragment();
      results.forEach((item) => {
        const card = P.el('article', { className: 'poster-card' });
        const choose = P.button(item.name || 'Untitled show', { className: 'linkbtn' });
        choose.dataset.showName = item.name || '';
        choose.addEventListener('click', async () => {
          if (!selectedShows.has(item.name)) selectedShows.set(item.name, { poster_url: item.poster_url || '', seasons: new Set(), loadedSeasons: [] });
          activeShow = item.name;
          renderShowSelectionStates();
          const show = selectedShows.get(item.name);
          if (!show.loadedSeasons.length) await loadSeasons(item.name); else renderSeasons(item.name);
        });
        card.append(P.safeImage(item.poster_url, item.name || '', ''), choose);
        fragment.append(card);
      });
      list.replaceChildren(fragment);
      status.textContent = `${results.length} result${results.length === 1 ? '' : 's'}. Choose a show, then select seasons.`;
      renderShowSelectionStates();
    } catch (error) { P.state(list, 'error', 'TV search failed', error.message); status.textContent = 'Search failed.'; }
  };

  const searchMovies = async () => {
    const query = document.getElementById('movieQuery').value;
    const status = document.getElementById('movieStatus');
    const list = document.getElementById('movieResults');
    P.state(list, 'loading', 'Searching movie folders');
    status.textContent = 'Searching…';
    try {
      const results = (await P.requestJSON(`/api/prepare/movie/search?q=${encodeURIComponent(query)}`)).results || [];
      if (!results.length) { P.state(list, 'empty', 'No matching movies'); status.textContent = 'No results found.'; return; }
      const fragment = document.createDocumentFragment();
      results.forEach((item) => {
        const card = P.el('article', { className: 'poster-card' });
        const choose = P.button(item.name || 'Untitled movie', { className: 'linkbtn' });
        choose.dataset.movieName = item.name || '';
        choose.setAttribute('aria-pressed', String(selectedMovies.has(item.name)));
        choose.addEventListener('click', () => {
          if (queueBusy) return;
          if (selectedMovies.has(item.name)) {
            selectedMovies.delete(item.name);
            queueItems = queueItems.filter((entry) => !(entry.kind === 'movie' && entry.movie_name === item.name));
          } else {
            selectedMovies.set(item.name, { poster_url: item.poster_url || '' });
            queueItems.push({ kind: 'movie', movie_name: item.name, label: item.name, poster_url: item.poster_url || '' });
          }
          previewData = [];
          choose.setAttribute('aria-pressed', String(selectedMovies.has(item.name)));
          renderMovieSelectionStates();
          updateSummaries();
          renderQueue();
          previewStatus.textContent = 'Queue changed. Build a preview before starting.';
        });
        card.append(P.safeImage(item.poster_url, item.name || '', ''), choose);
        fragment.append(card);
      });
      list.replaceChildren(fragment);
      status.textContent = `${results.length} result${results.length === 1 ? '' : 's'}. Choose one or more movies.`;
      renderMovieSelectionStates();
    } catch (error) { P.state(list, 'error', 'Movie search failed', error.message); status.textContent = 'Search failed.'; }
  };

  const buildPreview = async () => {
    if (queueBusy || !queueItems.length) return;
    const submittedItems = queueItems.slice();
    queueBusy = true;
    queueSubmissionErrors.clear();
    P.setBusy(previewButton, true, 'Building…');
    previewData = [];
    renderQueue();
    const bracket = document.getElementById('bracketOverride').value;
    try {
      for (const [index, item] of submittedItems.entries()) {
        previewStatus.textContent = `Building preview ${index + 1} of ${submittedItems.length}…`;
        const movie = item.kind === 'movie';
        try {
          const payload = movie ? { movie_name: item.movie_name, bracket_override: bracket } : { show_name: item.show_name, season_name: item.season_name, bracket_override: bracket };
          const preview = await P.requestJSON(`/api/prepare/${movie ? 'movie' : 'tv'}/preview`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
          if (!preview?.preview_token) throw new Error('The server did not return a start token. Rebuild after updating PrepaC.');
          previewData.push(preview);
        } catch (error) { previewData.push({ ok: false, error: error.message }); }
        renderQueue();
      }
      const failures = previewData.filter((item) => item?.ok === false).length;
      previewStatus.textContent = failures ? `${previewData.length - failures} previews ready; ${failures} failed.` : `${previewData.length} previews ready to start.`;
    } finally {
      queueBusy = false;
      P.setBusy(previewButton, false);
      renderQueue();
    }
  };

  const startJobs = async () => {
    if (
      queueBusy
      || !queueItems.length
      || previewData.length !== queueItems.length
      || previewData.some((item) => !item?.preview_token || item.ok === false)
    ) {
      previewStatus.textContent = 'The queue changed or its preview is incomplete. Build a new preview before starting.';
      syncQueueButtons();
      return;
    }
    const submittedItems = queueItems.slice();
    const submittedPreviews = previewData.slice();
    queueBusy = true;
    queueSubmissionErrors.clear();
    P.setBusy(startButton, true, 'Queueing…');
    renderQueue();
    let started = 0, skipped = 0;
    const errors = [];
    const completedItems = new Set();
    const submissions = submittedPreviews.map((preview, index) => ({
      kind: submittedItems[index].kind,
      preview_token: preview.preview_token
    }));
    previewStatus.textContent = `Queueing ${submissions.length} item${submissions.length === 1 ? '' : 's'}…`;
    try {
      const data = await P.requestJSON('/api/prepare/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: submissions })
      });
      const handledIndexes = new Set();
      (Array.isArray(data.results) ? data.results : []).forEach((result) => {
        const index = Number(result?.index);
        if (!Number.isInteger(index) || index < 0 || index >= submittedItems.length || handledIndexes.has(index)) return;
        handledIndexes.add(index);
        const item = submittedItems[index];
        if (result.ok) {
          if (result.duplicate) skipped += 1; else started += 1;
          completedItems.add(item);
        } else {
          const message = result.error || 'Could not queue item';
          queueSubmissionErrors.set(item, message);
          errors.push(`${item.label || 'Item'}: ${message}`);
        }
      });
      submissions.forEach((_item, index) => {
        if (handledIndexes.has(index)) return;
        const item = submittedItems[index];
        const message = 'No queue result was returned. Retry after checking Active prepare jobs.';
        queueSubmissionErrors.set(item, message);
        errors.push(`${item.label || 'Item'}: ${message}`);
      });
    } catch (error) {
      const message = `Queue request failed: ${error.message}`;
      submittedItems.forEach((item) => {
        if (!completedItems.has(item)) queueSubmissionErrors.set(item, message);
      });
      errors.push(message);
    }
    completedItems.forEach((item) => {
      queueSubmissionErrors.delete(item);
      if (item?.kind === 'tv') selectedShows.get(item.show_name)?.seasons.delete(item.season_name);
      else if (item?.kind === 'movie') selectedMovies.delete(item.movie_name);
    });
    const previewByItem = new Map(submittedItems.map((item, index) => [item, submittedPreviews[index]]));
    queueItems = queueItems.filter((item) => !completedItems.has(item));
    previewData = queueItems.map((item) => previewByItem.get(item));
    queueBusy = false;
    P.setBusy(startButton, false);
    renderSeasons(activeShow);
    renderMovieSelectionStates();
    renderShowSelectionStates();
    updateSummaries();
    renderQueue();
    previewStatus.textContent = `${started} queued${skipped ? `, ${skipped} already active` : ''}${errors.length ? `, ${errors.length} failed. Review the highlighted items` : ''}.`;
    await refreshJobs();
  };

  const renderJobs = (data) => {
    const jobs = data.jobs || [];
    if (!jobs.length) { P.state(jobsBox, 'empty', 'No active jobs', 'New Prepare work will appear here.'); return; }
    const fragment = document.createDocumentFragment();
    jobs.forEach((job) => {
      const status = String(job.status || '').toLowerCase();
      const card = P.el('article', { className: `job ${status === 'outcome_unknown' ? 'job-danger' : ''}` });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: `#${job.id} — ${job.media_type || 'prepare'}` }), P.badge(job.status)]));
      if (job.percent != null) card.append(P.progress(job.percent, `Prepare job ${job.id} progress`));
      const meta = P.el('div', { className: 'job-meta' });
      [['Source', job.source_path || '', true], ['Destination', job.dest_path || '', true], ['Phase', job.phase || ''], ['Progress', job.percent == null ? '—' : `${job.percent}%`]].forEach(([label, value, code]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value }) : value])));
      card.append(meta, P.paragraph('Message', job.message || ''));
      if (['queued', 'running'].includes(status)) {
        const stop = P.button('Stop job', { className: 'danger small' });
        stop.addEventListener('click', async () => {
          P.setBusy(stop, true, 'Stopping…');
          try { await P.requestJSON('/api/prepare/cancel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id: job.id }) }); await refreshJobs(); }
          catch (error) { P.toast(error.message, 'error'); P.setBusy(stop, false); }
        });
        card.append(P.el('div', { className: 'actions' }, stop));
      }
      if (status === 'outcome_unknown') {
        card.append(P.outcomeUnknownRecovery({
          workflow: 'Prepare',
          jobId: job.id,
          endpoint: '/api/prepare/outcome-unknown/acknowledge',
          warning: 'The destination tree may already contain a complete or partial copy. Verify the expected media and reconcile or remove ambiguous output before allowing a new Prepare submission.',
          afterAcknowledge: refreshJobs
        }));
      }
      fragment.append(card);
    });
    jobsBox.replaceChildren(fragment);
  };

  const refreshJobs = async () => {
    try {
      const data = await P.requestJSON('/api/jobs');
      if (data.ok === false) throw new Error(data.error || 'Prepare jobs could not be listed');
      renderJobs(data);
    } catch (error) {
      P.state(jobsBox, 'error', 'Could not load Prepare jobs', error.message);
    }
  };
  document.getElementById('tabTvBtn').addEventListener('click', () => setTab('tv'));
  document.getElementById('tabMovieBtn').addEventListener('click', () => setTab('movie'));
  document.querySelector('.tabbar').addEventListener('keydown', (event) => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const tabs = [document.getElementById('tabTvBtn'), document.getElementById('tabMovieBtn')];
    const current = Math.max(0, tabs.indexOf(document.activeElement));
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? 1 : (current + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    setTab(next === 1 ? 'movie' : 'tv', false);
    tabs[next].focus();
  });
  document.getElementById('tvSearchForm').addEventListener('submit', (event) => { event.preventDefault(); searchTV(); });
  document.getElementById('movieSearchForm').addEventListener('submit', (event) => { event.preventDefault(); searchMovies(); });
  previewButton.addEventListener('click', buildPreview);
  clearButton.addEventListener('click', clearQueue);
  startButton.addEventListener('click', startJobs);
  updateSummaries(); renderQueue(); refreshJobs();
  let stream;
  const startStream = () => {
    stream?.close(); stream = new EventSource('/api/jobs/stream');
    stream.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data || '{}');
        if (data.ok === false) return;
        renderJobs(data);
      } catch (_error) {}
    };
    if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
  };
  document.addEventListener('visibilitychange', () => { if (document.hidden) stream?.close(); else startStream(); });
  startStream();
})();
