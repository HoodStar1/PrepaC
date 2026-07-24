(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;
  const candidatesBox = document.getElementById('shareCandidates');
  const destinationsBox = document.getElementById('shareDestinations');
  const filter = document.getElementById('shareDestinationFilter');
  const summaryBox = document.getElementById('shareSummaryBar');
  const jobsBox = document.getElementById('shareJobs');
  const reviewModal = document.getElementById('shareReviewModal');
  const reviewSummary = document.getElementById('shareReviewSummary');
  const reviewList = document.getElementById('shareReviewList');
  const reviewButton = document.getElementById('openShareReviewBtn');
  const selectedCount = document.getElementById('shareSelectedCount');
  let categoryOptions = P.parseJSONData('shareCategoryOptionsData', []);
  let candidates = [];
  let destinations = [];
  const selectedCandidates = new Set();
  const selectedDestinations = new Set();
  let pendingPayload = null;
  let reviewTimer = null;
  let reviewController = null;
  let reviewSequence = 0;

  const field = (label, value, code = false) => P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value ?? '' }) : String(value ?? '')]);
  const categoryFor = (candidateId) => document.querySelector(`.share-category[data-candidate-id="${CSS.escape(String(candidateId))}"]`)?.value;
  const candidateById = (id) => candidates.find((item) => String(item.candidate_id) === String(id));

  const setNotice = (kind, message) => {
    const box = document.getElementById('shareActionNotice');
    box.replaceChildren(message ? P.el('div', { className: `flash ${kind || 'info'}`, role: kind === 'error' ? 'alert' : 'status', text: message }) : document.createTextNode(''));
  };

  const resolveCategory = (destination, key) => {
    const standard = { movie_sd: ['2030', 'Movies SD'], movie_hd: ['2040', 'Movies HD'], movie_uhd: ['2045', 'Movies UHD'], tv_sd: ['5030', 'TV SD'], tv_hd: ['5040', 'TV HD'], tv_uhd: ['5045', 'TV UHD'] };
    const [standardId, standardLabel] = standard[key] || ['', key || 'Unknown'];
    const cache = Array.isArray(destination.categories_cache) ? destination.categories_cache : [];
    const overrideId = String(destination.category_overrides?.[key] || '').trim();
    if (overrideId) {
      const hit = cache.find((item) => String(item.id) === overrideId);
      return { id: overrideId, label: hit?.label || standardLabel, source: 'override' };
    }
    const exact = cache.find((item) => String(item.id) === standardId);
    if (exact) return { id: standardId, label: exact.label || standardLabel, source: 'capabilities' };
    const fuzzy = cache.find((item) => {
      const label = String(item.label || '').toLowerCase();
      if (key.endsWith('uhd')) return label.includes('uhd') || label.includes('2160');
      if (key.startsWith('tv')) return label.includes('tv') || label.includes('series');
      if (key.startsWith('movie')) return label.includes('movie') || label.includes('film');
      return key.endsWith('hd') && (label.includes('hd') || label.includes('1080') || label.includes('720'));
    });
    return fuzzy ? { id: String(fuzzy.id || ''), label: fuzzy.label || standardLabel, source: 'capabilities' } : { id: standardId, label: standardLabel, source: 'standard' };
  };

  const destinationCheckbox = (destination, index) => {
    const id = String(destination.id || '');
    const checkbox = P.el('input', { type: 'checkbox', value: id, checked: selectedDestinations.has(id), 'aria-label': `Use ${destination.name || id}` });
    checkbox.addEventListener('change', () => {
      if (checkbox.checked) selectedDestinations.add(id); else selectedDestinations.delete(id);
      renderResolvedPreviews(); applyFilters(); scheduleReview();
    });
    return P.el('label', { className: 'checkbox' }, [checkbox, P.el('span', { text: `${destination.name || id} (${destination.mode || 'manual'})` })]);
  };

  const renderDestinations = () => {
    const previous = new Set(selectedDestinations);
    if (!destinations.length) {
      selectedDestinations.clear();
      P.state(destinationsBox, 'empty', 'No destinations configured', 'Add one in Settings before sharing.');
      filter.replaceChildren(P.el('option', { value: '', text: 'No additional destination filter' }));
      return;
    }
    selectedDestinations.clear();
    destinations.forEach((destination, index) => {
      const id = String(destination.id || '');
      if (previous.has(id) || (!previous.size && index === 0)) selectedDestinations.add(id);
    });
    const fragment = document.createDocumentFragment();
    destinations.forEach((destination, index) => fragment.append(destinationCheckbox(destination, index)));
    destinationsBox.replaceChildren(fragment);
    const currentFilter = filter.value;
    const options = [P.el('option', { value: '', text: 'No additional destination filter' })];
    destinations.forEach((destination) => options.push(P.el('option', { value: destination.id || '', text: destination.name || destination.id || 'Destination' })));
    filter.replaceChildren(...options);
    if (destinations.some((destination) => String(destination.id) === currentFilter)) filter.value = currentFilter;
  };

  const categorySelect = (item) => {
    const select = P.el('select', { className: 'share-category', dataset: { candidateId: item.candidate_id } });
    categoryOptions.forEach((option) => select.append(P.el('option', { value: option.value, text: option.label, selected: option.value === (item.category_key || 'movie_hd') })));
    select.addEventListener('change', () => { renderResolvedPreview(item.candidate_id); scheduleReview(); });
    return select;
  };

  const renderResolvedPreview = (candidateId) => {
    const holder = document.querySelector(`.resolved-preview[data-candidate-id="${CSS.escape(String(candidateId))}"]`);
    if (!holder) return;
    if (!selectedDestinations.size) { holder.replaceChildren(P.el('span', { className: 'muted', text: 'Select a destination to preview its category.' })); return; }
    const key = categoryFor(candidateId) || 'movie_hd';
    const list = P.el('ul', { className: 'list-reset' });
    destinations.filter((destination) => selectedDestinations.has(String(destination.id))).forEach((destination) => {
      const resolved = resolveCategory(destination, key);
      list.append(P.el('li', {}, [P.el('strong', { text: destination.name || destination.id || 'Destination' }), ': ', P.el('code', { text: resolved.id }), ` — ${resolved.label} `, P.el('span', { className: 'muted', text: `(${resolved.source})` })]));
    });
    holder.replaceChildren(P.el('strong', { text: 'Resolved category' }), list);
  };
  const renderResolvedPreviews = () => candidates.forEach((item) => renderResolvedPreview(item.candidate_id));

  const candidateCard = (item) => {
    const id = String(item.candidate_id || '');
    const card = P.el('article', { className: 'job share-candidate-card', dataset: { candidateId: id } });
    const checkbox = P.el('input', { type: 'checkbox', className: 'share-cand', checked: selectedCandidates.has(id), 'aria-label': `Select ${item.release_name || item.job_name || id}` });
    checkbox.addEventListener('change', () => { if (checkbox.checked) selectedCandidates.add(id); else selectedCandidates.delete(id); updateSummary(); scheduleReview(); });
    const remove = P.button('Remove', { className: 'mini-remove' });
    remove.addEventListener('click', async () => {
      if (!window.confirm('Remove this share candidate from the Share page?')) return;
      P.setBusy(remove, true, 'Removing…');
      try { await P.requestJSON('/api/share/candidate/remove', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ candidate_id: id }) }); setNotice('success', 'Share candidate removed.'); await refreshCandidates(); }
      catch (error) { setNotice('error', error.message); P.setBusy(remove, false); }
    });
    card.append(P.el('div', { className: 'job-header' }, [P.el('label', { className: 'checkbox' }, [checkbox, P.el('span', { text: 'Select' })]), remove]), P.el('h3', { text: item.release_name || item.job_name || 'Unnamed release' }));
    const meta = P.el('div', { className: 'job-meta' });
    [['Source', item.source_type || ''], ['NZB RAR', item.nzb_rar_path || '', true], ['Template', item.template_path || '', true], ['Type', item.media_type || 'unknown'], ['Resolution', item.resolution || 'unknown'], ['Size', item.template_declared_size || P.formatGB(item.size_bytes)], ['Episodes', item.episode_count || '—'], ['Groups', item.groups_csv || 'Unknown', true], ['Video', item.video_summary || item.video_codec || '—'], ['Audio', item.audio_summary || item.audio_codec || '—']].forEach(([label, value, code]) => meta.append(field(label, value, code)));
    card.append(meta);
    if (item.matched_by) card.append(P.paragraph('Pairing', `${item.matched_by} (${item.match_score || 0}% confidence)`));
    if (item.hdr_flags?.length) card.append(P.paragraph('HDR', item.hdr_flags.join(', ')));
    card.append(P.el('label', {}, [P.el('span', { text: 'Category' }), categorySelect(item)]));
    card.append(P.el('div', { className: 'resolved-preview card-subtle', dataset: { candidateId: id } }));
    card.append(P.el('p', { className: 'share-selected-status muted' }));
    return card;
  };

  const renderCandidates = () => {
    if (!candidates.length) { P.state(candidatesBox, 'empty', 'No share candidates', 'Complete Posting or import a bundle to create one.'); updateSummary(); return; }
    const fragment = document.createDocumentFragment();
    candidates.forEach((item) => fragment.append(candidateCard(item)));
    candidatesBox.replaceChildren(fragment);
    renderResolvedPreviews();
    applyFilters();
  };

  const applyFilters = () => {
    const filterId = filter.value;
    candidatesBox.querySelectorAll('.share-candidate-card').forEach((card) => {
      const item = candidateById(card.dataset.candidateId) || {};
      const shared = Array.isArray(item.shared_destinations) ? item.shared_destinations : [];
      const sharedIds = new Set(shared.map((entry) => String(entry.destination_id || '')));
      const sharedSelected = Array.from(selectedDestinations).filter((id) => sharedIds.has(id));
      const fullyShared = selectedDestinations.size > 0 && sharedSelected.length === selectedDestinations.size;
      const filtered = Boolean(filterId && sharedIds.has(filterId));
      card.hidden = fullyShared || filtered;
      if (card.hidden) selectedCandidates.delete(String(item.candidate_id || ''));
      const checkbox = card.querySelector('.share-cand');
      if (checkbox) checkbox.checked = selectedCandidates.has(String(item.candidate_id || ''));
      const status = card.querySelector('.share-selected-status');
      if (status) {
        const names = shared.filter((entry) => selectedDestinations.has(String(entry.destination_id || ''))).map((entry) => entry.destination_name || entry.destination_id);
        status.textContent = !selectedDestinations.size ? 'Select a destination to see prior share status.' : names.length ? `Already shared to: ${names.join(', ')}` : 'Not shared to the selected destinations.';
      }
    });
    updateSummary();
  };

  const payload = () => ({
    destination_ids: Array.from(selectedDestinations),
    items: Array.from(selectedCandidates).filter((id) => !document.querySelector(`.share-candidate-card[data-candidate-id="${CSS.escape(id)}"]`)?.hidden).map((id) => ({ candidate_id: id, category_key: categoryFor(id) || candidateById(id)?.category_key }))
  });

  const updateSummary = (reviewData) => {
    const value = payload();
    selectedCount.textContent = `${value.items.length} selected`;
    reviewButton.disabled = !value.items.length || !value.destination_ids.length;
    const stats = [['Selected candidates', value.items.length], ['Destinations', value.destination_ids.length], ['Visible candidates', Array.from(candidatesBox.querySelectorAll('.share-candidate-card')).filter((card) => !card.hidden).length], ['Ready', reviewData?.summary?.ready ?? '—'], ['Blocked', reviewData?.summary?.blocked ?? '—']];
    const grid = P.el('div', { className: 'summary-grid' });
    stats.forEach(([label, count]) => grid.append(P.el('div', { className: 'summary-pill' }, [P.el('strong', { text: label }), P.el('div', { text: count })])));
    summaryBox.replaceChildren(grid);
  };

  const refreshReview = async () => {
    const value = payload();
    if (!value.destination_ids.length || !value.items.length) { updateSummary(); return; }
    reviewController?.abort();
    reviewController = new AbortController();
    const sequence = ++reviewSequence;
    try {
      const data = await P.requestJSON('/api/share/review', { method: 'POST', signal: reviewController.signal, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value) });
      if (sequence === reviewSequence) updateSummary(data);
    } catch (error) { if (error.name !== 'AbortError' && sequence === reviewSequence) updateSummary(); }
  };
  const scheduleReview = () => { window.clearTimeout(reviewTimer); reviewTimer = window.setTimeout(refreshReview, 220); };

  const refreshCandidates = async () => {
    P.setBusy(document.getElementById('refreshShareCandidatesBtn'), true, 'Refreshing…');
    try {
      const data = await P.requestJSON('/api/share/candidates');
      candidates = data.results || [];
      destinations = data.destinations || [];
      categoryOptions = data.category_options || categoryOptions;
      const validIds = new Set(candidates.map((item) => String(item.candidate_id || '')));
      Array.from(selectedCandidates).forEach((id) => { if (!validIds.has(id)) selectedCandidates.delete(id); });
      renderDestinations(); renderCandidates(); scheduleReview();
    } catch (error) { P.state(candidatesBox, 'error', 'Could not load share candidates', error.message); setNotice('error', error.message); }
    finally { P.setBusy(document.getElementById('refreshShareCandidatesBtn'), false); }
  };

  const renderReview = (data) => {
    const summary = data.summary || {};
    const grid = P.el('div', { className: 'summary-grid' });
    [['Ready', summary.ready || 0], ['Blocked', summary.blocked || 0], ['Warnings', summary.warnings || 0]].forEach(([label, value]) => grid.append(P.el('div', { className: 'summary-pill' }, [P.el('strong', { text: label }), P.el('div', { text: value })])));
    reviewSummary.replaceChildren(grid, P.el('p', { className: 'muted', text: 'Blocked items will not be queued.' }));
    const fragment = document.createDocumentFragment();
    (data.reviews || []).forEach((item) => {
      const card = P.el('article', { className: `job ${item.status === 'blocked' ? 'job-danger' : ''}` });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: item.release_name || 'Release' }), P.badge(item.status)]));
      const meta = P.el('div', { className: 'job-meta' });
      [['Destination', item.destination_name || item.destination_id || ''], ['Category', `${item.selected_category_id || ''} — ${item.selected_category_label || ''}`], ['NFO', item.nfo ? 'Included' : 'Not included'], ['MediaInfo', item.mediainfo ? 'Included' : 'Not included']].forEach(([label, value]) => meta.append(field(label, value)));
      card.append(meta);
      if (item.warning) card.append(P.el('div', { className: 'flash warning', text: item.warning }));
      fragment.append(card);
    });
    reviewList.replaceChildren(fragment.childNodes.length ? fragment : P.el('div', { className: 'empty-state' }, P.el('strong', { text: 'Nothing selected' })));
  };

  const openReview = async (event) => {
    pendingPayload = payload();
    if (!pendingPayload.destination_ids.length || !pendingPayload.items.length) return;
    P.state(reviewList, 'loading', 'Building final review');
    P.openDialog(reviewModal, event.currentTarget);
    try { renderReview(await P.requestJSON('/api/share/review', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(pendingPayload) })); }
    catch (error) { P.state(reviewList, 'error', 'Review failed', error.message); }
  };

  const confirmReview = async () => {
    const button = document.getElementById('shareReviewConfirmBtn');
    if (!pendingPayload) return;
    P.setBusy(button, true, 'Submitting…');
    try {
      const data = await P.requestJSON('/api/share/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(pendingPayload) });
      P.closeDialog(reviewModal);
      const queuedCount = Number(data.queued_count ?? (Array.isArray(data.queued) ? data.queued.length : 0));
      const skippedCount = Number(data.skipped_count ?? (Array.isArray(data.skipped) ? data.skipped.length : 0));
      setNotice(
        queuedCount ? 'success' : 'warning',
        queuedCount
          ? `Accepted and queued ${queuedCount} background Share job(s); ${skippedCount} skipped. Uploads are not complete yet—monitor Share Jobs for final status.`
          : `No Share jobs were queued; ${skippedCount} skipped. Review the skipped reasons and try again.`
      );
      const queuedCandidateIds = new Set((data.jobs || data.queued || []).map((job) => String(job.candidate_id || '')));
      queuedCandidateIds.forEach((candidateId) => selectedCandidates.delete(candidateId));
      await Promise.all([refreshJobs(), refreshCandidates()]);
    } catch (error) { setNotice('error', error.message); }
    finally { P.setBusy(button, false); }
  };

  const jobAction = async (job, route, label, button, extra = {}) => {
    P.setBusy(button, true, `${label}…`);
    try { await P.requestJSON(route, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id: job.id, ...extra }) }); setNotice('success', `${label} completed.`); await refreshJobs(); }
    catch (error) { setNotice('error', error.message); P.setBusy(button, false); }
  };

  const renderJobs = (jobs) => {
    const active = jobs.filter((job) => ['queued', 'running', 'uploading', 'failed', 'outcome_unknown'].includes(String(job.status || '').toLowerCase()));
    if (!active.length) { P.state(jobsBox, 'empty', 'No active or failed share jobs', 'Completed work is available in Share History.'); return; }
    const fragment = document.createDocumentFragment();
    active.forEach((job) => {
      const status = String(job.status || '').toLowerCase();
      const card = P.el('article', { className: 'job' });
      const actions = P.el('div', { className: 'actions' });
      if (status === 'failed') {
        const retry = P.button('Retry', { className: 'secondary small' });
        retry.addEventListener('click', () => jobAction(job, '/api/share/retry', 'Retry', retry));
        actions.append(retry);
      }
      if (status === 'outcome_unknown') {
        const retry = P.button('Force retry', { className: 'danger small' });
        retry.addEventListener('click', () => {
          if (window.confirm('The destination may already have accepted this upload. Force retry anyway?')) jobAction(job, '/api/share/retry', 'Force retry', retry, { force_outcome_unknown: true });
        });
        actions.append(retry);
      }
      if (['queued', 'running'].includes(status)) {
        const stop = P.button('Cancel', { className: 'danger small' });
        stop.addEventListener('click', () => jobAction(job, '/api/share/cancel', 'Cancel', stop));
        actions.append(stop);
      }
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: job.job_name || 'Share job' }), P.el('div', { className: 'row' }, [P.badge(job.status), actions])]), P.progress(job.percent, `${job.job_name || 'Share'} progress`));
      const meta = P.el('div', { className: 'job-meta' });
      [['Destination', job.destination_name || job.destination_id || ''], ['Category', job.selected_category_label || job.selected_category_id || ''], ['Phase', job.phase || ''], ['Remote ID', job.remote_id || '', true], ['GUID', job.remote_guid || '', true]].forEach(([label, value, code]) => meta.append(field(label, value, code)));
      card.append(meta, P.paragraph('Message', job.message || ''));
      if (status === 'outcome_unknown') card.append(P.el('div', { className: 'flash warning', role: 'alert', text: 'The connection ended after upload began. Check the destination before forcing a retry, which may create a duplicate.' }));
      fragment.append(card);
    });
    jobsBox.replaceChildren(fragment);
  };
  const refreshJobs = async () => { try { renderJobs((await P.requestJSON('/api/share/jobs')).jobs || []); } catch (error) { P.state(jobsBox, 'error', 'Could not load share jobs', error.message); } };

  const submitImport = async (event, route, bulk) => {
    event.preventDefault();
    const form = event.currentTarget;
    const button = form.querySelector('button[type="submit"]');
    P.setBusy(button, true, bulk ? 'Importing bundles…' : 'Importing…');
    P.state(document.getElementById('shareImportResult'), 'loading', 'Uploading and validating files');
    try {
      const data = await P.requestJSON(route, { method: 'POST', body: new FormData(form) });
      const result = document.getElementById('shareImportResult');
      const stats = P.el('div', { className: 'stat-grid' });
      const values = bulk ? [['Imported', data.imported || 0], ['Skipped', data.skipped || 0]] : [['Imported', data.ok ? 1 : 0], ['Candidate', data.candidate_id || data.job_id || 'Ready']];
      values.forEach(([label, value]) => stats.append(P.el('div', { className: 'stat' }, [P.el('span', { className: 'stat-label', text: label }), P.el('div', { className: 'stat-value', text: value })])));
      result.replaceChildren(stats, P.jsonDetails('Import details', data));
      form.reset(); setNotice('success', bulk ? 'Mass import completed.' : 'Bundle imported for Share.'); await refreshCandidates();
    } catch (error) { P.state(document.getElementById('shareImportResult'), 'error', 'Import failed', error.message); setNotice('error', error.message); }
    finally { P.setBusy(button, false); }
  };

  document.getElementById('refreshShareCandidatesBtn').addEventListener('click', refreshCandidates);
  document.getElementById('refreshShareCapsBtn').addEventListener('click', async (event) => {
    const button = event.currentTarget; P.setBusy(button, true, 'Refreshing…');
    try { await P.requestJSON('/api/share/caps/refresh', { method: 'POST' }); setNotice('success', 'Destination categories refreshed.'); await refreshCandidates(); }
    catch (error) { setNotice('error', error.message); }
    finally { P.setBusy(button, false); }
  });
  filter.addEventListener('change', applyFilters);
  reviewButton.addEventListener('click', openReview);
  document.getElementById('closeShareReviewBtn').addEventListener('click', () => P.closeDialog(reviewModal));
  document.getElementById('cancelShareReviewBtn').addEventListener('click', () => P.closeDialog(reviewModal));
  document.getElementById('shareReviewConfirmBtn').addEventListener('click', confirmReview);
  reviewModal.addEventListener('click', (event) => { if (event.target === reviewModal) P.closeDialog(reviewModal); });
  document.getElementById('shareImportForm').addEventListener('submit', (event) => submitImport(event, '/api/share/import', false));
  document.getElementById('shareBulkImportForm').addEventListener('submit', (event) => submitImport(event, '/api/share/import-bulk', true));
  refreshCandidates(); refreshJobs();
  let stream = new EventSource('/api/share/jobs/stream');
  stream.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data || '{}');
      if (data.ok === false) {
        setNotice('warning', data.error || 'Live Share updates are temporarily unavailable; the displayed jobs may be stale.');
        return;
      }
      renderJobs(data.jobs || []);
    } catch (_error) {
      setNotice('warning', 'A live Share update could not be read; the displayed jobs may be stale.');
    }
  };
  if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
})();
