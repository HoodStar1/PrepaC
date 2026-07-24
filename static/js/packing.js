(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;
  const candidatesBox = document.getElementById('packingCandidates');
  const jobsBox = document.getElementById('packingJobs');
  const scanButton = document.getElementById('scanPackingBtn');
  const startButton = document.getElementById('startPackingBtn');
  const selectionStatus = document.getElementById('packingSelectionStatus');
  const selectedCount = document.getElementById('packingSelectedCount');
  let candidates = [];
  const selected = new Set();

  const sync = () => {
    selectedCount.textContent = `${selected.size} selected`;
    startButton.disabled = selected.size === 0;
  };

  const renderCandidates = () => {
    if (!candidates.length) {
      P.state(candidatesBox, 'empty', 'No eligible jobs', 'Nothing in the watch folder is ready for packing.');
      selectionStatus.textContent = '0 eligible jobs.';
      sync();
      return;
    }
    const fragment = document.createDocumentFragment();
    candidates.forEach((item) => {
      const key = String(item.candidate_id || '');
      const card = P.el('article', { className: 'job' });
      const checkbox = P.el('input', { type: 'checkbox', checked: selected.has(key), 'aria-label': `Select ${item.job_name || key}` });
      checkbox.addEventListener('change', () => { if (checkbox.checked) selected.add(key); else selected.delete(key); sync(); });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: item.job_name || 'Unnamed job' }), P.el('label', { className: 'checkbox' }, [checkbox, P.el('span', { text: 'Select' })])]));
      const meta = P.el('div', { className: 'job-meta' });
      [['Source', item.source_path || '', true], ['Size', P.formatGB(item.size_bytes)], ['Largest video', item.largest_video || '—', true], ['Bracket', item.chosen_bracket || 'Auto', true], ['RAR parts', item.estimated_parts || 0], ['PAR2', `${item.estimated_par2_percent || 0}%`]].forEach(([label, value, code]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value }) : String(value)])));
      card.append(meta);
      if (item.largest_video) card.append(P.jsonDetails('Detected tags', item.detected_tags || {}));
      fragment.append(card);
    });
    candidatesBox.replaceChildren(fragment);
    selectionStatus.textContent = `${candidates.length} eligible job${candidates.length === 1 ? '' : 's'}.`;
    sync();
  };

  const scan = async () => {
    P.setBusy(scanButton, true, 'Scanning…');
    P.state(candidatesBox, 'loading', 'Scanning prepared media');
    try {
      const data = await P.requestJSON('/api/packing/scan', { method: 'POST' });
      candidates = Array.isArray(data.results) ? data.results : [];
      selected.clear();
      renderCandidates();
    } catch (error) {
      candidates = [];
      selected.clear();
      P.state(candidatesBox, 'error', 'Packing scan failed', error.message);
      selectionStatus.textContent = 'The scan could not be completed.';
      sync();
    } finally { P.setBusy(scanButton, false); }
  };

  const renderJobs = (jobs) => {
    const active = jobs.filter((job) => ['queued', 'running', 'finalizing', 'outcome_unknown'].includes(String(job.status || '').toLowerCase()));
    if (!active.length) { P.state(jobsBox, 'empty', 'No active packing jobs', 'Finished jobs are available in Packing History.'); return; }
    const fragment = document.createDocumentFragment();
    active.forEach((job) => {
      const status = String(job.status || '').toLowerCase();
      const card = P.el('article', { className: `job ${status === 'outcome_unknown' ? 'job-danger' : ''}` });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: job.job_name || 'Packing job' }), P.badge(job.status)]));
      card.append(P.progress(job.percent, `${job.job_name || 'Packing'} progress`));
      const meta = P.el('div', { className: 'job-meta' });
      [['Step', job.phase || ''], ['Progress', job.percent == null ? '—' : `${job.percent}%`], ['RAR estimate', `${job.rar_parts_estimate || 0} parts`], ['PAR2', `${job.par2_percent || 0}%`], ['Output', job.output_root || '', true]].forEach(([label, value, code]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value }) : value])));
      card.append(meta, P.paragraph('Detail', job.message || ''));
      if (String(job.phase || '').toLowerCase() === 'thumbnail_link' && String(job.message || '').toLowerCase().includes('thumbnail upload failed')) card.append(P.el('div', { className: 'flash error', text: job.message }));
      if (['queued', 'running'].includes(status) && !job.output_reset_claimed_at) {
        const stop = P.button('Stop job', { className: 'danger small', onClick: async () => {
          P.setBusy(stop, true, 'Stopping…');
          try { await P.requestJSON('/api/packing/cancel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id: job.id }) }); await refreshJobs(); }
          catch (error) { P.toast(error.message, 'error'); P.setBusy(stop, false); }
        }});
        card.append(P.el('div', { className: 'actions' }, stop));
      } else if (status === 'running' && job.output_reset_claimed_at) {
        card.append(P.el('p', {
          className: 'help-text',
          text: 'Cancellation is locked because this attempt has claimed and may have cleared its output directories.'
        }));
      }
      if (status === 'outcome_unknown') {
        card.append(P.outcomeUnknownRecovery({
          workflow: 'Packing',
          jobId: job.id,
          endpoint: '/api/packing/outcome-unknown/acknowledge',
          warning: 'Final archives or parity files may already exist. Inspect and reconcile the entire output folder before allowing the source to appear as a fresh Packing candidate.',
          afterAcknowledge: async () => { await Promise.all([refreshJobs(), scan()]); }
        }));
      }
      fragment.append(card);
    });
    jobsBox.replaceChildren(fragment);
  };

  const refreshJobs = async () => {
    try { renderJobs((await P.requestJSON('/api/packing/jobs')).jobs || []); }
    catch (error) { P.state(jobsBox, 'error', 'Could not load packing jobs', error.message); }
  };

  startButton.addEventListener('click', async () => {
    if (!selected.size) return;
    P.setBusy(startButton, true, 'Starting…');
    try {
      await P.requestJSON('/api/packing/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ candidate_ids: Array.from(selected) }) });
      await Promise.all([refreshJobs(), scan()]);
    } catch (error) { P.toast(`Could not start packing: ${error.message}`, 'error'); }
    finally { P.setBusy(startButton, false); sync(); }
  });
  scanButton.addEventListener('click', scan);
  refreshJobs();
  let stream;
  const startStream = () => {
    stream?.close();
    stream = new EventSource('/api/packing/jobs/stream');
    stream.onmessage = (event) => { try { renderJobs(JSON.parse(event.data || '{}').jobs || []); } catch (_error) {} };
    if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
  };
  document.addEventListener('visibilitychange', () => { if (document.hidden) stream?.close(); else startStream(); });
  startStream();
})();
