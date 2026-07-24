(() => {
  'use strict';
  const P = window.PrepaC;
  if (!P) return;
  const candidatesBox = document.getElementById('postingCandidates');
  const jobsBox = document.getElementById('postingJobs');
  const scanButton = document.getElementById('scanPostingBtn');
  const startButton = document.getElementById('startPostingBtn');
  const selectionStatus = document.getElementById('postingSelectionStatus');
  const selectedCount = document.getElementById('postingSelectedCount');
  let candidates = [];
  const selected = new Set();
  const sync = () => { selectedCount.textContent = `${selected.size} selected`; startButton.disabled = selected.size === 0; };

  const renderCandidates = () => {
    if (!candidates.length) { P.state(candidatesBox, 'empty', 'No eligible packed jobs', 'Nothing in the scan folder is ready for posting.'); selectionStatus.textContent = '0 eligible jobs.'; sync(); return; }
    const fragment = document.createDocumentFragment();
    candidates.forEach((item) => {
      const key = String(item.candidate_id || '');
      const card = P.el('article', { className: 'job' });
      const checkbox = P.el('input', { type: 'checkbox', checked: selected.has(key), 'aria-label': `Select ${item.job_name || key}` });
      checkbox.addEventListener('change', () => { if (checkbox.checked) selected.add(key); else selected.delete(key); sync(); });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: item.job_name || 'Unnamed release' }), P.el('label', { className: 'checkbox' }, [checkbox, P.el('span', { text: 'Select' })])]));
      const meta = P.el('div', { className: 'job-meta' });
      [['Packed root', item.packed_root || '', true], ['Output files', item.output_files_root || '', true], ['Template', item.template_path || '', true], ['Size', P.formatGB(item.size_bytes)], ['Header', item.header || '', true], ['Password', item.password_present ? 'Configured' : 'Not present']].forEach(([label, value, code]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value }) : value])));
      card.append(meta);
      fragment.append(card);
    });
    candidatesBox.replaceChildren(fragment);
    selectionStatus.textContent = `${candidates.length} eligible job${candidates.length === 1 ? '' : 's'}.`;
    sync();
  };

  const scan = async () => {
    P.setBusy(scanButton, true, 'Scanning…');
    P.state(candidatesBox, 'loading', 'Scanning packed releases');
    try { const data = await P.requestJSON('/api/posting/scan', { method: 'POST' }); candidates = data.results || []; selected.clear(); renderCandidates(); }
    catch (error) { candidates = []; selected.clear(); P.state(candidatesBox, 'error', 'Posting scan failed', error.message); selectionStatus.textContent = 'The scan could not be completed.'; sync(); }
    finally { P.setBusy(scanButton, false); }
  };

  const stopJob = async (job, button) => {
    P.setBusy(button, true, 'Stopping…');
    try { await P.requestJSON('/api/posting/cancel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id: job.id }) }); await refreshJobs(); }
    catch (error) { P.toast(error.message, 'error'); P.setBusy(button, false); }
  };

  const renderJobs = (jobs) => {
    const active = jobs.filter((job) => ['queued', 'running', 'finalizing', 'outcome_unknown'].includes(String(job.status || '').toLowerCase()));
    if (!active.length) { P.state(jobsBox, 'empty', 'No active posting jobs', 'Finished jobs are available in Posting History.'); return; }
    const fragment = document.createDocumentFragment();
    active.forEach((job) => {
      const status = String(job.status || '').toLowerCase();
      const card = P.el('article', { className: `job ${status === 'outcome_unknown' ? 'job-danger' : ''}`, dataset: { jobId: job.id } });
      card.append(P.el('div', { className: 'job-header' }, [P.el('h3', { text: job.job_name || 'Posting job' }), P.badge(job.status)]), P.progress(job.percent, `${job.job_name || 'Posting'} progress`));
      const stats = job.runtime_stats || {};
      const meta = P.el('div', { className: 'job-meta' });
      [['Step', job.phase || ''], ['Progress', job.percent == null ? '—' : `${job.percent}%`], ['Provider', job.provider_used || 'Waiting'], ['Transfer rate', stats.transfer_rate || '—'], ['Transferred', stats.percent_transferred || '—'], ['ETA', stats.eta || '—'], ['Header', job.header_value || '', true], ['Groups', job.groups_csv || '', true], ['From', job.from_header || '', true], ['NZB', job.nzb_path || '', true]].forEach(([label, value, code]) => meta.append(P.el('div', {}, [P.el('strong', { text: label }), code ? P.el('code', { text: value }) : value])));
      card.append(meta, P.paragraph('Live stage', job.message || ''));
      if (['queued', 'running'].includes(status)) {
        const stop = P.button('Stop job', { className: 'danger small' });
        stop.addEventListener('click', () => stopJob(job, stop));
        card.append(P.el('div', { className: 'actions' }, stop));
      }
      if (status === 'outcome_unknown') {
        card.append(P.outcomeUnknownRecovery({
          workflow: 'Posting',
          jobId: job.id,
          endpoint: '/api/posting/outcome-unknown/acknowledge',
          warning: 'The provider may already have accepted articles and an NZB may already exist. Check provider and local output records before allowing another Posting submission.',
          afterAcknowledge: async () => { await Promise.all([refreshJobs(), scan()]); }
        }));
      }
      fragment.append(card);
    });
    jobsBox.replaceChildren(fragment);
  };

  const refreshJobs = async () => { try { renderJobs((await P.requestJSON('/api/posting/jobs')).jobs || []); } catch (error) { P.state(jobsBox, 'error', 'Could not load posting jobs', error.message); } };
  startButton.addEventListener('click', async () => {
    if (!selected.size) return;
    P.setBusy(startButton, true, 'Starting…');
    try { await P.requestJSON('/api/posting/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ candidate_ids: Array.from(selected) }) }); await Promise.all([refreshJobs(), scan()]); }
    catch (error) { P.toast(`Could not start posting: ${error.message}`, 'error'); }
    finally { P.setBusy(startButton, false); sync(); }
  });
  scanButton.addEventListener('click', scan);
  refreshJobs();
  let stream;
  const startStream = () => {
    stream?.close();
    stream = new EventSource('/api/posting/jobs/stream');
    stream.onmessage = (event) => { try { renderJobs(JSON.parse(event.data || '{}').jobs || []); } catch (_error) {} };
    if (window.prepacEventStreamAuthErrorHandler) stream.onerror = window.prepacEventStreamAuthErrorHandler(stream);
  };
  document.addEventListener('visibilitychange', () => { if (document.hidden) stream?.close(); else startStream(); });
  startStream();
})();
