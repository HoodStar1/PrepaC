(() => {
  'use strict';

  const csrfToken = document.querySelector('meta[name="prepac-csrf-token"]')?.content || '';
  const unsafeMethods = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);
  const originalFetch = window.fetch ? window.fetch.bind(window) : null;

  const requestMethod = (input, init) => String(init?.method || input?.method || 'GET').toUpperCase();
  const requestUrl = (input) => typeof input === 'string' ? input : (input?.url || window.location.href);

  if (originalFetch) {
    window.fetch = (input, init = {}) => {
      const nextInit = { ...init };
      if (nextInit.credentials === undefined) nextInit.credentials = 'same-origin';
      if (csrfToken && unsafeMethods.has(requestMethod(input, nextInit))) {
        let sameOrigin = true;
        try {
          sameOrigin = new URL(requestUrl(input), window.location.href).origin === window.location.origin;
        } catch (_error) {
          sameOrigin = false;
        }
        if (sameOrigin) {
          const headers = new Headers(nextInit.headers || input?.headers || undefined);
          if (!headers.has('X-CSRF-Token')) headers.set('X-CSRF-Token', csrfToken);
          nextInit.headers = headers;
        }
      }
      return originalFetch(input, nextInit);
    };
  }

  const append = (parent, child) => {
    if (child === null || child === undefined || child === false) return;
    if (Array.isArray(child)) {
      child.forEach((item) => append(parent, item));
      return;
    }
    parent.append(child instanceof Node ? child : document.createTextNode(String(child)));
  };

  const el = (tag, attributes = {}, children = []) => {
    const node = document.createElement(tag);
    Object.entries(attributes || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === false) return;
      if (key === 'className') node.className = String(value);
      else if (key === 'text') node.textContent = String(value);
      else if (key === 'dataset') Object.entries(value).forEach(([name, item]) => { node.dataset[name] = String(item); });
      else if (key === 'checked' || key === 'disabled' || key === 'selected' || key === 'open' || key === 'hidden') node[key] = Boolean(value);
      else if (key === 'value') node.value = String(value);
      else node.setAttribute(key, String(value));
    });
    append(node, children);
    return node;
  };

  const clear = (node) => node?.replaceChildren();

  const paragraph = (label, value, asCode = false) => {
    const p = el('p');
    if (label) p.append(el('strong', { text: `${label}:` }), ' ');
    p.append(asCode ? el('code', { text: value ?? '' }) : document.createTextNode(String(value ?? '')));
    return p;
  };

  const badge = (status) => {
    const value = String(status || 'unknown');
    return el('span', { className: `badge ${value.toLowerCase().replace(/[^a-z0-9_-]+/g, '-')}`, text: value });
  };

  const progress = (value, label = 'Progress') => {
    const amount = Math.max(0, Math.min(100, Number(value) || 0));
    return el('progress', { className: 'native-progress', max: '100', value: String(amount), 'aria-label': label });
  };

  const state = (container, kind, title, message = '') => {
    if (!container) return;
    const box = el('div', { className: `${kind}-state`, role: kind === 'error' ? 'alert' : 'status' });
    box.append(el('strong', { text: title }));
    if (message) box.append(el('span', { text: message }));
    container.replaceChildren(box);
  };

  const jsonDetails = (summary, value, open = false) => {
    const details = el('details', { open });
    details.append(el('summary', { text: summary }), el('pre', { text: JSON.stringify(value ?? {}, null, 2) }));
    return details;
  };

  const button = (label, options = {}) => {
    const node = el('button', {
      type: options.type || 'button',
      className: options.className || '',
      disabled: options.disabled,
      'aria-label': options.ariaLabel
    }, label);
    if (typeof options.onClick === 'function') node.addEventListener('click', options.onClick);
    return node;
  };

  const safeImage = (url, alt, className, placeholderText = 'No image') => {
    const raw = String(url || '').trim();
    if (raw) {
      try {
        const parsed = new URL(raw, window.location.origin);
        if (parsed.origin === window.location.origin && ['http:', 'https:'].includes(parsed.protocol)) {
          const image = el('img', { src: parsed.href, alt: alt || '', className: className || '', loading: 'lazy', decoding: 'async' });
          image.addEventListener('error', () => image.replaceWith(el('div', { className: `poster-placeholder ${className?.includes('small') ? 'small' : ''}`, text: placeholderText })));
          return image;
        }
      } catch (_error) {}
    }
    return el('div', { className: `poster-placeholder ${className?.includes('small') ? 'small' : ''}`, text: placeholderText });
  };

  const parseJSONData = (id, fallback) => {
    const node = document.getElementById(id);
    if (!node) return fallback;
    try { return JSON.parse(node.textContent || ''); } catch (_error) { return fallback; }
  };

  const responseJSON = async (response) => {
    const raw = await response.text();
    let data = {};
    try { data = raw ? JSON.parse(raw) : {}; } catch (_error) {
      if (!response.ok) {
        throw new Error(`Request failed with HTTP ${response.status}; the server returned a non-JSON error response.`);
      }
      throw new Error(`Invalid server response (HTTP ${response.status}; expected JSON).`);
    }
    if (!response.ok || data?.ok === false) throw new Error(String(data?.error || `Request failed with HTTP ${response.status}`));
    return data;
  };

  const requestJSON = async (url, init) => responseJSON(await fetch(url, init));

  const setBusy = (control, busy, busyLabel) => {
    if (!control) return;
    if (busy) {
      control.dataset.idleLabel = control.textContent;
      if (busyLabel) control.textContent = busyLabel;
      control.disabled = true;
      control.setAttribute('aria-busy', 'true');
    } else {
      if (control.dataset.idleLabel) control.textContent = control.dataset.idleLabel;
      control.disabled = false;
      control.removeAttribute('aria-busy');
    }
  };

  const toast = (message, kind = 'info') => {
    const region = document.getElementById('appToastRegion');
    if (!region) return;
    const item = el('div', { className: `toast ${kind}`, role: kind === 'error' ? 'alert' : 'status', text: message });
    region.append(item);
    window.setTimeout(() => item.remove(), 5000);
  };

  const ambiguousOutcomeConfirmation = 'I VERIFIED THE DESTINATION';
  const outcomeUnknownRecovery = ({ workflow, jobId, endpoint, warning, afterAcknowledge }) => {
    const workflowName = String(workflow || 'Job');
    const numericJobId = Number(jobId) || 0;
    const warningId = `outcome-unknown-${workflowName.toLowerCase()}-${numericJobId}`;
    const panel = el('section', { className: 'outcome-recovery', 'aria-label': `${workflowName} ambiguous outcome recovery` });
    const warningBox = el('div', { className: 'flash warning', role: 'alert', id: warningId }, [
      el('strong', { text: 'Ambiguous outcome — retry remains blocked.' }),
      el('p', { text: String(warning || 'The prior operation may have completed. Inspect its destination and reconcile any output before allowing another submission.') }),
      el('p', {}, [
        'Only after verification, use the control below and type ',
        el('code', { text: ambiguousOutcomeConfirmation }),
        '. This acknowledgement cannot prove that the prior operation failed.'
      ])
    ]);
    const acknowledge = button('Acknowledge and allow resubmission', {
      className: 'danger small',
      ariaLabel: `Acknowledge ${workflowName} job ${numericJobId} ambiguous outcome after destination verification`
    });
    acknowledge.setAttribute('aria-describedby', warningId);
    acknowledge.addEventListener('click', async () => {
      const typed = window.prompt(
        `Verify the ${workflowName} destination first. The previous operation may have completed and a new submission may duplicate it.\n\nType ${ambiguousOutcomeConfirmation} to continue.`
      );
      if (typed === null) return;
      if (typed !== ambiguousOutcomeConfirmation) {
        toast('Acknowledgement phrase did not match. Retry remains blocked.', 'error');
        acknowledge.focus();
        return;
      }
      setBusy(acknowledge, true, 'Acknowledging…');
      try {
        const data = await requestJSON(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            job_id: numericJobId,
            acknowledge_ambiguous_outcome: true,
            confirmation: ambiguousOutcomeConfirmation
          })
        });
        toast(data.warning || 'Outcome acknowledged. Build a fresh submission only if it is safe.', 'warning');
        if (typeof afterAcknowledge === 'function') await afterAcknowledge(data);
      } catch (error) {
        toast(error.message, 'error');
      } finally {
        if (acknowledge.isConnected) setBusy(acknowledge, false);
      }
    });
    panel.append(warningBox, el('div', { className: 'actions' }, acknowledge));
    return panel;
  };

  let activeDialog = null;
  let dialogTrigger = null;
  const dialogFocusable = (dialog) => Array.from(dialog.querySelectorAll('a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'));

  const closeDialog = (dialog = activeDialog) => {
    if (!dialog) return;
    dialog.hidden = true;
    dialog.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('dialog-open');
    activeDialog = null;
    const target = dialogTrigger;
    dialogTrigger = null;
    target?.focus();
  };

  const openDialog = (dialog, trigger) => {
    if (!dialog) return;
    activeDialog = dialog;
    dialogTrigger = trigger || document.activeElement;
    dialog.hidden = false;
    dialog.setAttribute('aria-hidden', 'false');
    document.body.classList.add('dialog-open');
    (dialogFocusable(dialog)[0] || dialog).focus();
  };

  document.addEventListener('keydown', (event) => {
    if (!activeDialog) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      closeDialog();
      return;
    }
    if (event.key !== 'Tab') return;
    const focusable = dialogFocusable(activeDialog);
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  });

  window.prepacEventStreamAuthErrorHandler = (stream) => async () => {
    if (window.__prepacAuthRedirecting) return;
    try {
      const response = await fetch('/api/version', { cache: 'no-store' });
      const authLost = response.status === 401 || response.status === 403;
      if (!authLost) return;
      window.__prepacAuthRedirecting = true;
      stream?.close?.();
      window.location.href = `/login?next=${encodeURIComponent(window.location.pathname + window.location.search)}`;
    } catch (_error) {}
  };

  if (window.EventSource && !window.__prepacEventSourceWrapped) {
    const NativeEventSource = window.EventSource;
    const activeStreams = new Set();
    window.__prepacEventSourceWrapped = true;
    window.prepacCloseEventStreams = () => {
      activeStreams.forEach((stream) => { try { stream.close(); } catch (_error) {} });
      activeStreams.clear();
    };
    window.EventSource = function (...args) {
      const stream = new NativeEventSource(...args);
      activeStreams.add(stream);
      const nativeClose = stream.close.bind(stream);
      stream.close = () => { activeStreams.delete(stream); return nativeClose(); };
      return stream;
    };
    window.EventSource.prototype = NativeEventSource.prototype;
    window.addEventListener('pagehide', window.prepacCloseEventStreams);
    window.addEventListener('beforeunload', window.prepacCloseEventStreams);
  }

  window.prepacCheckForUpdates = async () => {
    try {
      const response = await fetch('/api/version/check', { method: 'POST', cache: 'no-store' });
      if (!response.ok) return null;
      const data = await response.json();
      const badgeNode = document.getElementById('updateBadge');
      if (badgeNode) {
        if (data?.update_available) {
          badgeNode.textContent = `Update ${data.latest_tag || data.latest_version || 'available'}`;
          const candidateUrl = String(data.asset_url || data.release_url || '');
          try {
            const parsed = new URL(candidateUrl);
            badgeNode.href = ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : '#';
          } catch (_error) {
            badgeNode.href = '#';
          }
          badgeNode.hidden = false;
        } else {
          badgeNode.hidden = true;
        }
      }
      return data;
    } catch (_error) {
      return null;
    }
  };

  const initializeShell = () => {
    document.body.classList.toggle('has-sticky-actions', Boolean(document.querySelector('.action-toolbar-bottom')));
    document.querySelectorAll('form').forEach((form) => {
      const method = String(form.getAttribute('method') || 'GET').toUpperCase();
      if (!unsafeMethods.has(method) || form.querySelector('input[name="csrf_token"]')) return;
      form.prepend(el('input', { type: 'hidden', name: 'csrf_token', value: csrfToken }));
    });

    document.querySelectorAll('table').forEach((table) => {
      table.querySelectorAll('thead th, tr:first-child > th').forEach((header) => {
        if (!header.hasAttribute('scope')) header.setAttribute('scope', 'col');
      });
      if (table.parentElement?.classList.contains('table-shell')) return;
      const wrapper = el('div', { className: 'table-shell', tabindex: '0', role: 'region', 'aria-label': 'Scrollable data table' });
      table.before(wrapper);
      wrapper.append(table);
    });

    const sidebar = document.getElementById('appSidebar');
    const toggle = document.getElementById('mobileNavToggle');
    const close = document.getElementById('mobileNavClose');
    const backdrop = document.getElementById('mobileNavBackdrop');
    const navMedia = window.matchMedia('(max-width: 900px)');
    const navFocusable = () => Array.from(sidebar?.querySelectorAll('a[href], button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])') || []);
    const syncNavMode = () => {
      const open = navMedia.matches && document.body.classList.contains('nav-open');
      toggle?.setAttribute('aria-expanded', String(open));
      if (backdrop) backdrop.hidden = !open;
      if (navMedia.matches) sidebar?.setAttribute('aria-hidden', String(!open));
      else sidebar?.removeAttribute('aria-hidden');
    };
    const setNav = (open, restoreFocus = true) => {
      document.body.classList.toggle('nav-open', Boolean(open && navMedia.matches));
      syncNavMode();
      if (open) sidebar?.querySelector('a, button')?.focus();
      else if (restoreFocus) toggle?.focus();
    };
    toggle?.addEventListener('click', () => setNav(true));
    close?.addEventListener('click', () => setNav(false));
    backdrop?.addEventListener('click', () => setNav(false));
    sidebar?.querySelectorAll('a[href]').forEach((link) => link.addEventListener('click', () => setNav(false, false)));
    navMedia.addEventListener?.('change', () => { document.body.classList.remove('nav-open'); syncNavMode(); });
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && document.body.classList.contains('nav-open')) setNav(false);
      if (event.key !== 'Tab' || !document.body.classList.contains('nav-open')) return;
      const focusable = navFocusable();
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    syncNavMode();

    const repairButton = document.getElementById('dbRepairBtn');
    repairButton?.addEventListener('click', async () => {
      const statusNode = document.getElementById('dbRepairStatus');
      setBusy(repairButton, true, 'Repairing…');
      if (statusNode) statusNode.textContent = '';
      try {
        const data = await requestJSON('/api/db/repair', { method: 'POST' });
        if (!data.ok) throw new Error((data.issues || []).join('; ') || 'Repair failed');
        if (statusNode) statusNode.textContent = 'Repair successful. Reloading…';
        document.getElementById('dbCorruptBanner')?.remove();
        window.setTimeout(() => window.location.reload(), 900);
      } catch (error) {
        if (statusNode) statusNode.textContent = `Repair failed: ${error.message}`;
        setBusy(repairButton, false);
      }
    });
  };

  window.PrepaC = Object.freeze({
    csrfToken,
    el,
    clear,
    paragraph,
    badge,
    progress,
    state,
    jsonDetails,
    button,
    safeImage,
    parseJSONData,
    responseJSON,
    requestJSON,
    setBusy,
    toast,
    outcomeUnknownRecovery,
    openDialog,
    closeDialog,
    formatGB: (value) => `${((Number(value) || 0) / (1024 ** 3)).toFixed(2)} GB`,
    formatMB: (value) => `${((Number(value) || 0) / (1024 ** 2)).toFixed(2)} MB`
  });

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initializeShell, { once: true });
  else initializeShell();
})();
