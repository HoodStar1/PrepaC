(() => {
  'use strict';

  const button = document.getElementById('goToBottom');
  if (!button) return;

  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
  let framePending = false;
  let lastY = window.scrollY;
  let hasBeenVisible = false;

  const documentHeight = () => Math.max(
    document.body.scrollHeight,
    document.body.offsetHeight,
    document.documentElement.clientHeight,
    document.documentElement.scrollHeight,
    document.documentElement.offsetHeight
  );

  const update = () => {
    framePending = false;
    const currentY = Math.max(0, window.scrollY || document.documentElement.scrollTop || 0);
    const viewport = window.innerHeight || document.documentElement.clientHeight;
    const height = documentHeight();
    const scrollable = height - viewport > 12;
    const atBottom = currentY + viewport >= height - 6;
    const movingUp = currentY < lastY - 2;
    const shouldShow = scrollable && !atBottom;

    if (shouldShow !== !button.hidden) {
      button.hidden = !shouldShow;
      button.setAttribute('aria-hidden', String(!shouldShow));
      if (shouldShow && (!hasBeenVisible || movingUp)) {
        button.classList.remove('is-entering');
        void button.offsetWidth;
        button.classList.add('is-entering');
        hasBeenVisible = true;
      }
    }
    lastY = currentY;
  };

  const schedule = () => {
    if (framePending) return;
    framePending = true;
    window.requestAnimationFrame(update);
  };

  button.addEventListener('click', () => {
    window.scrollTo({ top: documentHeight(), behavior: reduceMotion.matches ? 'auto' : 'smooth' });
  });

  window.addEventListener('scroll', schedule, { passive: true });
  window.addEventListener('resize', schedule, { passive: true });
  window.addEventListener('pageshow', schedule);
  window.addEventListener('load', schedule, { once: true });
  reduceMotion.addEventListener?.('change', schedule);

  if ('ResizeObserver' in window) {
    const observer = new ResizeObserver(schedule);
    observer.observe(document.body);
    observer.observe(document.documentElement);
  } else {
    const observer = new MutationObserver(schedule);
    observer.observe(document.body, { childList: true, subtree: true, attributes: true });
  }

  schedule();
})();
