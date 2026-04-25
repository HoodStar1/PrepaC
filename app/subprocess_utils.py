import subprocess
import time
import os
import signal
import threading
import queue


def terminate_process(proc, graceful_timeout: float = 5.0):
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=max(0.1, float(graceful_timeout)))
        return
    except Exception:
        pass
    try:
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, signal.SIGKILL)
            return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        pass


def run_command_with_output(cmd, cwd=None, retries: int = 1, retry_delay: float = 1.0, on_output=None, on_tick=None, tick_seconds: float = 1.0, text: bool = True, start_new_session: bool = False, should_stop=None, on_proc_start=None, inactivity_timeout_seconds: float = 0.0, runtime_timeout_seconds: float = 0.0):
    import re as _re
    attempt = 0
    last_rc = 1
    out_text = ""
    while attempt < max(1, int(retries)):
        attempt += 1
        # Use binary mode so we receive output as it arrives, including \r-only
        # updates (e.g. par2 in-place progress lines).  We decode manually.
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=0,
            start_new_session=start_new_session,
        )
        if on_proc_start:
            try:
                on_proc_start(proc)
            except Exception:
                pass
        output_parts = []
        last_tick = 0.0
        _leftover = b""
        started_ts = time.time()
        last_output_ts = started_ts
        out_queue = queue.Queue()

        def _reader_thread():
            try:
                while True:
                    chunk = proc.stdout.read(256) if proc.stdout else b""
                    if chunk:
                        out_queue.put(chunk)
                        continue
                    break
            except Exception:
                pass
            finally:
                out_queue.put(None)

        reader = threading.Thread(target=_reader_thread, daemon=True)
        reader.start()
        while True:
            try:
                chunk = out_queue.get(timeout=0.05)
            except queue.Empty:
                chunk = b""
            reader_done = chunk is None
            if reader_done:
                chunk = b""
            if chunk:
                _leftover += chunk
                last_output_ts = time.time()
                # Split on \r or \n, keeping \n-terminated lines intact
                segments = _re.split(b"(\r\n|\r|\n)", _leftover)
                # segments alternates: text, delimiter, text, delimiter, ..., remainder
                # Rebuild: emit every complete segment (text+delimiter pair)
                complete = []
                i = 0
                while i + 1 < len(segments):
                    complete.append(segments[i].decode("utf-8", errors="replace") + segments[i + 1].decode("utf-8", errors="replace"))
                    i += 2
                _leftover = segments[-1] if len(segments) % 2 == 1 else b""
                for seg in complete:
                    output_parts.append(seg)
                    if on_output:
                        on_output(seg)
            now_ts = time.time()
            if on_tick and (now_ts - last_tick >= tick_seconds):
                on_tick()
                last_tick = now_ts
            if should_stop:
                try:
                    if should_stop(proc):
                        terminate_process(proc)
                except Exception:
                    pass
            if inactivity_timeout_seconds and proc.poll() is None:
                if (time.time() - last_output_ts) >= float(inactivity_timeout_seconds):
                    terminate_process(proc)
            if runtime_timeout_seconds and proc.poll() is None:
                if (time.time() - started_ts) >= float(runtime_timeout_seconds):
                    terminate_process(proc)
            if reader_done and proc.poll() is not None:
                # Flush any remaining buffered bytes
                if proc.stdout:
                    rest = proc.stdout.read()
                    if rest:
                        _leftover += rest
                if _leftover:
                    seg = _leftover.decode("utf-8", errors="replace")
                    output_parts.append(seg)
                    if on_output:
                        on_output(seg)
                    _leftover = b""
                break
            if not chunk:
                time.sleep(0.05)
        last_rc = int(proc.wait())
        out_text = "".join(output_parts)
        if last_rc == 0:
            return last_rc, out_text
        if attempt < max(1, int(retries)):
            if should_stop:
                try:
                    if should_stop(None):
                        break
                except Exception:
                    pass
            time.sleep(max(0.0, float(retry_delay)) * attempt)
    return last_rc, out_text
