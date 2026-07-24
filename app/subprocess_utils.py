import os
import queue
import signal
import subprocess
import threading
import time
from collections import deque


DEFAULT_MAX_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_PARTIAL_LINE_BYTES = 64 * 1024


def _signal_own_process_group(proc, sig):
    if not hasattr(os, "killpg"):
        return False
    try:
        pgid = os.getpgid(proc.pid)
    except Exception:
        return False
    if pgid != proc.pid:
        return False
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return True
    except Exception:
        return False


def terminate_process(proc, graceful_timeout: float = 5.0):
    if proc.poll() is not None:
        return
    sent_group_term = _signal_own_process_group(proc, signal.SIGTERM)
    try:
        if not sent_group_term:
            proc.terminate()
        proc.wait(timeout=max(0.1, float(graceful_timeout)))
        return
    except Exception:
        pass
    if _signal_own_process_group(proc, signal.SIGKILL):
        try:
            proc.wait(timeout=2.0)
        except Exception:
            pass
        return
    if os.name == "nt":
        # Popen.kill() only terminates the direct Windows child. Native tools
        # may have spawned workers, so terminate the verified numeric tree.
        try:
            subprocess.run(
                ["taskkill", "/PID", str(int(proc.pid)), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            proc.wait(timeout=2.0)
            return
        except Exception:
            pass
    try:
        proc.kill()
    except Exception:
        pass


def run_command_with_output(
    cmd,
    cwd=None,
    retries: int = 1,
    retry_delay: float = 1.0,
    on_output=None,
    on_tick=None,
    tick_seconds: float = 1.0,
    text: bool = True,
    start_new_session: bool = False,
    should_stop=None,
    on_proc_start=None,
    inactivity_timeout_seconds: float = 0.0,
    runtime_timeout_seconds: float = 0.0,
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
    fail_on_output_limit: bool = False,
):
    import re as _re

    attempt = 0
    last_rc = 1
    out_text = ""
    while attempt < max(1, int(retries)):
        attempt += 1
        popen_kwargs = {}
        if os.name == "nt" and start_new_session:
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kwargs["start_new_session"] = start_new_session
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=0,
            **popen_kwargs,
        )
        if on_proc_start:
            try:
                on_proc_start(proc)
            except Exception:
                pass

        output_parts = deque()
        output_chars = 0
        output_limit_exceeded = False
        max_output_chars = max(1024, int(max_output_bytes or DEFAULT_MAX_OUTPUT_BYTES))

        def remember_output(value):
            nonlocal output_chars, output_limit_exceeded
            value = str(value or "")
            if not value:
                return
            output_parts.append(value)
            output_chars += len(value)
            if output_chars > max_output_chars:
                output_limit_exceeded = True
                if fail_on_output_limit and proc.poll() is None:
                    terminate_process(proc, graceful_timeout=0.5)
            while output_parts and output_chars > max_output_chars:
                overflow = output_chars - max_output_chars
                first = output_parts[0]
                if len(first) <= overflow:
                    output_parts.popleft()
                    output_chars -= len(first)
                else:
                    output_parts[0] = first[overflow:]
                    output_chars -= overflow
                    break

        last_tick = 0.0
        leftover = b""
        started_ts = time.time()
        last_output_ts = started_ts
        out_queue = queue.Queue(maxsize=256)

        def reader_thread():
            try:
                while True:
                    chunk = proc.stdout.read(4096) if proc.stdout else b""
                    if chunk:
                        out_queue.put(chunk)
                        continue
                    break
            except Exception:
                pass
            finally:
                out_queue.put(None)

        reader = threading.Thread(target=reader_thread, daemon=True)
        reader.start()
        inactivity_limit = float(inactivity_timeout_seconds or 0)
        runtime_limit = float(runtime_timeout_seconds or 0)
        reader_finished = False
        while True:
            try:
                chunk = out_queue.get(timeout=0.05)
            except queue.Empty:
                chunk = b""
            if chunk is None:
                reader_finished = True
                chunk = b""
            if chunk:
                leftover += chunk
                last_output_ts = time.time()
                if len(leftover) > MAX_PARTIAL_LINE_BYTES and b"\r" not in leftover and b"\n" not in leftover:
                    prefix = leftover[:-MAX_PARTIAL_LINE_BYTES]
                    leftover = leftover[-MAX_PARTIAL_LINE_BYTES:]
                    segment = prefix.decode("utf-8", errors="replace")
                    remember_output(segment)
                    if on_output:
                        on_output(segment)
                segments = _re.split(b"(\r\n|\r|\n)", leftover)
                complete = []
                index = 0
                while index + 1 < len(segments):
                    complete.append(
                        segments[index].decode("utf-8", errors="replace")
                        + segments[index + 1].decode("utf-8", errors="replace")
                    )
                    index += 2
                leftover = segments[-1] if len(segments) % 2 == 1 else b""
                for segment in complete:
                    remember_output(segment)
                    if on_output:
                        on_output(segment)

            now_ts = time.time()
            if on_tick and now_ts - last_tick >= tick_seconds:
                on_tick()
                last_tick = now_ts
            if should_stop:
                try:
                    if should_stop(proc):
                        terminate_process(proc)
                except Exception:
                    pass
            if inactivity_limit and proc.poll() is None and time.time() - last_output_ts >= inactivity_limit:
                remember_output("\n[PrepaC terminated command after output inactivity timeout]\n")
                terminate_process(proc)
                inactivity_limit = 0.0
            if runtime_limit and proc.poll() is None and time.time() - started_ts >= runtime_limit:
                remember_output("\n[PrepaC terminated command after runtime timeout]\n")
                terminate_process(proc)
                runtime_limit = 0.0
            if reader_finished and proc.poll() is not None:
                if proc.stdout:
                    rest = proc.stdout.read()
                    if rest:
                        leftover += rest
                if leftover:
                    segment = leftover.decode("utf-8", errors="replace")
                    remember_output(segment)
                    if on_output:
                        on_output(segment)
                break
            if not chunk:
                time.sleep(0.05)

        last_rc = int(proc.wait())
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        reader.join(timeout=1.0)
        out_text = "".join(output_parts)
        if output_limit_exceeded and fail_on_output_limit:
            raise RuntimeError("Command output exceeded the configured safety limit")
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
