import subprocess
import time
import os
import signal


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


def run_command_with_output(cmd, cwd=None, retries: int = 1, retry_delay: float = 1.0, on_output=None, on_tick=None, tick_seconds: float = 1.0, text: bool = True, start_new_session: bool = False, should_stop=None):
    attempt = 0
    last_rc = 1
    out_text = ""
    while attempt < max(1, int(retries)):
        attempt += 1
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=text,
            bufsize=1,
            start_new_session=start_new_session,
        )
        output_parts = []
        last_tick = 0.0
        while True:
            line = proc.stdout.readline() if proc.stdout else ""
            if line:
                output_parts.append(line)
                if on_output:
                    on_output(line)
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
            if proc.poll() is not None:
                if proc.stdout:
                    rest = proc.stdout.read()
                    if rest:
                        output_parts.append(rest)
                        if on_output:
                            on_output(rest)
                break
            time.sleep(0.1)
        last_rc = int(proc.wait())
        out_text = "".join(output_parts)
        if last_rc == 0:
            return last_rc, out_text
        if attempt < max(1, int(retries)):
            time.sleep(max(0.0, float(retry_delay)) * attempt)
    return last_rc, out_text
