import os
from pathlib import Path

try:
    import fcntl
except Exception:
    fcntl = None

try:
    import msvcrt
except Exception:
    msvcrt = None


def _ensure_lockfile_byte(handle):
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    handle.seek(0)


def _lock_handle(handle):
    if fcntl is not None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return
    if msvcrt is not None:
        _ensure_lockfile_byte(handle)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    raise OSError("File locking is not supported on this platform")


def try_acquire_lock(lock_path):
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+b")
    try:
        _lock_handle(handle)
    except OSError:
        try:
            handle.close()
        except Exception:
            pass
        return None
    return handle


def release_lock(handle):
    if handle is None:
        return
    try:
        if not handle.closed:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:
                _ensure_lockfile_byte(handle)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    except Exception:
        pass
    finally:
        try:
            handle.close()
        except Exception:
            pass
