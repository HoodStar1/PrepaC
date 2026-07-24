"""Cross-platform command-line launcher for PrepaC."""

from __future__ import annotations

import argparse
import os
import platform as platform_module
import shutil
import sys
import sysconfig
from collections.abc import Mapping, Sequence
from pathlib import Path

REQUIRED_TOOLS = (
    ("ffmpeg", ("ffmpeg",)),
    ("ffprobe", ("ffprobe",)),
    ("MediaInfo", ("mediainfo",)),
    ("RAR", ("rar",)),
    ("PAR2", ("par2",)),
    ("Node.js", ("node",)),
    ("Nyuu", ("nyuu",)),
)

SUPPORTED_X64_MACHINES = {"amd64", "x86_64"}


def validate_supported_platform(
    platform_name: str | None = None,
    machine_name: str | None = None,
) -> str:
    """Return ``linux`` or ``windows`` and reject unsupported host targets."""
    platform_value = (sys.platform if platform_name is None else platform_name).lower()
    machine_value = (platform_module.machine() if machine_name is None else machine_name).strip().lower()
    if machine_value not in SUPPORTED_X64_MACHINES:
        raise RuntimeError("PrepaC supports AMD64/x86-64 hosts only; ARM64 is not supported")
    if platform_value.startswith("linux"):
        return "linux"
    if platform_value == "win32":
        return "windows"
    raise RuntimeError("PrepaC direct installation supports Linux and Windows only; macOS is not supported")


def default_config_dir(
    environ: Mapping[str, str] | None = None,
    platform_name: str | None = None,
    home: Path | None = None,
) -> Path:
    """Return the platform-native persistent configuration directory."""
    env = os.environ if environ is None else environ
    override = env.get("PREPAC_CONFIG_DIR")
    if override:
        return Path(override).expanduser()

    platform_value = sys.platform if platform_name is None else platform_name
    home_path = Path.home() if home is None else home
    if platform_value == "win32":
        local_app_data = env.get("LOCALAPPDATA")
        base = Path(local_app_data) if local_app_data else home_path / "AppData" / "Local"
        return base / "PrepaC"

    xdg_config_home = env.get("XDG_CONFIG_HOME")
    base = Path(xdg_config_home).expanduser() if xdg_config_home else home_path / ".config"
    return base / "prepac"


def asset_root(module_file: str | Path | None = None, data_root: str | Path | None = None) -> Path:
    """Locate templates and static assets in a checkout or installed wheel."""
    module_path = Path(__file__ if module_file is None else module_file).resolve()
    source_root = module_path.parent
    if (source_root / "templates").is_dir() and (source_root / "static").is_dir():
        return source_root

    install_data = Path(sysconfig.get_path("data") if data_root is None else data_root)
    installed_root = install_data / "share" / "prepac"
    if (installed_root / "templates").is_dir() and (installed_root / "static").is_dir():
        return installed_root

    raise RuntimeError(
        "PrepaC web assets were not found. Reinstall the package or run from a complete source checkout."
    )


def create_app():
    """Load the Flask application and bind it to packaged web assets."""
    from app.app import app

    assets = asset_root()
    app.template_folder = str(assets / "templates")
    app.static_folder = str(assets / "static")
    return app


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _default_threads() -> int:
    cpu_count = os.cpu_count() or 2
    if cpu_count >= 16:
        return 8
    if cpu_count >= 8:
        return 6
    if cpu_count >= 4:
        return 4
    return 2


def _gunicorn_worker_count() -> int:
    workers = _env_int("GUNICORN_WORKERS", 1, 1)
    if workers != 1:
        raise ValueError(
            "GUNICORN_WORKERS must be 1 because PrepaC queue ownership is process-local; "
            "use GUNICORN_THREADS for concurrency"
        )
    return workers


def _run_gunicorn(host: str, port: int) -> None:
    try:
        from gunicorn.app.base import BaseApplication
    except ImportError as exc:
        raise RuntimeError(
            "Gunicorn is unavailable. Install requirements-linux.txt on Linux."
        ) from exc

    options: dict[str, object] = {
        "bind": os.environ.get("GUNICORN_BIND", f"{host}:{port}"),
        # Background jobs and active-job tracking are process-local.
        "workers": _gunicorn_worker_count(),
        "worker_class": "gthread",
        # Import the application after Gunicorn forks. Importing it in the
        # master would start reconciliation and file-system watcher threads in
        # a second process, which can contend with a worker's SQLite access on
        # Docker/FUSE-backed configuration storage.
        "preload_app": False,
        "threads": _env_int("GUNICORN_THREADS", _default_threads(), 1),
        "timeout": _env_int("GUNICORN_TIMEOUT", 120, 1),
        "graceful_timeout": _env_int("GUNICORN_GRACEFUL_TIMEOUT", 30, 1),
        "keepalive": _env_int("GUNICORN_KEEPALIVE", 5, 1),
        "max_requests": _env_int("GUNICORN_MAX_REQUESTS", 0, 0),
        "max_requests_jitter": _env_int("GUNICORN_MAX_REQUESTS_JITTER", 100, 0),
        "accesslog": "-",
        "errorlog": "-",
        "loglevel": os.environ.get("GUNICORN_LOG_LEVEL", "info"),
    }
    worker_tmp_dir = os.environ.get("GUNICORN_WORKER_TMP_DIR")
    if worker_tmp_dir:
        options["worker_tmp_dir"] = worker_tmp_dir
    elif Path("/dev/shm").is_dir():
        options["worker_tmp_dir"] = "/dev/shm"

    class PrepaCApplication(BaseApplication):
        def load_config(self) -> None:
            for key, value in options.items():
                if key in self.cfg.settings and value is not None:
                    self.cfg.set(key, value)

        def load(self):
            return create_app()

    PrepaCApplication().run()


def _run_waitress(flask_app, host: str, port: int) -> None:
    try:
        from waitress import serve
    except ImportError as exc:
        raise RuntimeError(
            "Waitress is unavailable. Install requirements-windows.txt on Windows."
        ) from exc

    serve(
        flask_app,
        host=host,
        port=port,
        threads=_env_int("WAITRESS_THREADS", max(4, _default_threads()), 1),
    )


def check_runtime_tools(platform_name: str | None = None) -> list[str]:
    """Return missing executable names for a direct host installation."""
    del platform_name  # Reserved for future platform-specific tool aliases.
    missing = []
    for display_name, candidates in REQUIRED_TOOLS:
        if not any(shutil.which(candidate) for candidate in candidates):
            missing.append(display_name)
    return missing


def _version() -> str:
    from app.version import FULL_VERSION

    return FULL_VERSION


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the PrepaC web application")
    parser.add_argument("--host", default=os.environ.get("PREPAC_HOST", "0.0.0.0"))
    parser.add_argument(
        "--port",
        type=int,
        default=_env_int("PREPAC_PORT", 1234, 1),
    )
    parser.add_argument(
        "--server",
        choices=("auto", "gunicorn", "waitress"),
        default="auto",
        help="auto selects Gunicorn on Linux and Waitress on Windows",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=None,
        help="persistent config directory (also available as PREPAC_CONFIG_DIR)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="check external runtime tools without starting the server",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {_version()}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        platform_kind = validate_supported_platform()
        config_dir = (args.config_dir or default_config_dir()).expanduser().resolve()
        os.environ["PREPAC_CONFIG_DIR"] = str(config_dir)

        if args.check:
            missing = check_runtime_tools()
            print(f"Config directory: {config_dir}")
            print(f"Web assets: {asset_root()}")
            if missing:
                print("Missing required tools: " + ", ".join(missing), file=sys.stderr)
                return 1
            print("All required external tools are available.")
            return 0

        config_dir.mkdir(parents=True, exist_ok=True)
        server = args.server
        if server == "auto":
            server = "waitress" if platform_kind == "windows" else "gunicorn"
        if (platform_kind == "windows" and server != "waitress") or (
            platform_kind == "linux" and server != "gunicorn"
        ):
            raise ValueError(f"{server} is not the supported server for {platform_kind}")
        if server == "waitress":
            _run_waitress(create_app(), args.host, args.port)
        else:
            _run_gunicorn(args.host, args.port)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"PrepaC startup failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
