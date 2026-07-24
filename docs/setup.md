# Setup

## Docker on Linux AMD64

Docker is the recommended installation because the image includes all native tools.

1. Install Docker Engine and Docker Compose on an AMD64 Linux host.
2. Copy `docker-compose.example.yml` to `docker-compose.yml`.
   Existing deployments should merge changes instead of overwriting their operator-specific Compose file. Optional `PREPAC_UID` and `PREPAC_GID` values run the service under numeric non-root IDs; leave them unset for the backward-compatible root default, or make every mount writable by the selected IDs first.
3. Review every host path under `volumes`.
4. Keep the `./config:/config` mapping persistent.
5. Build and start:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f prepac
```

6. Open <http://localhost:1234> and create the first admin account.

The image is intentionally `linux/amd64` only. Do not remove the Compose `platform` entry to imply another supported architecture.

## Direct Linux x86-64

Install Python 3.13 or 3.14 plus FFmpeg/ffprobe, MediaInfo CLI, RAR 7.23, PAR2, Node.js 24, and Nyuu 0.4.2. rsync is optional and accelerates large copies when available.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade "pip==26.1.2"
python -m pip install --require-hashes -r requirements-linux.txt
python -m pip install --no-deps .
prepac --check
prepac
```

The default config directory is `$XDG_CONFIG_HOME/prepac` or `~/.config/prepac`. Gunicorn listens on `0.0.0.0:1234` by default.

## Direct Windows x64

Install Python 3.13 or 3.14 x64. Install FFmpeg/ffprobe, MediaInfo CLI, RAR 7.23, PAR2, Node.js 24, and Nyuu 0.4.2, and add their executable directories to `PATH`. rsync is not required.

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade "pip==26.1.2"
python -m pip install --require-hashes -r requirements-windows.txt
python -m pip install --no-deps .
prepac --check
prepac
```

The default config directory is `%LOCALAPPDATA%\PrepaC`. Waitress listens on `0.0.0.0:1234` by default.

If PowerShell blocks activation, invoke `.venv\Scripts\python.exe` and `.venv\Scripts\prepac.exe` directly instead of changing the machine execution policy.

## Launcher options

```text
prepac --host 127.0.0.1 --port 1234
prepac --config-dir /srv/prepac/config
prepac --server gunicorn
prepac --check
prepac --version
```

`--server auto` is the default and selects Gunicorn on Linux or Waitress on Windows.

## First run

1. Create the first administrator.
2. Save all workflow paths in Settings.
3. Configure posting providers and optional Share destinations.
4. Test with small disposable input.
5. Back up the config directory before the first upgrade.
