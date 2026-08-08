# PrepaC 1.5.1

PrepaC is a self-hosted web app for preparing, packing, posting, sharing, and
cleaning media release jobs from one place.

## What it does

- **Prepare** creates working jobs from TV or movie folders.
- **Packing** creates RAR and PAR2 release files.
- **Posting** uploads packed jobs through your configured providers.
- **Share** submits NZBs to supported Newznab-style destinations.
- **Clean** lets you review and remove processed content safely.

PrepaC also includes provider priority rules, single and mass Share import,
Share history and retry controls, and optional Plex cleanup support.

## Supported systems

| Installation | Supported system |
| --- | --- |
| Docker | Linux AMD64 |
| Direct Linux | Linux x86-64 with Python 3.13 or 3.14 |
| Direct Windows | Windows x64 with Python 3.13 or 3.14 |

Docker is the easiest option. macOS and ARM64 are not supported.

## Install with Docker

### What you need

- Docker
- Docker Compose
- Enough free space for the media and working folders

### Installation

1. Clone or download this repository.
2. Open a terminal in the PrepaC folder.
3. Create your private Compose file:

```bash
cp docker-compose.example.yml docker-compose.yml
```

On Windows, copy the file in File Explorer instead.

4. Edit `docker-compose.yml` and change the folder paths if needed.

The `/config` folder stores your settings and database. Keep it persistent.
Media folders are shown inside the container as `/media/...`.

5. Check and start PrepaC:

```bash
docker compose config
docker compose up -d --build
```

6. Open:

```text
http://localhost:1234
```

To view logs:

```bash
docker compose logs --tail=200 -f prepac
```

## Install directly on Linux

Docker is recommended. A direct Linux installation requires:

- Python 3.13 or 3.14
- FFmpeg and ffprobe
- MediaInfo
- RAR
- PAR2
- Node.js and Nyuu
- rsync is optional

Install those tools with your Linux package manager, then run:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --require-hashes -r requirements-linux.txt
python -m pip install --no-deps .
prepac --check
prepac
```

Open `http://localhost:1234`.

The default settings folder is `~/.config/prepac`. To use another folder:

```bash
prepac --config-dir /path/to/prepac-config
```

## Install directly on Windows

A direct Windows installation requires:

- 64-bit Python 3.13 or 3.14
- FFmpeg and ffprobe
- MediaInfo
- RAR
- PAR2
- Node.js and Nyuu

Make sure every tool can be run from PowerShell, then run:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install --require-hashes -r requirements-windows.txt
python -m pip install --no-deps .
prepac --check
prepac
```

Open `http://localhost:1234`.

The default settings folder is `%LOCALAPPDATA%\PrepaC`. To use another folder:

```powershell
prepac --config-dir "D:\PrepaC\Config"
```

## First-time setup

1. Create the first administrator account.
2. Open **Settings**.
3. Set the TV, movie, destination, and workflow folders.
4. Add posting providers if you use Posting.
5. Add a Share destination if you use Share.
6. Add Plex only if you want Plex-assisted cleanup.
7. Run a small test job before processing a large queue.
8. Back up the complete config folder after setup.

## Share setup and errors

### Destination address

Enter the destination's main address, normally the part before `/api`.
PrepaC 1.5.0 also removes one trailing `/api` automatically.

The destination must support the nonstandard Newznab upload command
`t=nzbadd`. A successful connection or category check does not prove that
uploads are supported.

### RAR contains an unsafe member path

This PrepaC error is fixed in 1.5.0.

PrepaC checks that the RAR contains exactly one normal NZB file, then reads that
file without extracting its stored folder name. Older archives containing
absolute paths, `../`, brackets, wildcards, or names beginning with `-` are
therefore handled safely.

### Share returns HTTP 500

HTTP 500 is returned by the destination server or its reverse proxy. PrepaC
1.5.0:

- no longer displays the destination's raw HTML error page;
- fixes addresses that accidentally end in `/api`;
- keeps uncertain uploads as **Outcome unknown** instead of retrying them
  automatically.

If HTTP 500 continues:

1. Check the destination's own logs at the same time as the upload.
2. Confirm the API key is allowed to upload with `t=nzbadd`.
3. Confirm the selected category accepts uploads.
4. For a small disposable test, turn off optional NFO, MediaInfo, and metadata
   fields in the Share destination settings.
5. Check the destination before using **Force retry**. The first upload may
   already have arrived.

PrepaC cannot correct a server-side 500 response when the destination does not
support or permit NZB uploads.

## Update PrepaC

Always stop PrepaC and copy the complete config folder before updating.

### Docker

Replace or update the repository files, keep your private
`docker-compose.yml`, then run:

```bash
docker compose up -d --build --force-recreate prepac
```

PrepaC is built from the local files. `docker compose pull` does not update it.

### Linux or Windows

Replace the source files, activate the existing virtual environment, reinstall
the matching requirements file, and run:

```text
python -m pip install --no-deps .
prepac --check
```

Then start PrepaC normally.

## Remove PrepaC

For Docker:

```bash
docker compose down
```

Delete the application folder when you no longer need it. Delete the config
folder only if you also want to permanently remove all settings and history.

For a direct installation, stop PrepaC and remove the application folder and
virtual environment. The config folder can be kept for a future installation.

## Help

- Health check: `http://localhost:1234/health`
- Docker logs: `docker compose logs --tail=200 prepac`
- More help is available in the `docs` folder.

Never publish `docker-compose.yml`, the config folder, databases, logs,
passwords, API keys, or tokens.

## License

PrepaC is licensed under the GNU General Public License v3.0. See `LICENSE`.
