# PrepaC

PrepaC is a self-hosted Docker application for media workflow automation. It helps users prepare media releases, pack them into RAR/PAR2 jobs, post them with Nyuu, and clean up media afterwards through a browser-based interface.

## Features

- **Prepare** TV, movie, and YouTube source content into structured workflow folders
- **Pack** prepared jobs into RAR + PAR2 output sets
- **Post** packed jobs with Nyuu using one or more provider profiles
- **Clean** fully played or previously prepared content
- **Track** job progress, history, and summaries from the web UI
- **Configure** paths, Plex, provider settings, and workflow behavior from the Settings page
- **Authenticate** with a built-in first-run admin setup flow
- **Help** users with a built-in Help section inside the app

## Runtime

PrepaC runs behind **Gunicorn** in production.

Default Gunicorn behavior:
- workers = `(CPU cores × 2) + 1`
- threads scale by host size
- timeout defaults to `120`

Supported override environment variables:
- `GUNICORN_WORKERS`
- `GUNICORN_THREADS`
- `GUNICORN_TIMEOUT`
- `GUNICORN_GRACEFUL_TIMEOUT`
- `GUNICORN_KEEPALIVE`
- `GUNICORN_BIND`
- `GUNICORN_LOG_LEVEL`

## Quick Start

### Docker Compose

```yaml
services:
  prepac:
    build: .
    container_name: prepac
    ports:
      - "1234:1234"
    volumes:
      - ./config:/config
      - ./data/tv:/media/tv
      - ./data/movies:/media/movies
      - ./data/youtube:/media/youtube
      - ./data/destination:/media/dest
    restart: unless-stopped
```

Then open:

```text
http://localhost:1234
```

## Required Mounts

PrepaC expects these container paths:

- `/config`
- `/media/tv`
- `/media/movies`
- `/media/youtube`
- `/media/dest`

## First Run

1. Open the application in your browser.
2. Create the initial admin account.
3. Configure source and destination paths in **Settings**.
4. Optionally connect Plex for posters and watched-state cleanup.
5. Configure posting providers before using the Posting module.

## Core Workflow

```text
Prepare → Packing → Posting → Clean
```

## Settings Notes

- Default prepare end tag: **PrepaC**
- `packing_freeimage_api_key` is for **freeimage.host** thumbnail uploads during packing

## In-App Help

The application includes a Help section that covers:

- Getting Started
- Prepare
- Packing
- Posting
- Clean
- Settings
- Operations

## Static Documentation

This repository also includes docs for GitHub or static hosting:

- `docs/index.md`
- `docs/setup.md`
- `docs/settings.md`
- `docs/workflows.md`
- `docs/operations.md`
- `docs/troubleshooting.md`
- `docs/faq.md`

`mkdocs.yml` is included for static documentation hosting.

## Project Structure

```text
app/         Application code
templates/   HTML templates
static/      CSS and static assets
docs/        Repository documentation
```

## License

This repository includes the MIT License.
