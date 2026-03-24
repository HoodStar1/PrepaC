# PrepaC

PrepaC is a self-hosted Docker application for media workflow automation. It helps you prepare media releases, pack them into RAR/PAR2 jobs, post them with Nyuu, and clean up media afterwards.

**Current release:** PrepaC v1.0.0 (Build 2026.03.23-2117)

## Main Features

- **Prepare** TV, movie, and YouTube source content into structured workflow-ready folders
- **Pack** prepared jobs into RAR + PAR2 output sets
- **Post** packed jobs with Nyuu using one or more provider profiles
- **Clean** fully played or previously prepared content
- **Track** job progress, history, and summaries from a browser-based UI
- **Configure** paths, Plex, posting providers, and workflow behavior from the Settings page
- **Authenticate** with a built-in first-run admin setup flow
- **Reference built-in help** from the in-app Help section

## Quick Start

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
      - /mnt:/host_mnt
    restart: unless-stopped
```

Then open:

```text
http://localhost:1234
```

## Production Runtime

PrepaC now starts behind **Gunicorn** instead of Flask's development server.

### Default Gunicorn behavior

The included startup script auto-tunes to the host:

- `workers = (CPU cores × 2) + 1`
- threads scale by host CPU count
- timeout defaults to `120`

Optional overrides:
- `GUNICORN_WORKERS`
- `GUNICORN_THREADS`
- `GUNICORN_TIMEOUT`
- `GUNICORN_GRACEFUL_TIMEOUT`
- `GUNICORN_KEEPALIVE`
- `GUNICORN_BIND`
- `GUNICORN_LOG_LEVEL`

## Notes

- Default prepare end tag: **PrepaC**
- `packing_freeimage_api_key` is for **freeimage.host** thumbnail uploads during packing
- It is **not** a TMDB key
- Built-in end-user guidance is available at **Help**

## Documentation

- In-app: **Help**
- Static docs: `/docs`
- MkDocs config: `mkdocs.yml`


## Final packaging note

This build uses a proper Python package layout for Gunicorn:

- `app/__init__.py` included
- internal imports rewritten to `from app...`
- Gunicorn target: `app.app:app`

Current release: **PrepaC v1.0.0 (Build 2026.03.23-2211)**
