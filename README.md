# PrepaC

PrepaC is a self-hosted web app for preparing, packing, posting, sharing, and cleaning NZB release jobs from one interface.

## What it does

PrepaC organizes the workflow into clear stages:
- **Prepare**: create working jobs from TV or movie source folders
- **Packing**: build release output from prepared jobs
- **Posting**: upload packed jobs with your configured providers
- **Share**: submit NZBs to Newznab-compatible destinations
- **Clean**: review and remove processed content

## Main features

- TV and movie preparation flows
- Packing and posting job management
- Dynamic posting provider management in Settings
- Per-provider small-job priority routing with “Prioritize jobs up to (GB)” for providers after Provider 1
- Share destinations with category detection and manual override
- Single import and mass import for Share
- Generated NFO and metadata XML for Share submissions
- Active Share job controls with cancel or remove actions
- Share history and retry support
- Optional Plex integration for cleanup workflows
- Docker-based deployment

## Requirements

- Docker and Docker Compose
- Persistent storage for `/config`
- Your own source, destination, and output paths
- Posting provider credentials if you want to use Posting
- Newznab-compatible destination details if you want to use Share

## Quick start

1. Clone this repository.
2. Review `docker-compose.yml`.
3. Make sure `/config` is persistent.
4. Build and start the app:

```bash
docker compose up -d --build
```

5. Open the app in your browser:

```text
http://localhost:1234
```

## First-time setup

1. Create the first admin account.
2. Open **Settings**.
3. Configure your paths.
4. Configure one or more posting providers if you want to use Posting.
5. Configure Share destinations if you want to use Share.

## How to use PrepaC

### Prepare
Scan your TV or movie roots and create working jobs.

### Packing
Build release output from prepared jobs.

### Posting
Start uploads from packed jobs. For providers after Provider 1, use **Prioritize jobs up to (GB)** to prefer smaller jobs on specific providers first. Set the value to **0** to keep that provider in the same default availability pool as Provider 1.

### Share
Submit successful posting output or import RARred NZB + template bundles for submission. Active Share jobs can be cancelled or removed from the Share screen, and completed or cancelled jobs are available in Share History.

### Clean
Review deletion candidates before removing content.

## Important notes

- Keep `/config` persistent across upgrades.
- Share uploads depend on the limits and rules of the destination indexer.
- Some Share destinations may rate-limit uploads or API usage.
- Share mass import can pair files by filename and by template content.
- If a Share candidate is removed, re-importing the same bundle creates a fresh candidate again.

## Security and Compatibility Controls

PrepaC is designed for self-hosted Docker environments. Security hardening is configurable so LAN and HTTP-only setups remain supported.

- Session cookie mode (`PREPAC_SESSION_COOKIE_MODE`):
	- `legacy` (default): preserves previous behavior (`PREPAC_SESSION_COOKIE_SECURE`, default false)
	- `auto`: secure cookies for HTTPS requests; can trust proxy headers when `PREPAC_TRUST_PROXY_HEADERS=true`
	- `always`: always secure cookies (recommended for HTTPS-only deployments)
	- `never`: always non-secure cookies (HTTP-only local/LAN installs)
- Reverse proxy header trust (`PREPAC_TRUST_PROXY_HEADERS`):
	- `false` (default): ignore `X-Forwarded-Proto` and `X-Forwarded-Host` for generated external URLs
	- `true`: trust forwarded proto and host from your reverse proxy
- Share import upload cap (`PREPAC_SHARE_IMPORT_MAX_MB`, default `512`):
	- Limits single and bulk Share import request size
	- Increase only if your import bundles genuinely require it
- Metrics scrape token (`PREPAC_METRICS_TOKEN`):
	- When unset: `/metrics` keeps existing authenticated behavior.
	- When set: `/metrics` accepts `X-Prepac-Metrics-Token` header or `?token=...` for non-interactive scraping.
- Auth abuse controls:
	- `PREPAC_AUTH_RATE_WINDOW_SECONDS` (default `300`)
	- `PREPAC_AUTH_RATE_MAX_ATTEMPTS` (default `20`)
	- `PREPAC_AUTH_LOCKOUT_SECONDS` (default `600`)
	- Applies to sign-in and password reset attempts per user/IP key.
- Prepare permissions mode (`prepare_permissions_mode` setting or `PREPAC_PREPARE_PERMISSIONS_MODE` env):
	- `legacy_open` (default): dirs `777`, files `666` for backward compatibility.
	- `shared_safe`: dirs `775`, files `664`.
	- `owner_strict`: dirs `750`, files `640`.

### Self-hosted hardening guidance

- If using reverse proxy TLS, prefer `PREPAC_SESSION_COOKIE_MODE=always`.
- Keep mount scopes minimal and avoid broad host mounts when possible.
- For non-root container operation, verify ownership/permissions of mounted paths before switching runtime user.

## License

Licensed under the GNU General Public License v3.0. See the `LICENSE` file for details.
