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
2. Review `docker-compose.example.yml` or `docker-compose.yml`.
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

## License

Licensed under the GNU General Public License v3.0. See the `LICENSE` file for details.
