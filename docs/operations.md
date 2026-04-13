# Operations

## Runtime notes

- Keep `/config` persistent.
- Make sure your mapped media paths are correct before running jobs.
- Check container logs if the app does not start or jobs do not progress.
- Review Share History for completed, failed, cancelled, or removed Share activity.
- Session cookie mode is configurable for self-hosted compatibility:
	- use `legacy`/`never` for HTTP-only local or LAN installs,
	- use `auto`/`always` for HTTPS deployments.
- If `PREPAC_METRICS_TOKEN` is set, `/metrics` accepts token-based scraping.
	- If not set, `/metrics` keeps authenticated access behavior.
- Sign-in/reset protection applies temporary lockouts after repeated failed attempts.
