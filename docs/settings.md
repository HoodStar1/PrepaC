# Settings

## Important notes

- The default end tag is **PrepaC**.
- Plex is optional.
- Configure the final `http://` or `https://` Plex server URL. Authenticated Plex requests do not follow redirects, and response bodies are size-limited so a redirect cannot receive the token and an oversized response cannot exhaust application memory.
- The Freeimage key is only for **freeimage.host** image uploads during packing.
- Save your path settings before running Prepare, Packing, Posting, or Share.

## Posting providers

- Posting providers are managed from a dynamic builder in Settings.
- Existing Provider 1 and Provider 2 setups carry forward automatically for current users.
- You can add, remove, clone, and reorder providers from the builder.
- Set posting providers before starting uploads from Posting.
- For providers after Provider 1, use **Prioritize jobs up to (GB)** to prefer smaller jobs on those providers first.
- Set the value to **0** to keep that provider in the same default availability pool as Provider 1.
- Posting runs one active job per enabled provider, so three enabled providers can run three posting jobs at the same time.
- Set the same **Account Group** on provider entries that share one upstream account, subscription, or simultaneous-IP limit. Those entries remain separate providers, but only one job can use that account group at a time.
- **Upload Connections** is the requested Nyuu upload connection count for that provider.
- **Account Max Connections** is the provider account/server ceiling. PrepaC never asks Nyuu for more than this value.
- **Connection Headroom** is reserved from each provider's configured max connections before starting Nyuu. The default is `2`, so a provider configured for `25` max connections posts with `23` connections.
- **Failure Cooldown** applies only to the provider that reports an NNTP connection failure, including `482 too many connections`.
- **Disconnect Drain** briefly keeps only the provider that reported disconnect timeout warnings out of reuse so the NNTP server has time to release old sessions.

## Workflow folders

- **Prepare Destination Folder** controls where prepared media is copied.
- Prepare jobs run in the order they were selected and queued.
- Packing and Posting also claim queued jobs in FIFO order.
- If the Prepare destination does not have enough free space for the selected job, the running job pauses with a **waiting for space** message and resumes automatically once free space is available.
- **Packing Watch Folder** can be set separately. If it is blank, or still has the legacy `/media/dest` default while Prepare uses a different destination, Packing scans the Prepare destination.
- **Packing Output Folder** controls packed archives and generated output files. Posting scans this folder unless **Posting Scan Folder** is set.
- **Posted Output Root** and **NZB RAR Root** control Posting output.
- **Share Scan Folder** can be set separately. If it is blank, Share scans the Posted Output Root and also uses successful Posting history.
- Existing `/media/dest` installs continue to work without changing any settings.

## Share destinations

- Share destinations support multiple targets, category overrides, and optional attachments.
- Refresh destination categories after adding or changing a destination.

## Security and compatibility

- SQLite uses `DELETE` rollback-journal mode by default for compatibility with
  Docker bind mounts, Windows, and FUSE-backed storage such as Unraid user
  shares. Set `PREPAC_SQLITE_JOURNAL_MODE=WAL` only when `/config` is on a
  local filesystem with reliable file locking and shared-memory support.
- `PREPAC_SQLITE_BUSY_TIMEOUT_MS` controls how long a connection waits for a
  database lock. The default is 15000 milliseconds; supported values are 1000
  through 60000.
- Session cookies support compatibility-safe modes for self-hosted installs:
	- `legacy` keeps the previous fixed-cookie behavior.
	- `auto` (default) enables secure cookies on direct HTTPS and trusted proxy HTTPS headers while preserving LAN HTTP.
	- `always` forces secure cookies for HTTPS-only deployments.
	- `never` keeps non-secure cookies for HTTP-only local/LAN use.
- Metrics can be scraped non-interactively when `PREPAC_METRICS_TOKEN` is set.
	- If no token is set, metrics keep the existing authenticated behavior.
- Forwarded headers are disabled unless `PREPAC_TRUST_PROXY_HEADERS=true`.
	- `PREPAC_TRUSTED_PROXIES` is a comma-separated CIDR allowlist and defaults to `127.0.0.1/32,::1/128`.
	- Add the exact Docker/reverse-proxy network explicitly; private LAN ranges are not implicitly trusted.
	- `PREPAC_TRUSTED_HOSTS` can restrict accepted public hostnames.
- Prepare permission modes:
	- `legacy_open` (default): dirs `777`, files `666`
	- `shared_safe`: dirs `775`, files `664`
	- `owner_strict`: dirs `750`, files `640`

## Sign-in protection

- Sign-in and password reset apply temporary lockouts after too many failed attempts.
- Limits are configurable with environment variables for your local/LAN or reverse-proxy setup.
- Defaults are 20 failures in 300 seconds followed by a 600-second lockout; successful authentication clears the relevant counters.
- Existing blank, known-default, or policy-weak credentials are preserved to avoid lockout, but the next successful sign-in is restricted to the password-change flow until a password of at least 12 characters is saved.
- Password and recovery-secret changes revoke existing signed-in sessions through the persisted session epoch.

## Secret inputs

- A blank secret field preserves the existing saved value; use the explicit clear control to remove a saved value.
- Supported environment variables and `/run/secrets` files take precedence over saved values and cannot be overwritten from the interface.
- Secret values are not rendered back into HTML, job JSON, event streams, or operational logs.
