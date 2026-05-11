# Settings

## Important notes

- The default end tag is **PrepaC**.
- Plex is optional.
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

- Session cookies support compatibility-safe modes for self-hosted installs:
	- `legacy` keeps previous behavior (default).
	- `auto` enables secure cookies on HTTPS and trusted proxy HTTPS headers.
	- `always` forces secure cookies for HTTPS-only deployments.
	- `never` keeps non-secure cookies for HTTP-only local/LAN use.
- Metrics can be scraped non-interactively when `PREPAC_METRICS_TOKEN` is set.
	- If no token is set, metrics keep the existing authenticated behavior.
- Prepare permission modes:
	- `legacy_open` (default): dirs `777`, files `666`
	- `shared_safe`: dirs `775`, files `664`
	- `owner_strict`: dirs `750`, files `640`

## Sign-in protection

- Sign-in and password reset apply temporary lockouts after too many failed attempts.
- Limits are configurable with environment variables for your local/LAN or reverse-proxy setup.
