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
