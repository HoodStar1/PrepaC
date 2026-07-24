# Troubleshooting

## The app does not start

Run `prepac --check` for a direct install, or inspect `docker compose logs prepac` for Docker.

- Confirm Python is 3.13 or 3.14.
- Confirm `PREPAC_CONFIG_DIR` exists or can be created and is writable.
- Confirm the installed runtime lock matches the platform.
- Check the logged database migration or schema error before restarting repeatedly.
- On Docker, confirm the host is AMD64 and the Compose service uses `platform: linux/amd64`.

## A runtime tool is missing

Direct installs require `ffmpeg`, `ffprobe`, `mediainfo`, `rar`, `par2`, `node`, and `nyuu` on `PATH`. Close and reopen the terminal or restart the service after changing `PATH`.

On Windows, install command-line versions and verify each command from the same account that runs PrepaC. rsync is not required.

## Prepare copy fails

Check source readability, destination write access, free space, and the Active Prepare Job events.

On Linux, PrepaC can use rsync when it is available. If rsync is absent, it uses the built-in copy path. On Windows, the built-in copy path is expected. A failure is therefore not fixed by installing rsync unless the log specifically shows an rsync attempt.

## Every page fails after a job starts

If normal pages return an application error and `/health` returns `503`, inspect
`docker compose logs --tail=200 prepac` for a SQLite error. Every authenticated
page reads sign-in settings from the same config database, so a database
storage failure affects the whole interface even when the job itself is not
shown on that page.

- Keep `PREPAC_SQLITE_JOURNAL_MODE=DELETE` for Docker bind mounts, Windows, and
  Unraid `/mnt/user` paths. Rebuild and restart so the mode is selected during
  locked startup; PrepaC creates and verifies a retained backup before changing
  an existing database's mode.
- Confirm `/config` is writable, has free space, and is not opened by a second
  PrepaC container.
- Treat `database is locked`, `disk I/O error`, `readonly database`, and missing
  schema messages as storage or permission faults, then correct that condition
  before retrying jobs.
- If starting a queue waits for the configured busy timeout and then reports
  `database is locked`, confirm the container is running a current build with
  worker-local Gunicorn initialization and a single Gunicorn worker. Rebuild
  the image after updating rather than restarting an older container.
- On Unraid, move the complete stopped config directory to a direct cache/pool
  path if errors continue. Follow
  [SQLite storage compatibility](operations.md#sqlite-storage-compatibility);
  never delete SQLite `-wal`/`-shm` files or copy only `prepac.db` while the
  service is running.

## Packing fails

- Run `rar` and `par2` from the service account.
- Confirm RAR licensing and executable access.
- Check free space in both working and output folders.
- Confirm FFmpeg, ffprobe, and MediaInfo can read the input file.

## Posting does not start

Check provider settings, provider order, account groups, priority thresholds, connection ceilings, and path configuration. If every eligible provider is busy, the job remains queued.

For `502 max number of simultaneous IPs` or `482 too many connections`:

- Put provider entries sharing one upstream account in the same Account Group.
- Reduce Upload Connections or increase Connection Headroom.
- Confirm the provider allows the number of source IPs in use.

Run `node --version` and `nyuu --help` from the service account when Nyuu cannot launch.

## Share does not submit

- Confirm the destination is a plain `http://` or `https://` base URL. Enter
  the URL before `/api`; PrepaC 1.5.0 also normalizes one trailing `/api`
  segment for compatibility.
- Put credentials in their dedicated fields, not in the URL.
- A successful Newznab capabilities check does not prove upload support. The
  destination must also permit the nonstandard `nzbadd` API command used for
  Share uploads.
- Check destination and reverse-proxy logs for the corresponding request when
  it returns HTTP 500. As a disposable diagnostic, retry a small test after
  disabling optional NFO, MediaInfo, and metadata multipart fields; do not
  force-retry a real job until you have confirmed the first request did not
  arrive.
- PrepaC never shows or stores the raw remote HTML error page. It records only
  a bounded diagnostic containing safe response metadata and a body hash.
- Redirects, timeouts, 5xx responses, and unverifiable 2xx responses become
  `outcome_unknown` because the destination may have accepted the upload before
  its response failed. Reconcile the destination before using **Force retry**.
- An imported RAR must contain exactly one regular NZB no larger than 16 MiB.
  PrepaC validates the archive and streams that entry without selecting or
  extracting its stored path, so legacy absolute, traversal-like, bracketed,
  wildcard, and leading-dash member names are safe.
- The default imported bundle cap is 128 MiB; increase
  `PREPAC_SHARE_IMPORT_MAX_MB` only when needed.

## A job is finalizing, uploading, or outcome unknown

`finalizing` and `uploading` mean an irreversible local or remote step may be in progress. Cancellation is intentionally unavailable in these states. Do not stop the service or submit the same work again merely because progress appears quiet; review the latest job event first.

Packing also locks cancellation while its status is still `running` once the
job has claimed its output directories. At that point it may already have
cleared prior output. This is deliberate: only a cancellation that commits
before the output claim can guarantee that no destructive reset follows.

`outcome_unknown` means PrepaC could not safely determine whether that step completed. Inspect and reconcile the Prepare destination, Packing outputs, posting provider/NZB records, or Share destination as appropriate. Prepare, Packing, and Posting remain deduplication-blocked until an authenticated administrator uses **Acknowledge and allow resubmission** and types the verification phrase; acknowledgement does not retry the job. Share's confirmed **Force retry** can duplicate a submission that actually succeeded, so use it only after checking the remote destination. See [Job shutdown and recovery](operations.md#job-shutdown-and-recovery) for the full state and restart behavior.

## Sign-in says too many attempts

Wait for the lockout window, then retry. Adjust the auth rate window, attempt limit, or lockout duration only for a documented operational need.

## Plex callback uses the wrong URL

Set `PREPAC_TRUST_PROXY_HEADERS=true` only behind a trusted reverse proxy that sets the external scheme and host. Add the proxy's exact address or CIDR to `PREPAC_TRUSTED_PROXIES`; only loopback is trusted by default. Otherwise leave forwarded-header trust disabled.

## A form says “Security check failed”

Reload the page to obtain a new CSRF token. Confirm the reverse proxy preserves cookies and the app origin. Manual API clients must send the session's `X-CSRF-Token`.

## Clean does not remove files

Check Dry Run, the exact `DELETE` confirmation, filesystem permissions, allowed roots, and recycle-bin configuration.

## Metrics scraping fails

If `PREPAC_METRICS_TOKEN` is set, send it in the `X-Prepac-Metrics-Token` header. Without a token, metrics require the normal authenticated access.
