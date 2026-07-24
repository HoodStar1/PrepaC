# Operations

## Persistence and backups

The config directory contains the database, settings, credentials, and job history. Keep it persistent and restrict it to the service account.

- Docker: `/config`
- Direct Linux: `$XDG_CONFIG_HOME/prepac` or `~/.config/prepac`
- Direct Windows: `%LOCALAPPDATA%\PrepaC`

Before an upgrade:

1. Stop or finish active Prepare, Packing, Posting, Share, and Clean jobs.
2. Stop the service.
3. Copy the complete config directory to a separate backup location.
4. Update and start PrepaC.
5. Check `/health`, logs, settings, and job history before removing the backup.

Schema upgrades are automatic. Before changing an existing schema, PrepaC creates a retained timestamped SQLite backup, validates it, applies the migration transactionally, and verifies database integrity before commit. A verified backup is also retained before changing an existing database's journal mode. No manual data migration is required. If startup reports a migration failure, stop the service and restore the named pre-change backup before retrying.

## SQLite storage compatibility

PrepaC defaults to SQLite `DELETE` rollback-journal mode. It is selected once
during locked startup rather than on every connection, making it suitable for
Docker bind mounts and FUSE-backed paths such as Unraid user shares. `WAL`
remains available through `PREPAC_SQLITE_JOURNAL_MODE=WAL`, but use it only
when the config directory is on a local filesystem with reliable SQLite
locking and shared-memory support. The connection lock wait defaults to 15000
milliseconds and can be set from 1000 through 60000 with
`PREPAC_SQLITE_BUSY_TIMEOUT_MS`.

On Unraid, prefer a direct cache or pool path such as
`/mnt/cache/Systems/appdata/prepac` for the `/config` bind mount instead of a
FUSE `/mnt/user/...` path. Move it safely:

1. Finish or stop active jobs, then stop the PrepaC container.
2. Copy the complete config directory to the direct pool path. Include
   `prepac.db`, any `prepac.db-wal` and `prepac.db-shm` sidecars, the Flask
   secret, locks, and migration backups. Never copy only `prepac.db` while the
   service is running.
3. Verify the copied ownership and permissions, then update only the
   `/config` volume source.
4. Start PrepaC and verify `/health`, container logs, settings, and job history.
5. Keep the stopped original directory untouched until the new location has
   been validated.

Do not manually delete SQLite `-wal` or `-shm` files. They can contain
committed database state and must be handled with the database while PrepaC is
stopped.

## Docker lifecycle

```bash
docker compose ps
docker compose logs --tail=200 prepac
docker compose pull
docker compose up -d --build
docker compose stop
```

The health check requests `http://localhost:1234/health`. Keep the `linux/amd64` platform declaration and the persistent `/config` mapping.

## Direct lifecycle

Start with `prepac`; use the host service manager for unattended operation. Preserve `PREPAC_CONFIG_DIR`, `PATH`, and any proxy or security environment variables in the service definition.

Linux uses Gunicorn with exactly one worker because job state is process-local; use `GUNICORN_THREADS` for concurrency. PrepaC rejects `GUNICORN_WORKERS` values other than `1`. It is imported inside the worker so the SQLite reconciliation and watcher services are not duplicated in the Gunicorn master process. Keep Gunicorn preloading disabled. Windows uses Waitress and can tune `WAITRESS_THREADS`.

## Job shutdown and recovery

On a graceful stop, PrepaC enters a draining state, blocks new job starts and retries, and waits for process-local workers. The wait defaults to 10 seconds and can be set with `PREPAC_SHUTDOWN_DRAIN_SECONDS`; values are limited to 0 through 120 seconds. Configure the Docker or host-service stop grace period to be longer than this drain interval. The health endpoint returns `503` with a `draining` status while shutdown is in progress.

Job states have deliberately different recovery behavior:

| State | Meaning | Safe action |
| --- | --- | --- |
| `queued` | Waiting for an in-memory worker or capacity slot | May be cancelled while this process owns the queue |
| `running` | Work is active before its irreversible completion step | May be cancelled; an abandoned stale job is marked failed. Packing is the exception after it atomically claims its output reset: from that point, prior output may already be cleared and cancellation is locked for that attempt. |
| `finalizing` | Prepare, Packing, or Posting has entered an irreversible or externally visible completion step | Do not cancel or resubmit; wait for completion |
| `uploading` | A Share request may already be reaching the remote destination | Do not cancel or resubmit; wait for completion |
| `outcome_unknown` | PrepaC lost reliable confirmation during `finalizing` or `uploading` | Keep retry blocked, inspect the destination, then use the explicit acknowledgement only after reconciliation |

Queue workers are process-local. At startup, jobs left `queued` by a previous process are marked failed instead of being replayed automatically. Likewise, stale `running` jobs are marked failed, while stale `finalizing` and `uploading` jobs become `outcome_unknown`. This conservative recovery prevents a restart from silently repeating a file commit, post, or remote submission.

For Packing, path and source preflight remains cancellable. Immediately before
clearing either output directory, PrepaC atomically records output-reset
ownership in the job row. A concurrent cancellation and that ownership claim
cannot both succeed: cancellation that commits first prevents all output
cleanup, while ownership that commits first prevents cancellation and protects
the attempt from being reported as cancelled after destructive work began.

For an `outcome_unknown` job, check the workflow's destination and the job events:

- Prepare: verify the destination tree and expected media files.
- Packing: verify the archive, parity files, and output folder.
- Posting: verify the provider result and generated NZB/output records.
- Share: search the selected destination for the submission before retrying.

For Prepare, Packing, and Posting, `outcome_unknown` remains part of the active deduplication set. A new submission for the same source is blocked until an authenticated administrator explicitly releases it:

1. Inspect the destination and job events listed above.
2. Reconcile any complete, partial, or remote output. Do not use acknowledgement as a substitute for this check.
3. On the affected workflow page, choose **Acknowledge and allow resubmission**.
4. Read the warning and type `I VERIFIED THE DESTINATION` exactly. The request is authenticated and CSRF-protected.
5. Build a new Prepare preview or rescan Packing/Posting, then submit fresh work manually only if it is safe.

Acknowledgement changes only the ambiguous job to a failed, manually reconciled record; it does not automatically retry or claim that the first operation failed. A fresh submission can duplicate output or externally visible effects if reconciliation was incomplete.

Share offers a separate explicit **Force retry** action after confirmation. Use it only after checking the remote destination: the original upload may have succeeded, so forcing a retry can create a duplicate.

## Logs and health

- Set `PREPAC_LOG_LEVEL` to `DEBUG`, `INFO`, `WARNING`, or `ERROR`.
- Set `PREPAC_LOG_JSON=true` for structured logs.
- Check `/health` after startup and upgrades.
- If `PREPAC_METRICS_TOKEN` is set, send it in `X-Prepac-Metrics-Token` for non-interactive `/metrics` scraping.
- Passwords, tokens, API keys, authentication headers, provider credentials, and sensitive command arguments are redacted from application logs and public job payloads.

## Interface assets

All runtime interface CSS and JavaScript is served locally. A strict same-origin Content Security Policy blocks inline/runtime third-party assets. The reusable go-to-bottom button appears on sufficiently long pages, scrolls the document rather than a nested panel, and honors the browser's reduced-motion preference.

## Reverse proxy

Terminate TLS at a trusted reverse proxy and set `PREPAC_TRUST_PROXY_HEADERS=true` only when untrusted clients cannot inject forwarded headers. Set `PREPAC_TRUSTED_PROXIES` to the proxy's exact IP/CIDR (the default trusts loopback only), and set `PREPAC_TRUSTED_HOSTS` to the expected external hostname when practical. Prefer `PREPAC_SESSION_COOKIE_MODE=always` for HTTPS-only deployments.

## Storage

Use narrow source and destination mounts. Confirm path settings refer to paths as seen by the process, not host paths hidden outside the container. Ensure adequate temporary and destination free space before large packing jobs.
