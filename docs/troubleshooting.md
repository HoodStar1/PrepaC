# Troubleshooting

## The app does not start

Check your container logs and confirm your Docker configuration is valid.

- If startup now reports a schema migration or index creation failure, check the logged SQL statement and database error before restarting repeatedly.
- Already-applied column additions are skipped automatically; other migration failures now stop startup so the database is not left silently inconsistent.

## Sign-in says too many attempts

PrepaC temporarily locks sign-in and password reset after repeated failed attempts.

- Wait for the lockout window to expire, then try again.
- If needed, adjust auth rate-limit environment variables for your deployment size.

## Posting does not start

Check provider settings, provider order, priority thresholds, and path configuration.

If all eligible providers are busy, the posting job remains queued until an eligible provider becomes available.

## Prepare fails with rsync errors

Prepare now requires `rsync` to succeed and reports the direct rsync error in the job events.

- Check the Active Prepare Job details for the exact rsync message and command.
- Verify the container has `rsync` installed and available on `PATH`.
- Verify the configured source and destination mounts exist inside the container and are writable where needed.
- If the copy runs for a long time, ensure rsync can emit progress output and is not being blocked by mount or filesystem issues.

## Share does not submit

Check destination settings, API access, and any rate limits on the target indexer.

- Destination URLs must now be plain `http://` or `https://` base URLs.
- Remove embedded credentials, query strings, and fragments from the destination URL.
- Keep credentials in the dedicated API key or basic-auth fields instead of the URL itself.

## Share import says "Upload too large"

Share imports are now capped per request.

- The default limit is `512` MiB.
- Increase `PREPAC_SHARE_IMPORT_MAX_MB` only if your real import bundle size requires it.
- Retry the upload after adjusting the limit and restarting the app.

## Plex sign-in or callback uses the wrong external URL

- If you are behind a reverse proxy, set `PREPAC_TRUST_PROXY_HEADERS=true`.
- If you are not using a trusted reverse proxy, leave it disabled so external URLs use the direct request scheme and host.

## A form save or API action says "Security check failed"

PrepaC now requires a session-backed CSRF token for mutating requests.

- Reload the page and retry the action.
- If you use a reverse proxy, make sure it preserves cookies for the app origin.
- If you call the API manually, send the `X-CSRF-Token` header from the active browser session.

## An active Share job needs to stop

Use the cancel or remove action on the Active Share Jobs card. The job result is then available in Share History.

## Clean does not remove files

Check Dry Run, confirmation text `DELETE`, permissions, and recycle-bin configuration.

## Metrics scraping does not work

- If `PREPAC_METRICS_TOKEN` is set, include it as `X-Prepac-Metrics-Token` (or `?token=`).
- If no token is configured, metrics continue to use authenticated access.
