# Troubleshooting

## The app does not start

Check your container logs and confirm your Docker configuration is valid.

## Posting does not start

Check provider settings, provider order, priority thresholds, and path configuration.

If all eligible providers are busy, the posting job remains queued until an eligible provider becomes available.

## Share does not submit

Check destination settings, API access, and any rate limits on the target indexer.

## An active Share job needs to stop

Use the cancel or remove action on the Active Share Jobs card. The job result is then available in Share History.

## Clean does not remove files

Check Dry Run, confirmation text `DELETE`, permissions, and recycle-bin configuration.
