# Troubleshooting

## The app warns about Flask development server

Use the included Dockerfile and startup script; they run Gunicorn.

## Posters or watched-state cleanup are not working

Check Plex URL, token, library names, and container-to-Plex network access.

## Posting does not start

Check provider settings and path configuration.

## Clean does not remove files

Check Dry Run, confirmation text `DELETE`, permissions, and recycle-bin configuration.
