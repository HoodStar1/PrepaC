# Operations

PrepaC runs behind Gunicorn.

## Default runtime behavior

- workers scale from CPU count
- threads scale by host size
- settings persist in `/config`
- use a normal Docker restart policy for resilience

## Useful overrides

- `GUNICORN_WORKERS`
- `GUNICORN_THREADS`
- `GUNICORN_TIMEOUT`
- `GUNICORN_GRACEFUL_TIMEOUT`
- `GUNICORN_KEEPALIVE`
- `GUNICORN_BIND`
- `GUNICORN_LOG_LEVEL`
