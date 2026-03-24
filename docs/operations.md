# Operations

PrepaC runs behind Gunicorn using a host-aware startup script.

## Default runtime behavior

- workers scale from CPU count
- threads scale by host size
- settings persist in `/config`
- use normal Docker restart policies for resilience
