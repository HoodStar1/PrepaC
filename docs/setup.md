# Setup

## Requirements

- Docker
- Docker Compose
- Bind-mounted folders for `/config`, `/media/tv`, `/media/movies`, `/media/youtube`, and `/media/dest`

## Example Compose

```yaml
services:
  prepac:
    build: .
    container_name: prepac
    ports:
      - "1234:1234"
    volumes:
      - ./config:/config
      - ./data/tv:/media/tv
      - ./data/movies:/media/movies
      - ./data/youtube:/media/youtube
      - ./data/destination:/media/dest
    restart: unless-stopped
```

## First Run

1. Open the application in your browser
2. Complete the first-run admin account setup
3. Configure paths in **Settings**
4. Optionally connect Plex
5. Configure posting providers before using the Posting module
