FROM node:20.20.1-bookworm AS nyuu-builder

ARG NODE_GYP_VERSION=12.2.0
ARG NYUU_VERSION=0.4.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        make \
        g++ \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g "node-gyp@${NODE_GYP_VERSION}" \
    && npm install -g "nyuu@${NYUU_VERSION}" --unsafe-perm


FROM debian:bookworm-slim AS par2-builder

ARG PAR2_TAG=v1.2.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        make \
        g++ \
        autoconf \
        automake \
        libtool \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --branch "${PAR2_TAG}" --depth=1 https://github.com/animetosho/par2cmdline-turbo.git /tmp/par2cmdline-turbo \
    && cd /tmp/par2cmdline-turbo \
    && (./automake.sh || autoreconf -fi) \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && strip /usr/local/bin/par2 || true \
    && rm -rf /tmp/par2cmdline-turbo


FROM python:3.13.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_PREFER_BINARY=1

WORKDIR /app

ARG RAR_URL=https://www.rarlab.com/rar/rarlinux-x64-720.tar.gz
ARG RAR_SHA256=d3e7fba3272385b1d0255ee332a1e8c1a6779bb5a5ff9d4d8ac2be846e49ca46

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        mediainfo \
        rsync \
        curl \
        ca-certificates \
        libstdc++6 \
        libjpeg62-turbo \
        libpng16-16 \
        libwebp7 \
        libtiff6 \
        libfreetype6 \
        liblcms2-2 \
        libopenjp2-7 \
        libharfbuzz0b \
        libfribidi0 \
        libxcb1 \
        tcl8.6 \
        tk8.6 \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Copy exact Node runtime and Nyuu install from builder to avoid ABI mismatch.
COPY --from=nyuu-builder /usr/local/bin/node /usr/local/bin/node
RUN ln -sf /usr/local/bin/node /usr/local/bin/nodejs
COPY --from=nyuu-builder /usr/local/lib/node_modules/nyuu /usr/local/lib/node_modules/nyuu
COPY --from=nyuu-builder /usr/local/bin/nyuu /usr/local/bin/nyuu
RUN ln -sf /usr/local/lib/node_modules/nyuu/bin/nyuu.js /usr/local/bin/nyuu

# Copy par2 binary from dedicated builder stage.
COPY --from=par2-builder /usr/local/bin/par2 /usr/local/bin/par2

# Download official RAR binary at build time instead of committing the tarball.
RUN curl -L --retry 3 --retry-delay 2 "${RAR_URL}" -o /tmp/rarlinux.tar.gz \
    && echo "${RAR_SHA256}  /tmp/rarlinux.tar.gz" | sha256sum -c - \
    && cd /tmp \
    && tar -xzf /tmp/rarlinux.tar.gz \
    && cp /tmp/rar/rar /usr/local/bin/rar \
    && chmod +x /usr/local/bin/rar \
    && strip /usr/local/bin/rar || true \
    && rm -rf /tmp/rar /tmp/rarlinux.tar.gz

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY templates ./templates
COPY static ./static
COPY start-gunicorn.sh /usr/local/bin/start-gunicorn.sh
RUN chmod +x /usr/local/bin/start-gunicorn.sh

EXPOSE 1234

STOPSIGNAL SIGTERM
ENTRYPOINT ["tini", "--"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 CMD curl -fs http://localhost:1234/health || exit 1

CMD ["/usr/local/bin/start-gunicorn.sh"]
