# The published container is intentionally Linux AMD64 only.
FROM --platform=linux/amd64 node:24.18.0-bookworm@sha256:5711a0d445a1af54af9589066c646df387d1831a608226f4cd694fc59e745059 AS nyuu-builder

WORKDIR /opt/nyuu

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        g++ \
        make \
        python3 \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund


FROM --platform=linux/amd64 debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS par2-builder

ARG PAR2_COMMIT=4db49ca45ab258c230061fb3f0d29273f7c524ea

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        ca-certificates \
        g++ \
        git \
        libtool \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN git init /tmp/par2cmdline-turbo \
    && cd /tmp/par2cmdline-turbo \
    && git remote add origin https://github.com/animetosho/par2cmdline-turbo.git \
    && git fetch --depth=1 origin "${PAR2_COMMIT}" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "${PAR2_COMMIT}" \
    && (./automake.sh || autoreconf -fi) \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && strip /usr/local/bin/par2 \
    && rm -rf /tmp/par2cmdline-turbo


FROM --platform=linux/amd64 python:3.14.6-slim-bookworm@sha256:86f975aca15cf04a40b399eebede9aea7c82eae084d1f1a0a6ef6bcaae871a30 AS cython-builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_PREFER_BINARY=1 \
    PREPAC_BUILD_CYTHON=1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir \
        "pip==26.1.2" \
        "setuptools==83.0.0" \
        "wheel==0.47.0" \
        "Cython==3.2.8"

COPY pyproject.toml setup.py ./
COPY app ./app
RUN python setup.py build_ext --inplace \
    && find app -type f -name '*.c' -delete \
    && find app -type f -name '*.py' \
        ! -name '__init__.py' \
        ! -name 'name_randomizer_data.py' \
        ! -name 'version.py' \
        -delete


FROM --platform=linux/amd64 python:3.14.6-slim-bookworm@sha256:86f975aca15cf04a40b399eebede9aea7c82eae084d1f1a0a6ef6bcaae871a30

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_PREFER_BINARY=1 \
    PREPAC_CONFIG_DIR=/config \
    PREPAC_SQLITE_JOURNAL_MODE=DELETE \
    PREPAC_LOG_LEVEL=INFO \
    PREPAC_LOG_JSON=false

WORKDIR /app

ARG RAR_URL=https://www.rarlab.com/rar/rarlinux-x64-723.tar.gz
ARG RAR_SHA256=759b4b6aa0d9f77131882162951193f3a0e54bf60e1d8dc4255aa308accab588

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        ffmpeg \
        libfreetype6 \
        libfribidi0 \
        libharfbuzz0b \
        libjpeg62-turbo \
        liblcms2-2 \
        libopenjp2-7 \
        libpng16-16 \
        libstdc++6 \
        libtiff6 \
        libwebp7 \
        libxcb1 \
        mediainfo \
        rsync \
        tini \
        tk8.6 \
        tcl8.6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=nyuu-builder /usr/local/bin/node /usr/local/bin/node
COPY --from=nyuu-builder /opt/nyuu/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/bin/node /usr/local/bin/nodejs \
    && ln -s /usr/local/lib/node_modules/nyuu/bin/nyuu.js /usr/local/bin/nyuu

COPY --from=par2-builder /usr/local/bin/par2 /usr/local/bin/par2

RUN curl --fail --location --retry 3 --retry-delay 2 "${RAR_URL}" -o /tmp/rarlinux.tar.gz \
    && echo "${RAR_SHA256}  /tmp/rarlinux.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/rarlinux.tar.gz -C /tmp \
    && install -m 0755 /tmp/rar/rar /usr/local/bin/rar \
    && rm -rf /tmp/rar /tmp/rarlinux.tar.gz

COPY requirements-linux.txt ./
RUN python -m pip install --no-cache-dir \
        "pip==26.1.2" \
        "setuptools==83.0.0" \
        "wheel==0.47.0" \
    && python -m pip install --no-cache-dir --require-hashes -r requirements-linux.txt

COPY --from=cython-builder /build/app ./app
COPY prepac.py ./
COPY templates ./templates
COPY static ./static
COPY start-gunicorn.sh /usr/local/bin/start-gunicorn.sh
RUN chmod 0755 /usr/local/bin/start-gunicorn.sh \
    && mkdir -p /config

EXPOSE 1234

STOPSIGNAL SIGTERM
ENTRYPOINT ["tini", "--"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://localhost:1234/health || exit 1

CMD ["/usr/local/bin/start-gunicorn.sh"]
