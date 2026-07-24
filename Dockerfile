# syntax=docker/dockerfile:1.7

FROM python:3.14-slim-bookworm AS builder

# Pin uv so dependency resolution remains reproducible across image rebuilds.
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install locked third-party dependencies before copying source code so this
# expensive layer is reused when only application code changes.
COPY pyproject.toml uv.lock ./
RUN uv sync \
    --frozen \
    --no-dev \
    --all-extras \
    --no-install-project

COPY src ./src
RUN uv sync \
    --frozen \
    --no-dev \
    --all-extras \
    --no-editable


FROM python:3.14-slim-bookworm AS runtime

# Spark 4 requires a JVM. The package exposes a stable JAVA_HOME symlink on
# both amd64 and arm64 Debian images.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 fraudstream \
    && useradd \
        --uid 10001 \
        --gid fraudstream \
        --create-home \
        --shell /usr/sbin/nologin \
        fraudstream

WORKDIR /app

COPY --from=builder --chown=fraudstream:fraudstream /app/.venv ./.venv
COPY --chown=fraudstream:fraudstream configs ./configs

# Generated data and reports can be bind-mounted or copied from these paths.
RUN mkdir -p data reports \
    && chown -R fraudstream:fraudstream /app

USER fraudstream

# This is a batch CLI image rather than a long-running network service.
# Override CMD with any installed fraudstream-* command.
CMD ["fraudstream-generate-offline", "--help"]
