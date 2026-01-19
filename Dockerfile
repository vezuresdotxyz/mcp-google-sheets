# syntax=docker/dockerfile:1

# Multi-stage build for mcp-google-sheets using local source
FROM alpine:latest AS base

WORKDIR /app

# Set environment variables for non-interactive installs and minimal locale
ENV LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1

# Update and install basic packages
RUN apk update && \
    apk upgrade && \
    apk add --no-cache \
        bash \
        curl \
        tini \
        coreutils \
        git \
        python3 \
        py3-pip

# Set tini as the init system to handle PID 1
ENTRYPOINT ["/sbin/tini", "--"]

# Install uv (fast Python package installer)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Ensure uv is on PATH
ENV PATH="/root/.local/bin:${PATH}"

# Copy Python version file first for better caching
COPY .python-version ./

# Create virtual environment
RUN uv venv

# ============================================
# Builder stage - builds from local source
# ============================================
FROM base AS builder

# Copy dependency files and source structure for better layer caching
# README.md is needed because pyproject.toml references it
# Source structure is needed because uv sync installs the package in editable mode
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

# Install dependencies (this layer will be cached if dependencies don't change)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Copy all local source code
COPY . .

# Build the project from local source (produces dist/*.whl)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv build

# ============================================
# Runner stage - minimal runtime image
# ============================================
FROM base AS runner

# Copy built wheel from builder stage
COPY --from=builder /app/dist/*.whl /app/

# Install the built package
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install /app/*.whl

# Default environment variables (can be overridden)
ENV HOST=0.0.0.0 \
    PORT=8000 \
    SSE_TIMEOUT=3600 \
    SSE_KEEPALIVE_TIMEOUT=30 \
    SSE_INACTIVITY_TIMEOUT=300

# Expose the default port
EXPOSE 8000

# Default command - runs with SSE transport
# Override with docker run or docker-compose to use stdio transport
CMD ["uv", "run", "mcp-google-sheets", "--transport", "sse"]
