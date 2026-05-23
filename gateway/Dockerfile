# ============================================================================
# TensorIPC — Gateway Dockerfile (Multi-Stage Distroless Build)
#
# Stage 1 (builder):  Compile the Go binary with CGO disabled so the result
#                     is a static binary with zero external library deps.
# Stage 2 (runtime):  Copy the binary into ``gcr.io/distroless/static:nonroot``
#                     which contains only the bare essentials (ca-certificates,
#                     tzdata, and the binary itself) — no shell, no package
#                     manager, no attack surface.
# ============================================================================

# ----------------------------------------------------------------------------
# Stage 1 — Builder
# ----------------------------------------------------------------------------
FROM golang:1.21 AS builder

# Set the working directory inside the builder container.
WORKDIR /src

# Copy the Go module manifest first to leverage Docker layer caching.
# If go.mod hasn't changed in a subsequent build, Docker reuses the cached
# layer instead of re-downloading dependencies.
COPY go.mod ./

# Download dependencies (if any were added later).  The ``-x`` flag makes
# the output verbose so you can see what's happening.
# Note: ``go.sum`` is intentionally not copied — this project has zero
# external dependencies so no ``go.sum`` file exists.
RUN go mod download -x 2>/dev/null || true

# Copy the entire source tree.
COPY . .

# Build the binary.
#   CGO_ENABLED=0   — Disable C Go interop, producing a fully static binary.
#   GOOS=linux      — Explicitly target Linux (redundant inside a Linux
#                      container, but makes the intent clear).
#   -ldflags="-s -w" — Strip debug info and the symbol table, reducing the
#                      binary size by ~30 %.
#   -o /app/gateway — Output path for the compiled binary.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/gateway ./cmd/server/main.go

# ----------------------------------------------------------------------------
# Stage 2 — Distroless Runtime
# ----------------------------------------------------------------------------
# ``gcr.io/distroless/static:nonroot`` is a Google-maintained minimal image.
# It contains:
#   - The Go standard library's static files (CA certs, timezone data)
#   - A ``nonroot`` user with UID 65532
# It does NOT contain:
#   - A shell (/bin/sh, /bin/bash)
#   - Package managers (apt, apk)
#   - Any utilities (ls, cat, curl, etc.)
# This dramatically reduces the attack surface and image size.
FROM gcr.io/distroless/static:nonroot

# Copy the compiled binary from the builder stage.
COPY --from=builder /app/gateway /app/gateway

# Use the distroless-provided non-root user for security.
# If a vulnerability is ever exploited in the Go runtime, the attacker's
# shell (if one existed) would be running as this low-privilege user.
USER nonroot:nonroot

# Expose the port that the gateway listens on (mapped to host port 8000
# by docker-compose.yml).
EXPOSE 8000

# The ENTRYPOINT instruction defines the executable that runs when the
# container starts.  Unlike CMD, ENTRYPOINT is not overridden by arguments
# passed to ``docker run``.
ENTRYPOINT ["/app/gateway"]
