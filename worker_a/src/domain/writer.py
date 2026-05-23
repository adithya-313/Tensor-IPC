"""
TensorIPC — Worker A / Domain / Writer (Control-Plane HTTP Server)

Refactored from a one-shot script into a long-lived FastAPI application that
exposes ``POST /tensor``.  Each request creates (or overwrites) the POSIX
shared-memory segment ``tensor_data`` and writes a fresh byte pattern into it.

This is the **HTTP-to-IPC Bridge**: external callers (or the Gateway) send an
HTTP POST, and the writer translates it into a kernel-level shared-memory write
that Worker B (the Reader) can consume instantly across the IPC namespace.

Resilience design
------------------
*   FastAPI exception handlers — catch and return structured JSON errors.
*   try / finally in the write path — guarantee ``shm.close()`` if an error
    occurs mid-write.  The segment itself persists until the server shuts down.
*   Lifespan-based cleanup — when the ASGI server receives SIGINT/SIGTERM,
    the ``lifespan`` handler calls ``shm.unlink()`` to release kernel resources.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from multiprocessing import shared_memory
from typing import Final

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SHM_NAME: Final[str] = "tensor_data"
"""Well-known POSIX shared-memory segment name (must match the Reader)."""

TENSOR_BYTE_COUNT: Final[int] = 64
"""Number of bytes written into shared memory per ``POST /tensor`` request."""

HOST: Final[str] = "0.0.0.0"
"""Bind address — listen on all interfaces so the Gateway can reach us via
the ``tensor_net`` Docker bridge network."""

PORT: Final[int] = 8080
"""Container-internal HTTP port mapped into docker-compose."""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
_log: logging.Logger = logging.getLogger("writer")


# ---------------------------------------------------------------------------
# Pydantic models (request / response schemas)
# ---------------------------------------------------------------------------

class TensorResponse(BaseModel):
    """Schema for the JSON body returned by ``POST /tensor``."""
    status: str
    shm_name: str
    bytes_written: int


# ---------------------------------------------------------------------------
# Shared-memory lifecycle module state
#
# We keep the ``SharedMemory`` instance at *module level* so it survives
# across multiple HTTP requests.  The segment is created on the first
# ``POST /tensor`` and persists until the server shuts down.
# ---------------------------------------------------------------------------

_shm: shared_memory.SharedMemory | None = None
"""Module-global handle to the persistent shared-memory segment."""


# ---------------------------------------------------------------------------
# Core domain function
# ---------------------------------------------------------------------------

def _write_tensor(name: str, size: int) -> dict:
    """
    Create (or overwrite) a named shared-memory segment and fill it with a
    deterministic byte pattern (0, 1, 2, …, size-1).

    If the segment already exists (from a previous request), we simply
    overwrite its contents — no need to unlink and re-create.

    Parameters
    ----------
    name : str
        POSIX shared-memory segment name.
    size : int
        Number of bytes to write.

    Returns
    -------
    dict
        A dictionary suitable for JSON serialisation:
        ``{"status": ..., "shm_name": ..., "bytes_written": ...}``
    """
    global _shm  # we are modifying the module-level reference

    # --- Step 1: Create the segment if it does not exist yet -----------------
    if _shm is None:
        _log.info("Creating shared-memory segment '%s' (%d bytes) on first request …", name, size)
        try:
            _shm = shared_memory.SharedMemory(create=True, name=name, size=size)
        except FileExistsError:
            # A stale segment from a previous run.  Attach to it and warn.
            # (The kernel may not have released it yet; we'll use it anyway.)
            _log.warning(
                "Segment '%s' already exists (stale from prior run). Attaching in-place.", name,
            )
            _shm = shared_memory.SharedMemory(create=False, name=name)
        except (PermissionError, OSError) as exc:
            _log.error("OS error creating shared memory: %s", exc)
            return {"status": "error", "shm_name": name, "bytes_written": 0}
    else:
        _log.info("Reusing existing segment '%s'.", name)

    # --- Step 2: Write data into the segment ---------------------------------
    try:
        data: bytearray = bytearray(range(size))
        _shm.buf[:size] = data
        _log.info("Wrote %d bytes: %s", size, list(data))
        return {"status": "success", "shm_name": name, "bytes_written": size}
    except Exception as exc:
        _log.exception("Failed to write to shared memory: %s", exc)
        return {"status": "error", "shm_name": name, "bytes_written": 0}


# ---------------------------------------------------------------------------
# FastAPI application with lifespan-based cleanup
#
# ``@asynccontextmanager`` lets us run code before the server starts handling
# requests (yield) and after it receives a shutdown signal (cleanup after
# yield).  This is the modern replacement for startup / shutdown event hooks.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Lifespan context manager for the FastAPI application.

    *Before yield*  — nothing to do; the segment is created lazily on the
                     first ``POST /tensor``.
    *After yield*   — clean up the shared-memory segment.
    """
    _log.info("Writer server starting (listening on %s:%d) …", HOST, PORT)
    yield  # server runs here
    _log.info("Shutting down writer server — cleaning up shared memory …")
    _cleanup_shm()


def _cleanup_shm() -> None:
    """Release the kernel-level shared-memory object."""
    global _shm
    if _shm is not None:
        try:
            _shm.close()
            _shm.unlink()
            _log.info("Shared-memory segment '%s' closed and unlinked.", SHM_NAME)
        except Exception as exc:
            _log.warning("Error during shared-memory cleanup: %s", exc)
        finally:
            _shm = None


# ---------------------------------------------------------------------------
# FastAPI application instance
# ---------------------------------------------------------------------------

app: FastAPI = FastAPI(
    title="TensorIPC — Worker A Writer",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.post("/tensor", response_model=TensorResponse)
async def write_tensor_endpoint() -> TensorResponse:
    """
    **POST /tensor** — HTTP-to-IPC Bridge

    Creates or overwrites the ``tensor_data`` shared-memory segment and fills
    it with a fresh byte pattern.  Returns a JSON payload confirming the write.

    This is the control-plane entry point: the Gateway reverse-proxies external
    requests here so that any HTTP client can trigger a shared-memory update
    without needing low-level POSIX IPC knowledge.
    """
    _log.info("Received POST /tensor request.")
    result: dict = _write_tensor(name=SHM_NAME, size=TENSOR_BYTE_COUNT)
    return TensorResponse(**result)


@app.get("/health")
async def health_check() -> JSONResponse:
    """Simple liveness probe (used by Docker healthchecks and the Gateway)."""
    return JSONResponse(
        content={"status": "healthy", "service": "writer", "shm_initialized": _shm is not None},
    )


# ---------------------------------------------------------------------------
# Entry point (called by ``python -m domain.writer``)
# ---------------------------------------------------------------------------

def main() -> None:
    """Start the Uvicorn ASGI server programmatically.

    This is equivalent to running::

        uvicorn domain.writer:app --host 0.0.0.0 --port 8080

    but keeps everything self-contained inside the ``python -m`` invocation.
    """
    import uvicorn
    uvicorn.run(
        "domain.writer:app",
        host=HOST,
        port=PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
