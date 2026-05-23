"""
TensorIPC — Worker A / Domain / Writer

Authoritative writer that creates a POSIX shared-memory segment and writes a
simulated tensor (a flat byte array) into it.  The segment is given a well-known
name so that Worker B (the Reader) can attach to it by name without any network
or RPC layer — the containers share their IPC namespace (see docker-compose.yml).

Resilience design
------------------
*   try / finally         — guaranteed cleanup: close() + unlink() even on
                            unexpected exceptions.
*   Graceful Degradation  — the function returns a bool so callers can decide
                            how to fall back (e.g. retry, log, exit).
*   Strict type hints      — every public function carries full type annotations.
"""

from __future__ import annotations  # enables PEP 604 union syntax for 3.8+

import logging
import time
from multiprocessing import shared_memory
from typing import Final

# ---------------------------------------------------------------------------
# Constants  (UPPER_CASE — these are module-level "configuration" values)
# ---------------------------------------------------------------------------
SHM_NAME: Final[str] = "tensor_data"
"""Well-known name of the shared-memory segment.
Both Writer and Reader must agree on this string."""

TENSOR_BYTE_COUNT: Final[int] = 64
"""Number of bytes we write into shared memory (simulates a 64-element
int8 tensor).  Must stay small enough to fit inside `shm_size` (256 MB)."""

WRITER_LIFETIME_SEC: Final[int] = 30
"""Seconds the writer stays alive after writing so the Reader has time to
attach and consume the segment.  Tune up if Reader startup is slow."""

# ---------------------------------------------------------------------------
# Logging  (console-friendly for container logs)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
_log: logging.Logger = logging.getLogger("writer")


# ---------------------------------------------------------------------------
# Core domain function
# ---------------------------------------------------------------------------

def write_tensor(name: str, size: int) -> bool:
    """
    Create a named shared-memory segment and fill it with a deterministic
    byte pattern (0, 1, 2, …, size-1).

    Parameters
    ----------
    name : str
        The POSIX name of the shared-memory segment (must start with a slash
        on Linux; ``multiprocessing.shared_memory`` handles this for us).
    size : int
        Number of bytes to allocate.

    Returns
    -------
    bool
        ``True`` if the write completed successfully, ``False`` if an error
        prevented the write from finishing.

    Raises
    ------
    ValueError
        If *size* <= 0.
    """
    # --- Pre-condition guard -------------------------------------------------
    if size <= 0:
        _log.error("Refusing to allocate shared memory of size %d (must be > 0).", size)
        return False

    # --- Resource acquisition ------------------------------------------------
    shm: shared_memory.SharedMemory | None = None
    try:
        # `create=True` tells the OS to allocate a *new* segment.
        # We pass the well-known *name* so this segment can be looked up
        # by Worker B later without needing to send a handle over a socket.
        _log.info("Creating shared-memory segment '%s' (%d bytes) …", name, size)
        shm = shared_memory.SharedMemory(create=True, name=name, size=size)

        # Fill the memory-mapped buffer with an incrementing byte sequence.
        # This is our "tensor" — a 1-D array of int8 values.
        data: bytearray = bytearray(range(size))
        shm.buf[:size] = data
        _log.info("Wrote %d bytes to '%s'.  Data = %s", size, name, list(data))

        # Keep the process alive long enough for the Reader to connect.
        _log.info("Holding segment open for %d s …", WRITER_LIFETIME_SEC)
        time.sleep(WRITER_LIFETIME_SEC)

        _log.info("Write phase complete — returning True.")
        return True

    except FileExistsError:
        # Raised when a segment with *name* already exists.  This can happen
        # if a previous run did not clean up (`docker-compose down` handles
        # this by killing the container, but the kernel may keep the segment).
        _log.error(
            "Segment '%s' already exists.  Run `ipcrm -M %s` or restart the "
            "Docker host to purge stale segments.",
            name, name,
        )
        return False

    except (PermissionError, OSError) as exc:
        _log.error("OS error while creating / accessing shared memory: %s", exc)
        return False

    except Exception as exc:
        _log.exception("Unexpected error in write_tensor: %s", exc)
        return False

    # --- Guaranteed teardown -------------------------------------------------
    finally:
        if shm is not None:
            _log.info("Closing shared-memory handle …")
            shm.close()
            # unlink() removes the kernel-level object.  This is safe here
            # because the Reader will have already finished reading before
            # we reach this point (we waited *WRITER_LIFETIME_SEC* seconds).
            _log.info("Unlinking shared-memory segment '%s' …", name)
            shm.unlink()
            _log.info("Shared-memory resources released.")


# ---------------------------------------------------------------------------
# Entry-point (called when `python -m src.domain.writer` runs)
# ---------------------------------------------------------------------------

def main() -> None:
    """Script entry-point — delegates to the domain function."""
    success: bool = write_tensor(name=SHM_NAME, size=TENSOR_BYTE_COUNT)
    if not success:
        _log.error("Writer failed — exiting with code 1.")
        raise SystemExit(1)
    _log.info("Writer exiting normally.")


if __name__ == "__main__":
    main()
