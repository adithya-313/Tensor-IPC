"""
TensorIPC — Worker B / Domain / Reader

Connects to the shared-memory segment created by Worker A (the Writer) using
the well-known name ``tensor_data``, reads the byte contents, and validates
that they match the expected sequence 0, 1, 2, …, 63.

Because containers share an IPC namespace (see docker-compose.yml), the
``SharedMemory`` constructor with ``create=False`` can resolve the segment
by name as if it were a local POSIX shared-memory object — no network IPC
is involved.

Resilience design
------------------
*   Retry with back-off  — the segment may not exist yet if Worker A is
                          still booting; we poll with a short sleep.
*   try / finally         — always call ``shm.close()`` (but **not** unlink,
                          because *unlinking* is the Writer's responsibility).
*   Graceful Degradation  — returns a bool so callers can decide behaviour.
*   Strict type hints      — every public function is fully annotated.
"""

from __future__ import annotations

import logging
import time
from multiprocessing import shared_memory
from typing import Final

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SHM_NAME: Final[str] = "tensor_data"
"""Must match ``writer.SHM_NAME`` exactly."""

EXPECTED_BYTE_COUNT: Final[int] = 64
"""How many bytes we expect the Writer to have written."""

RETRY_ATTEMPTS: Final[int] = 20
"""Number of times we try to attach to the shared-memory segment before
giving up.  20 attempts × 2 s sleep = 40 s max wait.  The writer holds
the segment for 30 s, so 40 s gives us a comfortable margin."""

RETRY_SLEEP_SEC: Final[float] = 2.0
"""Seconds to wait between each retry attempt."""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
_log: logging.Logger = logging.getLogger("reader")


# ---------------------------------------------------------------------------
# Core domain function
# ---------------------------------------------------------------------------

def read_tensor(name: str, expected_size: int) -> bool:
    """
    Attach to an existing named shared-memory segment, read its contents,
    and validate the byte sequence.

    The read is retried up to ``RETRY_ATTEMPTS`` times because the Writer
    may not have created the segment yet when this function first runs.

    Parameters
    ----------
    name : str
        POSIX name of the shared-memory segment (must match the Writer's
        segment name).
    expected_size : int
        Number of bytes we expect to find.  A mismatch is logged as a
        validation failure.

    Returns
    -------
    bool
        ``True`` if the data was read and validated successfully.
        ``False`` otherwise.

    Raises
    ------
    ValueError
        If *expected_size* <= 0.
    """
    # --- Pre-condition guard -------------------------------------------------
    if expected_size <= 0:
        _log.error("expected_size must be > 0 (got %d).", expected_size)
        return False

    # --- Retry loop: attach to the already-created segment -------------------
    shm: shared_memory.SharedMemory | None = None
    last_error: Exception | None = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            # ``create=False`` (the default) means "attach to an existing
            # segment".  This will raise a ``FileNotFoundError`` until the
            # Writer actually creates and ``shm.unlink()`` has not been called.
            _log.info(
                "Attempt %d/%d — attaching to shared memory '%s' …",
                attempt, RETRY_ATTEMPTS, name,
            )
            shm = shared_memory.SharedMemory(create=False, name=name)

            # --- Successfully attached — break out of the retry loop ---------
            _log.info("Attached to segment '%s' (size=%d bytes).", name, shm.size)
            break

        except FileNotFoundError:
            last_error = None  # expected while Writer is still booting
            _log.info("Segment '%s' not ready yet — retrying in %g s …", name, RETRY_SLEEP_SEC)
            time.sleep(RETRY_SLEEP_SEC)

        except Exception as exc:
            last_error = exc
            _log.warning("Unexpected error during attach attempt %d: %s", attempt, exc)
            time.sleep(RETRY_SLEEP_SEC)

    # --- Did we exhaust all retries without attaching? -----------------------
    if shm is None:
        _log.error(
            "Could not attach to segment '%s' after %d attempts.  "
            "Last error: %s",
            name, RETRY_ATTEMPTS, last_error,
        )
        return False

    # --- Read and validate ---------------------------------------------------
    try:
        # Copy the contents out of the shared buffer into a local bytearray.
        actual_data: bytes = bytes(shm.buf[:expected_size])
        expected_data: bytes = bytes(range(expected_size))

        _log.info("Read %d bytes: %s", len(actual_data), list(actual_data))

        if actual_data == expected_data:
            _log.info(
                "VALIDATION PASSED — data matches expected sequence "
                "0..%d.", expected_size - 1  # e.g. 0..63
            )
            return True

        # --- Data mismatch ---------------------------------------------------
        mismatches = sum(
            1 for i in range(min(len(actual_data), expected_size))
            if actual_data[i] != expected_data[i]
        )
        _log.error(
            "VALIDATION FAILED — %d byte(s) differ from expected.  "
            "Expected=%s, Actual=%s",
            mismatches,
            list(expected_data),
            list(actual_data),
        )
        return False

    except Exception as exc:
        _log.exception("Error while reading / validating shared memory: %s", exc)
        return False

    # --- Guaranteed teardown -------------------------------------------------
    finally:
        if shm is not None:
            # IMPORTANT: We call close() but *not* unlink().
            # The Writer owns the lifecycle of the kernel object and will
            # call unlink() once it finishes its sleep.
            _log.info("Closing reader's handle to '%s' …", name)
            shm.close()


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

def main() -> None:
    """Script entry-point — delegates to the domain function."""
    success: bool = read_tensor(name=SHM_NAME, expected_size=EXPECTED_BYTE_COUNT)
    if not success:
        _log.error("Reader failed — exiting with code 1.")
        raise SystemExit(1)
    _log.info("Reader exiting normally.")


if __name__ == "__main__":
    main()
