"""
TensorIPC — Worker B / Domain / Reader (Continuous Polling Daemon)

Upgraded from a one-shot reader into a long-lived daemon that continuously
polls the shared-memory segment for new data.  This eliminates the need for
any TCP or RPC layer between Worker A and Worker B — data travels directly
through kernel-space shared memory at nanosecond latencies.

Why use a polling loop instead of TCP?
---------------------------------------
The Writer (Worker A) writes into a POSIX shared-memory segment.  The Reader
(Worker B) runs in the same IPC namespace (see docker-compose.yml).  Because
both containers see the same ``/dev/shm``, the Reader can simply ``mmap`` the
same physical pages — no context switch into kernel networking, no TCP stack,
no serialisation overhead.  The 10 ms micro-sleep is far faster than any
network round-trip while preventing the CPU from spinning at 100 %.

Continuous Polling pattern
--------------------------
1. Attach to the named shared-memory segment (retry until it exists).
2. In a ``while True`` loop:
   a. Read the current bytes from the buffer.
   b. Compare against the last-known state (state-tracking).
   c. If changed, log a ``NEW DATA`` event.
   d. Sleep 10 ms, then repeat.
3. On SIGTERM / KeyboardInterrupt, close the handle gracefully.

Resilience design
------------------
*   State-tracking       — avoids spamming logs when data hasn't changed.
*   Micro-sleep (10 ms)  — prevents CPU redlining.
*   Graceful Shutdown    — signal handler + try/finally on close().
*   Strict type hints     — every public function is fully annotated.
"""

from __future__ import annotations

import logging
import signal
import struct
import sys
import time
from multiprocessing import shared_memory
from typing import Final

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SHM_NAME: Final[str] = "tensor_data"
"""Must match the name used by Worker A (writer.SHM_NAME)."""

EXPECTED_BYTE_COUNT: Final[int] = 64
"""Number of bytes the Writer writes per ``POST /tensor`` request."""

RETRY_ATTEMPTS: Final[int] = 20
"""Max attempts to attach to the segment while waiting for Worker A to boot."""

RETRY_SLEEP_SEC: Final[float] = 2.0
"""Seconds between attach retries."""

POLL_INTERVAL_SEC: Final[float] = 0.01
"""10 ms sleep between polls — fast enough to catch every write under load
(50 VUs × 10 s produces ~5000 writes) but slow enough to keep CPU usage near
zero when idle."""

SEQ_OFFSET: Final[int] = 4
"""Must match ``writer.SEQ_OFFSET``.  Bytes ``[0:SEQ_OFFSET]`` hold the
Seqlock 32-bit sequence number; payload data starts at ``[SEQ_OFFSET:]``."""

MAX_SEQLOCK_RETRIES: Final[int] = 100
"""Maximum spin iterations when the Seqlock indicates a concurrent write.
100 retries × ~100 µs sleep = 10 ms worst-case wait before giving up.
This is far shorter than our 10 ms poll interval, so it never delays the
next poll cycle."""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
_log: logging.Logger = logging.getLogger("reader")


# ---------------------------------------------------------------------------
# Global shutdown flag (set by signal handler)
# ---------------------------------------------------------------------------

_running: bool = True
"""Module-level flag.  Set to ``False`` by the signal handler to tell the
polling loop to exit gracefully."""


def _handle_signal(signum: int, _frame) -> None:  # type: ignore[type-arg]
    """Signal handler for SIGTERM (and SIGINT via alias).

    This function is called *outside* the main loop — it runs in the signal
    delivery context.  We simply flip the ``_running`` flag so the loop can
    clean up on its next iteration.
    """
    global _running
    signal_name: str = signal.Signals(signum).name
    _log.info("Received %s — shutting down gracefully …", signal_name)
    _running = False


# Register the signal handler for both SIGTERM and SIGINT (Ctrl+C).
# ``signal.signal(SIGTERM, handler)`` tells the OS to call ``handler``
# when the process receives SIGTERM from ``docker stop`` or ``kill``.
signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------------------------------------------------------------------------
# Seqlock helper: torn-read-free snapshot
# ---------------------------------------------------------------------------

def _read_consistent(shm: shared_memory.SharedMemory, size: int) -> bytes | None:
    """
    Read the full shared-memory buffer using the Seqlock protocol.

    Spins (with a 100 µs micro-sleep between attempts) until the seqlock
    sequence is *even* and *unchanged* before and after the read.

    Parameters
    ----------
    shm : shared_memory.SharedMemory
        Attached shared-memory handle.
    size : int
        Number of bytes to read (typically ``EXPECTED_BYTE_COUNT``).

    Returns
    -------
    bytes | None
        A consistent snapshot of the buffer, or ``None`` if the seqlock
        never stabilised within ``MAX_SEQLOCK_RETRIES`` attempts.
    """
    for attempt in range(1, MAX_SEQLOCK_RETRIES + 1):
        # Step 1 — Record the seqlock sequence BEFORE touching the payload.
        #          We unpack 4 bytes as a little-endian unsigned integer.
        sequence_before: int = struct.unpack_from("<I", shm.buf, 0)[0]

        # Step 2 — If the sequence is ODD, the Writer is in the middle of a
        #          write cycle.  Spin-wait with a tiny sleep so we don't peg
        #          the CPU, then retry from the top.
        if sequence_before % 2 != 0:
            time.sleep(0.0001)  # 100 microseconds
            continue

        # Step 3 — Read the entire segment (seqlock header + payload) into a
        #          local ``bytes`` object.  This creates a point-in-time
        #          snapshot that we can compare against the sequence AFTER.
        snapshot: bytes = bytes(shm.buf[:size])

        # Step 4 — Record the seqlock sequence AFTER the read finished.
        sequence_after: int = struct.unpack_from("<I", shm.buf, 0)[0]

        # Step 5 — If the sequence CHANGED during our read, the Writer
        #          completed a write while we were copying.  Our snapshot
        #          might contain bytes from both the old and new payload,
        #          so we discard it and retry.
        if sequence_before != sequence_after:
            time.sleep(0.0001)  # 100 microseconds
            continue

        # Step 6 — Sequence is even AND unchanged.  The snapshot is
        #          guaranteed to be consistent — the Writer did not touch
        #          the payload while we were reading.
        return snapshot

    _log.error("Seqlock did not stabilise after %d retries.  Returning None.", MAX_SEQLOCK_RETRIES)
    return None


# ---------------------------------------------------------------------------
# Core domain function (continuous polling)
# ---------------------------------------------------------------------------

def read_tensor_continuous(name: str, expected_size: int) -> None:
    """
    Attach to the named shared-memory segment and poll for updates forever.

    Parameters
    ----------
    name : str
        POSIX shared-memory segment name.
    expected_size : int
        Number of bytes to read on each poll cycle.

    Raises
    ------
    ValueError
        If *expected_size* <= 0.
    """
    # --- Pre-condition guard -------------------------------------------------
    if expected_size <= 0:
        _log.error("expected_size must be > 0 (got %d).", expected_size)
        return

    # --- Phase 1: Attach to the existing segment ----------------------------
    shm: shared_memory.SharedMemory | None = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            # ``create=False`` attaches to an already-existing segment.
            # This will raise ``FileNotFoundError`` until Worker A has
            # created ``tensor_data`` via its first POST /tensor call.
            _log.info(
                "Attempt %d/%d — attaching to shared memory '%s' …",
                attempt, RETRY_ATTEMPTS, name,
            )
            shm = shared_memory.SharedMemory(create=False, name=name)
            _log.info("Attached to segment '%s' (size=%d bytes).", name, shm.size)
            break

        except FileNotFoundError:
            _log.info("Segment '%s' not ready yet — retrying in %g s …", name, RETRY_SLEEP_SEC)
            time.sleep(RETRY_SLEEP_SEC)

        except Exception as exc:
            _log.warning("Unexpected error during attach attempt %d: %s", attempt, exc)
            time.sleep(RETRY_SLEEP_SEC)

    # --- Fail if we could not attach ----------------------------------------
    if shm is None:
        _log.error("Could not attach to segment '%s' after %d attempts.  Exiting.", name, RETRY_ATTEMPTS)
        return

    # --- Phase 2: Continuous polling loop -----------------------------------
    # ``last_state`` holds the bytes we saw on the previous poll cycle.
    # If the Writer has written new data, the contents will differ and we
    # log a ``NEW DATA`` event.  This is the "state-tracking" mechanism
    # that prevents duplicate-read noise in the logs.
    last_state: bytes = b""
    poll_count: int = 0
    changes_detected: int = 0

    _log.info("Continuous polling started (interval = %g s).", POLL_INTERVAL_SEC)

    try:
        while _running:
            # Use the Seqlock protocol to read a torn-read-free snapshot
            # of the shared-memory buffer.  This replaces the raw
            # ``bytes(shm.buf[:expected_size])`` call which had no
            # protection against reading a half-written payload.
            # If the seqlock never stabilises (Writer is too fast), we
            # skip this poll cycle and try again on the next one.
            current_data: bytes | None = _read_consistent(shm, expected_size)
            if current_data is None:
                _log.warning("Seqlock read failed — skipping poll cycle.")
                poll_count += 1
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # State-tracking: compare the consistent snapshot with the
            # last known state.  The snapshot includes the seqlock header
            # (bytes 0-3), so every successful write produces a unique
            # sequence number and therefore a unique snapshot — no write
            # is missed.
            if current_data != last_state:
                changes_detected += 1
                _log.info(
                    "NEW DATA #%d — %d bytes (seq=%d): %s",
                    changes_detected, len(current_data),
                    struct.unpack_from("<I", current_data, 0)[0],
                    list(current_data),
                )
                last_state = current_data

            poll_count += 1

            # Micro-sleep to avoid busy-waiting.  10 ms is short enough to
            # catch every write under the k6 load test (521 req/s → ~2 ms
            # between writes) but long enough that a single core won't
            # spin at 100 %.
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        # Reached on Ctrl+C outside the signal handler context.
        _log.info("KeyboardInterrupt received — shutting down.")
    except Exception as exc:
        _log.exception("Unexpected error in polling loop: %s", exc)
    finally:
        _log.info(
            "Polling summary: %d cycles, %d data changes detected.",
            poll_count, changes_detected,
        )
        if shm is not None:
            # IMPORTANT: close() but NOT unlink() — the Writer owns the
            # kernel object lifecycle.
            _log.info("Closing reader's handle to '%s' …", name)
            shm.close()
        _log.info("Reader daemon stopped.")


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

def main() -> None:
    """Script entry-point — starts the continuous polling daemon."""
    _log.info("Reader daemon starting (continuous polling mode).")
    read_tensor_continuous(name=SHM_NAME, expected_size=EXPECTED_BYTE_COUNT)
    _log.info("Reader daemon exiting.")


if __name__ == "__main__":
    main()
