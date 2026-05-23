#!/usr/bin/env bash
# ============================================================================
# TensorIPC — Self-Validation Loop
#
# Builds the two Docker images, starts the stack, waits for both containers
# to finish, inspects their logs (especially Worker B's validation result),
# and then tears everything down.
#
# Usage:
#   chmod +x validate.sh && ./validate.sh
# ============================================================================

set -euo pipefail  # strict mode: exit on error, undefined var, or pipe failure

COMPOSE_FILE="docker-compose.yml"
WRITER="tensoripc-writer"
READER="tensoripc-reader"
LOG_FILE="validate.log"

# ---- helpers ---------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
ok()    { echo "[OK]    $*"; }
fail()  { echo "[FAIL]  $*"; }

# ---- 1. Build & start ------------------------------------------------------
info "Step 1/4 — Building images and starting containers ..."
docker-compose -f "$COMPOSE_FILE" up -d --build 2>&1 | tee -a "$LOG_FILE"

info "Waiting 5 s for containers to initialise ..."
sleep 5

# ---- 2. Assert both containers are running ---------------------------------
info "Step 2/4 — Checking container status ..."

WRITER_STATUS=$(docker inspect "$WRITER" --format='{{.State.Status}}' 2>/dev/null || echo "missing")
READER_STATUS=$(docker inspect "$READER" --format='{{.State.Status}}' 2>/dev/null || echo "missing")

if [ "$WRITER_STATUS" != "running" ] && [ "$WRITER_STATUS" != "exited" ]; then
    fail "Writer container status is '$WRITER_STATUS' (expected 'running' or 'exited')."
    docker logs "$WRITER" 2>&1 | tail -20
    docker-compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
    exit 1
fi
ok "Writer container = $WRITER_STATUS"

if [ "$READER_STATUS" != "running" ] && [ "$READER_STATUS" != "exited" ]; then
    fail "Reader container status is '$READER_STATUS' (expected 'running' or 'exited')."
    docker logs "$READER" 2>&1 | tail -20
    docker-compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
    exit 1
fi
ok "Reader container = $READER_STATUS"

# ---- 3. Wait for completion & check logs -----------------------------------
info "Step 3/4 — Waiting up to 45 s for Reader to produce output ..."

# The reader retries for up to ~40 s; we'll poll logs for the key message.
SUCCESS=1
for i in $(seq 1 45); do
    if docker logs "$READER" 2>&1 | grep -q "VALIDATION PASSED"; then
        SUCCESS=0
        break
    fi
    if docker logs "$READER" 2>&1 | grep -q "VALIDATION FAILED"; then
        SUCCESS=2
        break
    fi
    sleep 1
done

echo ""
echo "====================  WRITER LOG  ===================="
docker logs "$WRITER" 2>&1
echo ""
echo "====================  READER LOG  ===================="
docker logs "$READER" 2>&1
echo "======================================================"
echo ""

case "$SUCCESS" in
    0)
        ok "VALIDATION PASSED — Reader successfully read the shared-memory segment written by Worker A."
        ;;
    2)
        fail "VALIDATION FAILED — Reader detected data mismatch. See Reader log above."
        docker-compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
        exit 1
        ;;
    *)
        fail "Reader did not produce a validation result within 45 s."
        fail "Check the READER LOG above for errors."
        docker-compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
        exit 1
        ;;
esac

# ---- 4. Clean up -----------------------------------------------------------
info "Step 4/4 — Cleaning up ..."
docker-compose -f "$COMPOSE_FILE" down --volumes 2>&1 | tee -a "$LOG_FILE"
ok "Cleanup complete."

echo ""
echo "=== TensorIPC Self-Validation Loop: ALL CHECKS PASSED ==="
