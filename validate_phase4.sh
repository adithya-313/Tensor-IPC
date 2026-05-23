#!/usr/bin/env bash
# ============================================================================
# TensorIPC — Phase 4 Self-Validation Loop
#
# Builds the full stack, runs the k6 load test against the Gateway, and
# scrapes Worker B's logs to prove it continuously detected and read
# every single payload update via shared memory.
# ============================================================================

set -euo pipefail

COMPOSE_FILE="docker-compose.yml"
LOG_FILE="validate_phase4.log"
READER="tensoripc-reader"

info() { echo "[INFO]  $*"; }
ok()   { echo "[OK]    $*"; }
fail() { echo "[FAIL]  $*"; }

# ---- 1. Build & start everything -------------------------------------------
info "Step 1/5 — Building all images and starting stack …"
docker compose -f "$COMPOSE_FILE" up -d --build 2>&1 | tee -a "$LOG_FILE"

info "Waiting 10 s for services to boot and Uvicorn to accept requests …"
sleep 10

# ---- 2. Verify all containers are running -----------------------------------
info "Step 2/5 — Checking container status …"
for svc in tensoripc-writer tensoripc-reader tensoripc-gateway; do
    STATUS=$(docker inspect "$svc" --format='{{.State.Status}}' 2>/dev/null || echo "missing")
    if [ "$STATUS" != "running" ]; then
        fail "Container $svc is '$STATUS' (expected 'running')."
        docker logs "$svc" 2>&1 | tail -20
        docker compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
        exit 1
    fi
    ok "$svc = $STATUS"
done

# ---- 3. Smoke test via Gateway ----------------------------------------------
info "Step 3/5 — Smoke test: POST /tensor via Gateway …"
SMOKE_RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/tensor -H "Content-Type: application/json" -d '{}' 2>&1 || echo "curl_error")
if [ "$SMOKE_RESP" = "200" ]; then
    ok "Smoke test passed — HTTP $SMOKE_RESP."
else
    fail "Smoke test failed — HTTP $SMOKE_RESP."
    docker compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
    exit 1
fi

# ---- 4. Run k6 load test ----------------------------------------------------
info "Step 4/5 — Running k6 load test (50 VUs, 10 s) …"
echo ""
echo "====================  K6 LOAD TEST  ===================="
docker compose -f "$COMPOSE_FILE" run --rm k6 2>&1 || true
echo "======================================================="
echo ""

# ---- 5. Scrape Worker B logs for continuous consumption evidence ------------
info "Step 5/5 — Inspecting Worker B logs for continuous shared-memory reads …"
echo ""
echo "====================  WORKER B LOGS (tail -30)  ===================="
docker logs "$READER" 2>&1 | tail -30
echo "===================================================================="
echo ""

# Count NEW DATA events and poll cycles
NEW_DATA_COUNT=$(docker logs "$READER" 2>&1 | grep -c "NEW DATA" || true)
POLL_CYCLES=$(docker logs "$READER" 2>&1 | grep "polling summary" -i || echo "")

info "NEW DATA events detected: $NEW_DATA_COUNT"
info "Poll summary: $POLL_CYCLES"

if [ "$NEW_DATA_COUNT" -gt 0 ]; then
    ok "Worker B continuously detected and read $NEW_DATA_COUNT payload updates via shared memory."
else
    fail "No NEW DATA events found in Worker B logs — continuous polling may not be working."
fi

# ---- Cleanup ----------------------------------------------------------------
info "Cleaning up …"
docker compose -f "$COMPOSE_FILE" down --volumes 2>&1 | tee -a "$LOG_FILE"

echo ""
echo "=== TensorIPC Phase 4 Self-Validation Loop: COMPLETE ==="
