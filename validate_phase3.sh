#!/usr/bin/env bash
# ============================================================================
# TensorIPC — Phase 3 Self-Validation Loop
#
# Builds the upgraded Worker A and Gateway, waits for Uvicorn to boot, kicks
# off the k6 load test, and captures detailed telemetry.
# ============================================================================

set -euo pipefail

COMPOSE_FILE="docker-compose.yml"
LOG_FILE="validate_phase3.log"

info()  { echo "[INFO]  $*"; }
ok()    { echo "[OK]    $*"; }
fail()  { echo "[FAIL]  $*"; }

# ---- 1. Build & start gateway + worker_a -----------------------------------
info "Step 1/5 — Building gateway and worker_a images …"
docker compose -f "$COMPOSE_FILE" up -d --build gateway worker_a 2>&1 | tee -a "$LOG_FILE"

info "Waiting 10 s for Uvicorn to boot and healthchecks to pass …"
sleep 10

# ---- 2. Verify both containers are running ----------------------------------
info "Step 2/5 — Checking container status …"

for svc in tensoripc-gateway tensoripc-writer; do
    STATUS=$(docker inspect "$svc" --format='{{.State.Status}}' 2>/dev/null || echo "missing")
    if [ "$STATUS" != "running" ]; then
        fail "Container $svc is '$STATUS' (expected 'running')."
        docker logs "$svc" 2>&1 | tail -20
        docker compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
        exit 1
    fi
    ok "$svc = $STATUS"
done

# ---- 3. Quick smoke test via the Gateway ------------------------------------
info "Step 3/5 — Smoke test: POST /tensor via Gateway …"

SMOKE_RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/tensor -H "Content-Type: application/json" -d '{}' 2>&1 || echo "curl_error")
SMOKE_BODY=$(curl -s -X POST http://localhost:8000/tensor -H "Content-Type: application/json" -d '{}' 2>&1 || echo "curl_error")

if [ "$SMOKE_RESP" = "200" ]; then
    ok "Smoke test passed — HTTP $SMOKE_RESP — body: $SMOKE_BODY"
else
    fail "Smoke test failed — HTTP $SMOKE_RESP — body: $SMOKE_BODY"
    docker compose -f "$COMPOSE_FILE" down >> "$LOG_FILE" 2>&1
    exit 1
fi

# ---- 4. Execute k6 load test -----------------------------------------------
info "Step 4/5 — Running k6 load test (50 VUs, 10 s) …"
echo ""
echo "====================  K6 LOAD TEST  ===================="

# Capture k6 stdout/stderr.  The container runs and exits.
K6_OUTPUT=$(docker compose -f "$COMPOSE_FILE" run --rm k6 2>&1 || true)
echo "$K6_OUTPUT"

echo "======================================================="
echo ""

# ---- 5. Scrape worker_a logs for shared-memory evidence ----
info "Step 5/5 — Inspecting worker_a logs for shared-memory writes …"
echo ""
echo "====================  WORKER A LOGS (last 20)  ===================="
docker logs tensoripc-writer 2>&1 | tail -20
echo "==================================================================="
echo ""

# Check for evidence of shared-memory allocation under load
SHM_LINES=$(docker logs tensoripc-writer 2>&1 | grep -c "Wrote\|Creating shared-memory" || true)
if [ "$SHM_LINES" -gt 0 ]; then
    ok "Shared memory was dynamically allocated ($SHM_LINES write events detected in logs)."
else
    fail "No shared-memory write events found in worker_a logs."
fi

# ---- Summary ---------------------------------------------------------------
echo ""
echo "=== TensorIPC Phase 3 Self-Validation Loop: COMPLETE ==="
echo "Check the K6 OUTPUT above for p95/p99 latency and RPS."
