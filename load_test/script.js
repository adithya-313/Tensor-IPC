// ============================================================================
// TensorIPC — Phase 3 Load Test (k6)
//
// Simulates 50 concurrent virtual users (VUs) sending POST /tensor requests
// to the Gateway over a 10-second period.  The Gateway reverse-proxies each
// request to Worker A, which allocates kernel-level shared memory.
//
// Metrics captured automatically by k6:
//   - http_req_duration  (p50, p90, p95, p99 latency)
//   - http_reqs          (total requests → RPS)
//   - http_req_failed    (failure rate)
// ============================================================================

import http from 'k6/http';
import { check } from 'k6';

// ---------------------------------------------------------------------------
// Test configuration
// ---------------------------------------------------------------------------
export let options = {
    // vus — number of simultaneous virtual users (connections).
    vus: 50,
    // duration — how long the test runs.
    duration: '10s',
    // thresholds — fail the test if p95 exceeds 2 seconds or error rate > 1 %.
    thresholds: {
        http_req_duration: ['p(95)<2000'],  // 95th percentile must be < 2 s
        http_req_failed: ['rate<0.01'],      // failure rate must be < 1 %
    },
};

// ---------------------------------------------------------------------------
// Default function — called once per VU per iteration
// ---------------------------------------------------------------------------
export default function () {
    // The Gateway is reachable by its Docker Compose service name on the
    // ``tensor_net`` bridge network.  Port 8000 is the gateway's external
    // port (mapped from container port 8000).
    let url = 'http://gateway:8000/tensor';

    // Send a POST request with an empty JSON body (the writer ignores it).
    let response = http.post(url, '{}', {
        headers: { 'Content-Type': 'application/json' },
    });

    // Validate the response.
    // ``check`` is a k6 built-in that records pass/fail metrics.
    check(response, {
        'status is 200': (r) => r.status === 200,
        'body has success status': (r) => JSON.parse(r.body).status === 'success',
        'body has shm_name tensor_data': (r) => JSON.parse(r.body).shm_name === 'tensor_data',
        'body reports 64 bytes written': (r) => JSON.parse(r.body).bytes_written === 64,
    });
}
