// ============================================================================
// TensorIPC — Gateway
//
// A multi-stage distroless reverse proxy that routes external HTTP requests
// to Worker A (the shared-memory writer) over an internal bridge network.
//
// Key design decisions (all beginner-friendly explanations inline):
//
//   1.  Structured JSON logging — every request is logged as a JSON object
//       so operators can grep / jq the logs for observability.
//
//   2.  Rate Limiter — a token-bucket middleware that protects Worker A from
//       being overwhelmed by too many concurrent requests (Resilience).
//
//   3.  Reverse Proxy — uses Go's stdlib ``httputil.ReverseProxy`` to forward
//       requests to ``http://worker_a:8080`` with zero extra dependencies.
//
//   4.  Graceful Shutdown — listens for SIGINT / SIGTERM on a separate
//       goroutine and drains in-flight connections before exiting.
// ============================================================================

package main

import (
	"context"       // Used to propagate cancellation signals across goroutines.
	"encoding/json" // For structured log output (one JSON object per line).
	"log"           // Standard logger; we wrap it for JSON output.
	"net/http"      // The entire HTTP server & client stack.
	"net/http/httputil" // ``ReverseProxy`` — the core of our gateway.
	"net/url"           // Used to parse the backend URL for the proxy.
	"os"               // Access to environment variables and OS signals.
	"os/signal"        // Lets us intercept OS signals (SIGINT, SIGTERM).
	"sync/atomic"      // Lock-free counters for the rate limiter and stats.
	"syscall"          // Provides the signal constants (SIGINT, SIGTERM).
	"time"             // Duration parsing, tickers, and timeout enforcement.
)

// ============================================================================
// Constants
// ============================================================================

// backendURL is where the reverse proxy forwards requests.
// Docker Compose DNS resolves "worker_a" to the container's internal IP on
// the ``tensor_net`` bridge network.  The port 8080 is where Worker A's
// hypothetical HTTP server would listen (to be implemented in a later phase).
const backendURL = "http://worker_a:8080"

// listenAddr is the address the gateway's HTTP server binds to *inside*
// the container.  Docker maps host port 8000 → container port 8000.
const listenAddr = ":8000"

// rateLimitTokens is the maximum number of requests that can be in-flight
// concurrently.  This is a simple token-bucket: each incoming request
// "takes" a token and releases it when done.
// Increased from 10 to 100 for the Phase 3 k6 benchmark (50 VUs).
// In production, tune this value to match Worker A's capacity.
const rateLimitTokens = 100

// readTimeoutSeconds is the maximum time we wait to *read* the entire
// request from a client (including the body).  Prevents slow-loris attacks.
const readTimeoutSeconds = 10

// writeTimeoutSeconds is the maximum time allowed to *write* the response
// back to the client.  Prevents a slow client from holding a connection open.
const writeTimeoutSeconds = 30

// shutdownTimeoutSeconds is the maximum time we wait for active requests
// to finish after receiving a termination signal.
const shutdownTimeoutSeconds = 15

// ============================================================================
// Structured JSON Logger
//
// Instead of using a third-party library like logrus or zap, we build a tiny
// wrapper around the standard ``log`` package that emits one JSON object per
// line.  This keeps the binary small and dependency-free.
// ============================================================================

// jsonLogger writes structured log entries as JSON lines to stderr.
// Using stderr keeps stdout clean for potential future machine parsing.
type jsonLogger struct {
	inner *log.Logger
}

// newJSONLogger creates a logger that writes JSON to stderr.
func newJSONLogger() *jsonLogger {
	return &jsonLogger{
		inner: log.New(os.Stderr, "", 0), // 0 = no prefix or flags; we control formatting entirely.
	}
}

// Log emits a JSON object with a fixed "ts" (timestamp) and "msg" field,
// plus any extra key-value pairs provided in ``fields``.
//
// Example output:
//
//	{"ts":"2026-05-23T12:00:00Z","msg":"request started","method":"GET","path":"/health"}
func (l *jsonLogger) Log(msg string, fields map[string]any) {
	// Merge the fixed fields with the caller-supplied fields.
	entry := map[string]any{
		"ts":   time.Now().UTC().Format(time.RFC3339Nano),
		"msg":  msg,
	}
	for k, v := range fields {
		entry[k] = v
	}

	// Marshal to JSON and write.  If marshalling fails, fall back to a
	// plain-text error so we never silently drop a log line.
	line, err := json.Marshal(entry)
	if err != nil {
		l.inner.Printf(`{"ts":"%s","msg":"json marshal error","error":"%s"}`, time.Now().UTC().Format(time.RFC3339Nano), err)
		return
	}
	l.inner.Println(string(line))
}

// ============================================================================
// Request-scoped context key (for passing values through middleware)
// ============================================================================

// ctxKey is a private type used as the key in context.WithValue so that no
// external package can accidentally overwrite or read our values.
type ctxKey string

// startTimeKey stores the time.Time when the request entered the middleware chain.
const startTimeKey ctxKey = "startTime"

// ============================================================================
// Middleware: Structured Access Logging
//
// Every HTTP request is logged as a JSON line containing:
//   - method, path, query parameters
//   - response status code
//   - duration (how long the request took)
//   - client IP
//
// This satisfies the Observability requirement from the architecture spec.
// ============================================================================

// loggingMiddleware wraps an http.Handler and logs each request as JSON.
func loggingMiddleware(logger *jsonLogger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Store the start time in the request context so the response
		// writer can read it later and compute the duration.
		ctx := context.WithValue(r.Context(), startTimeKey, time.Now())
		r = r.WithContext(ctx)

		// Wrap the ResponseWriter so we can capture the status code.
		// The standard http.ResponseWriter does not expose the status
		// code once WriteHeader() has been called.
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Log the "request started" event.
		logger.Log("request started", map[string]any{
			"method": r.Method,
			"path":   r.URL.Path,
			"query":  r.URL.RawQuery,
			"remote": r.RemoteAddr,
		})

		// Call the next handler in the chain.
		next.ServeHTTP(lrw, r)

		// After the handler returns, the status code and duration are known.
		duration := time.Since(r.Context().Value(startTimeKey).(time.Time))
		logger.Log("request completed", map[string]any{
			"method":   r.Method,
			"path":     r.URL.Path,
			"status":   lrw.statusCode,
			"duration": duration.String(),
			"remote":   r.RemoteAddr,
		})
	})
}

// loggingResponseWriter wraps http.ResponseWriter to capture the status code.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code before delegating to the real writer.
func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// ============================================================================
// Middleware: Rate Limiter (Token-Bucket)
//
// We use a buffered channel as a counting semaphore.  Each incoming request
// must acquire a "token" from the channel before proceeding.  When the
// request finishes, it releases the token back.
//
// If the channel is full (all tokens are taken), the request is rejected
// immediately with HTTP 429 Too Many Requests.  This is a "hard" rate limit.
//
// This satisfies the Resilience Engineering requirement.
// ============================================================================

// rateLimiterMiddleware creates a middleware that limits concurrency to
// ``maxTokens`` simultaneous requests using a buffered channel semaphore.
func rateLimiterMiddleware(maxTokens int, logger *jsonLogger, next http.Handler) http.Handler {
	// semaphore is a buffered channel that acts as our token bucket.
	// - buffer size = maxTokens
	// - each token is an empty struct{} (zero bytes allocation)
	semaphore := make(chan struct{}, maxTokens)

	// Pre-fill the channel with tokens so that the first N requests can
	// acquire one immediately.
	for i := 0; i < maxTokens; i++ {
		semaphore <- struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to acquire a token from the semaphore.
		// The "default" case makes this a non-blocking select — if the
		// channel is empty, we reject immediately instead of waiting.
		select {
		case <-semaphore:
			// Token acquired — proceed with the request.
			// After the request finishes, release the token back.
			defer func() { semaphore <- struct{}{} }()
			next.ServeHTTP(w, r)

		default:
			// All tokens are in use — reject with 429.
			logger.Log("rate limit exceeded", map[string]any{
				"method": r.Method,
				"path":   r.URL.Path,
				"remote": r.RemoteAddr,
			})
			http.Error(w, `{"error":"too many requests"}`, http.StatusTooManyRequests)
		}
	})
}

// ============================================================================
// Health Check Handler
// ============================================================================

// healthHandler responds with HTTP 200 and a simple JSON status.
// Worker A is not an HTTP server yet, so this only checks the gateway itself.
func healthHandler(logger *jsonLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"gateway"}`))
	}
}

// ============================================================================
// Reverse Proxy Handler
//
// ``httputil.NewSingleHostReverseProxy`` creates a proxy that rewrites
// incoming HTTP requests and forwards them to a fixed backend URL.
//
// How it works:
//
//	1. Client sends GET /tensor to the gateway on port 8000.
//	2. The proxy rewrites the URL to http://worker_a:8080/tensor.
//	3. Docker's built-in DNS resolves "worker_a" to Worker A's container IP
//	   on the tensor_net bridge network.
//	4. The proxy forwards the request and streams the response back.
//
// If the backend is unreachable (e.g. Worker A is not listening on HTTP yet),
// the proxy returns HTTP 502 Bad Gateway or 503 Service Unavailable.
// This is the Graceful Degradation behaviour we verify in Phase 2.
// ============================================================================

// newProxyHandler parses the backend URL and returns an http.Handler that
// reverse-proxies all requests to that backend.
func newProxyHandler(logger *jsonLogger) (http.Handler, error) {
	backend, err := url.Parse(backendURL)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(backend)

	// Custom error handler — if the backend is unreachable, log the failure
	// and return a JSON error response instead of the default HTML.
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Log("proxy error", map[string]any{
			"method":     r.Method,
			"path":       r.URL.Path,
			"backend":    backendURL,
			"error":      err.Error(),
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream unavailable","backend":"` + backendURL + `"}`))
	}

	return proxy, nil
}

// ============================================================================
// Server Metrics (atomic counters — accessible via /metrics in future)
// ============================================================================

var (
	totalRequests atomic.Int64
	activeRequests atomic.Int64
)

// metricsMiddleware increments the request counters.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalRequests.Add(1)
		activeRequests.Add(1)
		defer activeRequests.Add(-1)
		next.ServeHTTP(w, r)
	})
}

// ============================================================================
// Main — entry point
// ============================================================================

func main() {
	logger := newJSONLogger()

	logger.Log("starting gateway", map[string]any{
		"listen":      listenAddr,
		"backend":     backendURL,
		"rate_limit":  rateLimitTokens,
	})

	// ---- Build the middleware chain ----------------------------------------
	// Each middleware wraps the next handler.  The order is:
	//   1. Metrics
	//   2. Rate limiter
	//   3. Access log
	//   4. Router (health check OR reverse proxy)

	// Step 1: Create the proxy handler.
	proxyHandler, err := newProxyHandler(logger)
	if err != nil {
		logger.Log("failed to create proxy", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	// Step 2: Build the HTTP mux (router).
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler(logger))

	// All other paths (including /tensor) are forwarded to the backend.
	mux.Handle("/", proxyHandler)

	// Step 3: Wrap with middleware (outermost first).
	var h http.Handler = mux
	h = loggingMiddleware(logger, h)
	h = rateLimiterMiddleware(rateLimitTokens, logger, h)
	h = metricsMiddleware(h)

	// ---- HTTP Server ------------------------------------------------------
	// We use http.Server instead of http.ListenAndServe because it gives us
	// access to the Shutdown() method for graceful termination.
	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      h,
		ReadTimeout:  readTimeoutSeconds * time.Second,
		WriteTimeout: writeTimeoutSeconds * time.Second,
	}

	// ---- Graceful Shutdown (SIGINT / SIGTERM) -------------------------------
	// 1. Create a channel that receives OS signals.
	// 2. Start the server in a separate goroutine so it doesn't block.
	// 3. Wait for a signal on the main goroutine.
	// 4. Call srv.Shutdown() with a timeout context to drain connections.
	//
	// A goroutine is a lightweight thread managed by the Go runtime.
	// "go function()" starts function() concurrently.
	// The caller does NOT wait for it to finish — execution continues
	// immediately on the next line.

	// Channel to receive OS signals (buffered so we don't miss it).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Start the server in a background goroutine.
	go func() {
		logger.Log("server listening", map[string]any{"addr": srv.Addr})
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log("server error", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
	}()

	// Block the main goroutine until we receive a termination signal.
	sig := <-quit
	logger.Log("shutdown signal received", map[string]any{"signal": sig.String()})

	// Create a context with a deadline for the shutdown to complete.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeoutSeconds*time.Second)
	defer cancel()

	// Shutdown gracefully — this blocks until all active connections are
	// closed OR the context deadline expires, whichever comes first.
	if err := srv.Shutdown(ctx); err != nil {
		logger.Log("shutdown error", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	logger.Log("server stopped", nil)
}
