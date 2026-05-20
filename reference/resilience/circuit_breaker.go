package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// CircuitBreaker implements the circuit breaker pattern to prevent
// cascading failures in distributed systems. It monitors for failures
// and temporarily blocks requests when failure threshold is exceeded.
//
// States:
// - Closed: requests pass through normally
// - Open: requests are immediately rejected
// - HalfOpen: limited requests allowed to test recovery
type CircuitBreaker struct {
	mu sync.RWMutex

	// Name identifies this circuit breaker
	Name string

	// State current circuit state
	State CircuitState

	// Config circuit breaker configuration
	Config CircuitBreakerConfig

	// Metrics tracks circuit breaker performance
	Metrics *CircuitBreakerMetrics

	// failures consecutive failure count
	failures int

	// successCount consecutive success count in half-open state
	successCount int

	// lastFailureTime when last failure occurred
	lastFailureTime time.Time

	// lastStateChange when state last changed
	lastStateChange time.Time

	// openUntil when to transition from open to half-open
	openUntil time.Time
}

// CircuitState represents the circuit breaker state.
type CircuitState string

const (
	// StateClosed circuit is closed, requests pass through
	StateClosed CircuitState = "closed"

	// StateOpen circuit is open, requests are rejected
	StateOpen CircuitState = "open"

	// StateHalfOpen circuit is testing recovery
	StateHalfOpen CircuitState = "half_open"
)

// CircuitBreakerConfig holds circuit breaker configuration.
type CircuitBreakerConfig struct {
	// MaxFailures failures before opening circuit
	MaxFailures int

	// ResetTimeout time to wait before transitioning to half-open
	ResetTimeout time.Duration

	// HalfOpenRequests requests to allow in half-open state
	HalfOpenRequests int

	// FailureThreshold percentage of failures to trigger open (0.0-1.0)
	FailureThreshold float64

	// VolumeThreshold minimum requests before considering failure rate
	VolumeThreshold int

	// Timeout maximum execution time before considering timeout a failure
	Timeout time.Duration

	// IsFailure custom function to determine if error is a failure
	IsFailure func(error) bool
}

// CircuitBreakerMetrics tracks circuit breaker statistics.
type CircuitBreakerMetrics struct {
	// TotalRequests total number of requests
	TotalRequests int64

	// SuccessfulRequests successful requests
	SuccessfulRequests int64

	// FailedRequests failed requests
	FailedRequests int64

	// RejectedRequests rejected due to open circuit
	RejectedRequests int64

	// StateChanges number of state transitions
	StateChanges int64

	// LastFailure last failure time
	LastFailure time.Time

	// TimeInState time spent in each state
	TimeInState map[CircuitState]time.Duration

	mu sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(name string, config CircuitBreakerConfig) *CircuitBreaker {
	// Set defaults
	if config.MaxFailures == 0 {
		config.MaxFailures = 5
	}
	if config.ResetTimeout == 0 {
		config.ResetTimeout = 30 * time.Second
	}
	if config.HalfOpenRequests == 0 {
		config.HalfOpenRequests = 3
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 0.5 // 50%
	}
	if config.VolumeThreshold == 0 {
		config.VolumeThreshold = 10
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.IsFailure == nil {
		config.IsFailure = defaultIsFailure
	}

	return &CircuitBreaker{
		Name:            name,
		State:           StateClosed,
		Config:          config,
		Metrics:         &CircuitBreakerMetrics{
			TimeInState: make(map[CircuitState]time.Duration),
		},
		lastStateChange: time.Now(),
	}
}

// Execute executes a function with circuit breaker protection.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	cb.mu.Lock()

	// Check if circuit is open
	if cb.State == StateOpen {
		// Check if it's time to transition to half-open
		if time.Now().After(cb.openUntil) {
			cb.setState(StateHalfOpen)
			cb.successCount = 0
		} else {
			cb.mu.Unlock()
			cb.recordRejection()
			return nil, ErrCircuitOpen
		}
	}

	// In half-open state, limit concurrent requests
	if cb.State == StateHalfOpen {
		if cb.successCount >= cb.Config.HalfOpenRequests {
			cb.mu.Unlock()
			cb.recordRejection()
			return nil, ErrCircuitOpen
		}
	}

	cb.mu.Unlock()

	// Execute with timeout
	ctx, cancel := context.WithTimeout(ctx, cb.Config.Timeout)
	defer cancel()

	resultChan := make(chan executeResult, 1)
	go func() {
		result, err := fn(ctx)
		resultChan <- executeResult{result: result, err: err}
	}()

	// Wait for result or timeout
	select {
	case res := <-resultChan:
		if res.err != nil && cb.Config.IsFailure(res.err) {
			cb.recordFailure(res.err)
			return res.result, res.err
		}
		cb.recordSuccess()
		return res.result, res.err

	case <-ctx.Done():
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			cb.recordFailure(ErrTimeout)
			return nil, ErrTimeout
		}
		return nil, err
	}
}

// executeResult holds function execution result.
type executeResult struct {
	result interface{}
	err    error
}

// recordSuccess records a successful execution.
func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0

	cb.Metrics.mu.Lock()
	cb.Metrics.TotalRequests++
	cb.Metrics.SuccessfulRequests++
	cb.Metrics.mu.Unlock()

	if cb.State == StateHalfOpen {
		cb.successCount++
		// If enough successes, close the circuit
		if cb.successCount >= cb.Config.HalfOpenRequests {
			cb.setState(StateClosed)
		}
	}
}

// recordFailure records a failed execution.
func (cb *CircuitBreaker) recordFailure(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	cb.Metrics.mu.Lock()
	cb.Metrics.TotalRequests++
	cb.Metrics.FailedRequests++
	cb.Metrics.LastFailure = cb.lastFailureTime
	cb.Metrics.mu.Unlock()

	// Check if should open circuit
	if cb.shouldOpen() {
		cb.setState(StateOpen)
		cb.openUntil = time.Now().Add(cb.Config.ResetTimeout)
	}

	// In half-open state, any failure reopens circuit
	if cb.State == StateHalfOpen {
		cb.setState(StateOpen)
		cb.openUntil = time.Now().Add(cb.Config.ResetTimeout)
	}
}

// recordRejection records a rejected request.
func (cb *CircuitBreaker) recordRejection() {
	cb.Metrics.mu.Lock()
	defer cb.Metrics.mu.Unlock()

	cb.Metrics.RejectedRequests++
}

// shouldOpen determines if circuit should open.
func (cb *CircuitBreaker) shouldOpen() bool {
	// Check if enough requests to evaluate
	if cb.Metrics.TotalRequests < int64(cb.Config.VolumeThreshold) {
		return false
	}

	// Check consecutive failures
	if cb.failures >= cb.Config.MaxFailures {
		return true
	}

	// Check failure rate
	cb.Metrics.mu.RLock()
	totalRequests := cb.Metrics.TotalRequests
	failedRequests := cb.Metrics.FailedRequests
	cb.Metrics.mu.RUnlock()

	if totalRequests == 0 {
		return false
	}

	failureRate := float64(failedRequests) / float64(totalRequests)
	return failureRate >= cb.Config.FailureThreshold
}

// setState transitions to a new state.
func (cb *CircuitBreaker) setState(newState CircuitState) {
	if cb.State == newState {
		return
	}

	// Update time in previous state
	elapsed := time.Since(cb.lastStateChange)
	cb.Metrics.mu.Lock()
	cb.Metrics.TimeInState[cb.State] += elapsed
	cb.Metrics.StateChanges++
	cb.Metrics.mu.Unlock()

	cb.State = newState
	cb.lastStateChange = time.Now()

	// Reset counters on state change
	if newState == StateClosed {
		cb.failures = 0
		cb.successCount = 0
	}
}

// GetState returns the current circuit state.
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.State
}

// Reset manually resets the circuit breaker.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	cb.failures = 0
	cb.successCount = 0
}

// GetMetrics returns a copy of the metrics.
func (cb *CircuitBreaker) GetMetrics() CircuitBreakerMetrics {
	cb.Metrics.mu.RLock()
	defer cb.Metrics.mu.RUnlock()

	metrics := *cb.Metrics
	metrics.TimeInState = make(map[CircuitState]time.Duration, len(cb.Metrics.TimeInState))
	for k, v := range cb.Metrics.TimeInState {
		metrics.TimeInState[k] = v
	}

	return metrics
}

// defaultIsFailure default failure detection.
func defaultIsFailure(err error) bool {
	// Consider all errors as failures except context cancellation
	return !errors.Is(err, context.Canceled)
}

// Circuit breaker errors
var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
	ErrTimeout     = errors.New("execution timeout")
)

// CircuitBreakerRegistry manages multiple circuit breakers.
type CircuitBreakerRegistry struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
}

// NewCircuitBreakerRegistry creates a new registry.
func NewCircuitBreakerRegistry() *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets an existing circuit breaker or creates a new one.
func (r *CircuitBreakerRegistry) GetOrCreate(name string, config CircuitBreakerConfig) *CircuitBreaker {
	r.mu.RLock()
	if breaker, exists := r.breakers[name]; exists {
		r.mu.RUnlock()
		return breaker
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if breaker, exists := r.breakers[name]; exists {
		return breaker
	}

	breaker := NewCircuitBreaker(name, config)
	r.breakers[name] = breaker
	return breaker
}

// Get retrieves a circuit breaker by name.
func (r *CircuitBreakerRegistry) Get(name string) (*CircuitBreaker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	breaker, exists := r.breakers[name]
	if !exists {
		return nil, fmt.Errorf("circuit breaker %s not found", name)
	}

	return breaker, nil
}

// List returns all circuit breaker names.
func (r *CircuitBreakerRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.breakers))
	for name := range r.breakers {
		names = append(names, name)
	}

	return names
}

// GetAllMetrics returns metrics for all circuit breakers.
func (r *CircuitBreakerRegistry) GetAllMetrics() map[string]CircuitBreakerMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metrics := make(map[string]CircuitBreakerMetrics, len(r.breakers))
	for name, breaker := range r.breakers {
		metrics[name] = breaker.GetMetrics()
	}

	return metrics
}
