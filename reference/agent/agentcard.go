package agent

import (
	"encoding/json"
	"time"
)

// AgentCard represents an agent's capabilities and metadata according to
// the A2A (Agent-to-Agent) protocol specification. It enables agent discovery,
// capability negotiation, and interoperability in multi-agent systems.
//
// The AgentCard follows the A2A protocol standard for agent description
// and capability advertisement. It can be serialized to JSON for exchange
// between agents and service registries.
type AgentCard struct {
	// Core identification
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Version     string    `json:"version"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Capabilities describe what the agent can do
	Capabilities []A2ACapability `json:"capabilities"`

	// Services expose agent functionality
	Services []A2AService `json:"services"`

	// Protocols supported communication protocols
	Protocols []string `json:"protocols"`

	// Endpoints for agent communication
	Endpoints []A2AEndpoint `json:"endpoints"`

	// Metadata arbitrary agent attributes
	Metadata map[string]interface{} `json:"metadata"`

	// Status current operational status
	Status A2AStatus `json:"status"`

	// Trust information for security
	Trust A2ATrust `json:"trust,omitempty"`

	// Constraints operational limits
	Constraints A2AConstraints `json:"constraints,omitempty"`
}

// A2ACapability describes a specific ability of the agent.
type A2ACapability struct {
	// ID uniquely identifies this capability
	ID string `json:"id"`

	// Name human-readable capability name
	Name string `json:"name"`

	// Description explains what this capability does
	Description string `json:"description"`

	// Category groups related capabilities
	Category string `json:"category"`

	// InputSchema JSON Schema describing expected input
	InputSchema json.RawMessage `json:"input_schema"`

	// OutputSchema JSON Schema describing produced output
	OutputSchema json.RawMessage `json:"output_schema"`

	// Cost estimated computational cost (arbitrary units)
	Cost float64 `json:"cost,omitempty"`

	// Latency expected response time
	Latency A2ALatency `json:"latency,omitempty"`

	// Concurrency maximum parallel executions
	Concurrency int `json:"concurrency,omitempty"`

	// Dependencies required capabilities or services
	Dependencies []string `json:"dependencies,omitempty"`

	// Tags for classification and discovery
	Tags []string `json:"tags,omitempty"`
}

// A2AService represents a callable service provided by the agent.
type A2AService struct {
	// ID uniquely identifies this service
	ID string `json:"id"`

	// Name service name
	Name string `json:"name"`

	// Description explains the service purpose
	Description string `json:"description"`

	// Method invocation method ("POST", "RPC", "GraphQL")
	Method string `json:"method"`

	// Path service endpoint path
	Path string `json:"path"`

	// RequestFormat expected request format ("json", "protobuf", "msgpack")
	RequestFormat string `json:"request_format"`

	// ResponseFormat response format
	ResponseFormat string `json:"response_format"`

	// Parameters service parameters
	Parameters []A2AParameter `json:"parameters"`

	// Returns service return values
	Returns []A2AReturn `json:"returns"`

	// Authentication required authentication
	Authentication []string `json:"authentication,omitempty"`

	// RateLimit maximum requests per time window
	RateLimit A2ARateLimit `json:"rate_limit,omitempty"`
}

// A2AParameter describes a service parameter.
type A2AParameter struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Description string          `json:"description"`
	Required    bool            `json:"required"`
	Default     interface{}     `json:"default,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Constraints interface{}     `json:"constraints,omitempty"`
}

// A2AReturn describes a service return value.
type A2AReturn struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// A2AEndpoint specifies a communication endpoint.
type A2AEndpoint struct {
	// Protocol communication protocol ("http", "grpc", "mqtt", "kafka")
	Protocol string `json:"protocol"`

	// Address endpoint address
	Address string `json:"address"`

	// Port listening port
	Port int `json:"port,omitempty"`

	// Path endpoint path
	Path string `json:"path,omitempty"`

	// Secure indicates if the endpoint uses encryption
	Secure bool `json:"secure"`

	// Priority endpoint preference (lower = higher priority)
	Priority int `json:"priority,omitempty"`
}

// A2AStatus describes the agent's current operational status.
type A2AStatus struct {
	// State current state ("active", "idle", "busy", "offline")
	State string `json:"state"`

	// Health health status ("healthy", "degraded", "unhealthy")
	Health string `json:"health"`

	// Uptime duration since last start
	Uptime time.Duration `json:"uptime"`

	// LoadAverage current load (0.0 - 1.0)
	LoadAverage float64 `json:"load_average,omitempty"`

	// ActiveTasks number of tasks in progress
	ActiveTasks int `json:"active_tasks,omitempty"`

	// QueuedTasks number of tasks waiting
	QueuedTasks int `json:"queued_tasks,omitempty"`

	// LastActive timestamp of last activity
	LastActive time.Time `json:"last_active"`

	// Message optional status message
	Message string `json:"message,omitempty"`
}

// A2ATrust contains security and trust information.
type A2ATrust struct {
	// PublicKey agent's public key for verification
	PublicKey string `json:"public_key,omitempty"`

	// Certificate X.509 certificate
	Certificate string `json:"certificate,omitempty"`

	// Signature digital signature of the AgentCard
	Signature string `json:"signature,omitempty"`

	// Issuer trusted authority that issued credentials
	Issuer string `json:"issuer,omitempty"`

	// ExpiresAt credential expiration time
	ExpiresAt time.Time `json:"expires_at,omitempty"`

	// Revoked indicates if credentials are revoked
	Revoked bool `json:"revoked,omitempty"`

	// TrustScore agent trust score (0.0 - 1.0)
	TrustScore float64 `json:"trust_score,omitempty"`
}

// A2AConstraints defines operational limits.
type A2AConstraints struct {
	// MaxConcurrentRequests maximum parallel requests
	MaxConcurrentRequests int `json:"max_concurrent_requests,omitempty"`

	// MaxQueueSize maximum queued tasks
	MaxQueueSize int `json:"max_queue_size,omitempty"`

	// RequestTimeout maximum request processing time
	RequestTimeout time.Duration `json:"request_timeout,omitempty"`

	// MemoryLimit maximum memory usage in bytes
	MemoryLimit int64 `json:"memory_limit,omitempty"`

	// CPULimit maximum CPU usage (cores)
	CPULimit float64 `json:"cpu_limit,omitempty"`

	// RateLimit global rate limit
	RateLimit A2ARateLimit `json:"rate_limit,omitempty"`

	// AllowedOrigins permitted caller origins
	AllowedOrigins []string `json:"allowed_origins,omitempty"`
}

// A2ALatency describes expected response times.
type A2ALatency struct {
	// Min minimum latency
	Min time.Duration `json:"min"`

	// Max maximum latency
	Max time.Duration `json:"max"`

	// Average typical latency
	Average time.Duration `json:"average"`

	// P50 median latency
	P50 time.Duration `json:"p50,omitempty"`

	// P95 95th percentile latency
	P95 time.Duration `json:"p95,omitempty"`

	// P99 99th percentile latency
	P99 time.Duration `json:"p99,omitempty"`
}

// A2ARateLimit defines rate limiting parameters.
type A2ARateLimit struct {
	// Requests maximum requests per window
	Requests int `json:"requests"`

	// Window time window duration
	Window time.Duration `json:"window"`

	// Burst maximum burst size
	Burst int `json:"burst,omitempty"`
}

// ToJSON serializes the AgentCard to JSON.
func (ac *AgentCard) ToJSON() ([]byte, error) {
	return json.MarshalIndent(ac, "", "  ")
}

// FromJSON deserializes an AgentCard from JSON.
func FromJSON(data []byte) (*AgentCard, error) {
	var ac AgentCard
	err := json.Unmarshal(data, &ac)
	return &ac, err
}

// Validate checks if the AgentCard is valid according to A2A spec.
func (ac *AgentCard) Validate() error {
	if ac.ID == "" {
		return &A2AError{Code: "INVALID_ID", Message: "agent ID is required"}
	}
	if ac.Name == "" {
		return &A2AError{Code: "INVALID_NAME", Message: "agent name is required"}
	}
	if ac.Type == "" {
		return &A2AError{Code: "INVALID_TYPE", Message: "agent type is required"}
	}
	if len(ac.Endpoints) == 0 {
		return &A2AError{Code: "NO_ENDPOINTS", Message: "at least one endpoint is required"}
	}
	return nil
}

// FindCapability searches for a capability by ID or name.
func (ac *AgentCard) FindCapability(idOrName string) *A2ACapability {
	for i := range ac.Capabilities {
		cap := &ac.Capabilities[i]
		if cap.ID == idOrName || cap.Name == idOrName {
			return cap
		}
	}
	return nil
}

// FindService searches for a service by ID or name.
func (ac *AgentCard) FindService(idOrName string) *A2AService {
	for i := range ac.Services {
		svc := &ac.Services[i]
		if svc.ID == idOrName || svc.Name == idOrName {
			return svc
		}
	}
	return nil
}

// HasCapability checks if the agent has a specific capability.
func (ac *AgentCard) HasCapability(capabilityID string) bool {
	return ac.FindCapability(capabilityID) != nil
}

// MatchesCriteria checks if the agent matches search criteria.
func (ac *AgentCard) MatchesCriteria(criteria map[string]interface{}) bool {
	if agentType, ok := criteria["type"].(string); ok {
		if ac.Type != agentType {
			return false
		}
	}

	if requiredCaps, ok := criteria["capabilities"].([]string); ok {
		for _, reqCap := range requiredCaps {
			if !ac.HasCapability(reqCap) {
				return false
			}
		}
	}

	if minTrust, ok := criteria["min_trust"].(float64); ok {
		if ac.Trust.TrustScore < minTrust {
			return false
		}
	}

	return true
}

// A2AError represents an A2A protocol error.
type A2AError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

func (e *A2AError) Error() string {
	return e.Code + ": " + e.Message
}

// A2ARegistry manages AgentCard registration and discovery.
type A2ARegistry interface {
	// Register adds an agent to the registry
	Register(card *AgentCard) error

	// Deregister removes an agent from the registry
	Deregister(agentID string) error

	// Get retrieves an agent card by ID
	Get(agentID string) (*AgentCard, error)

	// Find searches for agents matching criteria
	Find(criteria map[string]interface{}) ([]*AgentCard, error)

	// Update modifies an existing agent card
	Update(card *AgentCard) error

	// Watch subscribes to registry changes
	Watch() (<-chan RegistryChange, error)

	// Health checks registry health
	Health() error
}

// RegistryChange represents a change in the registry.
type RegistryChange struct {
	Type      string     // "added", "updated", "removed"
	AgentCard *AgentCard // nil for removed
	Timestamp time.Time
}
