package agent

import (
	"context"
	"time"
)

// Agent represents an autonomous entity capable of perceiving its environment,
// making decisions, and taking actions to achieve specific goals.
// 
// All agents in the CIRIYC PRO framework implement this interface, enabling
// composability, testability, and uniform lifecycle management.
type Agent interface {
	// ID returns the unique identifier for this agent instance.
	// The ID must be stable across restarts if state persistence is enabled.
	ID() string

	// Type returns the agent type identifier (e.g., "researcher", "coordinator").
	// Used for agent discovery and routing in multi-agent systems.
	Type() string

	// Initialize prepares the agent for operation. This includes:
	// - Loading configuration
	// - Establishing connections to required services
	// - Initializing internal state
	// - Registering with the agent registry
	//
	// Initialize must be called before Start().
	Initialize(ctx context.Context) error

	// Start begins the agent's main execution loop.
	// The agent will start processing messages and executing its tasks.
	// Start should be non-blocking and return immediately.
	//
	// The provided context controls the agent's lifecycle. When the context
	// is cancelled, the agent should gracefully shut down.
	Start(ctx context.Context) error

	// Stop requests a graceful shutdown of the agent.
	// The agent should:
	// - Stop accepting new messages
	// - Complete processing of in-flight messages
	// - Persist any necessary state
	// - Close connections
	//
	// Stop should block until shutdown is complete or the timeout expires.
	Stop(ctx context.Context) error

	// Health returns the current health status of the agent.
	// This is used for readiness and liveness probes in orchestrated environments.
	Health(ctx context.Context) (*HealthStatus, error)

	// Capabilities returns the set of capabilities this agent provides.
	// Capabilities are used for agent discovery and task routing.
	Capabilities() []Capability

	// HandleMessage processes an incoming message.
	// This is the primary method for agent communication.
	//
	// The implementation should:
	// - Validate the message
	// - Process it according to the agent's logic
	// - Optionally send response messages
	// - Update internal state as needed
	HandleMessage(ctx context.Context, msg *Message) error

	// GetState retrieves the current state of the agent.
	// Returns a snapshot of the agent's internal state for persistence or debugging.
	GetState(ctx context.Context) (State, error)

	// SetState restores the agent to a previous state.
	// Used for state recovery, migration, or rollback scenarios.
	SetState(ctx context.Context, state State) error
}

// Message represents a message in the agent communication system.
// Messages are the primary mechanism for agent interaction.
type Message struct {
	// ID uniquely identifies this message
	ID string

	// From identifies the sender agent
	From string

	// To identifies the recipient agent(s)
	To []string

	// Type categorizes the message (e.g., "task", "query", "response")
	Type string

	// Payload contains the message data
	Payload []byte

	// Metadata stores additional message attributes
	Metadata map[string]string

	// Timestamp when the message was created
	Timestamp time.Time

	// CorrelationID links related messages (for request/response patterns)
	CorrelationID string

	// Priority affects message processing order (0 = highest)
	Priority int

	// TTL specifies message time-to-live
	TTL time.Duration
}

// State represents the persistent state of an agent.
// The state can be serialized and stored for recovery purposes.
type State interface {
	// Snapshot returns a serializable representation of the state
	Snapshot() ([]byte, error)

	// Restore rebuilds the state from a snapshot
	Restore(data []byte) error

	// Version returns the state schema version
	Version() string

	// LastUpdated returns when the state was last modified
	LastUpdated() time.Time
}

// Capability describes a specific ability that an agent possesses.
// Capabilities enable runtime discovery and dynamic task allocation.
type Capability struct {
	// Name uniquely identifies the capability
	Name string

	// Description provides human-readable information
	Description string

	// InputSchema defines the expected input format (JSON Schema)
	InputSchema string

	// OutputSchema defines the output format (JSON Schema)
	OutputSchema string

	// Requirements specifies any prerequisites for this capability
	Requirements map[string]string

	// Cost estimates the computational cost (arbitrary units)
	Cost float64

	// MaxConcurrency limits parallel executions of this capability
	MaxConcurrency int
}

// HealthStatus describes the health state of an agent.
type HealthStatus struct {
	// Healthy indicates whether the agent is functioning correctly
	Healthy bool

	// Status provides a summary status string
	Status string

	// Details contains specific health metrics
	Details map[string]interface{}

	// LastCheck when the health status was last updated
	LastCheck time.Time

	// Issues lists any current problems
	Issues []HealthIssue
}

// HealthIssue represents a specific health problem.
type HealthIssue struct {
	// Severity indicates the issue severity (critical, warning, info)
	Severity string

	// Component identifies the affected component
	Component string

	// Message describes the issue
	Message string

	// DetectedAt when the issue was first observed
	DetectedAt time.Time
}

// AgentFactory creates agent instances.
// Factories enable dependency injection and testability.
type AgentFactory interface {
	// Create instantiates a new agent with the given configuration
	Create(ctx context.Context, config AgentConfig) (Agent, error)

	// SupportedTypes returns the list of agent types this factory can create
	SupportedTypes() []string
}

// AgentConfig holds agent configuration parameters.
type AgentConfig struct {
	// ID for the agent instance (generated if empty)
	ID string

	// Type specifies which kind of agent to create
	Type string

	// Properties contains type-specific configuration
	Properties map[string]interface{}

	// MessageBus configuration
	MessageBusConfig map[string]interface{}

	// StateManager configuration
	StateManagerConfig map[string]interface{}

	// Timeout values
	InitTimeout  time.Duration
	StartTimeout time.Duration
	StopTimeout  time.Duration
}

// Registry manages agent discovery and registration.
type Registry interface {
	// Register adds an agent to the registry
	Register(ctx context.Context, agent Agent) error

	// Deregister removes an agent from the registry
	Deregister(ctx context.Context, agentID string) error

	// Get retrieves an agent by ID
	Get(ctx context.Context, agentID string) (Agent, error)

	// Find locates agents matching the given criteria
	Find(ctx context.Context, criteria Criteria) ([]Agent, error)

	// Watch subscribes to registry changes
	Watch(ctx context.Context) (<-chan RegistryEvent, error)
}

// Criteria specifies agent search parameters.
type Criteria struct {
	// Type filters by agent type
	Type string

	// Capabilities filters by required capabilities
	Capabilities []string

	// Tags filters by metadata tags
	Tags map[string]string

	// Status filters by health status
	Status string
}

// RegistryEvent represents a change in the agent registry.
type RegistryEvent struct {
	// Type of event (added, updated, removed)
	Type string

	// AgentID identifies the affected agent
	AgentID string

	// Agent is the agent instance (nil for removed events)
	Agent Agent

	// Timestamp when the event occurred
	Timestamp time.Time
}
