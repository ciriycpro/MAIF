package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// BaseAgent provides a foundational implementation of the Agent interface.
// It handles common concerns like lifecycle management, state persistence,
// and message routing. Concrete agents can embed BaseAgent and override
// specific methods to implement custom behavior.
type BaseAgent struct {
	mu sync.RWMutex

	id           string
	agentType    string
	config       AgentConfig
	capabilities []Capability

	// Lifecycle state
	initialized bool
	running     bool
	stopped     bool

	// Message handling
	messageChan chan *Message
	stopChan    chan struct{}
	doneChan    chan struct{}

	// State management
	state     State
	stateSync sync.RWMutex

	// Health tracking
	health     *HealthStatus
	healthMu   sync.RWMutex
	lastActive time.Time

	// Dependencies (injected)
	messageBus   MessageBus
	stateManager StateManager
	logger       Logger
	metrics      MetricsCollector
}

// NewBaseAgent creates a new base agent with the given configuration.
func NewBaseAgent(config AgentConfig) (*BaseAgent, error) {
	if config.ID == "" {
		config.ID = uuid.New().String()
	}

	if config.Type == "" {
		return nil, fmt.Errorf("agent type cannot be empty")
	}

	return &BaseAgent{
		id:          config.ID,
		agentType:   config.Type,
		config:      config,
		messageChan: make(chan *Message, 100), // buffered channel
		stopChan:    make(chan struct{}),
		doneChan:    make(chan struct{}),
		health: &HealthStatus{
			Healthy:   false,
			Status:    "initializing",
			Details:   make(map[string]interface{}),
			LastCheck: time.Now(),
			Issues:    make([]HealthIssue, 0),
		},
		lastActive: time.Now(),
	}, nil
}

// ID returns the unique identifier for this agent.
func (a *BaseAgent) ID() string {
	return a.id
}

// Type returns the agent type.
func (a *BaseAgent) Type() string {
	return a.agentType
}

// Initialize prepares the agent for operation.
func (a *BaseAgent) Initialize(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.initialized {
		return fmt.Errorf("agent already initialized")
	}

	// Initialize dependencies
	if err := a.initializeDependencies(ctx); err != nil {
		a.recordHealthIssue("initialization", "critical", 
			fmt.Sprintf("failed to initialize dependencies: %v", err))
		return err
	}

	// Load persisted state if available
	if a.stateManager != nil {
		if savedState, err := a.stateManager.Load(ctx, a.id); err == nil {
			a.state = savedState
		}
	}

	// Initialize capabilities
	a.capabilities = a.discoverCapabilities()

	a.initialized = true
	a.updateHealthStatus(true, "initialized", nil)
	
	if a.logger != nil {
		a.logger.Info("agent initialized", "id", a.id, "type", a.agentType)
	}

	return nil
}

// Start begins the agent's main execution loop.
func (a *BaseAgent) Start(ctx context.Context) error {
	a.mu.Lock()
	if !a.initialized {
		a.mu.Unlock()
		return fmt.Errorf("agent not initialized")
	}
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("agent already running")
	}
	a.running = true
	a.mu.Unlock()

	// Start the message processing loop
	go a.processMessages(ctx)

	// Start health check loop
	go a.healthCheckLoop(ctx)

	a.updateHealthStatus(true, "running", nil)

	if a.logger != nil {
		a.logger.Info("agent started", "id", a.id)
	}

	return nil
}

// Stop gracefully shuts down the agent.
func (a *BaseAgent) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	a.mu.Unlock()

	// Signal stop
	close(a.stopChan)

	// Wait for graceful shutdown or timeout
	select {
	case <-a.doneChan:
		// Clean shutdown
	case <-ctx.Done():
		// Timeout
		if a.logger != nil {
			a.logger.Warn("agent stop timed out", "id", a.id)
		}
	}

	// Persist state before shutdown
	if a.stateManager != nil && a.state != nil {
		if err := a.stateManager.Save(ctx, a.id, a.state); err != nil {
			if a.logger != nil {
				a.logger.Error("failed to save state on stop", "error", err)
			}
		}
	}

	a.updateHealthStatus(false, "stopped", nil)

	if a.logger != nil {
		a.logger.Info("agent stopped", "id", a.id)
	}

	return nil
}

// Health returns the current health status.
func (a *BaseAgent) Health(ctx context.Context) (*HealthStatus, error) {
	a.healthMu.RLock()
	defer a.healthMu.RUnlock()

	// Clone to avoid mutations
	health := *a.health
	health.Details = make(map[string]interface{}, len(a.health.Details))
	for k, v := range a.health.Details {
		health.Details[k] = v
	}

	return &health, nil
}

// Capabilities returns the agent's capabilities.
func (a *BaseAgent) Capabilities() []Capability {
	a.mu.RLock()
	defer a.mu.RUnlock()

	caps := make([]Capability, len(a.capabilities))
	copy(caps, a.capabilities)
	return caps
}

// HandleMessage processes an incoming message.
func (a *BaseAgent) HandleMessage(ctx context.Context, msg *Message) error {
	a.mu.RLock()
	if !a.running {
		a.mu.RUnlock()
		return fmt.Errorf("agent not running")
	}
	a.mu.RUnlock()

	select {
	case a.messageChan <- msg:
		a.lastActive = time.Now()
		if a.metrics != nil {
			a.metrics.IncCounter("messages_received", map[string]string{
				"agent_id":   a.id,
				"agent_type": a.agentType,
				"msg_type":   msg.Type,
			})
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetState retrieves the agent's current state.
func (a *BaseAgent) GetState(ctx context.Context) (State, error) {
	a.stateSync.RLock()
	defer a.stateSync.RUnlock()

	if a.state == nil {
		return nil, fmt.Errorf("no state available")
	}

	return a.state, nil
}

// SetState restores the agent to a previous state.
func (a *BaseAgent) SetState(ctx context.Context, state State) error {
	a.stateSync.Lock()
	defer a.stateSync.Unlock()

	a.state = state

	if a.stateManager != nil {
		if err := a.stateManager.Save(ctx, a.id, state); err != nil {
			return fmt.Errorf("failed to persist state: %w", err)
		}
	}

	return nil
}

// processMessages is the main message processing loop.
func (a *BaseAgent) processMessages(ctx context.Context) {
	defer close(a.doneChan)

	for {
		select {
		case msg := <-a.messageChan:
			if err := a.processMessage(ctx, msg); err != nil {
				if a.logger != nil {
					a.logger.Error("failed to process message",
						"error", err, "msg_id", msg.ID)
				}
				if a.metrics != nil {
					a.metrics.IncCounter("message_errors", map[string]string{
						"agent_id": a.id,
						"error":    err.Error(),
					})
				}
			}

		case <-a.stopChan:
			// Drain remaining messages
			for {
				select {
				case msg := <-a.messageChan:
					_ = a.processMessage(ctx, msg)
				default:
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// processMessage handles a single message (to be overridden by subclasses).
func (a *BaseAgent) processMessage(ctx context.Context, msg *Message) error {
	// Default implementation - subclasses should override
	if a.logger != nil {
		a.logger.Debug("processing message", "msg_id", msg.ID, "type", msg.Type)
	}
	return nil
}

// healthCheckLoop periodically checks agent health.
func (a *BaseAgent) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.performHealthCheck(ctx)
		case <-a.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// performHealthCheck evaluates agent health.
func (a *BaseAgent) performHealthCheck(ctx context.Context) {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()

	a.health.LastCheck = time.Now()

	// Check if agent is active
	if time.Since(a.lastActive) > 5*time.Minute {
		a.health.Healthy = false
		a.health.Status = "inactive"
		a.recordHealthIssue("activity", "warning", "no recent activity")
	} else {
		a.health.Healthy = true
		a.health.Status = "healthy"
		// Clear activity-related issues
		a.clearHealthIssues("activity")
	}

	a.health.Details["last_active"] = a.lastActive
	a.health.Details["uptime"] = time.Since(a.lastActive)
}

// initializeDependencies sets up required services.
func (a *BaseAgent) initializeDependencies(ctx context.Context) error {
	// In a real implementation, this would initialize:
	// - MessageBus from config
	// - StateManager from config
	// - Logger from config
	// - MetricsCollector from config
	return nil
}

// discoverCapabilities detects agent capabilities.
func (a *BaseAgent) discoverCapabilities() []Capability {
	// Default implementation - subclasses should override
	return []Capability{}
}

// updateHealthStatus updates the health status.
func (a *BaseAgent) updateHealthStatus(healthy bool, status string, details map[string]interface{}) {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()

	a.health.Healthy = healthy
	a.health.Status = status
	a.health.LastCheck = time.Now()

	if details != nil {
		for k, v := range details {
			a.health.Details[k] = v
		}
	}
}

// recordHealthIssue adds a health issue.
func (a *BaseAgent) recordHealthIssue(component, severity, message string) {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()

	issue := HealthIssue{
		Component:  component,
		Severity:   severity,
		Message:    message,
		DetectedAt: time.Now(),
	}

	a.health.Issues = append(a.health.Issues, issue)
}

// clearHealthIssues removes issues for a component.
func (a *BaseAgent) clearHealthIssues(component string) {
	filtered := make([]HealthIssue, 0)
	for _, issue := range a.health.Issues {
		if issue.Component != component {
			filtered = append(filtered, issue)
		}
	}
	a.health.Issues = filtered
}

// Supporting interfaces (would be in separate packages in real implementation)

type MessageBus interface {
	Publish(ctx context.Context, topic string, msg *Message) error
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
}

type StateManager interface {
	Save(ctx context.Context, key string, state State) error
	Load(ctx context.Context, key string) (State, error)
}

type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

type MetricsCollector interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}
