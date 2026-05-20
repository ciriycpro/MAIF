package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// StateGraph represents a graph-based workflow for agent orchestration.
// Nodes represent states or agent actions, edges represent transitions.
// The graph supports:
// - Conditional branching
// - Parallel execution
// - Human-in-the-loop approval
// - State persistence and recovery
type StateGraph struct {
	mu sync.RWMutex

	// ID uniquely identifies this graph
	ID string

	// Name graph name
	Name string

	// Nodes map of node ID to node
	Nodes map[string]*Node

	// Edges list of edges between nodes
	Edges []*Edge

	// EntryPoint starting node ID
	EntryPoint string

	// State current graph state
	State GraphState

	// Config graph configuration
	Config GraphConfig

	// Checkpointer for state persistence
	Checkpointer Checkpointer

	// Metrics execution metrics
	Metrics *GraphMetrics
}

// Node represents a state or action in the graph.
type Node struct {
	// ID node identifier
	ID string

	// Type node type ("agent", "condition", "parallel", "human_approval")
	Type NodeType

	// Handler executes the node logic
	Handler NodeHandler

	// Metadata node metadata
	Metadata map[string]interface{}

	// Retryable indicates if node can be retried on failure
	Retryable bool

	// MaxRetries maximum retry attempts
	MaxRetries int

	// Timeout node execution timeout
	Timeout time.Duration
}

// NodeType defines the type of node.
type NodeType string

const (
	// AgentNode executes agent logic
	AgentNode NodeType = "agent"

	// ConditionNode branches based on conditions
	ConditionNode NodeType = "condition"

	// ParallelNode executes multiple paths concurrently
	ParallelNode NodeType = "parallel"

	// HumanApprovalNode waits for human approval
	HumanApprovalNode NodeType = "human_approval"

	// SubgraphNode executes a nested graph
	SubgraphNode NodeType = "subgraph"

	// EndNode marks workflow completion
	EndNode NodeType = "end"
)

// NodeHandler executes node logic.
type NodeHandler func(ctx context.Context, state GraphState) (GraphState, error)

// Edge represents a transition between nodes.
type Edge struct {
	// From source node ID
	From string

	// To destination node ID
	To string

	// Condition optional condition for the edge
	Condition EdgeCondition

	// Weight edge weight (for prioritization)
	Weight int

	// Metadata edge metadata
	Metadata map[string]interface{}
}

// EdgeCondition determines if an edge should be taken.
type EdgeCondition func(ctx context.Context, state GraphState) (bool, error)

// GraphState holds the current state of graph execution.
type GraphState struct {
	// CurrentNode currently executing node
	CurrentNode string

	// Data state data
	Data map[string]interface{}

	// History execution history
	History []StateTransition

	// StartedAt execution start time
	StartedAt time.Time

	// UpdatedAt last update time
	UpdatedAt time.Time

	// Status execution status
	Status ExecutionStatus

	// Error last error encountered
	Error error

	// Metadata state metadata
	Metadata map[string]interface{}
}

// StateTransition records a state change.
type StateTransition struct {
	// FromNode previous node
	FromNode string

	// ToNode next node
	ToNode string

	// Timestamp transition time
	Timestamp time.Time

	// Data state data at transition
	Data map[string]interface{}

	// Reason transition reason
	Reason string
}

// ExecutionStatus describes graph execution status.
type ExecutionStatus string

const (
	StatusPending      ExecutionStatus = "pending"
	StatusRunning      ExecutionStatus = "running"
	StatusWaiting      ExecutionStatus = "waiting"
	StatusCompleted    ExecutionStatus = "completed"
	StatusFailed       ExecutionStatus = "failed"
	StatusCancelled    ExecutionStatus = "cancelled"
	StatusHumanApproval ExecutionStatus = "human_approval"
)

// GraphConfig holds graph configuration.
type GraphConfig struct {
	// MaxExecutionTime maximum total execution time
	MaxExecutionTime time.Duration

	// EnableCheckpoints enables state checkpointing
	EnableCheckpoints bool

	// CheckpointInterval checkpoint interval
	CheckpointInterval time.Duration

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// ParallelExecutionLimit max parallel branches
	ParallelExecutionLimit int

	// RetryBackoff backoff between retries
	RetryBackoff time.Duration
}

// NewStateGraph creates a new state graph.
func NewStateGraph(id, name string) *StateGraph {
	return &StateGraph{
		ID:    id,
		Name:  name,
		Nodes: make(map[string]*Node),
		Edges: make([]*Edge, 0),
		State: GraphState{
			Data:      make(map[string]interface{}),
			History:   make([]StateTransition, 0),
			Status:    StatusPending,
			Metadata:  make(map[string]interface{}),
			UpdatedAt: time.Now(),
		},
		Metrics: &GraphMetrics{},
	}
}

// AddNode adds a node to the graph.
func (sg *StateGraph) AddNode(node *Node) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	if _, exists := sg.Nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}

	sg.Nodes[node.ID] = node
	return nil
}

// AddEdge adds an edge between nodes.
func (sg *StateGraph) AddEdge(edge *Edge) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if _, exists := sg.Nodes[edge.From]; !exists {
		return fmt.Errorf("source node %s not found", edge.From)
	}

	if _, exists := sg.Nodes[edge.To]; !exists {
		return fmt.Errorf("destination node %s not found", edge.To)
	}

	sg.Edges = append(sg.Edges, edge)
	return nil
}

// SetEntryPoint sets the starting node.
func (sg *StateGraph) SetEntryPoint(nodeID string) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if _, exists := sg.Nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	sg.EntryPoint = nodeID
	return nil
}

// Execute runs the state graph workflow.
func (sg *StateGraph) Execute(ctx context.Context, initialState map[string]interface{}) error {
	sg.mu.Lock()
	if sg.EntryPoint == "" {
		sg.mu.Unlock()
		return fmt.Errorf("entry point not set")
	}

	// Initialize state
	sg.State.CurrentNode = sg.EntryPoint
	sg.State.Data = initialState
	sg.State.StartedAt = time.Now()
	sg.State.UpdatedAt = time.Now()
	sg.State.Status = StatusRunning
	sg.mu.Unlock()

	// Start execution
	for {
		select {
		case <-ctx.Done():
			sg.updateStatus(StatusCancelled)
			return ctx.Err()

		default:
			// Execute current node
			node, err := sg.getCurrentNode()
			if err != nil {
				sg.updateStatus(StatusFailed)
				sg.State.Error = err
				return err
			}

			// Check if we've reached an end node
			if node.Type == EndNode {
				sg.updateStatus(StatusCompleted)
				return nil
			}

			// Execute node with timeout
			nodeCtx, cancel := context.WithTimeout(ctx, node.Timeout)
			newState, err := sg.executeNode(nodeCtx, node)
			cancel()

			if err != nil {
				// Handle retry logic
				if node.Retryable && sg.shouldRetry(node) {
					sg.recordRetry(node.ID)
					time.Sleep(sg.Config.RetryBackoff)
					continue
				}

				sg.updateStatus(StatusFailed)
				sg.State.Error = err
				return err
			}

			// Update state
			sg.mu.Lock()
			sg.State.Data = newState.Data
			sg.State.UpdatedAt = time.Now()
			sg.mu.Unlock()

			// Find next node
			nextNode, err := sg.findNextNode(ctx, node.ID)
			if err != nil {
				sg.updateStatus(StatusFailed)
				sg.State.Error = err
				return err
			}

			// Record transition
			sg.recordTransition(node.ID, nextNode, "normal")

			// Create checkpoint if enabled
			if sg.Config.EnableCheckpoints && sg.Checkpointer != nil {
				if err := sg.Checkpointer.Save(ctx, sg.State); err != nil {
					// Log error but continue
				}
			}

			// Move to next node
			sg.mu.Lock()
			sg.State.CurrentNode = nextNode
			sg.mu.Unlock()
		}
	}
}

// executeNode executes a single node.
func (sg *StateGraph) executeNode(ctx context.Context, node *Node) (GraphState, error) {
	startTime := time.Now()

	// Execute handler
	newState, err := node.Handler(ctx, sg.State)

	// Update metrics
	sg.recordNodeExecution(node.ID, time.Since(startTime), err == nil)

	return newState, err
}

// findNextNode determines the next node to execute.
func (sg *StateGraph) findNextNode(ctx context.Context, currentNodeID string) (string, error) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	// Find applicable edges
	var applicableEdges []*Edge
	for _, edge := range sg.Edges {
		if edge.From == currentNodeID {
			// Check condition if present
			if edge.Condition != nil {
				matches, err := edge.Condition(ctx, sg.State)
				if err != nil {
					return "", err
				}
				if matches {
					applicableEdges = append(applicableEdges, edge)
				}
			} else {
				applicableEdges = append(applicableEdges, edge)
			}
		}
	}

	if len(applicableEdges) == 0 {
		return "", fmt.Errorf("no valid edge from node %s", currentNodeID)
	}

	// Select edge with highest weight
	selectedEdge := applicableEdges[0]
	for _, edge := range applicableEdges[1:] {
		if edge.Weight > selectedEdge.Weight {
			selectedEdge = edge
		}
	}

	return selectedEdge.To, nil
}

// getCurrentNode returns the current node.
func (sg *StateGraph) getCurrentNode() (*Node, error) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	node, exists := sg.Nodes[sg.State.CurrentNode]
	if !exists {
		return nil, fmt.Errorf("node %s not found", sg.State.CurrentNode)
	}

	return node, nil
}

// recordTransition records a state transition.
func (sg *StateGraph) recordTransition(from, to, reason string) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	transition := StateTransition{
		FromNode:  from,
		ToNode:    to,
		Timestamp: time.Now(),
		Data:      sg.copyData(sg.State.Data),
		Reason:    reason,
	}

	sg.State.History = append(sg.State.History, transition)
}

// updateStatus updates the execution status.
func (sg *StateGraph) updateStatus(status ExecutionStatus) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	sg.State.Status = status
	sg.State.UpdatedAt = time.Now()
}

// copyData creates a copy of state data.
func (sg *StateGraph) copyData(data map[string]interface{}) map[string]interface{} {
	copied := make(map[string]interface{}, len(data))
	for k, v := range data {
		copied[k] = v
	}
	return copied
}

// shouldRetry checks if a node should be retried.
func (sg *StateGraph) shouldRetry(node *Node) bool {
	// Implementation would track retry count per node
	return true
}

// recordRetry records a retry attempt.
func (sg *StateGraph) recordRetry(nodeID string) {
	sg.Metrics.mu.Lock()
	defer sg.Metrics.mu.Unlock()

	sg.Metrics.Retries++
}

// recordNodeExecution records node execution metrics.
func (sg *StateGraph) recordNodeExecution(nodeID string, duration time.Duration, success bool) {
	sg.Metrics.mu.Lock()
	defer sg.Metrics.mu.Unlock()

	sg.Metrics.NodesExecuted++
	if success {
		sg.Metrics.NodesSucceeded++
	} else {
		sg.Metrics.NodesFailed++
	}

	// Update latency (simplified)
	if duration > sg.Metrics.MaxNodeLatency {
		sg.Metrics.MaxNodeLatency = duration
	}
}

// Checkpointer saves and restores graph state.
type Checkpointer interface {
	// Save persists current state
	Save(ctx context.Context, state GraphState) error

	// Load retrieves saved state
	Load(ctx context.Context, graphID string) (GraphState, error)

	// List returns available checkpoints
	List(ctx context.Context, graphID string) ([]CheckpointMetadata, error)

	// Delete removes a checkpoint
	Delete(ctx context.Context, checkpointID string) error
}

// CheckpointMetadata describes a saved checkpoint.
type CheckpointMetadata struct {
	ID        string
	GraphID   string
	NodeID    string
	Timestamp time.Time
	Size      int64
}

// GraphMetrics tracks graph execution metrics.
type GraphMetrics struct {
	NodesExecuted   int64
	NodesSucceeded  int64
	NodesFailed     int64
	Retries         int64
	MaxNodeLatency  time.Duration
	TotalDuration   time.Duration

	mu sync.RWMutex
}

// GraphBuilder provides a fluent API for building state graphs.
type GraphBuilder struct {
	graph *StateGraph
}

// NewGraphBuilder creates a new graph builder.
func NewGraphBuilder(id, name string) *GraphBuilder {
	return &GraphBuilder{
		graph: NewStateGraph(id, name),
	}
}

// WithNode adds a node to the graph.
func (gb *GraphBuilder) WithNode(node *Node) *GraphBuilder {
	gb.graph.AddNode(node)
	return gb
}

// WithEdge adds an edge to the graph.
func (gb *GraphBuilder) WithEdge(from, to string, condition EdgeCondition) *GraphBuilder {
	edge := &Edge{
		From:      from,
		To:        to,
		Condition: condition,
		Weight:    0,
	}
	gb.graph.AddEdge(edge)
	return gb
}

// WithEntryPoint sets the entry point.
func (gb *GraphBuilder) WithEntryPoint(nodeID string) *GraphBuilder {
	gb.graph.SetEntryPoint(nodeID)
	return gb
}

// Build returns the constructed graph.
func (gb *GraphBuilder) Build() *StateGraph {
	return gb.graph
}
