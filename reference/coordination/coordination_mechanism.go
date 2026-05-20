package coordination

import (
	"context"
	"time"
)

// Mechanism defines the interface for agent coordination strategies.
// Different mechanisms can be plugged in based on the application requirements.
type Mechanism interface {
	// Initialize prepares the coordination mechanism
	Initialize(ctx context.Context, config MechanismConfig) error

	// Coordinate executes the coordination logic for a task
	Coordinate(ctx context.Context, task *Task) (*CoordinationResult, error)

	// Negotiate performs negotiation between agents
	Negotiate(ctx context.Context, proposal *Proposal) (*Agreement, error)

	// AllocateTasks assigns tasks to agents based on the coordination strategy
	AllocateTasks(ctx context.Context, tasks []*Task, agents []AgentDescriptor) (*AllocationPlan, error)

	// ResolveConflict handles conflicts between agents
	ResolveConflict(ctx context.Context, conflict *Conflict) (*Resolution, error)

	// Type returns the coordination mechanism type
	Type() string

	// Metrics returns performance metrics
	Metrics(ctx context.Context) (*MechanismMetrics, error)
}

// Task represents a unit of work to be coordinated.
type Task struct {
	// ID uniquely identifies this task
	ID string

	// Type categorizes the task
	Type string

	// Description explains what needs to be done
	Description string

	// Requirements specify task constraints
	Requirements TaskRequirements

	// Deadline when the task must be completed
	Deadline time.Time

	// Priority task importance (0 = highest)
	Priority int

	// Dependencies tasks that must complete first
	Dependencies []string

	// Payload task-specific data
	Payload interface{}

	// Metadata arbitrary task attributes
	Metadata map[string]interface{}
}

// TaskRequirements specifies task execution requirements.
type TaskRequirements struct {
	// Capabilities required agent capabilities
	Capabilities []string

	// MinResources minimum computational resources
	MinResources ResourceRequirements

	// MaxCost maximum acceptable cost
	MaxCost float64

	// MinQuality minimum quality score (0.0 - 1.0)
	MinQuality float64

	// Constraints additional constraints
	Constraints map[string]interface{}
}

// ResourceRequirements specifies computational resource needs.
type ResourceRequirements struct {
	// CPUCores number of CPU cores
	CPUCores float64

	// Memory in bytes
	Memory int64

	// GPU indicates if GPU is required
	GPU bool

	// Network minimum network bandwidth (bytes/sec)
	Network int64

	// Storage in bytes
	Storage int64
}

// AgentDescriptor describes an agent's characteristics for coordination.
type AgentDescriptor struct {
	// ID agent identifier
	ID string

	// Type agent type
	Type string

	// Capabilities agent's capabilities
	Capabilities []string

	// AvailableResources resources currently available
	AvailableResources ResourceRequirements

	// Cost agent's cost per task
	Cost float64

	// Quality historical quality score
	Quality float64

	// Reliability historical reliability score
	Reliability float64

	// CurrentLoad number of tasks in progress
	CurrentLoad int

	// MaxLoad maximum concurrent tasks
	MaxLoad int

	// Location agent's geographic location
	Location string

	// Status current operational status
	Status string
}

// Proposal represents a coordination proposal from an agent.
type Proposal struct {
	// ID proposal identifier
	ID string

	// TaskID associated task
	TaskID string

	// AgentID proposing agent
	AgentID string

	// Bid agent's bid for the task
	Bid Bid

	// EstimatedDuration expected completion time
	EstimatedDuration time.Duration

	// QualityEstimate expected quality score
	QualityEstimate float64

	// Conditions proposal terms and conditions
	Conditions map[string]interface{}

	// ExpiresAt proposal expiration time
	ExpiresAt time.Time

	// Timestamp when proposal was made
	Timestamp time.Time
}

// Bid represents an agent's offer for a task.
type Bid struct {
	// Amount bid amount (in arbitrary units)
	Amount float64

	// Currency currency or unit of account
	Currency string

	// Resources resources to be committed
	Resources ResourceRequirements

	// Guarantees performance guarantees
	Guarantees map[string]interface{}
}

// Agreement represents a coordination agreement between agents.
type Agreement struct {
	// ID agreement identifier
	ID string

	// TaskID associated task
	TaskID string

	// Participants agents involved in the agreement
	Participants []string

	// Terms agreement terms
	Terms map[string]interface{}

	// WinningBid selected bid
	WinningBid *Bid

	// StartTime when execution should begin
	StartTime time.Time

	// Deadline when execution must complete
	Deadline time.Time

	// SignedAt when agreement was signed
	SignedAt time.Time

	// Status agreement status
	Status string
}

// CoordinationResult contains the outcome of coordination.
type CoordinationResult struct {
	// TaskID task that was coordinated
	TaskID string

	// Assignments map of agent ID to assigned tasks
	Assignments map[string][]string

	// Agreements formed agreements
	Agreements []*Agreement

	// Metrics coordination performance metrics
	Metrics ResultMetrics

	// Status coordination outcome status
	Status string

	// Message optional status message
	Message string
}

// ResultMetrics captures coordination performance.
type ResultMetrics struct {
	// Duration time taken for coordination
	Duration time.Duration

	// ProposalsReceived number of proposals
	ProposalsReceived int

	// AgreementsFormed number of agreements
	AgreementsFormed int

	// AverageQuality average quality of assignments
	AverageQuality float64

	// TotalCost total cost of assignments
	TotalCost float64

	// UtilizationRate resource utilization
	UtilizationRate float64
}

// AllocationPlan describes how tasks are allocated to agents.
type AllocationPlan struct {
	// Allocations map of task ID to agent ID
	Allocations map[string]string

	// Schedule execution schedule
	Schedule map[string]time.Time

	// LoadBalancing load distribution across agents
	LoadBalancing map[string]int

	// TotalCost total cost of the plan
	TotalCost float64

	// ExpectedQuality expected quality score
	ExpectedQuality float64

	// Metrics planning metrics
	Metrics PlanMetrics
}

// PlanMetrics captures allocation planning metrics.
type PlanMetrics struct {
	// PlanningTime time taken to create the plan
	PlanningTime time.Duration

	// TasksAllocated number of tasks successfully allocated
	TasksAllocated int

	// TasksUnallocated number of tasks that couldn't be allocated
	TasksUnallocated int

	// AverageLoad average load across agents
	AverageLoad float64

	// LoadVariance variance in load distribution
	LoadVariance float64
}

// Conflict represents a coordination conflict.
type Conflict struct {
	// ID conflict identifier
	ID string

	// Type conflict type
	Type string

	// Agents involved agents
	Agents []string

	// Resources contested resources
	Resources []string

	// Description conflict description
	Description string

	// Severity conflict severity (low, medium, high, critical)
	Severity string

	// DetectedAt when conflict was detected
	DetectedAt time.Time
}

// Resolution describes how a conflict was resolved.
type Resolution struct {
	// ConflictID original conflict
	ConflictID string

	// Strategy resolution strategy used
	Strategy string

	// Outcome resolution outcome
	Outcome string

	// Actions taken to resolve
	Actions []ResolutionAction

	// ResolvedAt when resolution was achieved
	ResolvedAt time.Time

	// Cost cost of resolution
	Cost float64
}

// ResolutionAction describes an action taken during resolution.
type ResolutionAction struct {
	// Type action type
	Type string

	// Agent agent affected by the action
	Agent string

	// Description action description
	Description string

	// Impact impact of the action
	Impact map[string]interface{}
}

// MechanismConfig holds configuration for coordination mechanisms.
type MechanismConfig struct {
	// Type mechanism type
	Type string

	// Parameters mechanism-specific parameters
	Parameters map[string]interface{}

	// Timeout maximum coordination time
	Timeout time.Duration

	// Strategy conflict resolution strategy
	Strategy string

	// Preferences optimization preferences
	Preferences OptimizationPreferences
}

// OptimizationPreferences specifies coordination objectives.
type OptimizationPreferences struct {
	// OptimizeFor primary optimization target
	// ("cost", "quality", "speed", "utilization")
	OptimizeFor string

	// Weights for multi-objective optimization
	Weights map[string]float64

	// Constraints hard constraints
	Constraints map[string]interface{}
}

// MechanismMetrics contains performance metrics for a mechanism.
type MechanismMetrics struct {
	// TotalCoordinations number of coordinations performed
	TotalCoordinations int64

	// SuccessfulCoordinations number of successful coordinations
	SuccessfulCoordinations int64

	// FailedCoordinations number of failed coordinations
	FailedCoordinations int64

	// AverageLatency average coordination latency
	AverageLatency time.Duration

	// P95Latency 95th percentile latency
	P95Latency time.Duration

	// P99Latency 99th percentile latency
	P99Latency time.Duration

	// AverageCost average coordination cost
	AverageCost float64

	// AverageQuality average result quality
	AverageQuality float64

	// ConflictsDetected number of conflicts detected
	ConflictsDetected int64

	// ConflictsResolved number of conflicts resolved
	ConflictsResolved int64

	// LastUpdated when metrics were last updated
	LastUpdated time.Time
}

// MarketBasedMechanism implements market-based coordination.
type MarketBasedMechanism struct {
	config  MechanismConfig
	metrics *MechanismMetrics
}

// AuctionBasedMechanism implements auction-based coordination.
type AuctionBasedMechanism struct {
	config     MechanismConfig
	metrics    *MechanismMetrics
	auctioneer Auctioneer
}

// ContractNetMechanism implements Contract Net Protocol.
type ContractNetMechanism struct {
	config  MechanismConfig
	metrics *MechanismMetrics
	manager ContractManager
}

// VotingBasedMechanism implements voting-based coordination.
type VotingBasedMechanism struct {
	config  MechanismConfig
	metrics *MechanismMetrics
	voting  VotingProtocol
}

// Auctioneer manages auction processes.
type Auctioneer interface {
	// StartAuction initiates an auction for a task
	StartAuction(ctx context.Context, task *Task, agents []AgentDescriptor) (string, error)

	// CollectBids gathers bids from agents
	CollectBids(ctx context.Context, auctionID string) ([]*Proposal, error)

	// EvaluateBids determines the winning bid
	EvaluateBids(ctx context.Context, bids []*Proposal) (*Proposal, error)

	// AwardContract finalizes the auction
	AwardContract(ctx context.Context, auctionID string, winner *Proposal) (*Agreement, error)
}

// ContractManager handles contract negotiations.
type ContractManager interface {
	// Announce broadcasts a task announcement
	Announce(ctx context.Context, task *Task) error

	// CollectProposals gathers proposals from interested agents
	CollectProposals(ctx context.Context, taskID string) ([]*Proposal, error)

	// SelectContractor chooses the best proposal
	SelectContractor(ctx context.Context, proposals []*Proposal) (*Proposal, error)

	// FormContract creates an agreement
	FormContract(ctx context.Context, taskID string, proposal *Proposal) (*Agreement, error)
}

// VotingProtocol defines voting-based decision making.
type VotingProtocol interface {
	// ProposeDecision proposes a decision to agents
	ProposeDecision(ctx context.Context, decision *Decision) error

	// CollectVotes gathers votes from agents
	CollectVotes(ctx context.Context, decisionID string) ([]*Vote, error)

	// TallyVotes counts votes and determines the outcome
	TallyVotes(ctx context.Context, votes []*Vote) (*VotingResult, error)
}

// Decision represents a decision to be voted on.
type Decision struct {
	ID          string
	Description string
	Options     []string
	Deadline    time.Time
}

// Vote represents an agent's vote.
type Vote struct {
	DecisionID string
	AgentID    string
	Option     string
	Weight     float64
	Timestamp  time.Time
}

// VotingResult contains the outcome of a vote.
type VotingResult struct {
	DecisionID string
	Winner     string
	Votes      map[string]int
	TotalVotes int
	Timestamp  time.Time
}
