package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HumanInTheLoop provides mechanisms for human approval and intervention
// in automated workflows. It enables critical decision points where human
// judgment is required before proceeding.
type HumanInTheLoop struct {
	mu sync.RWMutex

	// ID identifies this HITL instance
	ID string

	// Approvers authorized approvers
	Approvers map[string]*Approver

	// PendingApprovals approvals waiting for response
	PendingApprovals map[string]*ApprovalRequest

	// ApprovalHistory completed approvals
	ApprovalHistory []ApprovalRecord

	// Notifier sends approval requests
	Notifier ApprovalNotifier

	// Config HITL configuration
	Config HITLConfig

	// Metrics HITL metrics
	Metrics *HITLMetrics
}

// Approver represents a person who can approve requests.
type Approver struct {
	// ID approver identifier
	ID string

	// Name approver name
	Name string

	// Email contact email
	Email string

	// Role approver role
	Role string

	// Capabilities approval capabilities
	Capabilities []string

	// MaxConcurrent maximum concurrent approvals
	MaxConcurrent int

	// CurrentLoad current number of pending approvals
	CurrentLoad int

	// ResponseTime average response time
	ResponseTime time.Duration

	// Metadata approver metadata
	Metadata map[string]interface{}
}

// ApprovalRequest represents a request for human approval.
type ApprovalRequest struct {
	// ID request identifier
	ID string

	// Title request title
	Title string

	// Description request description
	Description string

	// Type request type
	Type ApprovalType

	// Context contextual information
	Context map[string]interface{}

	// Options available approval options
	Options []ApprovalOption

	// RequiredApprovers number of approvals needed
	RequiredApprovers int

	// AssignedApprovers assigned approvers
	AssignedApprovers []string

	// CreatedAt request creation time
	CreatedAt time.Time

	// Deadline approval deadline
	Deadline time.Time

	// Priority request priority (0 = highest)
	Priority int

	// Status current status
	Status ApprovalStatus

	// Responses received responses
	Responses []ApprovalResponse

	// Metadata request metadata
	Metadata map[string]interface{}
}

// ApprovalType categorizes approval requests.
type ApprovalType string

const (
	// ApprovalTypeDecision simple yes/no decision
	ApprovalTypeDecision ApprovalType = "decision"

	// ApprovalTypeSelection choose from multiple options
	ApprovalTypeSelection ApprovalType = "selection"

	// ApprovalTypeReview review and provide feedback
	ApprovalTypeReview ApprovalType = "review"

	// ApprovalTypeVerification verify information
	ApprovalTypeVerification ApprovalType = "verification"

	// ApprovalTypeAuthorization authorize an action
	ApprovalTypeAuthorization ApprovalType = "authorization"
)

// ApprovalOption represents a choice in an approval request.
type ApprovalOption struct {
	// ID option identifier
	ID string

	// Label display label
	Label string

	// Description option description
	Description string

	// Value option value
	Value interface{}

	// Consequences what happens if selected
	Consequences string

	// Recommended indicates if this is the recommended option
	Recommended bool
}

// ApprovalStatus describes the approval request status.
type ApprovalStatus string

const (
	StatusApprovalPending  ApprovalStatus = "pending"
	StatusApprovalApproved ApprovalStatus = "approved"
	StatusApprovalRejected ApprovalStatus = "rejected"
	StatusApprovalExpired  ApprovalStatus = "expired"
	StatusApprovalCanceled ApprovalStatus = "canceled"
)

// ApprovalResponse represents an approver's response.
type ApprovalResponse struct {
	// ApprovalID associated approval request
	ApprovalID string

	// ApproverID who responded
	ApproverID string

	// Decision the decision made
	Decision ApprovalDecision

	// SelectedOption selected option (if applicable)
	SelectedOption string

	// Comments approver comments
	Comments string

	// Timestamp response time
	Timestamp time.Time

	// Metadata response metadata
	Metadata map[string]interface{}
}

// ApprovalDecision represents the approval decision.
type ApprovalDecision string

const (
	DecisionApprove    ApprovalDecision = "approve"
	DecisionReject     ApprovalDecision = "reject"
	DecisionRequestInfo ApprovalDecision = "request_info"
	DecisionDelegate   ApprovalDecision = "delegate"
)

// ApprovalRecord records a completed approval.
type ApprovalRecord struct {
	Request     *ApprovalRequest
	Responses   []ApprovalResponse
	FinalStatus ApprovalStatus
	CompletedAt time.Time
	Duration    time.Duration
}

// HITLConfig holds HITL configuration.
type HITLConfig struct {
	// DefaultTimeout default approval timeout
	DefaultTimeout time.Duration

	// EscalationEnabled enables escalation on timeout
	EscalationEnabled bool

	// EscalationDelay time before escalation
	EscalationDelay time.Duration

	// AutoRejectOnTimeout rejects on timeout if true
	AutoRejectOnTimeout bool

	// RequireConsensus all approvers must agree
	RequireConsensus bool

	// MinimumApprovers minimum number of approvals
	MinimumApprovers int
}

// NewHumanInTheLoop creates a new HITL instance.
func NewHumanInTheLoop(id string, config HITLConfig) *HumanInTheLoop {
	return &HumanInTheLoop{
		ID:               id,
		Approvers:        make(map[string]*Approver),
		PendingApprovals: make(map[string]*ApprovalRequest),
		ApprovalHistory:  make([]ApprovalRecord, 0),
		Config:           config,
		Metrics:          &HITLMetrics{},
	}
}

// RegisterApprover registers a new approver.
func (hitl *HumanInTheLoop) RegisterApprover(approver *Approver) error {
	hitl.mu.Lock()
	defer hitl.mu.Unlock()

	if approver.ID == "" {
		return fmt.Errorf("approver ID cannot be empty")
	}

	hitl.Approvers[approver.ID] = approver
	return nil
}

// RequestApproval submits a new approval request.
func (hitl *HumanInTheLoop) RequestApproval(ctx context.Context, request *ApprovalRequest) error {
	hitl.mu.Lock()
	defer hitl.mu.Unlock()

	// Validate request
	if request.ID == "" {
		return fmt.Errorf("request ID cannot be empty")
	}

	// Set defaults
	if request.CreatedAt.IsZero() {
		request.CreatedAt = time.Now()
	}
	if request.Deadline.IsZero() {
		request.Deadline = time.Now().Add(hitl.Config.DefaultTimeout)
	}
	if request.Status == "" {
		request.Status = StatusApprovalPending
	}

	// Assign approvers if not specified
	if len(request.AssignedApprovers) == 0 {
		approvers, err := hitl.selectApprovers(request)
		if err != nil {
			return err
		}
		request.AssignedApprovers = approvers
	}

	// Store request
	hitl.PendingApprovals[request.ID] = request

	// Send notifications
	if hitl.Notifier != nil {
		for _, approverID := range request.AssignedApprovers {
			if approver, exists := hitl.Approvers[approverID]; exists {
				if err := hitl.Notifier.Notify(ctx, approver, request); err != nil {
					// Log error but continue
				}
			}
		}
	}

	// Start timeout monitor
	go hitl.monitorTimeout(ctx, request.ID)

	// Update metrics
	hitl.Metrics.mu.Lock()
	hitl.Metrics.RequestsCreated++
	hitl.Metrics.mu.Unlock()

	return nil
}

// SubmitResponse submits an approval response.
func (hitl *HumanInTheLoop) SubmitResponse(ctx context.Context, response *ApprovalResponse) error {
	hitl.mu.Lock()
	defer hitl.mu.Unlock()

	// Find request
	request, exists := hitl.PendingApprovals[response.ApprovalID]
	if !exists {
		return fmt.Errorf("approval request %s not found", response.ApprovalID)
	}

	// Verify approver
	isAssigned := false
	for _, approverID := range request.AssignedApprovers {
		if approverID == response.ApproverID {
			isAssigned = true
			break
		}
	}
	if !isAssigned {
		return fmt.Errorf("approver %s not assigned to request %s", 
			response.ApproverID, response.ApprovalID)
	}

	// Check if already responded
	for _, existing := range request.Responses {
		if existing.ApproverID == response.ApproverID {
			return fmt.Errorf("approver %s already responded", response.ApproverID)
		}
	}

	// Add response
	if response.Timestamp.IsZero() {
		response.Timestamp = time.Now()
	}
	request.Responses = append(request.Responses, *response)

	// Check if decision is final
	if hitl.isFinalDecision(request) {
		hitl.finalizeRequest(request)
	}

	return nil
}

// WaitForApproval waits for approval decision.
func (hitl *HumanInTheLoop) WaitForApproval(ctx context.Context, requestID string) (*ApprovalRequest, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-ticker.C:
			hitl.mu.RLock()
			request, exists := hitl.PendingApprovals[requestID]
			hitl.mu.RUnlock()

			if !exists {
				// Check history
				for i := len(hitl.ApprovalHistory) - 1; i >= 0; i-- {
					if hitl.ApprovalHistory[i].Request.ID == requestID {
						return hitl.ApprovalHistory[i].Request, nil
					}
				}
				return nil, fmt.Errorf("approval request %s not found", requestID)
			}

			if request.Status != StatusApprovalPending {
				return request, nil
			}
		}
	}
}

// GetApprovalStatus returns the current status of an approval.
func (hitl *HumanInTheLoop) GetApprovalStatus(requestID string) (ApprovalStatus, error) {
	hitl.mu.RLock()
	defer hitl.mu.RUnlock()

	if request, exists := hitl.PendingApprovals[requestID]; exists {
		return request.Status, nil
	}

	// Check history
	for i := len(hitl.ApprovalHistory) - 1; i >= 0; i-- {
		if hitl.ApprovalHistory[i].Request.ID == requestID {
			return hitl.ApprovalHistory[i].FinalStatus, nil
		}
	}

	return "", fmt.Errorf("approval request %s not found", requestID)
}

// CancelApproval cancels a pending approval request.
func (hitl *HumanInTheLoop) CancelApproval(ctx context.Context, requestID string) error {
	hitl.mu.Lock()
	defer hitl.mu.Unlock()

	request, exists := hitl.PendingApprovals[requestID]
	if !exists {
		return fmt.Errorf("approval request %s not found", requestID)
	}

	request.Status = StatusApprovalCanceled
	hitl.finalizeRequest(request)

	return nil
}

// Helper methods

func (hitl *HumanInTheLoop) selectApprovers(request *ApprovalRequest) ([]string, error) {
	// Simple selection: find approvers with lowest load
	var selectedIDs []string
	for id, approver := range hitl.Approvers {
		if approver.CurrentLoad < approver.MaxConcurrent {
			selectedIDs = append(selectedIDs, id)
			if len(selectedIDs) >= request.RequiredApprovers {
				break
			}
		}
	}

	if len(selectedIDs) < request.RequiredApprovers {
		return nil, fmt.Errorf("insufficient available approvers")
	}

	return selectedIDs, nil
}

func (hitl *HumanInTheLoop) isFinalDecision(request *ApprovalRequest) bool {
	// Check if enough approvals received
	approvals := 0
	rejections := 0

	for _, response := range request.Responses {
		switch response.Decision {
		case DecisionApprove:
			approvals++
		case DecisionReject:
			rejections++
		}
	}

	// If consensus required, all must approve
	if hitl.Config.RequireConsensus {
		return len(request.Responses) == len(request.AssignedApprovers)
	}

	// Otherwise, check if minimum approvals met
	return approvals >= request.RequiredApprovers || rejections > 0
}

func (hitl *HumanInTheLoop) finalizeRequest(request *ApprovalRequest) {
	// Determine final status
	approvals := 0
	for _, response := range request.Responses {
		if response.Decision == DecisionApprove {
			approvals++
		}
	}

	if approvals >= request.RequiredApprovers {
		request.Status = StatusApprovalApproved
	} else {
		request.Status = StatusApprovalRejected
	}

	// Move to history
	record := ApprovalRecord{
		Request:     request,
		Responses:   request.Responses,
		FinalStatus: request.Status,
		CompletedAt: time.Now(),
		Duration:    time.Since(request.CreatedAt),
	}

	hitl.ApprovalHistory = append(hitl.ApprovalHistory, record)
	delete(hitl.PendingApprovals, request.ID)

	// Update metrics
	hitl.Metrics.mu.Lock()
	if request.Status == StatusApprovalApproved {
		hitl.Metrics.RequestsApproved++
	} else {
		hitl.Metrics.RequestsRejected++
	}
	hitl.Metrics.mu.Unlock()
}

func (hitl *HumanInTheLoop) monitorTimeout(ctx context.Context, requestID string) {
	timer := time.NewTimer(hitl.Config.DefaultTimeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		hitl.handleTimeout(requestID)
	case <-ctx.Done():
		return
	}
}

func (hitl *HumanInTheLoop) handleTimeout(requestID string) {
	hitl.mu.Lock()
	defer hitl.mu.Unlock()

	request, exists := hitl.PendingApprovals[requestID]
	if !exists || request.Status != StatusApprovalPending {
		return
	}

	if hitl.Config.AutoRejectOnTimeout {
		request.Status = StatusApprovalExpired
		hitl.finalizeRequest(request)
	} else if hitl.Config.EscalationEnabled {
		// Escalation logic would go here
	}

	hitl.Metrics.mu.Lock()
	hitl.Metrics.RequestsExpired++
	hitl.Metrics.mu.Unlock()
}

// ApprovalNotifier sends approval notifications.
type ApprovalNotifier interface {
	// Notify sends a notification to an approver
	Notify(ctx context.Context, approver *Approver, request *ApprovalRequest) error
}

// HITLMetrics tracks HITL performance metrics.
type HITLMetrics struct {
	RequestsCreated  int64
	RequestsApproved int64
	RequestsRejected int64
	RequestsExpired  int64
	RequestsCanceled int64

	AverageResponseTime time.Duration
	P95ResponseTime     time.Duration

	mu sync.RWMutex
}

// ApprovalBuilder provides a fluent API for building approval requests.
type ApprovalBuilder struct {
	request *ApprovalRequest
}

// NewApprovalBuilder creates a new approval builder.
func NewApprovalBuilder(id, title string) *ApprovalBuilder {
	return &ApprovalBuilder{
		request: &ApprovalRequest{
			ID:       id,
			Title:    title,
			Context:  make(map[string]interface{}),
			Options:  make([]ApprovalOption, 0),
			Metadata: make(map[string]interface{}),
		},
	}
}

// WithDescription sets the description.
func (ab *ApprovalBuilder) WithDescription(description string) *ApprovalBuilder {
	ab.request.Description = description
	return ab
}

// WithType sets the approval type.
func (ab *ApprovalBuilder) WithType(approvalType ApprovalType) *ApprovalBuilder {
	ab.request.Type = approvalType
	return ab
}

// WithOption adds an approval option.
func (ab *ApprovalBuilder) WithOption(option ApprovalOption) *ApprovalBuilder {
	ab.request.Options = append(ab.request.Options, option)
	return ab
}

// WithDeadline sets the deadline.
func (ab *ApprovalBuilder) WithDeadline(deadline time.Time) *ApprovalBuilder {
	ab.request.Deadline = deadline
	return ab
}

// WithApprovers assigns approvers.
func (ab *ApprovalBuilder) WithApprovers(approverIDs ...string) *ApprovalBuilder {
	ab.request.AssignedApprovers = approverIDs
	return ab
}

// RequireApprovals sets the number of required approvals.
func (ab *ApprovalBuilder) RequireApprovals(count int) *ApprovalBuilder {
	ab.request.RequiredApprovers = count
	return ab
}

// Build returns the approval request.
func (ab *ApprovalBuilder) Build() *ApprovalRequest {
	return ab.request
}
