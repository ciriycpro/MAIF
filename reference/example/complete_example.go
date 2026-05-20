package example

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DemoApplication demonstrates a complete multi-agent workflow using CIRIYC PRO.
// This example implements a research analysis system where multiple agents
// collaborate to process documents, extract insights, and generate reports.
type DemoApplication struct {
	// Framework components
	messageBus   *messaging.KafkaBus
	stateManager *state.MultiBackendManager
	registry     agent.Registry
	orchestrator *orchestration.StateGraph
	tracer       *otel.Tracer
	breaker      *resilience.CircuitBreaker

	// Application agents
	researchAgent   *ResearchAgent
	analysisAgent   *AnalysisAgent
	reportAgent     *ReportAgent
	coordinatorAgent *CoordinatorAgent
}

// ResearchAgent performs document research and data collection.
type ResearchAgent struct {
	*agent.BaseAgent
	capabilities []agent.Capability
}

// AnalysisAgent analyzes collected data and extracts insights.
type AnalysisAgent struct {
	*agent.BaseAgent
	inferenceEngine inference.Engine
}

// ReportAgent generates final reports from analysis results.
type ReportAgent struct {
	*agent.BaseAgent
	documentGenerator *document.Parser
}

// CoordinatorAgent orchestrates the multi-agent workflow.
type CoordinatorAgent struct {
	*agent.BaseAgent
	humanInLoop *orchestration.HumanInTheLoop
}

// NewDemoApplication creates and configures the complete system.
func NewDemoApplication() (*DemoApplication, error) {
	ctx := context.Background()

	// 1. Configure and create message bus
	busConfig := messaging.BusConfig{
		Backend: "kafka",
		Brokers: []string{"localhost:9092"},
		ClientID: "ciriyc-demo",
		ProducerConfig: messaging.ProducerConfig{
			Acks:            -1, // all replicas
			CompressionType: "snappy",
			Idempotent:      true,
		},
		ConsumerConfig: messaging.ConsumerConfig{
			GroupID:      "ciriyc-agents",
			AutoCommit:   false,
			OffsetReset:  "earliest",
		},
	}

	messageBus := messaging.NewKafkaBus(busConfig)
	if err := messageBus.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect message bus: %w", err)
	}

	// 2. Configure and create state manager (hybrid: PostgreSQL + Redis)
	stateConfig := state.ManagerConfig{
		Backend: "hybrid",
		Caching: state.CachingConfig{
			Enabled:      true,
			Backend:      "redis",
			TTL:          5 * time.Minute,
			WriteThrough: true,
		},
		Versioning: true,
	}

	stateManager, err := state.NewMultiBackendManager(stateConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	// 3. Configure and create tracer
	tracerConfig := otel.TracerConfig{
		ServiceName:    "ciriyc-demo",
		ServiceVersion: "1.0.0",
		Environment:    "development",
		Exporter: otel.ExporterConfig{
			Type:     "jaeger",
			Endpoint: "http://localhost:14268/api/traces",
		},
		Sampling: otel.SamplingConfig{
			Strategy: "ratio",
			Ratio:    0.1, // 10% sampling
		},
	}

	tracer, err := otel.NewTracer(tracerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracer: %w", err)
	}

	// 4. Create circuit breaker for resilience
	breakerConfig := resilience.CircuitBreakerConfig{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		FailureThreshold: 0.5,
		Timeout:          10 * time.Second,
	}

	breaker := resilience.NewCircuitBreaker("demo-breaker", breakerConfig)

	// 5. Create agent registry
	registry := agent.NewDefaultRegistry()

	// 6. Create and register agents
	app := &DemoApplication{
		messageBus:   messageBus,
		stateManager: stateManager,
		tracer:       tracer,
		breaker:      breaker,
		registry:     registry,
	}

	// Create Research Agent
	app.researchAgent = &ResearchAgent{
		BaseAgent: agent.NewBaseAgent(agent.AgentConfig{
			Type: "researcher",
			Properties: map[string]interface{}{
				"max_documents": 100,
				"timeout":       "30s",
			},
		}),
		capabilities: []agent.Capability{
			{
				Name:        "document_search",
				Description: "Search for relevant documents",
				Cost:        10.0,
			},
			{
				Name:        "data_extraction",
				Description: "Extract data from documents",
				Cost:        15.0,
			},
		},
	}

	// Create Analysis Agent
	app.analysisAgent = &AnalysisAgent{
		BaseAgent: agent.NewBaseAgent(agent.AgentConfig{
			Type: "analyzer",
		}),
	}

	// Create Report Agent
	app.reportAgent = &ReportAgent{
		BaseAgent: agent.NewBaseAgent(agent.AgentConfig{
			Type: "reporter",
		}),
	}

	// Create Coordinator Agent with human-in-the-loop
	hitwConfig := orchestration.HITLConfig{
		DefaultTimeout:      5 * time.Minute,
		EscalationEnabled:   true,
		MinimumApprovers:    1,
	}

	app.coordinatorAgent = &CoordinatorAgent{
		BaseAgent: agent.NewBaseAgent(agent.AgentConfig{
			Type: "coordinator",
		}),
		humanInLoop: orchestration.NewHumanInTheLoop("demo-hitl", hitwConfig),
	}

	// Register all agents
	registry.Register(ctx, app.researchAgent)
	registry.Register(ctx, app.analysisAgent)
	registry.Register(ctx, app.reportAgent)
	registry.Register(ctx, app.coordinatorAgent)

	// 7. Build orchestration graph
	app.orchestrator = app.buildOrchestrationGraph()

	return app, nil
}

// buildOrchestrationGraph creates the workflow state graph.
func (app *DemoApplication) buildOrchestrationGraph() *orchestration.StateGraph {
	graph := orchestration.NewStateGraph("research-workflow", "Research Analysis Workflow")

	// Define workflow nodes
	startNode := &orchestration.Node{
		ID:   "start",
		Type: orchestration.AgentNode,
		Handler: func(ctx context.Context, state orchestration.GraphState) (orchestration.GraphState, error) {
			// Initialize workflow
			state.Data["status"] = "initialized"
			state.Data["start_time"] = time.Now()
			return state, nil
		},
	}

	researchNode := &orchestration.Node{
		ID:   "research",
		Type: orchestration.AgentNode,
		Handler: app.handleResearchPhase,
		Timeout: 5 * time.Minute,
		Retryable: true,
		MaxRetries: 3,
	}

	analysisNode := &orchestration.Node{
		ID:   "analysis",
		Type: orchestration.AgentNode,
		Handler: app.handleAnalysisPhase,
		Timeout: 10 * time.Minute,
	}

	approvalNode := &orchestration.Node{
		ID:   "approval",
		Type: orchestration.HumanApprovalNode,
		Handler: app.handleApprovalPhase,
	}

	reportNode := &orchestration.Node{
		ID:   "report",
		Type: orchestration.AgentNode,
		Handler: app.handleReportPhase,
		Timeout: 3 * time.Minute,
	}

	endNode := &orchestration.Node{
		ID:   "end",
		Type: orchestration.EndNode,
	}

	// Add nodes to graph
	graph.AddNode(startNode)
	graph.AddNode(researchNode)
	graph.AddNode(analysisNode)
	graph.AddNode(approvalNode)
	graph.AddNode(reportNode)
	graph.AddNode(endNode)

	// Define edges with conditions
	graph.AddEdge(&orchestration.Edge{
		From: "start",
		To:   "research",
	})

	graph.AddEdge(&orchestration.Edge{
		From: "research",
		To:   "analysis",
		Condition: func(ctx context.Context, state orchestration.GraphState) (bool, error) {
			// Proceed if research completed successfully
			status, ok := state.Data["research_status"].(string)
			return ok && status == "completed", nil
		},
	})

	graph.AddEdge(&orchestration.Edge{
		From: "analysis",
		To:   "approval",
	})

	graph.AddEdge(&orchestration.Edge{
		From: "approval",
		To:   "report",
		Condition: func(ctx context.Context, state orchestration.GraphState) (bool, error) {
			// Proceed if approved
			approved, ok := state.Data["approved"].(bool)
			return ok && approved, nil
		},
	})

	graph.AddEdge(&orchestration.Edge{
		From: "report",
		To:   "end",
	})

	// Set entry point
	graph.SetEntryPoint("start")

	return graph
}

// handleResearchPhase executes the research phase.
func (app *DemoApplication) handleResearchPhase(ctx context.Context, state orchestration.GraphState) (orchestration.GraphState, error) {
	// Start tracing span
	ctx, span := app.tracer.StartAgentSpan(ctx, app.researchAgent.ID(), "researcher", "search_documents")
	defer span.End()

	// Execute with circuit breaker protection
	result, err := app.breaker.Execute(ctx, func(ctx context.Context) (interface{}, error) {
		// Create A2A message for research task
		envelope, err := messaging.NewA2AEnvelope(
			messaging.A2AAgent{
				ID:   app.coordinatorAgent.ID(),
				Type: "coordinator",
			},
			messaging.A2AAgent{
				ID:   app.researchAgent.ID(),
				Type: "researcher",
			},
			"research_task",
			map[string]interface{}{
				"query":        state.Data["query"],
				"max_results":  100,
				"timeout":      "5m",
			},
		)
		if err != nil {
			return nil, err
		}

		// Send message via message bus
		msg := &messaging.Message{
			Key:   app.researchAgent.ID(),
			Value: []byte(envelope.ToJSON()),
		}

		if err := app.messageBus.Publish(ctx, "agent-tasks", msg); err != nil {
			return nil, err
		}

		// Simulate research work
		time.Sleep(2 * time.Second)

		// Save research results to state
		documents := []string{"doc1", "doc2", "doc3"}
		if err := app.stateManager.Save(ctx, "research_results", documents); err != nil {
			return nil, err
		}

		return documents, nil
	})

	if err != nil {
		app.tracer.RecordError(ctx, err)
		return state, err
	}

	// Update state with results
	state.Data["research_results"] = result
	state.Data["research_status"] = "completed"

	return state, nil
}

// handleAnalysisPhase executes the analysis phase.
func (app *DemoApplication) handleAnalysisPhase(ctx context.Context, state orchestration.GraphState) (orchestration.GraphState, error) {
	ctx, span := app.tracer.StartAgentSpan(ctx, app.analysisAgent.ID(), "analyzer", "analyze_data")
	defer span.End()

	// Load research results from state
	results, err := app.stateManager.Load(ctx, "research_results")
	if err != nil {
		return state, err
	}

	// Perform analysis
	insights := map[string]interface{}{
		"total_documents": len(results.([]string)),
		"key_findings":    []string{"finding1", "finding2"},
		"confidence":      0.85,
	}

	// Save analysis results
	if err := app.stateManager.Save(ctx, "analysis_results", insights); err != nil {
		return state, err
	}

	state.Data["analysis_results"] = insights
	state.Data["analysis_status"] = "completed"

	return state, nil
}

// handleApprovalPhase waits for human approval.
func (app *DemoApplication) handleApprovalPhase(ctx context.Context, state orchestration.GraphState) (orchestration.GraphState, error) {
	// Create approval request
	approval := orchestration.NewApprovalBuilder(
		"research-approval",
		"Approve Research Analysis Results",
	).
		WithDescription("Please review the analysis results before generating the report").
		WithType(orchestration.ApprovalTypeDecision).
		WithDeadline(time.Now().Add(5 * time.Minute)).
		RequireApprovals(1).
		Build()

	// Request approval
	if err := app.coordinatorAgent.humanInLoop.RequestApproval(ctx, approval); err != nil {
		return state, err
	}

	// Wait for approval
	approvedRequest, err := app.coordinatorAgent.humanInLoop.WaitForApproval(ctx, approval.ID)
	if err != nil {
		return state, err
	}

	state.Data["approved"] = approvedRequest.Status == orchestration.StatusApprovalApproved
	return state, nil
}

// handleReportPhase generates the final report.
func (app *DemoApplication) handleReportPhase(ctx context.Context, state orchestration.GraphState) (orchestration.GraphState, error) {
	ctx, span := app.tracer.StartAgentSpan(ctx, app.reportAgent.ID(), "reporter", "generate_report")
	defer span.End()

	// Generate report
	report := map[string]interface{}{
		"title":       "Research Analysis Report",
		"generated_at": time.Now(),
		"results":     state.Data["analysis_results"],
		"status":      "completed",
	}

	// Save final report
	if err := app.stateManager.Save(ctx, "final_report", report); err != nil {
		return state, err
	}

	state.Data["report"] = report
	state.Data["workflow_status"] = "completed"

	return state, nil
}

// Run executes the complete workflow.
func (app *DemoApplication) Run(ctx context.Context, query string) error {
	// Initialize workflow state
	initialState := map[string]interface{}{
		"query":      query,
		"started_at": time.Now(),
	}

	// Execute orchestration graph
	if err := app.orchestrator.Execute(ctx, initialState); err != nil {
		return fmt.Errorf("workflow execution failed: %w", err)
	}

	// Retrieve final report
	report, err := app.stateManager.Load(ctx, "final_report")
	if err != nil {
		return fmt.Errorf("failed to load report: %w", err)
	}

	// Print results
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	fmt.Printf("Research completed successfully:\n%s\n", reportJSON)

	return nil
}

// Shutdown gracefully shuts down the application.
func (app *DemoApplication) Shutdown(ctx context.Context) error {
	// Close message bus
	if err := app.messageBus.Disconnect(ctx); err != nil {
		return err
	}

	// Close state manager
	if err := app.stateManager.Close(ctx); err != nil {
		return err
	}

	// Shutdown tracer
	if err := app.tracer.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

// Main demonstrates running the application.
func Main() {
	ctx := context.Background()

	// Create application
	app, err := NewDemoApplication()
	if err != nil {
		panic(err)
	}
	defer app.Shutdown(ctx)

	// Run workflow
	if err := app.Run(ctx, "AI multi-agent systems research"); err != nil {
		panic(err)
	}
}
