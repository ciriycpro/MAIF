package streaming

import (
	"context"
	"time"
)

// Stream represents a continuous flow of data elements.
// Streams can be transformed, filtered, aggregated, and joined with other streams.
type Stream interface {
	// Map applies a transformation function to each element
	Map(ctx context.Context, fn MapFunc) (Stream, error)

	// Filter selects elements matching a predicate
	Filter(ctx context.Context, fn FilterFunc) (Stream, error)

	// FlatMap expands each element into zero or more elements
	FlatMap(ctx context.Context, fn FlatMapFunc) (Stream, error)

	// Reduce aggregates elements using a reducer function
	Reduce(ctx context.Context, fn ReduceFunc) (interface{}, error)

	// Window groups elements into windows for aggregation
	Window(ctx context.Context, spec WindowSpec) (WindowedStream, error)

	// Join combines this stream with another based on a key
	Join(ctx context.Context, other Stream, spec JoinSpec) (Stream, error)

	// Sink writes stream elements to an output
	Sink(ctx context.Context, sink Sink) error

	// ToChannel returns a channel that emits stream elements
	ToChannel(ctx context.Context) (<-chan interface{}, error)

	// Metadata returns stream metadata
	Metadata() StreamMetadata
}

// WindowedStream represents a stream partitioned into windows.
type WindowedStream interface {
	// Aggregate applies an aggregation function to each window
	Aggregate(ctx context.Context, fn AggregateFunc) (Stream, error)

	// Count counts elements in each window
	Count(ctx context.Context) (Stream, error)

	// Sum sums numeric values in each window
	Sum(ctx context.Context, keyExtractor KeyExtractorFunc) (Stream, error)

	// Min finds minimum value in each window
	Min(ctx context.Context, keyExtractor KeyExtractorFunc) (Stream, error)

	// Max finds maximum value in each window
	Max(ctx context.Context, keyExtractor KeyExtractorFunc) (Stream, error)

	// GroupBy groups window elements by a key
	GroupBy(ctx context.Context, keyExtractor KeyExtractorFunc) (Stream, error)
}

// MapFunc transforms a single element.
type MapFunc func(ctx context.Context, element interface{}) (interface{}, error)

// FilterFunc tests if an element should be included.
type FilterFunc func(ctx context.Context, element interface{}) (bool, error)

// FlatMapFunc expands an element into multiple elements.
type FlatMapFunc func(ctx context.Context, element interface{}) ([]interface{}, error)

// ReduceFunc combines two elements into one.
type ReduceFunc func(ctx context.Context, acc, element interface{}) (interface{}, error)

// AggregateFunc aggregates window elements.
type AggregateFunc func(ctx context.Context, window Window, elements []interface{}) (interface{}, error)

// KeyExtractorFunc extracts a key from an element.
type KeyExtractorFunc func(ctx context.Context, element interface{}) (string, error)

// WindowSpec defines how to partition a stream into windows.
type WindowSpec struct {
	// Type window type ("tumbling", "sliding", "session")
	Type WindowType

	// Size window duration for tumbling/sliding windows
	Size time.Duration

	// Slide slide interval for sliding windows
	Slide time.Duration

	// Gap session timeout for session windows
	Gap time.Duration

	// AllowedLateness maximum lateness for out-of-order events
	AllowedLateness time.Duration

	// TimestampExtractor extracts event time from elements
	TimestampExtractor TimestampExtractorFunc
}

// WindowType defines the window partitioning strategy.
type WindowType string

const (
	// TumblingWindow non-overlapping, fixed-size windows
	TumblingWindow WindowType = "tumbling"

	// SlidingWindow overlapping, fixed-size windows
	SlidingWindow WindowType = "sliding"

	// SessionWindow dynamic windows based on activity gaps
	SessionWindow WindowType = "session"

	// GlobalWindow single window containing all elements
	GlobalWindow WindowType = "global"
)

// TimestampExtractorFunc extracts the event timestamp.
type TimestampExtractorFunc func(ctx context.Context, element interface{}) (time.Time, error)

// Window represents a time-bounded partition of a stream.
type Window struct {
	// Start window start time
	Start time.Time

	// End window end time
	End time.Time

	// Key window key (for keyed windows)
	Key string

	// Count number of elements in window
	Count int64

	// Metadata window metadata
	Metadata map[string]interface{}
}

// JoinSpec defines how to join two streams.
type JoinSpec struct {
	// Type join type ("inner", "left", "right", "full")
	Type JoinType

	// LeftKeyExtractor extracts join key from left stream
	LeftKeyExtractor KeyExtractorFunc

	// RightKeyExtractor extracts join key from right stream
	RightKeyExtractor KeyExtractorFunc

	// Window join window specification
	Window WindowSpec

	// JoinFunc combines matching elements
	JoinFunc JoinFunc
}

// JoinType defines the join strategy.
type JoinType string

const (
	InnerJoin JoinType = "inner"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
	FullJoin  JoinType = "full"
)

// JoinFunc combines elements from two streams.
type JoinFunc func(ctx context.Context, left, right interface{}) (interface{}, error)

// Sink writes stream elements to an output.
type Sink interface {
	// Write writes an element to the sink
	Write(ctx context.Context, element interface{}) error

	// WriteBatch writes multiple elements
	WriteBatch(ctx context.Context, elements []interface{}) error

	// Flush ensures all pending writes are persisted
	Flush(ctx context.Context) error

	// Close releases sink resources
	Close(ctx context.Context) error
}

// Source produces stream elements.
type Source interface {
	// Read reads the next element
	Read(ctx context.Context) (interface{}, error)

	// ReadBatch reads multiple elements
	ReadBatch(ctx context.Context, maxElements int) ([]interface{}, error)

	// Close releases source resources
	Close(ctx context.Context) error

	// Metadata returns source metadata
	Metadata() SourceMetadata
}

// StreamMetadata describes stream characteristics.
type StreamMetadata struct {
	// ID stream identifier
	ID string

	// Source stream source
	Source string

	// Schema element schema (if available)
	Schema string

	// Partitions number of partitions
	Partitions int

	// Parallelism processing parallelism
	Parallelism int

	// Watermark current watermark timestamp
	Watermark time.Time

	// Lag processing lag
	Lag time.Duration
}

// SourceMetadata describes source characteristics.
type SourceMetadata struct {
	// Type source type ("kafka", "file", "socket", "generator")
	Type string

	// Location source location/address
	Location string

	// Schema element schema
	Schema string

	// Bounded indicates if source is finite
	Bounded bool

	// EstimatedSize estimated total elements (for bounded sources)
	EstimatedSize int64
}

// StreamBuilder provides a fluent API for building stream processing pipelines.
type StreamBuilder interface {
	// AddSource adds a data source
	AddSource(ctx context.Context, source Source) (Stream, error)

	// CreateStream creates a new stream from a channel
	CreateStream(ctx context.Context, ch <-chan interface{}) (Stream, error)

	// Build finalizes the stream processing pipeline
	Build(ctx context.Context) (*StreamPipeline, error)
}

// StreamPipeline represents an executable stream processing pipeline.
type StreamPipeline struct {
	// ID pipeline identifier
	ID string

	// Streams streams in the pipeline
	Streams []Stream

	// Sources data sources
	Sources []Source

	// Sinks output sinks
	Sinks []Sink

	// Started pipeline start time
	Started time.Time

	// Status pipeline status
	Status string

	// Metrics pipeline metrics
	Metrics *PipelineMetrics
}

// Start starts the stream processing pipeline.
func (sp *StreamPipeline) Start(ctx context.Context) error {
	// Implementation would start all stream processors
	sp.Status = "running"
	sp.Started = time.Now()
	return nil
}

// Stop stops the stream processing pipeline.
func (sp *StreamPipeline) Stop(ctx context.Context) error {
	// Implementation would gracefully stop all processors
	sp.Status = "stopped"
	return nil
}

// PipelineMetrics tracks stream processing metrics.
type PipelineMetrics struct {
	// ElementsProcessed total elements processed
	ElementsProcessed int64

	// BytesProcessed total bytes processed
	BytesProcessed int64

	// ErrorsEncountered processing errors
	ErrorsEncountered int64

	// Throughput current throughput (elements/sec)
	Throughput float64

	// Latency processing latency metrics
	Latency LatencyMetrics

	// BackpressureEvents backpressure occurrences
	BackpressureEvents int64

	// WindowsFired windows that completed
	WindowsFired int64

	// LateElements late-arriving elements
	LateElements int64

	mu sync.RWMutex
}

// LatencyMetrics captures latency statistics.
type LatencyMetrics struct {
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

// StreamConfig holds stream processing configuration.
type StreamConfig struct {
	// Parallelism processing parallelism
	Parallelism int

	// BufferSize channel buffer size
	BufferSize int

	// CheckpointInterval state checkpoint interval
	CheckpointInterval time.Duration

	// BackpressureStrategy backpressure handling strategy
	BackpressureStrategy BackpressureStrategy

	// WatermarkStrategy watermark generation strategy
	WatermarkStrategy WatermarkStrategy

	// StateBackend state storage backend
	StateBackend StateBackend
}

// BackpressureStrategy defines how to handle backpressure.
type BackpressureStrategy string

const (
	// DropOldest drops oldest elements when buffer is full
	DropOldest BackpressureStrategy = "drop_oldest"

	// DropNewest drops newest elements when buffer is full
	DropNewest BackpressureStrategy = "drop_newest"

	// Block blocks upstream when buffer is full
	Block BackpressureStrategy = "block"

	// Spill spills to disk when buffer is full
	Spill BackpressureStrategy = "spill"
)

// WatermarkStrategy defines watermark generation.
type WatermarkStrategy interface {
	// GenerateWatermark generates a watermark based on observed timestamps
	GenerateWatermark(ctx context.Context, timestamp time.Time) (time.Time, error)

	// OnPeriodicEmit periodically emits watermarks
	OnPeriodicEmit(ctx context.Context) (time.Time, error)
}

// StateBackend stores stream processing state.
type StateBackend interface {
	// Put stores a key-value pair
	Put(ctx context.Context, key string, value interface{}) error

	// Get retrieves a value by key
	Get(ctx context.Context, key string) (interface{}, error)

	// Delete removes a key
	Delete(ctx context.Context, key string) error

	// Checkpoint creates a state checkpoint
	Checkpoint(ctx context.Context) (CheckpointID, error)

	// Restore restores state from a checkpoint
	Restore(ctx context.Context, checkpointID CheckpointID) error
}

// CheckpointID identifies a state checkpoint.
type CheckpointID string

// Watermark represents a time threshold for out-of-order processing.
type Watermark struct {
	// Timestamp watermark timestamp
	Timestamp time.Time

	// Source watermark source
	Source string

	// Metadata additional watermark information
	Metadata map[string]interface{}
}

// ComplexEventPattern defines a pattern for CEP (Complex Event Processing).
type ComplexEventPattern struct {
	// Name pattern name
	Name string

	// Conditions pattern conditions
	Conditions []PatternCondition

	// Within time constraint for pattern matching
	Within time.Duration

	// Action action to execute when pattern matches
	Action PatternAction
}

// PatternCondition tests if an element matches part of a pattern.
type PatternCondition func(ctx context.Context, element interface{}) (bool, error)

// PatternAction executes when a pattern is detected.
type PatternAction func(ctx context.Context, matchedElements []interface{}) error

// CEPEngine detects complex event patterns in streams.
type CEPEngine interface {
	// RegisterPattern registers a pattern for detection
	RegisterPattern(ctx context.Context, pattern *ComplexEventPattern) error

	// Process processes stream elements for pattern matching
	Process(ctx context.Context, stream Stream) (Stream, error)

	// GetMatches returns detected pattern matches
	GetMatches(ctx context.Context, patternName string) ([][]interface{}, error)
}

import "sync"
