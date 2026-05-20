package state

import (
	"context"
	"time"
)

// StateManager defines the interface for managing agent state across
// different storage backends. It supports multi-backend strategies
// for optimal performance and consistency.
type StateManager interface {
	// Save persists agent state to the underlying storage.
	// The key typically identifies the agent, state can be any serializable data.
	Save(ctx context.Context, key string, state interface{}) error

	// Load retrieves agent state from storage.
	// Returns ErrStateNotFound if the state doesn't exist.
	Load(ctx context.Context, key string) (interface{}, error)

	// Delete removes state from storage.
	Delete(ctx context.Context, key string) error

	// Exists checks if state exists for the given key.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns all state keys matching the pattern.
	// Pattern syntax depends on the backend (e.g., "agent:*" for Redis).
	List(ctx context.Context, pattern string) ([]string, error)

	// Transaction begins a transaction for atomic state updates.
	// Useful for updating multiple agents' states atomically.
	Transaction(ctx context.Context) (Transaction, error)

	// Watch returns a channel that emits events when state changes.
	// Enables reactive patterns and real-time synchronization.
	Watch(ctx context.Context, key string) (<-chan StateEvent, error)

	// Close releases resources and closes connections.
	Close(ctx context.Context) error

	// Health returns the health status of the storage backend.
	Health(ctx context.Context) (*HealthStatus, error)
}

// Transaction represents an atomic state operation.
// All operations within a transaction succeed or fail together.
type Transaction interface {
	// Save persists state within the transaction
	Save(ctx context.Context, key string, state interface{}) error

	// Load retrieves state within the transaction
	Load(ctx context.Context, key string) (interface{}, error)

	// Delete removes state within the transaction
	Delete(ctx context.Context, key string) error

	// Commit applies all changes atomically
	Commit(ctx context.Context) error

	// Rollback discards all changes
	Rollback(ctx context.Context) error
}

// StateEvent represents a change in stored state.
type StateEvent struct {
	// Type of event ("created", "updated", "deleted")
	Type string

	// Key identifies the affected state
	Key string

	// State is the new state value (nil for delete events)
	State interface{}

	// PreviousState is the old state value (if available)
	PreviousState interface{}

	// Timestamp when the event occurred
	Timestamp time.Time

	// Version is the state version after this event
	Version int64
}

// HealthStatus describes the health of the state manager.
type HealthStatus struct {
	// Healthy indicates if the backend is functioning
	Healthy bool

	// Backend identifies the storage system
	Backend string

	// Connected indicates if connected to storage
	Connected bool

	// Latency recent operation latencies
	Latency LatencyMetrics

	// StorageUsed in bytes (if supported)
	StorageUsed int64

	// KeyCount total number of stored keys
	KeyCount int64

	// LastError most recent error
	LastError string

	// LastCheck when health was evaluated
	LastCheck time.Time
}

// LatencyMetrics captures performance statistics.
type LatencyMetrics struct {
	// SaveP50 median save operation latency
	SaveP50 time.Duration

	// SaveP99 99th percentile save latency
	SaveP99 time.Duration

	// LoadP50 median load operation latency
	LoadP50 time.Duration

	// LoadP99 99th percentile load latency
	LoadP99 time.Duration
}

// ManagerConfig holds configuration for creating a StateManager.
type ManagerConfig struct {
	// Backend specifies the storage system
	// ("postgres", "redis", "neo4j", "mongodb", "hybrid")
	Backend string

	// ConnectionString for the backend
	ConnectionString string

	// PoolSize for connection pooling
	PoolSize int

	// Timeout for operations
	Timeout time.Duration

	// RetryPolicy for transient failures
	RetryPolicy RetryPolicy

	// Serialization format ("json", "msgpack", "protobuf")
	Serialization string

	// Compression enables state compression
	Compression bool

	// Encryption enables at-rest encryption
	Encryption EncryptionConfig

	// Caching configuration for hybrid strategies
	Caching CachingConfig

	// Versioning enables state versioning
	Versioning bool

	// TTL default time-to-live for state entries
	TTL time.Duration
}

// RetryPolicy defines retry behavior for failed operations.
type RetryPolicy struct {
	// MaxAttempts maximum retry attempts
	MaxAttempts int

	// InitialBackoff starting delay between retries
	InitialBackoff time.Duration

	// MaxBackoff maximum delay between retries
	MaxBackoff time.Duration

	// BackoffMultiplier exponential backoff multiplier
	BackoffMultiplier float64

	// RetryableErrors error types that trigger retries
	RetryableErrors []string
}

// EncryptionConfig specifies encryption parameters.
type EncryptionConfig struct {
	// Enabled turns on encryption
	Enabled bool

	// Algorithm ("AES-256-GCM", "ChaCha20-Poly1305")
	Algorithm string

	// KeyID identifies the encryption key
	KeyID string

	// RotationInterval for automatic key rotation
	RotationInterval time.Duration
}

// CachingConfig specifies caching behavior for hybrid state managers.
type CachingConfig struct {
	// Enabled turns on caching
	Enabled bool

	// Backend for caching ("redis", "memcached", "memory")
	Backend string

	// TTL cache entry time-to-live
	TTL time.Duration

	// MaxSize maximum cache size in bytes
	MaxSize int64

	// EvictionPolicy ("LRU", "LFU", "FIFO")
	EvictionPolicy string

	// WriteThrough immediately updates persistent storage
	WriteThrough bool

	// WriteBehind delays persistent storage updates
	WriteBehind bool

	// RefreshInterval for background cache refresh
	RefreshInterval time.Duration
}

// StateManagerFactory creates StateManager instances.
type StateManagerFactory interface {
	// Create instantiates a state manager with the given configuration
	Create(ctx context.Context, config ManagerConfig) (StateManager, error)

	// SupportedBackends returns available storage backends
	SupportedBackends() []string
}

// MultiBackendManager manages state across multiple storage backends.
// Enables strategies like:
// - Write to PostgreSQL + cache in Redis
// - Store metadata in Neo4j + documents in MongoDB
type MultiBackendManager interface {
	StateManager

	// Primary returns the primary storage backend
	Primary() StateManager

	// Cache returns the cache backend (if configured)
	Cache() StateManager

	// Graph returns the graph database backend (if configured)
	Graph() StateManager

	// Document returns the document database backend (if configured)
	Document() StateManager

	// Strategy returns the multi-backend coordination strategy
	Strategy() string
}

// Strategy defines how multiple backends coordinate.
type Strategy int

const (
	// StrategyCacheAside caches are populated on read
	StrategyCacheAside Strategy = iota

	// StrategyWriteThrough writes go to both cache and primary
	StrategyWriteThrough

	// StrategyWriteBehind writes go to cache, async to primary
	StrategyWriteBehind

	// StrategyReadThrough reads populate cache automatically
	StrategyReadThrough

	// StrategyRefreshAhead proactively refreshes hot cache entries
	StrategyRefreshAhead
)

// StateSnapshot represents a point-in-time state snapshot.
// Used for backup, migration, and temporal queries.
type StateSnapshot struct {
	// ID uniquely identifies this snapshot
	ID string

	// Keys included in the snapshot
	Keys []string

	// States map from key to state value
	States map[string]interface{}

	// Version snapshot version number
	Version int64

	// Timestamp when snapshot was created
	Timestamp time.Time

	// Metadata additional snapshot information
	Metadata map[string]string
}

// SnapshotManager manages state snapshots.
type SnapshotManager interface {
	// Create creates a new snapshot
	Create(ctx context.Context, keys []string) (*StateSnapshot, error)

	// Restore restores state from a snapshot
	Restore(ctx context.Context, snapshotID string) error

	// List returns available snapshots
	List(ctx context.Context) ([]*StateSnapshot, error)

	// Delete removes a snapshot
	Delete(ctx context.Context, snapshotID string) error

	// Export exports a snapshot to external storage
	Export(ctx context.Context, snapshotID string, destination string) error

	// Import imports a snapshot from external storage
	Import(ctx context.Context, source string) (*StateSnapshot, error)
}

// ErrStateNotFound is returned when requested state doesn't exist.
var ErrStateNotFound = &StateError{
	Code:    "STATE_NOT_FOUND",
	Message: "state not found",
}

// StateError represents a state management error.
type StateError struct {
	Code    string
	Message string
	Cause   error
}

func (e *StateError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}
