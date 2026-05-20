package messaging

import (
	"context"
	"time"
)

// MessageBus defines the core abstraction for publish-subscribe messaging.
// It enables decoupled communication between agents and system components.
type MessageBus interface {
	// Connect establishes a connection to the message broker.
	// Must be called before using other methods.
	Connect(ctx context.Context) error

	// Disconnect closes the connection to the message broker.
	// Should be called during graceful shutdown.
	Disconnect(ctx context.Context) error

	// Publish sends a message to a topic.
	// The message will be delivered to all active subscribers.
	Publish(ctx context.Context, topic string, msg *Message) error

	// PublishBatch sends multiple messages in a single operation.
	// More efficient than calling Publish repeatedly.
	PublishBatch(ctx context.Context, topic string, messages []*Message) error

	// Subscribe registers a consumer for messages on a topic.
	// Returns a channel that will receive messages.
	//
	// The consumer group parameter enables load balancing:
	// - Same group = messages distributed across consumers
	// - Different groups = all consumers receive all messages
	Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan *Message, error)

	// Unsubscribe removes a subscription.
	// The message channel will be closed.
	Unsubscribe(ctx context.Context, topic string, consumerGroup string) error

	// CreateTopic creates a new topic with the specified configuration.
	// May be a no-op for some backends.
	CreateTopic(ctx context.Context, topic string, config TopicConfig) error

	// DeleteTopic removes a topic and all its messages.
	// Use with caution - this is irreversible.
	DeleteTopic(ctx context.Context, topic string) error

	// TopicExists checks if a topic exists.
	TopicExists(ctx context.Context, topic string) (bool, error)

	// GetOffset returns the current offset for a consumer group on a topic.
	// Used for monitoring consumer lag.
	GetOffset(ctx context.Context, topic string, consumerGroup string) (int64, error)

	// Commit explicitly commits the offset for a consumer group.
	// Required for at-least-once delivery semantics.
	Commit(ctx context.Context, topic string, consumerGroup string, offset int64) error

	// Health returns the health status of the message bus connection.
	Health(ctx context.Context) (*HealthStatus, error)
}

// Message represents a message in the messaging system.
type Message struct {
	// Headers contain metadata about the message
	Headers map[string]string

	// Key is used for partition assignment (Kafka) or routing (RabbitMQ)
	Key string

	// Value is the message payload
	Value []byte

	// Timestamp when the message was produced
	Timestamp time.Time

	// Partition assignment (Kafka-specific, -1 for auto)
	Partition int32

	// Offset in the partition (Kafka-specific, set by broker)
	Offset int64

	// Topic the message belongs to
	Topic string

	// Attributes for extended metadata
	Attributes map[string]interface{}
}

// TopicConfig specifies topic creation parameters.
type TopicConfig struct {
	// Partitions defines the number of partitions (Kafka)
	Partitions int32

	// ReplicationFactor defines replication factor (Kafka)
	ReplicationFactor int16

	// RetentionMs defines message retention time in milliseconds
	RetentionMs int64

	// CleanupPolicy determines retention behavior ("delete" or "compact")
	CleanupPolicy string

	// MinInSyncReplicas minimum replicas that must acknowledge writes
	MinInSyncReplicas int16

	// Compression defines compression codec ("none", "gzip", "snappy", "lz4")
	Compression string

	// MaxMessageBytes maximum size of a single message
	MaxMessageBytes int32

	// CustomConfig for backend-specific settings
	CustomConfig map[string]string
}

// HealthStatus describes the health of the message bus connection.
type HealthStatus struct {
	// Connected indicates if connected to the broker
	Connected bool

	// Brokers lists reachable broker addresses
	Brokers []string

	// Latency recent operation latency metrics
	Latency LatencyMetrics

	// Topics number of accessible topics
	Topics int

	// LastError most recent error encountered
	LastError string

	// LastCheck when health was last evaluated
	LastCheck time.Time
}

// LatencyMetrics captures timing statistics.
type LatencyMetrics struct {
	// ProduceP50 median produce latency
	ProduceP50 time.Duration

	// ProduceP99 99th percentile produce latency
	ProduceP99 time.Duration

	// ConsumeP50 median consume latency
	ConsumeP50 time.Duration

	// ConsumeP99 99th percentile consume latency
	ConsumeP99 time.Duration
}

// BusConfig holds configuration for creating a MessageBus instance.
type BusConfig struct {
	// Backend specifies the messaging system ("kafka", "rabbitmq", "nats")
	Backend string

	// Brokers lists broker addresses
	Brokers []string

	// ClientID identifies this client
	ClientID string

	// Authentication credentials
	Auth AuthConfig

	// TLS configuration
	TLS TLSConfig

	// ProducerConfig for publish operations
	ProducerConfig ProducerConfig

	// ConsumerConfig for subscribe operations
	ConsumerConfig ConsumerConfig

	// Timeouts for various operations
	Timeouts TimeoutConfig
}

// AuthConfig specifies authentication parameters.
type AuthConfig struct {
	// Mechanism defines the auth mechanism ("PLAIN", "SCRAM-SHA-256", "SASL")
	Mechanism string

	// Username for authentication
	Username string

	// Password for authentication
	Password string

	// Token for token-based auth
	Token string
}

// TLSConfig specifies TLS/SSL parameters.
type TLSConfig struct {
	// Enabled turns on TLS
	Enabled bool

	// CACert path to CA certificate
	CACert string

	// ClientCert path to client certificate
	ClientCert string

	// ClientKey path to client private key
	ClientKey string

	// InsecureSkipVerify skips certificate validation (DANGER)
	InsecureSkipVerify bool
}

// ProducerConfig specifies producer behavior.
type ProducerConfig struct {
	// Acks determines acknowledgment behavior
	// -1 = all replicas, 0 = no ack, 1 = leader only
	Acks int16

	// CompressionType ("none", "gzip", "snappy", "lz4", "zstd")
	CompressionType string

	// MaxRetries for transient failures
	MaxRetries int

	// RetryBackoff delay between retries
	RetryBackoff time.Duration

	// Idempotent enables idempotent producer mode
	Idempotent bool

	// BatchSize maximum bytes per batch
	BatchSize int

	// LingerMs time to wait for batching
	LingerMs int

	// BufferMemory total memory for buffering
	BufferMemory int64
}

// ConsumerConfig specifies consumer behavior.
type ConsumerConfig struct {
	// GroupID identifies the consumer group
	GroupID string

	// AutoCommit enables automatic offset commits
	AutoCommit bool

	// AutoCommitInterval commit interval when AutoCommit is true
	AutoCommitInterval time.Duration

	// SessionTimeout maximum time between heartbeats
	SessionTimeout time.Duration

	// RebalanceTimeout maximum time for rebalance
	RebalanceTimeout time.Duration

	// FetchMinBytes minimum bytes per fetch request
	FetchMinBytes int32

	// FetchMaxBytes maximum bytes per fetch request
	FetchMaxBytes int32

	// MaxPollRecords maximum records per poll
	MaxPollRecords int

	// IsolationLevel ("read_uncommitted" or "read_committed")
	IsolationLevel string

	// OffsetReset behavior when no offset exists ("earliest", "latest")
	OffsetReset string
}

// TimeoutConfig specifies operation timeouts.
type TimeoutConfig struct {
	// Connect timeout for establishing connection
	Connect time.Duration

	// Publish timeout for produce operations
	Publish time.Duration

	// Subscribe timeout for consumer operations
	Subscribe time.Duration

	// Health timeout for health checks
	Health time.Duration
}

// MessageBusFactory creates MessageBus instances.
type MessageBusFactory interface {
	// Create instantiates a message bus with the given configuration
	Create(ctx context.Context, config BusConfig) (MessageBus, error)

	// SupportedBackends returns available messaging backends
	SupportedBackends() []string
}

// MessageHandler processes messages from a subscription.
// Used for callback-based consumption patterns.
type MessageHandler func(ctx context.Context, msg *Message) error

// ConsumerGroup represents a managed consumer group.
// Provides higher-level abstractions over raw subscriptions.
type ConsumerGroup interface {
	// Start begins consuming messages
	Start(ctx context.Context) error

	// Stop gracefully shuts down the consumer
	Stop(ctx context.Context) error

	// RegisterHandler sets the message processing function
	RegisterHandler(handler MessageHandler)

	// Pause pauses message consumption
	Pause(ctx context.Context, topics []string) error

	// Resume resumes message consumption
	Resume(ctx context.Context, topics []string) error

	// Committed returns committed offsets for topics
	Committed(ctx context.Context, topics []string) (map[string]int64, error)
}
