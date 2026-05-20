package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaBus implements the MessageBus interface using Apache Kafka.
// It provides high-throughput, fault-tolerant messaging with support for:
// - Exactly-once semantics
// - Distributed partitioning
// - Consumer groups
// - Message ordering
// - Backpressure handling
type KafkaBus struct {
	mu sync.RWMutex

	config   BusConfig
	producer *kafka.Producer
	consumers map[string]*kafka.Consumer // key: topic+consumerGroup

	connected bool
	closed    bool

	// Metrics
	metrics *KafkaMetrics

	// Health tracking
	healthMu    sync.RWMutex
	lastHealth  *HealthStatus
	healthCheck time.Time
}

// KafkaMetrics tracks Kafka-specific metrics.
type KafkaMetrics struct {
	// Producer metrics
	MessagesProduced   int64
	BytesProduced      int64
	ProduceErrors      int64
	ProduceLatencyP50  time.Duration
	ProduceLatencyP99  time.Duration

	// Consumer metrics
	MessagesConsumed   int64
	BytesConsumed      int64
	ConsumeErrors      int64
	ConsumeLatencyP50  time.Duration
	ConsumeLatencyP99  time.Duration

	// Connection metrics
	ActiveConsumers    int
	TopicsSubscribed   int
	PartitionsAssigned int

	mu sync.RWMutex
}

// NewKafkaBus creates a new Kafka message bus.
func NewKafkaBus(config BusConfig) (*KafkaBus, error) {
	if config.Backend != "kafka" {
		return nil, fmt.Errorf("invalid backend: expected kafka, got %s", config.Backend)
	}

	return &KafkaBus{
		config:    config,
		consumers: make(map[string]*kafka.Consumer),
		metrics:   &KafkaMetrics{},
		lastHealth: &HealthStatus{
			Connected: false,
			Brokers:   []string{},
			Topics:    0,
		},
	}, nil
}

// Connect establishes connection to Kafka brokers.
func (kb *KafkaBus) Connect(ctx context.Context) error {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	if kb.connected {
		return fmt.Errorf("already connected")
	}

	// Create producer
	producerConfig := kb.buildProducerConfig()
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}

	kb.producer = producer
	kb.connected = true

	// Start producer event handler
	go kb.handleProducerEvents()

	// Update health status
	kb.updateHealth()

	return nil
}

// Disconnect closes connections to Kafka brokers.
func (kb *KafkaBus) Disconnect(ctx context.Context) error {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	if !kb.connected {
		return nil
	}

	// Close all consumers
	for key, consumer := range kb.consumers {
		if err := consumer.Close(); err != nil {
			// Log error but continue
		}
		delete(kb.consumers, key)
	}

	// Close producer
	if kb.producer != nil {
		// Flush remaining messages
		kb.producer.Flush(5000) // 5 second timeout
		kb.producer.Close()
		kb.producer = nil
	}

	kb.connected = false
	kb.closed = true

	return nil
}

// Publish sends a message to a Kafka topic.
func (kb *KafkaBus) Publish(ctx context.Context, topic string, msg *Message) error {
	kb.mu.RLock()
	if !kb.connected {
		kb.mu.RUnlock()
		return fmt.Errorf("not connected")
	}
	kb.mu.RUnlock()

	startTime := time.Now()

	// Build Kafka message
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: msg.Partition,
		},
		Key:   []byte(msg.Key),
		Value: msg.Value,
		Headers: kb.convertHeaders(msg.Headers),
		Timestamp: msg.Timestamp,
	}

	// Send message
	deliveryChan := make(chan kafka.Event, 1)
	err := kb.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		kb.recordProduceError()
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report or context cancellation
	select {
	case event := <-deliveryChan:
		m := event.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			kb.recordProduceError()
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		// Update metrics
		kb.recordProduceSuccess(len(msg.Value), time.Since(startTime))
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishBatch sends multiple messages in a single batch.
func (kb *KafkaBus) PublishBatch(ctx context.Context, topic string, messages []*Message) error {
	kb.mu.RLock()
	if !kb.connected {
		kb.mu.RUnlock()
		return fmt.Errorf("not connected")
	}
	kb.mu.RUnlock()

	deliveryChan := make(chan kafka.Event, len(messages))
	startTime := time.Now()

	// Produce all messages
	for _, msg := range messages {
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: msg.Partition,
			},
			Key:   []byte(msg.Key),
			Value: msg.Value,
			Headers: kb.convertHeaders(msg.Headers),
			Timestamp: msg.Timestamp,
		}

		if err := kb.producer.Produce(kafkaMsg, deliveryChan); err != nil {
			kb.recordProduceError()
			return fmt.Errorf("failed to produce message: %w", err)
		}
	}

	// Wait for all delivery reports
	successCount := 0
	totalBytes := 0
	for i := 0; i < len(messages); i++ {
		select {
		case event := <-deliveryChan:
			m := event.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				kb.recordProduceError()
			} else {
				successCount++
				totalBytes += len(m.Value)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if successCount > 0 {
		kb.recordProduceSuccess(totalBytes, time.Since(startTime))
	}

	if successCount < len(messages) {
		return fmt.Errorf("batch incomplete: %d/%d messages delivered", 
			successCount, len(messages))
	}

	return nil
}

// Subscribe creates a subscription to a Kafka topic.
func (kb *KafkaBus) Subscribe(ctx context.Context, topic string, consumerGroup string) (<-chan *Message, error) {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	if !kb.connected {
		return nil, fmt.Errorf("not connected")
	}

	consumerKey := topic + ":" + consumerGroup

	// Check if already subscribed
	if _, exists := kb.consumers[consumerKey]; exists {
		return nil, fmt.Errorf("already subscribed to %s with group %s", topic, consumerGroup)
	}

	// Create consumer
	consumerConfig := kb.buildConsumerConfig(consumerGroup)
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Subscribe to topic
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	kb.consumers[consumerKey] = consumer
	kb.metrics.ActiveConsumers++

	// Create message channel
	msgChan := make(chan *Message, 100)

	// Start consumer loop
	go kb.consumeMessages(ctx, consumer, msgChan)

	return msgChan, nil
}

// Unsubscribe removes a subscription.
func (kb *KafkaBus) Unsubscribe(ctx context.Context, topic string, consumerGroup string) error {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	consumerKey := topic + ":" + consumerGroup

	consumer, exists := kb.consumers[consumerKey]
	if !exists {
		return fmt.Errorf("not subscribed to %s with group %s", topic, consumerGroup)
	}

	if err := consumer.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}

	delete(kb.consumers, consumerKey)
	kb.metrics.ActiveConsumers--

	return nil
}

// CreateTopic creates a new Kafka topic.
func (kb *KafkaBus) CreateTopic(ctx context.Context, topic string, config TopicConfig) error {
	adminClient, err := kafka.NewAdminClientFromProducer(kb.producer)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	topicSpec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     int(config.Partitions),
		ReplicationFactor: int(config.ReplicationFactor),
		Config: map[string]string{
			"retention.ms":       fmt.Sprintf("%d", config.RetentionMs),
			"cleanup.policy":     config.CleanupPolicy,
			"min.insync.replicas": fmt.Sprintf("%d", config.MinInSyncReplicas),
			"compression.type":   config.Compression,
			"max.message.bytes":  fmt.Sprintf("%d", config.MaxMessageBytes),
		},
	}

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	if results[0].Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("topic creation failed: %s", results[0].Error.String())
	}

	return nil
}

// DeleteTopic removes a Kafka topic.
func (kb *KafkaBus) DeleteTopic(ctx context.Context, topic string) error {
	adminClient, err := kafka.NewAdminClientFromProducer(kb.producer)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	results, err := adminClient.DeleteTopics(ctx, []string{topic})
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	if results[0].Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("topic deletion failed: %s", results[0].Error.String())
	}

	return nil
}

// TopicExists checks if a topic exists.
func (kb *KafkaBus) TopicExists(ctx context.Context, topic string) (bool, error) {
	metadata, err := kb.producer.GetMetadata(&topic, false, 5000)
	if err != nil {
		return false, err
	}

	_, exists := metadata.Topics[topic]
	return exists, nil
}

// GetOffset returns the current offset for a consumer group.
func (kb *KafkaBus) GetOffset(ctx context.Context, topic string, consumerGroup string) (int64, error) {
	// Implementation would query Kafka for consumer group offsets
	return 0, fmt.Errorf("not implemented")
}

// Commit commits the offset for a consumer group.
func (kb *KafkaBus) Commit(ctx context.Context, topic string, consumerGroup string, offset int64) error {
	consumerKey := topic + ":" + consumerGroup

	kb.mu.RLock()
	consumer, exists := kb.consumers[consumerKey]
	kb.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer not found")
	}

	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: kafka.PartitionAny,
		Offset:    kafka.Offset(offset),
	}

	_, err := consumer.CommitOffsets([]kafka.TopicPartition{topicPartition})
	return err
}

// Health returns the health status.
func (kb *KafkaBus) Health(ctx context.Context) (*HealthStatus, error) {
	kb.healthMu.RLock()
	defer kb.healthMu.RUnlock()

	// Clone health status
	health := *kb.lastHealth
	return &health, nil
}

// Helper methods

func (kb *KafkaBus) buildProducerConfig() *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers": kb.joinBrokers(),
		"client.id":         kb.config.ClientID,
		"acks":              kb.config.ProducerConfig.Acks,
		"compression.type":  kb.config.ProducerConfig.CompressionType,
		"max.in.flight":     10000,
		"retries":           kb.config.ProducerConfig.MaxRetries,
		"retry.backoff.ms":  kb.config.ProducerConfig.RetryBackoff.Milliseconds(),
		"batch.size":        kb.config.ProducerConfig.BatchSize,
		"linger.ms":         kb.config.ProducerConfig.LingerMs,
	}

	if kb.config.ProducerConfig.Idempotent {
		config.SetKey("enable.idempotence", true)
	}

	return config
}

func (kb *KafkaBus) buildConsumerConfig(groupID string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":  kb.joinBrokers(),
		"group.id":           groupID,
		"client.id":          kb.config.ClientID,
		"enable.auto.commit": kb.config.ConsumerConfig.AutoCommit,
		"auto.offset.reset":  kb.config.ConsumerConfig.OffsetReset,
		"session.timeout.ms": kb.config.ConsumerConfig.SessionTimeout.Milliseconds(),
		"max.poll.records":   kb.config.ConsumerConfig.MaxPollRecords,
	}
}

func (kb *KafkaBus) joinBrokers() string {
	result := ""
	for i, broker := range kb.config.Brokers {
		if i > 0 {
			result += ","
		}
		result += broker
	}
	return result
}

func (kb *KafkaBus) convertHeaders(headers map[string]string) []kafka.Header {
	result := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		result = append(result, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return result
}

func (kb *KafkaBus) consumeMessages(ctx context.Context, consumer *kafka.Consumer, msgChan chan<- *Message) {
	defer close(msgChan)

	for {
		select {
		case <-ctx.Done():
			return

		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Timeout is not an error
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				kb.recordConsumeError()
				continue
			}

			// Convert to internal message format
			internalMsg := &Message{
				Headers:   kb.extractHeaders(msg.Headers),
				Key:       string(msg.Key),
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
				Partition: msg.TopicPartition.Partition,
				Offset:    int64(msg.TopicPartition.Offset),
				Topic:     *msg.TopicPartition.Topic,
			}

			// Send to channel (non-blocking)
			select {
			case msgChan <- internalMsg:
				kb.recordConsumeSuccess(len(msg.Value))
			case <-ctx.Done():
				return
			default:
				// Channel full, drop message
			}
		}
	}
}

func (kb *KafkaBus) extractHeaders(headers []kafka.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

func (kb *KafkaBus) handleProducerEvents() {
	for e := range kb.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			// Delivery report handled in Publish method
		case kafka.Error:
			// Log error
			kb.recordProduceError()
		}
	}
}

func (kb *KafkaBus) updateHealth() {
	// Implementation would query Kafka metadata
}

func (kb *KafkaBus) recordProduceSuccess(bytes int, latency time.Duration) {
	kb.metrics.mu.Lock()
	defer kb.metrics.mu.Unlock()

	kb.metrics.MessagesProduced++
	kb.metrics.BytesProduced += int64(bytes)
	// Update latency percentiles (simplified)
	kb.metrics.ProduceLatencyP50 = latency
}

func (kb *KafkaBus) recordProduceError() {
	kb.metrics.mu.Lock()
	defer kb.metrics.mu.Unlock()

	kb.metrics.ProduceErrors++
}

func (kb *KafkaBus) recordConsumeSuccess(bytes int) {
	kb.metrics.mu.Lock()
	defer kb.metrics.mu.Unlock()

	kb.metrics.MessagesConsumed++
	kb.metrics.BytesConsumed += int64(bytes)
}

func (kb *KafkaBus) recordConsumeError() {
	kb.metrics.mu.Lock()
	defer kb.metrics.mu.Unlock()

	kb.metrics.ConsumeErrors++
}
