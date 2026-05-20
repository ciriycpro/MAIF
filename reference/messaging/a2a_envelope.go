package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// A2AEnvelope represents a message envelope conforming to the A2A protocol.
// It wraps messages exchanged between agents with metadata for routing,
// security, and reliability.
//
// The envelope format supports:
// - Agent-to-agent communication
// - Request-response patterns
// - Publish-subscribe patterns
// - Distributed tracing
// - Message authentication and encryption
type A2AEnvelope struct {
	// Header contains routing and metadata
	Header A2AHeader `json:"header"`

	// Payload is the actual message content
	Payload json.RawMessage `json:"payload"`

	// Security contains authentication and encryption data
	Security *A2ASecurity `json:"security,omitempty"`

	// Trace contains distributed tracing information
	Trace *A2ATrace `json:"trace,omitempty"`
}

// A2AHeader contains message routing and metadata.
type A2AHeader struct {
	// MessageID uniquely identifies this message
	MessageID string `json:"message_id"`

	// CorrelationID links related messages (for request-response)
	CorrelationID string `json:"correlation_id,omitempty"`

	// CausationID identifies the message that caused this message
	CausationID string `json:"causation_id,omitempty"`

	// ConversationID groups messages in a conversation
	ConversationID string `json:"conversation_id,omitempty"`

	// From identifies the sender agent
	From A2AAgent `json:"from"`

	// To identifies the recipient agent(s)
	To []A2AAgent `json:"to"`

	// ReplyTo specifies where replies should be sent
	ReplyTo *A2AAgent `json:"reply_to,omitempty"`

	// Type categorizes the message
	Type string `json:"type"`

	// Subject describes the message purpose
	Subject string `json:"subject,omitempty"`

	// Priority affects message processing order (0 = highest)
	Priority int `json:"priority,omitempty"`

	// Timestamp when the message was created
	Timestamp time.Time `json:"timestamp"`

	// Expiration when the message expires
	Expiration *time.Time `json:"expiration,omitempty"`

	// Version A2A protocol version
	Version string `json:"version"`

	// Metadata arbitrary key-value pairs
	Metadata map[string]interface{} `json:"metadata,omitempty"`

	// ContentType MIME type of the payload
	ContentType string `json:"content_type"`

	// ContentEncoding payload encoding ("gzip", "deflate", "br")
	ContentEncoding string `json:"content_encoding,omitempty"`

	// ContentLength payload size in bytes
	ContentLength int64 `json:"content_length,omitempty"`
}

// A2AAgent represents an agent in the A2A protocol.
type A2AAgent struct {
	// ID agent unique identifier
	ID string `json:"id"`

	// Name agent name
	Name string `json:"name,omitempty"`

	// Type agent type
	Type string `json:"type,omitempty"`

	// Address agent endpoint address
	Address string `json:"address,omitempty"`

	// Metadata additional agent information
	Metadata map[string]string `json:"metadata,omitempty"`
}

// A2ASecurity contains security-related information.
type A2ASecurity struct {
	// Signature digital signature of the message
	Signature string `json:"signature,omitempty"`

	// SignatureAlgorithm algorithm used for signing
	SignatureAlgorithm string `json:"signature_algorithm,omitempty"`

	// PublicKey sender's public key
	PublicKey string `json:"public_key,omitempty"`

	// Certificate X.509 certificate
	Certificate string `json:"certificate,omitempty"`

	// Encrypted indicates if payload is encrypted
	Encrypted bool `json:"encrypted"`

	// EncryptionAlgorithm algorithm used for encryption
	EncryptionAlgorithm string `json:"encryption_algorithm,omitempty"`

	// KeyID identifies the encryption key
	KeyID string `json:"key_id,omitempty"`

	// AccessControl defines access permissions
	AccessControl *A2AAccessControl `json:"access_control,omitempty"`
}

// A2AAccessControl defines message access permissions.
type A2AAccessControl struct {
	// AllowedAgents agents permitted to process this message
	AllowedAgents []string `json:"allowed_agents,omitempty"`

	// DeniedAgents agents explicitly denied
	DeniedAgents []string `json:"denied_agents,omitempty"`

	// RequiredCapabilities capabilities needed to process
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`

	// MinTrustLevel minimum trust score required
	MinTrustLevel float64 `json:"min_trust_level,omitempty"`
}

// A2ATrace contains distributed tracing information.
type A2ATrace struct {
	// TraceID identifies the entire trace
	TraceID string `json:"trace_id"`

	// SpanID identifies this span
	SpanID string `json:"span_id"`

	// ParentSpanID identifies the parent span
	ParentSpanID string `json:"parent_span_id,omitempty"`

	// SamplingRate probability this trace is sampled
	SamplingRate float64 `json:"sampling_rate,omitempty"`

	// Baggage arbitrary trace context
	Baggage map[string]string `json:"baggage,omitempty"`

	// Hops number of agents this message passed through
	Hops int `json:"hops,omitempty"`

	// Path agents in the message path
	Path []string `json:"path,omitempty"`
}

// NewA2AEnvelope creates a new A2A envelope with defaults.
func NewA2AEnvelope(from, to A2AAgent, msgType string, payload interface{}) (*A2AEnvelope, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	return &A2AEnvelope{
		Header: A2AHeader{
			MessageID:     uuid.New().String(),
			From:          from,
			To:            []A2AAgent{to},
			Type:          msgType,
			Timestamp:     time.Now(),
			Version:       "1.0",
			ContentType:   "application/json",
			ContentLength: int64(len(payloadBytes)),
			Priority:      5, // medium priority
		},
		Payload: payloadBytes,
	}, nil
}

// ToJSON serializes the envelope to JSON.
func (e *A2AEnvelope) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON deserializes an envelope from JSON.
func FromJSON(data []byte) (*A2AEnvelope, error) {
	var envelope A2AEnvelope
	err := json.Unmarshal(data, &envelope)
	return &envelope, err
}

// Validate checks if the envelope is valid.
func (e *A2AEnvelope) Validate() error {
	if e.Header.MessageID == "" {
		return &A2AProtocolError{Code: "MISSING_MESSAGE_ID", Message: "message ID is required"}
	}
	if e.Header.From.ID == "" {
		return &A2AProtocolError{Code: "MISSING_SENDER", Message: "sender ID is required"}
	}
	if len(e.Header.To) == 0 {
		return &A2AProtocolError{Code: "MISSING_RECIPIENT", Message: "at least one recipient is required"}
	}
	if e.Header.Type == "" {
		return &A2AProtocolError{Code: "MISSING_TYPE", Message: "message type is required"}
	}
	if e.Header.Version == "" {
		return &A2AProtocolError{Code: "MISSING_VERSION", Message: "protocol version is required"}
	}
	return nil
}

// IsExpired checks if the message has expired.
func (e *A2AEnvelope) IsExpired() bool {
	if e.Header.Expiration == nil {
		return false
	}
	return time.Now().After(*e.Header.Expiration)
}

// IsReply checks if this is a reply message.
func (e *A2AEnvelope) IsReply() bool {
	return e.Header.CorrelationID != ""
}

// WithCorrelation sets the correlation ID.
func (e *A2AEnvelope) WithCorrelation(correlationID string) *A2AEnvelope {
	e.Header.CorrelationID = correlationID
	return e
}

// WithTrace adds tracing information.
func (e *A2AEnvelope) WithTrace(traceID, spanID, parentSpanID string) *A2AEnvelope {
	e.Trace = &A2ATrace{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Hops:         0,
		Path:         []string{e.Header.From.ID},
	}
	return e
}

// WithSecurity adds security information.
func (e *A2AEnvelope) WithSecurity(security *A2ASecurity) *A2AEnvelope {
	e.Security = security
	return e
}

// IncrementHop increments the hop counter and updates the path.
func (e *A2AEnvelope) IncrementHop(agentID string) {
	if e.Trace != nil {
		e.Trace.Hops++
		e.Trace.Path = append(e.Trace.Path, agentID)
	}
}

// CreateReply creates a reply envelope to this message.
func (e *A2AEnvelope) CreateReply(from A2AAgent, payload interface{}) (*A2AEnvelope, error) {
	reply, err := NewA2AEnvelope(from, e.Header.From, "reply", payload)
	if err != nil {
		return nil, err
	}

	reply.Header.CorrelationID = e.Header.MessageID
	reply.Header.CausationID = e.Header.MessageID

	if e.Header.ConversationID != "" {
		reply.Header.ConversationID = e.Header.ConversationID
	} else {
		reply.Header.ConversationID = e.Header.MessageID
	}

	if e.Trace != nil {
		reply.Trace = &A2ATrace{
			TraceID:      e.Trace.TraceID,
			SpanID:       uuid.New().String(),
			ParentSpanID: e.Trace.SpanID,
			SamplingRate: e.Trace.SamplingRate,
			Baggage:      e.Trace.Baggage,
			Hops:         e.Trace.Hops + 1,
			Path:         append(append([]string{}, e.Trace.Path...), from.ID),
		}
	}

	return reply, nil
}

// A2AProtocolError represents an A2A protocol error.
type A2AProtocolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func (e *A2AProtocolError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// EnvelopeBuilder provides a fluent interface for building A2A envelopes.
type EnvelopeBuilder struct {
	envelope *A2AEnvelope
	err      error
}

// NewEnvelopeBuilder creates a new builder.
func NewEnvelopeBuilder() *EnvelopeBuilder {
	return &EnvelopeBuilder{
		envelope: &A2AEnvelope{
			Header: A2AHeader{
				MessageID:   uuid.New().String(),
				Timestamp:   time.Now(),
				Version:     "1.0",
				ContentType: "application/json",
				Priority:    5,
			},
		},
	}
}

// From sets the sender.
func (b *EnvelopeBuilder) From(agent A2AAgent) *EnvelopeBuilder {
	b.envelope.Header.From = agent
	return b
}

// To sets recipients.
func (b *EnvelopeBuilder) To(agents ...A2AAgent) *EnvelopeBuilder {
	b.envelope.Header.To = agents
	return b
}

// Type sets the message type.
func (b *EnvelopeBuilder) Type(msgType string) *EnvelopeBuilder {
	b.envelope.Header.Type = msgType
	return b
}

// Payload sets the message payload.
func (b *EnvelopeBuilder) Payload(payload interface{}) *EnvelopeBuilder {
	data, err := json.Marshal(payload)
	if err != nil {
		b.err = err
		return b
	}
	b.envelope.Payload = data
	b.envelope.Header.ContentLength = int64(len(data))
	return b
}

// Priority sets the message priority.
func (b *EnvelopeBuilder) Priority(priority int) *EnvelopeBuilder {
	b.envelope.Header.Priority = priority
	return b
}

// Expiration sets the expiration time.
func (b *EnvelopeBuilder) Expiration(expiration time.Time) *EnvelopeBuilder {
	b.envelope.Header.Expiration = &expiration
	return b
}

// Metadata adds metadata key-value pairs.
func (b *EnvelopeBuilder) Metadata(key string, value interface{}) *EnvelopeBuilder {
	if b.envelope.Header.Metadata == nil {
		b.envelope.Header.Metadata = make(map[string]interface{})
	}
	b.envelope.Header.Metadata[key] = value
	return b
}

// Build finalizes the envelope.
func (b *EnvelopeBuilder) Build() (*A2AEnvelope, error) {
	if b.err != nil {
		return nil, b.err
	}
	if err := b.envelope.Validate(); err != nil {
		return nil, err
	}
	return b.envelope, nil
}
