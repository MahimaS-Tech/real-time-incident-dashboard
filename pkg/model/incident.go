package model

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
	SeverityResolved = "resolved"
	StatusOpen       = "open"
	StatusResolved   = "resolved"
)

var fallbackCounter uint64

// IncidentEvent is the raw signal accepted by the gateway.
type IncidentEvent struct {
	EventID    string            `json:"event_id,omitempty"`
	Source     string            `json:"source"`
	ExternalID string            `json:"external_id,omitempty"`
	Service    string            `json:"service"`
	Region     string            `json:"region,omitempty"`
	Signal     string            `json:"signal"`
	Message    string            `json:"message"`
	Severity   string            `json:"severity,omitempty"`
	StatusCode int               `json:"status_code,omitempty"`
	LatencyMS  float64           `json:"latency_ms,omitempty"`
	ErrorRate  float64           `json:"error_rate,omitempty"`
	Timestamp  time.Time         `json:"timestamp,omitempty"`
	ReceivedAt time.Time         `json:"received_at,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Incident is the canonical state stored for the dashboard.
type Incident struct {
	ID         string            `json:"id"`
	DedupeKey  string            `json:"dedupe_key"`
	Source     string            `json:"source"`
	ExternalID string            `json:"external_id,omitempty"`
	Service    string            `json:"service"`
	Region     string            `json:"region,omitempty"`
	Signal     string            `json:"signal"`
	Message    string            `json:"message"`
	Severity   string            `json:"severity"`
	Status     string            `json:"status"`
	StatusCode int               `json:"status_code,omitempty"`
	LatencyMS  float64           `json:"latency_ms,omitempty"`
	ErrorRate  float64           `json:"error_rate,omitempty"`
	Count      int64             `json:"count"`
	FirstSeen  time.Time         `json:"first_seen"`
	LastSeen   time.Time         `json:"last_seen"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (e *IncidentEvent) Normalize(now time.Time) {
	e.EventID = strings.TrimSpace(e.EventID)
	e.Source = strings.TrimSpace(e.Source)
	e.ExternalID = strings.TrimSpace(e.ExternalID)
	e.Service = strings.TrimSpace(e.Service)
	e.Region = strings.TrimSpace(e.Region)
	e.Signal = strings.TrimSpace(e.Signal)
	e.Message = strings.TrimSpace(e.Message)
	e.Severity = NormalizeSeverity(e.Severity)
	if e.EventID == "" {
		e.EventID = NewID()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = now.UTC()
	}
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = now.UTC()
	}
}

func (e IncidentEvent) Validate() error {
	if strings.TrimSpace(e.Source) == "" {
		return errors.New("source is required")
	}
	if strings.TrimSpace(e.Service) == "" {
		return errors.New("service is required")
	}
	if strings.TrimSpace(e.Signal) == "" {
		return errors.New("signal is required")
	}
	if strings.TrimSpace(e.Message) == "" {
		return errors.New("message is required")
	}
	if e.ErrorRate < 0 || e.ErrorRate > 1 {
		return errors.New("error_rate must be between 0 and 1")
	}
	if e.LatencyMS < 0 {
		return errors.New("latency_ms cannot be negative")
	}
	if e.Severity != "" && !IsValidSeverity(e.Severity) {
		return fmt.Errorf("invalid severity %q", e.Severity)
	}
	return nil
}

func NormalizeSeverity(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "", "none":
		return ""
	case "warn":
		return SeverityWarning
	case "crit", "fatal", "page":
		return SeverityCritical
	case "ok", "closed":
		return SeverityResolved
	default:
		return value
	}
}

func IsValidSeverity(value string) bool {
	switch NormalizeSeverity(value) {
	case SeverityInfo, SeverityWarning, SeverityCritical, SeverityResolved:
		return true
	default:
		return false
	}
}

func SeverityRank(value string) int {
	switch NormalizeSeverity(value) {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	case SeverityResolved:
		return 0
	default:
		return 1
	}
}

func (e IncidentEvent) DedupeKey() string {
	base := e.Source + "|" + e.ExternalID
	if strings.TrimSpace(e.ExternalID) == "" {
		base = strings.Join([]string{e.Source, e.Service, e.Region, e.Signal, normalizeMessage(e.Message)}, "|")
	}
	sum := sha256.Sum256([]byte(strings.ToLower(base)))
	return hex.EncodeToString(sum[:])
}

func normalizeMessage(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	fields := strings.Fields(value)
	return strings.Join(fields, " ")
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	counter := atomic.AddUint64(&fallbackCounter, 1)
	return fmt.Sprintf("%x-%d", time.Now().UnixNano(), counter)
}
