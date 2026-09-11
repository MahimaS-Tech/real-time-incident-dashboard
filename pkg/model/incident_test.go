package model

import (
	"testing"
	"time"
)

func TestIncidentEventNormalizeAndValidate(t *testing.T) {
	e := IncidentEvent{Source: " prometheus ", Service: " checkout ", Signal: "5xx", Message: "High error rate"}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	e.Normalize(now)
	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid event: %v", err)
	}
	if e.EventID == "" {
		t.Fatal("expected generated event id")
	}
	if !e.Timestamp.Equal(now) || !e.ReceivedAt.Equal(now) {
		t.Fatalf("expected timestamps to be set to now")
	}
}

func TestIncidentEventValidationFails(t *testing.T) {
	e := IncidentEvent{Source: "prometheus", Service: "checkout", Message: "missing signal"}
	e.Normalize(time.Now())
	if err := e.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestDedupeKeyUsesExternalID(t *testing.T) {
	a := IncidentEvent{Source: "prom", ExternalID: "abc", Service: "svc-a", Signal: "x", Message: "m1"}
	b := IncidentEvent{Source: "prom", ExternalID: "abc", Service: "svc-b", Signal: "y", Message: "m2"}
	if a.DedupeKey() != b.DedupeKey() {
		t.Fatal("same source/external_id should dedupe to same key")
	}
}
