package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"incident-dashboard/pkg/model"
	"incident-dashboard/pkg/store"
)

func TestProcessorIdempotentEventID(t *testing.T) {
	memory := store.NewMemoryStore()
	p := New(memory)
	event := model.IncidentEvent{
		EventID:   "event-1",
		Source:    "prometheus",
		Service:   "checkout",
		Signal:    "5xx",
		Message:   "error rate high",
		Timestamp: time.Now().UTC(),
		ErrorRate: 0.20,
	}
	payload, _ := json.Marshal(event)
	ctx := context.Background()
	if err := p.Process(ctx, payload); err != nil {
		t.Fatalf("first process failed: %v", err)
	}
	if err := p.Process(ctx, payload); err != nil {
		t.Fatalf("duplicate process failed: %v", err)
	}
	incidents, err := memory.ListOpenIncidents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("expected 1 incident, got %d", len(incidents))
	}
	if incidents[0].Count != 1 {
		t.Fatalf("duplicate event id should not increment count; got %d", incidents[0].Count)
	}
}

func TestProcessorCorrelatesDifferentEventsByExternalID(t *testing.T) {
	memory := store.NewMemoryStore()
	p := New(memory)
	base := model.IncidentEvent{
		Source:     "prometheus",
		ExternalID: "checkout-5xx",
		Service:    "checkout",
		Signal:     "5xx",
		Message:    "error rate high",
		Timestamp:  time.Now().UTC(),
		ErrorRate:  0.20,
	}
	for _, id := range []string{"event-1", "event-2"} {
		e := base
		e.EventID = id
		payload, _ := json.Marshal(e)
		if err := p.Process(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	incidents, _ := memory.ListOpenIncidents(context.Background(), 10)
	if len(incidents) != 1 {
		t.Fatalf("expected correlated incident")
	}
	if incidents[0].Count != 2 {
		t.Fatalf("expected count 2, got %d", incidents[0].Count)
	}
}
