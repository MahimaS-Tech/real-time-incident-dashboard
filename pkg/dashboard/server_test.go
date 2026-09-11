package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"incident-dashboard/pkg/model"
	"incident-dashboard/pkg/store"
)

func TestListIncidents(t *testing.T) {
	memory := store.NewMemoryStore()
	event := model.IncidentEvent{
		EventID:   "event-1",
		Source:    "prometheus",
		Service:   "checkout",
		Signal:    "5xx",
		Message:   "high errors",
		Severity:  model.SeverityCritical,
		Timestamp: time.Now().UTC(),
	}
	if _, _, err := memory.UpsertIncident(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	server := NewServer(memory, "../../web")
	req := httptest.NewRequest(http.MethodGet, "/v1/incidents?limit=10", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var response struct {
		Incidents []model.Incident `json:"incidents"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Incidents) != 1 {
		t.Fatalf("expected one incident, got %d", len(response.Incidents))
	}
}

func TestMetrics(t *testing.T) {
	memory := store.NewMemoryStore()
	server := NewServer(memory, "../../web")
	req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
