package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type fakeProducer struct {
	mu      sync.Mutex
	keys    []string
	values  [][]byte
	publish error
}

func (f *fakeProducer) Publish(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publish != nil {
		return f.publish
	}
	f.keys = append(f.keys, key)
	f.values = append(f.values, append([]byte(nil), value...))
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func TestCreateIncidentAccepted(t *testing.T) {
	producer := &fakeProducer{}
	server := NewServer(producer)
	req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(`{
		"source":"prometheus",
		"external_id":"abc",
		"service":"checkout",
		"signal":"5xx",
		"message":"errors high"
	}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(producer.values) != 1 {
		t.Fatalf("expected one published message")
	}
	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["event_id"] == "" || response["dedupe_key"] == "" {
		t.Fatalf("missing ids in response: %+v", response)
	}
}

func TestCreateIncidentBadRequest(t *testing.T) {
	server := NewServer(&fakeProducer{})
	req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(`{"source":"x"}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateIncidentProducerFailure(t *testing.T) {
	server := NewServer(&fakeProducer{publish: errors.New("down")})
	req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(`{
		"source":"prometheus",
		"service":"checkout",
		"signal":"5xx",
		"message":"errors high"
	}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
