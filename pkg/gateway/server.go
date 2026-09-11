package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"incident-dashboard/pkg/bus"
	"incident-dashboard/pkg/model"
)

const defaultMaxBodyBytes = 64 << 10

type Server struct {
	producer bus.Producer
	now      func() time.Time
	maxBody  int64
	stats    Stats
}

type Stats struct {
	Accepted atomic.Int64
	Rejected atomic.Int64
	Failed   atomic.Int64
}

func NewServer(producer bus.Producer) *Server {
	return &Server{
		producer: producer,
		now:      time.Now,
		maxBody:  defaultMaxBodyBytes,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /v1/incidents", s.createIncident)
	return recoverMiddleware(mux)
}

func (s *Server) createIncident(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var event model.IncidentEvent
	if err := decoder.Decode(&event); err != nil {
		s.stats.Rejected.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
		return
	}
	event.Normalize(s.now())
	if err := event.Validate(); err != nil {
		s.stats.Rejected.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		s.stats.Failed.Add(1)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "marshal failed"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.producer.Publish(ctx, event.DedupeKey(), payload); err != nil {
		s.stats.Failed.Add(1)
		slog.Error("failed to publish incident", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingestion unavailable"})
		return
	}
	s.stats.Accepted.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":     "accepted",
		"event_id":   event.EventID,
		"dedupe_key": event.DedupeKey(),
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int64{
		"accepted": s.stats.Accepted.Load(),
		"rejected": s.stats.Rejected.Load(),
		"failed":   s.stats.Failed.Load(),
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("panic recovered", "panic", recovered)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Error("failed writing response", "error", err)
	}
}
