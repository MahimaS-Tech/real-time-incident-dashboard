package store

import (
	"context"
	"sort"
	"sync"

	"incident-dashboard/pkg/model"
)

type MemoryStore struct {
	mu             sync.Mutex
	processed      map[string]string
	dedupeToID     map[string]string
	incidents      map[string]model.Incident
	metrics        Metrics
	subscribers    map[int]chan model.Incident
	nextSubscriber int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		processed:   map[string]string{},
		dedupeToID:  map[string]string{},
		incidents:   map[string]model.Incident{},
		metrics:     Metrics{BySeverity: map[string]int64{}},
		subscribers: map[int]chan model.Incident{},
	}
}

func (s *MemoryStore) UpsertIncident(_ context.Context, event model.IncidentEvent) (model.Incident, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.processed[event.EventID]; ok {
		return s.incidents[id], false, nil
	}
	dedupe := event.DedupeKey()
	id, exists := s.dedupeToID[dedupe]
	if !exists {
		id = model.NewID()
		s.dedupeToID[dedupe] = id
		status := model.StatusOpen
		if event.Severity == model.SeverityResolved {
			status = model.StatusResolved
		}
		incident := model.Incident{
			ID:         id,
			DedupeKey:  dedupe,
			Source:     event.Source,
			ExternalID: event.ExternalID,
			Service:    event.Service,
			Region:     event.Region,
			Signal:     event.Signal,
			Message:    event.Message,
			Severity:   event.Severity,
			Status:     status,
			StatusCode: event.StatusCode,
			LatencyMS:  event.LatencyMS,
			ErrorRate:  event.ErrorRate,
			Count:      1,
			FirstSeen:  event.Timestamp,
			LastSeen:   event.Timestamp,
			Attributes: cloneMap(event.Attributes),
		}
		s.incidents[id] = incident
		if status == model.StatusOpen {
			s.metrics.OpenIncidents++
		}
	} else {
		incident := s.incidents[id]
		previousStatus := incident.Status
		incident.Count++
		incident.LastSeen = event.Timestamp
		incident.Message = event.Message
		incident.StatusCode = event.StatusCode
		incident.LatencyMS = event.LatencyMS
		incident.ErrorRate = event.ErrorRate
		incident.Attributes = cloneMap(event.Attributes)
		if model.SeverityRank(event.Severity) >= model.SeverityRank(incident.Severity) {
			incident.Severity = event.Severity
		}
		if event.Severity == model.SeverityResolved {
			incident.Status = model.StatusResolved
		} else {
			incident.Status = model.StatusOpen
		}
		if previousStatus == model.StatusOpen && incident.Status == model.StatusResolved {
			s.metrics.OpenIncidents--
		}
		if previousStatus == model.StatusResolved && incident.Status == model.StatusOpen {
			s.metrics.OpenIncidents++
		}
		s.incidents[id] = incident
	}
	s.processed[event.EventID] = id
	s.metrics.EventsTotal++
	s.metrics.BySeverity[event.Severity]++
	return s.incidents[id], true, nil
}

func (s *MemoryStore) ListOpenIncidents(_ context.Context, limit int) ([]model.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]model.Incident, 0, len(s.incidents))
	for _, incident := range s.incidents {
		if incident.Status == model.StatusOpen {
			out = append(out, incident)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.After(out[j].LastSeen) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemoryStore) Metrics(_ context.Context) (Metrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySeverity := map[string]int64{}
	for k, v := range s.metrics.BySeverity {
		bySeverity[k] = v
	}
	return Metrics{EventsTotal: s.metrics.EventsTotal, OpenIncidents: s.metrics.OpenIncidents, BySeverity: bySeverity}, nil
}

func (s *MemoryStore) PublishIncident(_ context.Context, incident model.Incident) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- incident:
		default:
		}
	}
	return nil
}

func (s *MemoryStore) SubscribeIncidents(ctx context.Context) (<-chan model.Incident, func() error, error) {
	s.mu.Lock()
	id := s.nextSubscriber
	s.nextSubscriber++
	ch := make(chan model.Incident, 1024)
	s.subscribers[id] = ch
	s.mu.Unlock()

	closeFn := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if existing, ok := s.subscribers[id]; ok {
			delete(s.subscribers, id)
			close(existing)
		}
		return nil
	}
	go func() {
		<-ctx.Done()
		_ = closeFn()
	}()
	return ch, closeFn, nil
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
