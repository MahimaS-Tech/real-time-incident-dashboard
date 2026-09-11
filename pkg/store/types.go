package store

import (
	"context"

	"incident-dashboard/pkg/model"
)

type Metrics struct {
	EventsTotal   int64            `json:"events_total"`
	OpenIncidents int64            `json:"open_incidents"`
	BySeverity    map[string]int64 `json:"by_severity"`
}

type IncidentStore interface {
	UpsertIncident(ctx context.Context, event model.IncidentEvent) (incident model.Incident, changed bool, err error)
	ListOpenIncidents(ctx context.Context, limit int) ([]model.Incident, error)
	Metrics(ctx context.Context) (Metrics, error)
	PublishIncident(ctx context.Context, incident model.Incident) error
	SubscribeIncidents(ctx context.Context) (<-chan model.Incident, func() error, error)
}
