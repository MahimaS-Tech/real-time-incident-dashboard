package processor

import (
	"context"
	"encoding/json"
	"time"

	"incident-dashboard/pkg/model"
	"incident-dashboard/pkg/rules"
	"incident-dashboard/pkg/store"
)

type Processor struct {
	store store.IncidentStore
}

func New(store store.IncidentStore) *Processor {
	return &Processor{store: store}
}

func (p *Processor) Process(ctx context.Context, payload []byte) error {
	var event model.IncidentEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	event.Normalize(time.Now().UTC())
	if err := event.Validate(); err != nil {
		return err
	}
	event = rules.ApplySeverityRules(event)
	incident, changed, err := p.store.UpsertIncident(ctx, event)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	return p.store.PublishIncident(ctx, incident)
}
