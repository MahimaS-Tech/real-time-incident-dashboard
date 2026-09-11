package rules

import (
	"testing"

	"incident-dashboard/pkg/model"
)

func TestInferCritical(t *testing.T) {
	e := model.IncidentEvent{StatusCode: 503, ErrorRate: 0.01, LatencyMS: 50}
	if got := InferSeverity(e); got != model.SeverityCritical {
		t.Fatalf("expected critical, got %s", got)
	}
}

func TestInferWarning(t *testing.T) {
	e := model.IncidentEvent{StatusCode: 200, ErrorRate: 0.03, LatencyMS: 50}
	if got := InferSeverity(e); got != model.SeverityWarning {
		t.Fatalf("expected warning, got %s", got)
	}
}

func TestExplicitSeverityWins(t *testing.T) {
	e := model.IncidentEvent{Severity: "warning", StatusCode: 503}
	got := ApplySeverityRules(e)
	if got.Severity != model.SeverityWarning {
		t.Fatalf("expected explicit warning, got %s", got.Severity)
	}
}
