package rules

import "incident-dashboard/pkg/model"

// ApplySeverityRules preserves explicit valid severity values and infers severity otherwise.
func ApplySeverityRules(event model.IncidentEvent) model.IncidentEvent {
	if event.Severity != "" && model.IsValidSeverity(event.Severity) {
		event.Severity = model.NormalizeSeverity(event.Severity)
		return event
	}
	event.Severity = InferSeverity(event)
	return event
}

func InferSeverity(event model.IncidentEvent) string {
	if event.StatusCode >= 500 || event.ErrorRate >= 0.10 || event.LatencyMS >= 5000 {
		return model.SeverityCritical
	}
	if event.StatusCode >= 400 || event.ErrorRate >= 0.02 || event.LatencyMS >= 1500 {
		return model.SeverityWarning
	}
	return model.SeverityInfo
}
