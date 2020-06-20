package monitor

import (
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
)

const (
	// Api
	ErrAuth              = "error_authorization"
	ErrPersistence       = "error_persistence"
	WarnPersistence      = "warn_persistence"
	Ingress              = "ingress"
	Latency              = "latency"
	StreamLatency        = "stream_latency"
	BatchSize            = "batch_size"
	BatchBytes           = "batch_bytes"
	WarnAuth             = "warn_authorization"
	WarnDeduplication    = "warn_deduplication"
	WarnDelay            = "warn_delay"
	WarnMissingMessageID = "warn_missing_messageid"
	WarnParse            = "warn_parse"
	WarnSuspension       = "warn_suspension"
	WarnThrottled        = "warn_throttled"
	WarnUnauthorized     = "warn_unauthorized"
	WarnValidation       = "warn_validation"
	WarnInvalidWorkspace = "warn_invalid_workspace"

	// Router
	ErrAuthFailure                   = "error_authorization_failure"
	ErrDedupeFailure                 = "error_deduplication_failure"
	WarnDestinationAbsorbtionFailure = "warn_fallback_absorbtion_failure"
	ErrDestinationAbsorbtionFailure  = "error_fallback_absorbtion_failure"
	Egress                           = "egress"
	EgressUnits                      = "egress_units"
	EgressAbsorbed                   = "egress_absorbed"
	EgressBytes                      = "egress_bytes"
	EgressBytesAbsorbed              = "egress_bytes_absorbed"
	BatchSizeRetrieval               = "batch_size_retrieval"
	WarnRetryBatchFailure            = "warn_retry_batch_failure"
	WarnRetryEventFailure            = "warn_retry_event_failure"

	// Destination
	WarnDestinationS3BatchFailure = "warn_destination_s3_failure"
	WarnDestinationS3EventFailure = "warn_destination_s3_failures"
	WarnRetryS3BatchFailure       = "warn_destination_s3_failure"
	WarnRetryS3EventFailure       = "warn_destination_s3_failures"

	// Service
	ErrBatchDedupeCommitFailure   = "error_batch_deduplication_commit_failure"
	ErrBatchDedupeLockFailure     = "error_batch_deduplication_lock_failure"
	ErrBatchDedupeLockRawFailure  = "error_batch_deduplication_lock_raw_failure"
	ErrBatchDedupeRollbackFailure = "error_batch_deduplication_rollback_failure"
	ErrDedupeCommitFailure        = "error_deduplication_commit_failure"
	ErrDedupeLockFailure          = "error_deduplication_lock_failure"
	ErrDedupeRollbackFailure      = "error_deduplication_rollback_failure"
	InfoDedupeDetected            = "info_duplicate_detected"

	ErrWorkspaceHistoryUpdate    = "error_workspace_history_update"
	ErrWorkspaceHistoryRetrieval = "error_workspace_history_retrieval"

	DestinationLatency = "destination_latency"

	// Breakers
	BreakerEventStream = "breaker_event_stream"
	BreakerDestination = "breaker_destination"

	// Dedupe
	DedupeLockLatency   = "lock_latency"
	DedupeLockBatchSize = "lock_batch_size"
	DedupeDuplicates    = "duplicates"
	DedupeScanned       = "scanned"
	DedupeDeleted       = "deleted"

	// Web
	Hits    = "hits"
	Misses  = "misses"
	Stales  = "stales"
	Backend = "backend"
	Errors  = "errors"

	WebAuthFailed                  = "warn_auth_failed"
	WebAuthUnavailable             = "error_auth_unavailable"
	WebAuthVerificationUnavailable = "error_verification_unavailable"
	WebAuthSessionCookieMissing    = "warn_session_cookie_missing"
)

type Monitor interface {
	Count(name, err string, labels ...string)
	CountN(name, err string, n int, labels ...string)
	Histogram(name string, v float64, labels ...string)
	Summary(name string, v float64, labels ...string)
	AgentStart()
	AgentStop()
}

type monitor struct {
	monitors []Monitor
}

// New returns a new monitor based on configuration
func New(app string, l log.Logger, cfg config.Monitor) (m *monitor) {
	m = &monitor{}
	if cfg.Prometheus != nil {
		m.monitors = append(m.monitors, NewPrometheus(app, l, cfg.Prometheus))
	}
	if cfg.Log != nil {
		m.monitors = append(m.monitors, &logger{app, l})
	}
	return
}

// Count increments a counter
func (m *monitor) Count(name, err string, labels ...string) {
	for _, m := range m.monitors {
		m.Count(name, err, labels...)
	}
}

// CountN increments a counter by N
func (m *monitor) CountN(name, err string, n int, labels ...string) {
	for _, m := range m.monitors {
		m.CountN(name, err, n, labels...)
	}
}

// Histogram observes a value for a histogram
func (m *monitor) Histogram(name string, v float64, labels ...string) {
	for _, m := range m.monitors {
		m.Histogram(name, v, labels...)
	}
}

// Summary observes a value for a summary
func (m *monitor) Summary(name string, v float64, labels ...string) {
	for _, m := range m.monitors {
		m.Summary(name, v, labels...)
	}
}

// AgentStart starts the monitor's agent (if required)
func (m *monitor) AgentStart() {
	for _, m := range m.monitors {
		m.AgentStart()
	}
}

// AgentStop stops the monitor's agent (if required)
func (m *monitor) AgentStop() {
	for _, m := range m.monitors {
		m.AgentStop()
	}
}
