package monitor

import (
	"context"
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promversion "github.com/prometheus/common/version"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/version"
)

var prometheusCounters map[string]*prom.CounterVec
var prometheusHistograms map[string]*prom.HistogramVec
var prometheusSummaries map[string]*prom.SummaryVec
var prometheusGauges map[string]*prom.GaugeVec
var prometheusServer *http.Server

type prometheus struct {
	log        log.Logger
	port       string
	counters   map[string]*prom.CounterVec
	histograms map[string]*prom.HistogramVec
	summaries  map[string]*prom.SummaryVec
	gauges     map[string]*prom.GaugeVec
}

func NewPrometheus(app string, logger log.Logger, cfg *config.Prometheus) *prometheus {
	constLabels := map[string]string{
		"app": app,
	}
	newCounter := func(name, help, namespace, subsystem string, labelNames []string) *prom.CounterVec {
		return promauto.NewCounterVec(
			prom.CounterOpts{
				Subsystem:   subsystem,
				Namespace:   namespace,
				Name:        name,
				Help:        "Number of " + help + ".",
				ConstLabels: constLabels,
			},
			labelNames,
		)
	}
	newGauge := func(name, help, namespace, subsystem string, labelNames []string) *prom.GaugeVec {
		return promauto.NewGaugeVec(
			prom.GaugeOpts{
				Subsystem:   subsystem,
				Namespace:   namespace,
				Name:        name,
				Help:        help,
				ConstLabels: constLabels,
			},
			labelNames,
		)
	}
	/*
		newHistogram := func(name, namespace, subsystem string, labelNames []string, buckets []float64) *prom.HistogramVec {
			return promauto.NewHistogramVec(
				prom.HistogramOpts{
					Subsystem: subsystem,
					Namespace: namespace,
					Name:      name,
					Buckets:   buckets,
				},
				labelNames,
			)
		}
	*/
	newSummary := func(name, namespace, subsystem string, labelNames []string, objectives map[float64]float64) *prom.SummaryVec {
		return promauto.NewSummaryVec(
			prom.SummaryOpts{
				Subsystem:   subsystem,
				Namespace:   namespace,
				Name:        name,
				Objectives:  objectives,
				ConstLabels: constLabels,
			},
			labelNames,
		)
	}

	if len(prometheusCounters) == 0 {
		prometheusCounters = map[string]*prom.CounterVec{}

		for k, v := range map[string]string{
			ErrAuth:              "event authorization errors by workspace and writeKey",
			WarnAuth:             "event authorization warnings by workspace and writeKey",
			WarnParse:            "event parse errors by workspace and writeKey",
			ErrPersistence:       "event persistence errors by workspace and writeKey",
			WarnSuspension:       "event suspension errors by workspace and writeKey",
			WarnThrottled:        "event throttle errors by workspace and writeKey",
			WarnValidation:       "event validation errors by workspace and writeKey",
			Ingress:              "events successfully processed by workspace and writeKey",
			WarnDeduplication:    "event deduplication warnings by workspace and writeKey",
			WarnDelay:            "event delay warnings by workspace and writeKey",
			WarnMissingMessageID: "events missing messageid by workspace and writeKey",
			WarnInvalidWorkspace: "events with invalid workspace",
		} {
			prometheusCounters[k] = newCounter(k, v, "api", "event", []string{"workspaceId", "writeKey"})
		}

		for k, v := range map[string]string{
			WarnPersistence: "event persistence warnings by workspace and writeKey",
		} {
			prometheusCounters[k] = newCounter(k, v, "api", "event", []string{"streamType"})
		}

		for k, v := range map[string]string{
			ErrDedupeLockFailure:          "dedupe lock failures",
			ErrDedupeRollbackFailure:      "dedupe rollback failures",
			ErrDedupeCommitFailure:        "dedupe commit failures",
			ErrBatchDedupeLockFailure:     "batch dedupe lock failures",
			ErrBatchDedupeRollbackFailure: "batch dedupe rollback failures",
			ErrBatchDedupeCommitFailure:   "batch dedupe commit failures",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "deduplication", nil)
		}

		for k, v := range map[string]string{
			InfoDedupeDetected: "duplicate events detected",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "deduplication", []string{"workspaceId", "writeKey"})
		}

		for k, v := range map[string]string{
			ErrWorkspaceHistoryUpdate:    "workspace history update failure",
			ErrWorkspaceHistoryRetrieval: "workspace history retrieval failure",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "web", []string{"workspaceId"})
		}

		for k, v := range map[string]string{
			WarnDestinationS3BatchFailure: "S3 destination batch failures",
			WarnDestinationS3EventFailure: "S3 destination event failures",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "destination", []string{"workspaceId", "destId", "destType"})
		}

		for k, v := range map[string]string{
			WarnDestinationAbsorbtionFailure: "destination absorbtion soft failures",
			ErrDestinationAbsorbtionFailure:  "destination absorbtion hard failures",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "destination", []string{"destType"})
		}

		for k, v := range map[string]string{
			WarnRetryBatchFailure: "retry batch failures",
			WarnRetryEventFailure: "retry event failures",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "destination", []string{"retryMethod", "destType"})
		}

		for k, v := range map[string]string{
			EgressBytes:         "bytes delivered",
			EgressAbsorbed:      "events absorbed",
			EgressBytesAbsorbed: "event bytes absorbed",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "destination", []string{"workspaceId", "destId", "destType"})
		}

		for k, v := range map[string]string{
			Egress:      "events delivered",
			EgressUnits: "event units delivered",
		} {
			prometheusCounters[k] = newCounter(k, v, "service", "destination", []string{"workspaceId", "destId", "destType", "sourceId"})
		}

		for k, v := range map[string]string{
			Hits:                           "web cache hits",
			Misses:                         "web cache misses",
			Stales:                         "web cache stales",
			Backend:                        "web cache backend requests",
			Errors:                         "web cache errors",
			WebAuthFailed:                  "web auth failure warnings",
			WebAuthUnavailable:             "web auth unavailable errors",
			WebAuthVerificationUnavailable: "web auth verification unavailable errors",
			WebAuthSessionCookieMissing:    "web auth session cookie missing warnings",
		} {
			prometheusCounters[k] = newCounter(k, v, "http", "cache", nil)
		}

		for k, v := range map[string]string{
			DedupeDuplicates: "duplicates",
			DedupeScanned:    "keys scanned",
			DedupeDeleted:    "keys deleted",
		} {
			prometheusCounters[k] = newCounter(k, v, "dedupe", "batch", nil)
		}
	}
	if len(prometheusSummaries) == 0 {
		prometheusSummaries = map[string]*prom.SummaryVec{}
		prometheusSummaries[Latency] = newSummary(Latency, "api", "event", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001})
		prometheusSummaries[StreamLatency] = newSummary(StreamLatency, "api", "event", []string{"streamType"},
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001})
		prometheusSummaries[BatchSize] = newSummary(BatchSize, "api", "event", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001})
		prometheusSummaries[BatchBytes] = newSummary(BatchBytes, "api", "event", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001})
		prometheusSummaries[BatchSizeRetrieval] = newSummary(BatchSizeRetrieval, "router", "event", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001})
		prometheusSummaries[DedupeLockLatency] = newSummary(DedupeLockLatency, "dedupe", "batch", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001})
		prometheusSummaries[DedupeLockBatchSize] = newSummary(DedupeLockBatchSize, "dedupe", "batch", nil,
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001})
		prometheusSummaries[DestinationLatency] = newSummary(Latency, "service", "destination", []string{"destType"},
			map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001})
	}
	if len(prometheusGauges) == 0 {
		prometheusGauges = map[string]*prom.GaugeVec{}
		prometheusGauges[BreakerEventStream] = newGauge(BreakerEventStream, "Event Stream Breaker Open", "api", "event", []string{"stream"})
	}

	promversion.Version = version.Number
	promversion.Revision = version.Hash
	prom.MustRegister(promversion.NewCollector(app))

	return &prometheus{
		port:       cfg.Port,
		log:        logger,
		counters:   prometheusCounters,
		histograms: prometheusHistograms,
		summaries:  prometheusSummaries,
		gauges:     prometheusGauges,
	}
}

func (m *prometheus) Count(name, err string, labels ...string) {
	if counter, ok := m.counters[name]; ok {
		c, err := counter.GetMetricWithLabelValues(labels...)
		if err != nil {
			m.log.Error("Incorrect number of label values " + name)
			return
		}
		c.Inc()
	} else {
		m.log.Warn("Unknown counter " + name)
	}
}

func (m *prometheus) CountN(name, err string, n int, labels ...string) {
	if counter, ok := m.counters[name]; ok {
		c, err := counter.GetMetricWithLabelValues(labels...)
		if err != nil {
			m.log.Error("Incorrect number of label values " + name)
			return
		}
		c.Add(float64(n))
	} else {
		m.log.Warn("Unknown counter " + name)
	}
}

func (m *prometheus) Histogram(name string, v float64, labels ...string) {
	if hist, ok := m.histograms[name]; ok {
		c, err := hist.GetMetricWithLabelValues(labels...)
		if err != nil {
			m.log.Error("Incorrect number of label values " + name)
			return
		}
		c.Observe(v)
	} else {
		m.log.Warn("Unknown histogram " + name)
	}
}

func (m *prometheus) Summary(name string, v float64, labels ...string) {
	if hist, ok := m.summaries[name]; ok {
		c, err := hist.GetMetricWithLabelValues(labels...)
		if err != nil {
			m.log.Error("Incorrect number of label values " + name)
			return
		}
		c.Observe(v)
	} else {
		m.log.Warn("Unknown summary " + name)
	}
}

func (m *prometheus) Gauge(name string, v float64, labels ...string) {
	if gauge, ok := m.gauges[name]; ok {
		c, err := gauge.GetMetricWithLabelValues(labels...)
		if err != nil {
			m.log.Error("Incorrect number of label values " + name)
			return
		}
		c.Set(v)
	} else {
		m.log.Warn("Unknown gauge " + name)
	}
}

func (m *prometheus) AgentStart() {
	if prometheusServer != nil || m.port == "" {
		return
	}
	m.log.Infof("Starting Prometheus on port %s", m.port)
	prometheusServer = &http.Server{Addr: ":" + m.port, Handler: promhttp.Handler()}
	go func() {
		prometheusServer.ListenAndServe()
	}()
}

func (m *prometheus) AgentStop() {
	if prometheusServer == nil {
		return
	}
	prometheusServer.Shutdown(context.Background())
}
