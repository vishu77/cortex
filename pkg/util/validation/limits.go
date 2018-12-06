package validation

import (
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ingestionRateDesc = prometheus.NewDesc(
		"cortex_limits_ingestion_rate",
		"size of limit to how many samples a user can ingest per second",
		[]string{"user"},
		nil,
	)
	ingestionBurstSize = prometheus.NewDesc(
		"cortex_limits_ingestion_burst_size",
		"Size of ingestion rate limit over short periods of time",
		[]string{"user"},
		nil,
	)
	maxSeriesPerQueryDesc = prometheus.NewDesc(
		"cortex_limits_max_series_per_query",
		"Maximum number of series in a single query",
		[]string{"user"},
		nil,
	)
	maxSamplesPerQueryDesc = prometheus.NewDesc(
		"cortex_limits_max_samples_per_query",
		"Maximum number of samples allowed in a single query",
		[]string{"user"},
		nil,
	)
	maxSeriesPerUserDesc = prometheus.NewDesc(
		"cortex_limits_max_series_per_user",
		"Maximum number of series a user can maintain",
		[]string{"user"},
		nil,
	)
	maxSeriesPerMetricDesc = prometheus.NewDesc(
		"cortex_limits_max_series_per_metric",
		"Maximum number of series allowed for a single metric",
		[]string{"user"},
		nil,
	)
	maxChunksPerQuery = prometheus.NewDesc(
		"cortex_limits_max_chunks_per_query",
		"Maximum number of chunks in a single query",
		[]string{"user"},
		nil,
	)
)

// Limits describe all the limits for users; can be used to describe global default
// limits via flags, or per-user limits via yaml config.
type Limits struct {
	// Distributor enforced limits.
	IngestionRate          float64       `yaml:"ingestion_rate"`
	IngestionBurstSize     int           `yaml:"ingestion_burst_size"`
	MaxLabelNameLength     int           `yaml:"max_label_name_length"`
	MaxLabelValueLength    int           `yaml:"max_label_value_length"`
	MaxLabelNamesPerSeries int           `yaml:"max_label_names_per_series"`
	RejectOldSamples       bool          `yaml:"reject_old_samples"`
	RejectOldSamplesMaxAge time.Duration `yaml:"reject_old_samples_max_age"`
	CreationGracePeriod    time.Duration `yaml:"creation_grace_period"`

	// Ingester enforced limits.
	MaxSeriesPerQuery  int `yaml:"max_series_per_query"`
	MaxSamplesPerQuery int `yaml:"max_samples_per_query"`
	MaxSeriesPerUser   int `yaml:"max_series_per_user"`
	MaxSeriesPerMetric int `yaml:"max_series_per_metric"`

	// Querier enforced limits.
	MaxChunksPerQuery int           `yaml:"max_chunks_per_query"`
	MaxQueryLength    time.Duration `yaml:"max_query_length"`

	// Config for overrides, convenient if it goes here.
	PerTenantOverrideConfig string
	PerTenantOverridePeriod time.Duration
}

// Describe implements prometheus.Collector.
func (l *Limits) Describe(ch chan<- *prometheus.Desc) {
	ch <- ingestionRateDesc
	ch <- ingestionBurstSize
	ch <- maxSeriesPerQueryDesc
	ch <- maxSamplesPerQueryDesc
	ch <- maxSeriesPerUserDesc
	ch <- maxSeriesPerMetricDesc
	ch <- maxChunksPerQuery
}

// Collect implements prometheus.Collector.
func (l *Limits) Collect(user string, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		ingestionRateDesc,
		prometheus.GaugeValue,
		l.IngestionRate,
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		ingestionBurstSize,
		prometheus.GaugeValue,
		float64(l.IngestionBurstSize),
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		maxSeriesPerQueryDesc,
		prometheus.GaugeValue,
		float64(l.MaxSeriesPerQuery),
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		maxSamplesPerQueryDesc,
		prometheus.GaugeValue,
		float64(l.MaxSamplesPerQuery),
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		maxSeriesPerUserDesc,
		prometheus.GaugeValue,
		float64(l.MaxSeriesPerUser),
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		maxSeriesPerMetricDesc,
		prometheus.GaugeValue,
		float64(l.MaxSeriesPerMetric),
		user,
	)
	ch <- prometheus.MustNewConstMetric(
		maxChunksPerQuery,
		prometheus.GaugeValue,
		float64(l.MaxChunksPerQuery),
		user,
	)
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (l *Limits) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&l.IngestionRate, "distributor.ingestion-rate-limit", 25000, "Per-user ingestion rate limit in samples per second.")
	f.IntVar(&l.IngestionBurstSize, "distributor.ingestion-burst-size", 50000, "Per-user allowed ingestion burst size (in number of samples). Warning, very high limits will be reset every -distributor.limiter-reload-period.")
	f.IntVar(&l.MaxLabelNameLength, "validation.max-length-label-name", 1024, "Maximum length accepted for label names")
	f.IntVar(&l.MaxLabelValueLength, "validation.max-length-label-value", 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&l.MaxLabelNamesPerSeries, "validation.max-label-names-per-series", 30, "Maximum number of label names per series.")
	f.BoolVar(&l.RejectOldSamples, "validation.reject-old-samples", false, "Reject old samples.")
	f.DurationVar(&l.RejectOldSamplesMaxAge, "validation.reject-old-samples.max-age", 14*24*time.Hour, "Maximum accepted sample age before rejecting.")
	f.DurationVar(&l.CreationGracePeriod, "validation.create-grace-period", 10*time.Minute, "Duration which table will be created/deleted before/after it's needed; we won't accept sample from before this time.")

	f.IntVar(&l.MaxSeriesPerQuery, "ingester.max-series-per-query", 100000, "The maximum number of series that a query can return.")
	f.IntVar(&l.MaxSamplesPerQuery, "ingester.max-samples-per-query", 1000000, "The maximum number of samples that a query can return.")
	f.IntVar(&l.MaxSeriesPerUser, "ingester.max-series-per-user", 5000000, "Maximum number of active series per user.")
	f.IntVar(&l.MaxSeriesPerMetric, "ingester.max-series-per-metric", 50000, "Maximum number of active series per metric name.")

	f.IntVar(&l.MaxChunksPerQuery, "store.query-chunk-limit", 2e6, "Maximum number of chunks that can be fetched in a single query.")
	f.DurationVar(&l.MaxQueryLength, "store.max-query-length", 0, "Limit to length of chunk store queries, 0 to disable.")

	f.StringVar(&l.PerTenantOverrideConfig, "limits.per-user-override-config", "", "File name of per-user overrides.")
	f.DurationVar(&l.PerTenantOverridePeriod, "limits.per-user-override-period", 10*time.Second, "Period with this to reload the overrides.")
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *Limits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// We want to set c to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.  See prometheus/config.
	flagext.DefaultValues(l)
	type plain Limits
	return unmarshal((*plain)(l))
}
