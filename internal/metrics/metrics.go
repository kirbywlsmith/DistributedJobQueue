package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var JobsProcessed = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "jobqueue_jobs_processed_total",
		Help: "Total jobs processed, by type and outcome.",
	},
	[]string{"type", "status"},
)

var JobDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "jobqueue_job_duration_seconds",
		Help:    "Handler execution time in seconds.",
		Buckets: append(prometheus.DefBuckets, 25, 60, 120),
	},
	[]string{"type"},
)