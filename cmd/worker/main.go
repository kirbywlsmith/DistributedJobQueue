package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kirbywlsmith/DistributedJobQueue/internal/handlers"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/jobs"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/metrics"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/queue"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/storage"
)

const (
	leaseDuration = 60 * time.Second
	leaseRenewEvery = 20 * time.Second
)

type worker struct {
	store *storage.Store
	pub   *queue.Publisher
	log   *slog.Logger
	id    string // identifies this process as the claimant of a job
}

func workerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/" + uuid.NewString()[:8]
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.Handle("GET /metrics", promhttp.Handler())
		if err := http.ListenAndServe(envOr("HEALTH_ADDR", ":8080"), mux); err != nil {
			log.Error("health server", "err", err)
		}
	}()

	dsn := envOr("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable")
	amqpURL := envOr("AMQP_URL", "amqp://jobqueue:jobqueue@localhost:5672/")

	// ctx is cancelled on Ctrl+C / SIGTERM; the consume loop checks it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	pub, err := queue.NewPublisher(amqpURL, log)
	if err != nil {
		log.Error("connect rabbitmq", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	deliveries, err := pub.Consume(ctx)
	if err != nil {
		log.Error("start consuming", "err", err)
		os.Exit(1)
	}

	w := &worker{store: store, pub: pub, log: log, id: workerID()}
	log.Info("worker started", "worker_id", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return
		case d, ok := <-deliveries:
			if !ok {
				log.Info("consumer stopped, shutting down")
				return
			}
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery processes one message end to end
func (w *worker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if ctx.Err() != nil {
		_ = d.Nack(false, true)
		return
	}

	id, err := uuid.Parse(string(d.Body))
	if err != nil {
		w.log.Error("error while parsing uuid", "err", err)
		_ = d.Nack(false, false)
		return
	}

	logger := w.log.With("job_id", id)

	job, err := w.store.GetJob(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		logger.Warn("job not found")
		_ = d.Nack(false, false)
		return
	}
	if err != nil {
		logger.Error("error while getting the job", "err", err)
		_ = d.Nack(false, true)
		return
	}

	handler, exists := handlers.Get(job.Type)
	if !exists {
		logger.Warn("specified handler was not found")
		_ = d.Nack(false, true)
		return
	}

	logger.Info("starting job")

	job, err = w.store.StartJob(ctx, id, w.id, leaseDuration)
	if errors.Is(err, storage.ErrNotFound) {
		logger.Warn("unable to start the job")
		_ = d.Ack(false)
		return
	}
	if err != nil {
		logger.Error("error while starting the job", "err", err)
		_ = d.Nack(false, true)
		return
	}

	logger.Info("running job")

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	go w.renewLease(hbCtx, id, logger)

	start := time.Now()
	res, err := handler(ctx, job.Payload)
	elapsed := time.Since(start)

	stopHeartbeat()

	bookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if ctx.Err() != nil {
		logger.Info("interrupted by shutdown, releasing job")
		if rerr := w.store.ReleaseJob(bookCtx, id, w.id); rerr != nil {
			logger.Error("release job", "err", rerr)
		}
		_ = d.Nack(false, true)
		return
	}

	metrics.JobDuration.WithLabelValues(job.Type).Observe(elapsed.Seconds())

	if err != nil {
		metrics.JobsProcessed.WithLabelValues(job.Type, "failed").Inc()

		logger.Error("error while running the job", "err", err)

		job, ferr := w.store.FailJob(bookCtx, id, w.id, err.Error())
		if errors.Is(ferr, storage.ErrLostClaim) {
			logger.Warn("claim lost before recording failure, dropping")
			_ = d.Ack(false)
			return
		}
		if ferr != nil {
			logger.Error("record failure", "err", ferr)
			_ = d.Nack(false, true)
			return
		}
		if job.Status == jobs.StatusQueued {
			err = w.pub.PublishJobID(bookCtx, id.String())
			if err != nil {
				logger.Error("publish error", "err", err)
				_ = d.Nack(false, true)
				return
			}
		}

		_ = d.Ack(false)
		return
	}

	err = w.store.CompleteJob(bookCtx, id, w.id, res)
	if errors.Is(err, storage.ErrLostClaim) {
		logger.Warn("claim lost before recording completion, discarding result")
		_ = d.Ack(false)
		return
	}
	if err != nil {
		logger.Error("record completion", "err", err)
		_ = d.Nack(false, true)
		return
	}

	metrics.JobsProcessed.WithLabelValues(job.Type, "completed").Inc()

	_ = d.Ack(false)
}

func (w *worker) renewLease(ctx context.Context, id uuid.UUID, logger *slog.Logger) {
	ticker := time.NewTicker(leaseRenewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			err := w.store.RenewLease(rctx, id, w.id, leaseDuration)
			cancel()

			if errors.Is(err, storage.ErrLostClaim) {
				logger.Warn("lease lost, another worker owns this job now")
				return
			}
			if err != nil {
				logger.Warn("renew lease", "err", err)
			}
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
