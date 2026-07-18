package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kirbywlsmith/DistributedJobQueue/internal/jobs"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/queue"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/storage"
)

type Handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

// job_type => handler
var handlers = map[string]Handler{
	"sleep": handleSleep,
	"cpu":   handleCPU,
	"flaky": handleFlaky,
}

type worker struct {
	store *storage.Store
	pub   *queue.Publisher
	log   *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

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

	pub, err := queue.NewPublisher(amqpURL)
	if err != nil {
		log.Error("connect rabbitmq", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	deliveries, err := pub.Consume()
	if err != nil {
		log.Error("start consuming", "err", err)
		os.Exit(1)
	}

	w := &worker{store: store, pub: pub, log: log}
	log.Info("worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return
		case d, ok := <-deliveries:
			if !ok { // channel closed (broker connection lost)
				log.Error("delivery channel closed")
				os.Exit(1)
			}
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery processes one message end to end
func (w *worker) handleDelivery(ctx context.Context, d amqp.Delivery) {
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

	handler, exists := handlers[job.Type]
	if !exists {
		logger.Warn("specified handler was not found")
		_ = d.Nack(false, true)
		return
	}

	logger.Info("starting job")

	job, err = w.store.StartJob(ctx, id)
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

	res, err := handler(ctx, job.Payload)
	if err != nil {
		logger.Error("error while running the job", "err", err)

		job, ferr := w.store.FailJob(ctx, id, err.Error())
		if ferr != nil {
			logger.Error("record failure", "err", ferr)
			_ = d.Nack(false, true)
			return
		}
		if job.Status == jobs.StatusQueued {
			err = w.pub.PublishJobID(ctx, id.String())
			if err != nil {
				logger.Error("publish error", "err", ferr)
				_ = d.Nack(false, true)
				return
			}
		}
		
		_ = d.Ack(false)
		return
	} 

	err = w.store.CompleteJob(ctx, id, res)
	if err != nil {
		logger.Error("record completion", "err", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}

// --- test handlers ---

func handleSleep(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Seconds int `json:"seconds"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.Seconds) * time.Second):
		return json.RawMessage(`{"slept_seconds": ` + fmt.Sprint(p.Seconds) + `}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func handleCPU(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Iterations int `json:"iterations"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	if p.Iterations <= 0 {
		p.Iterations = 5_000_000
	}
	sum := sha256.Sum256([]byte("seed"))
	for i := 0; i < p.Iterations; i++ {
		if i%100_000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		sum = sha256.Sum256(sum[:])
	}
	return json.RawMessage(fmt.Sprintf(`{"iterations": %d, "digest": "%x"}`, p.Iterations, sum[:8])), nil
}

func handleFlaky(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		FailureRate float64 `json:"failure_rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	if p.FailureRate == 0 {
		p.FailureRate = 0.5
	}
	if rand.Float64() < p.FailureRate {
		return nil, errors.New("flaky job got unlucky")
	}
	return json.RawMessage(`{"lucky": true}`), nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
