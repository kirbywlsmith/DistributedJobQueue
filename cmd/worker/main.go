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

	amqp "github.com/rabbitmq/amqp091-go"

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

