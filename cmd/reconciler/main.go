package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/kirbywlsmith/DistributedJobQueue/internal/queue"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dsn := envOr("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable")
	amqpURL := envOr("AMQP_URL", "amqp://jobqueue:jobqueue@localhost:5672/")

	staleQueued := envDuration("STALE_QUEUED_SECONDS", 2*time.Minute, log)
	limit := envInt("RECONCILE_LIMIT", 100, log)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	rescued, err := store.ReleaseStaleRunning(ctx, limit)
	if err != nil {
		log.Error("release stale running", "err", err)
		os.Exit(1)
	}
	if len(rescued) > 0 {
		log.Info("rescued abandoned jobs", "count", len(rescued))
	}

	promoted, err := store.PromoteDueScheduled(ctx, limit)
	if err != nil {
		log.Error("promote due scheduled", "err", err)
		os.Exit(1)
	}
	if len(promoted) > 0 {
		log.Info("promoted scheduled jobs", "count", len(promoted))
	}

	stale, err := store.FindStaleQueued(ctx, staleQueued, limit)
	if err != nil {
		log.Error("find stale queued", "err", err)
		os.Exit(1)
	}

	republished, failed := 0, 0
	for _, p := range dedupe(rescued, promoted, stale) {
		if err := pub.PublishJobID(ctx, p.ID.String(), p.Priority); err != nil {
			log.Error("republish", "job_id", p.ID, "err", err)
			failed++
			continue
		}
		republished++
	}

	log.Info("reconcile complete",
		"rescued", len(rescued),
		"promoted", len(promoted),
		"stale_queued", len(stale),
		"republished", republished,
		"failed", failed)

	if failed > 0 {
		os.Exit(1)
	}
}

func dedupe(sets ...[]storage.PendingJob) []storage.PendingJob {
	seen := make(map[uuid.UUID]struct{})
	var out []storage.PendingJob
	for _, set := range sets {
		for _, p := range set {
			if _, dup := seen[p.ID]; dup {
				continue
			}
			seen[p.ID] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration, log *slog.Logger) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	secs, err := strconv.Atoi(v)
	if err != nil {
		log.Warn("bad duration env, using default", "key", key, "value", v)
		return fallback
	}
	return time.Duration(secs) * time.Second
}

func envInt(key string, fallback int, log *slog.Logger) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Warn("bad int env, using default", "key", key, "value", v)
		return fallback
	}
	return n
}
