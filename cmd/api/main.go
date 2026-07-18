package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/queue"
	"github.com/kirbywlsmith/DistributedJobQueue/internal/storage"
)

type server struct {
	store *storage.Store
	pub   *queue.Publisher
	log   *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dsn := envOr("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable")
	amqpURL := envOr("AMQP_URL", "amqp://jobqueue:jobqueue@localhost:5672/")
	addr := envOr("HTTP_ADDR", ":8080")

	ctx := context.Background()

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

	s := &server{store: store, pub: pub, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handleCreateJob)
	mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)

	log.Info("api listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("http server", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// createJobRequest is the POST /jobs body.
type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage(`{}`)
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 5
	}

	job, err := s.store.CreateJob(r.Context(), req.Type, req.Payload, req.MaxAttempts)
	if err != nil {
		s.log.Error("create job", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	if err := s.pub.PublishJobID(r.Context(), job.ID.String()); err != nil {
		s.log.Error("publish job id", "job_id", job.ID, "err", err)
	}

	writeJSON(w, http.StatusCreated, job)
}

func (s *server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	rawId := r.PathValue("id");
	id, err := uuid.Parse(rawId)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid uuid")
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if err == storage.ErrNotFound {
		writeError(w, http.StatusNotFound, "id not found")
		return;
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "error occurred while retrieving job")
		return;
	}

	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
