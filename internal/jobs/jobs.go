package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusScheduled Status = "scheduled"
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID          uuid.UUID       `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Priority    int             `json:"priority"`
	Result      json.RawMessage `json:"result,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`

	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
}

type RunStatus string

const (
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunReleased  RunStatus = "released"
	RunAbandoned RunStatus = "abandoned"
)

// Run is one attempt at a job. Jobs hold current state and overwrite it on
// every retry; runs are the append-only history of what actually happened.
type Run struct {
	ID         int64     `json:"id"`
	JobID      uuid.UUID `json:"job_id"`
	Attempt    int       `json:"attempt"`
	WorkerID   string    `json:"worker_id"`
	Status     RunStatus `json:"status"`
	Error      *string   `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}
