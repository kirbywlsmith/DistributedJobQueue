package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kirbywlsmith/DistributedJobQueue/internal/jobs"
)

var ErrNotFound = errors.New("job not found")

type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and verifies the connection with a ping
//
// dsn looks like: postgres://user:pass@host:port/dbname
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

const jobColumns = `id, job_type, payload, status, attempts, max_attempts,
	result, last_error, created_at, updated_at, started_at, finished_at`

// scanJob reads one row into a Job. pgx.Row covers both QueryRow results and collected rows
func scanJob(row pgx.Row) (*jobs.Job, error) {
	var j jobs.Job
	err := row.Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.Result, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan job: %w", err)
	}
	return &j, nil
}

// CreateJob inserts a new queued job and returns the full row (the DB generates the ID and timestamps)
func (s *Store) CreateJob(ctx context.Context, jobType string, payload json.RawMessage, maxAttempts int) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (job_type, payload, max_attempts)
		VALUES ($1, $2, $3)
		RETURNING ` + jobColumns,
		jobType, payload, maxAttempts)
	return scanJob(row)
}

// GetJob fetches a job by ID, returning ErrNotFound if it doesn't exist
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	return scanJob(row)
}

// StartJob atomically transitions a job from queued to running
func (s *Store) StartJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET
			status = 'running',
			attempts = attempts + 1,
		    started_at = now(),
			updated_at = now()
		WHERE id = $1 AND status = 'queued'
		RETURNING `+jobColumns,
		id)
	return scanJob(row)
}

// CompleteJob atomically records a successful result
func (s *Store) CompleteJob(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'completed',
			result = $2,
		    finished_at = now(),
			updated_at = now()
		WHERE id = $1 AND status = 'running'`,
		id, result)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) FailJob(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
// FailJob atomically records a failed attempt
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = CASE
				WHEN attempts < max_attempts THEN 'queued',
				ELSE 'failed'
			END,
			last_error = $2,
		    finished_at = now(),
			updated_at = now()
		WHERE id = $1 AND status = 'running'`,
		id, result)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
