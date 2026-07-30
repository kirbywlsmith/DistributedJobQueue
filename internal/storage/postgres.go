package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const jobColumns = `id, job_type, payload, status, attempts, max_attempts, priority,
	result, last_error, created_at, updated_at, started_at, finished_at, next_attempt_at`

type PendingJob struct {
	ID       uuid.UUID
	Priority int
}

// scanJob reads one row into a Job. pgx.Row covers both QueryRow results and collected rows
func scanJob(row pgx.Row) (*jobs.Job, error) {
	var j jobs.Job
	err := row.Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.Priority,
		&j.Result, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt,
		&j.NextAttemptAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan job: %w", err)
	}
	return &j, nil
}

// CreateJob inserts a new job and returns the full row (the DB generates the ID
// and timestamps). A non-nil runAt holds the job in 'scheduled' until the
// reconciler promotes it; nil queues it for immediate publication.
func (s *Store) CreateJob(ctx context.Context, jobType string, payload json.RawMessage, maxAttempts, priority int, runAt *time.Time) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (job_type, payload, max_attempts, priority, status, next_attempt_at)
		VALUES ($1, $2, $3, $4,
			CASE WHEN $5::timestamptz IS NULL THEN 'queued' ELSE 'scheduled' END::job_status,
			$5)
		RETURNING `+jobColumns,
		jobType, payload, maxAttempts, priority, runAt)
	return scanJob(row)
}

// GetJob fetches a job by ID, returning ErrNotFound if it doesn't exist
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	return scanJob(row)
}

// ErrLostClaim means the job is no longer ours: the lease expired and the
// reconciler handed it to someone else, so our result is stale and must not be
// written. Distinct from ErrNotFound, which means we never had the claim.
var ErrLostClaim = errors.New("claim lost")

// StartJob atomically transitions a job from queued to running and takes a lease
// on it. workerID identifies the claimant; only that claimant may finish the job.
func (s *Store) StartJob(ctx context.Context, id uuid.UUID, workerID string, lease time.Duration) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET
			status = 'running',
			attempts = attempts + 1,
		    started_at = now(),
			claimed_by = $2,
			lease_expires_at = now() + make_interval(secs => $3),
			updated_at = now()
		WHERE id = $1 AND status = 'queued'
		RETURNING `+jobColumns,
		id, workerID, lease.Seconds())
	return scanJob(row)
}

// RenewLease pushes out the lease while the handler is still working, so a long
// job isn't mistaken for an abandoned one. Returns ErrLostClaim if we no longer
// own the job - the caller should stop, since anything it writes would be stale.
func (s *Store) RenewLease(ctx context.Context, id uuid.UUID, workerID string, lease time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			lease_expires_at = now() + make_interval(secs => $3),
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND claimed_by = $2`,
		id, workerID, lease.Seconds())
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLostClaim
	}
	return nil
}

// CompleteJob atomically records a successful result.
//
// The claimed_by check is what stops a worker whose lease expired from
// overwriting the result of whoever picked the job up afterwards.
func (s *Store) CompleteJob(ctx context.Context, id uuid.UUID, workerID string, result json.RawMessage) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'completed',
			result = $3,
		    finished_at = now(),
			claimed_by = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND claimed_by = $2`,
		id, workerID, result)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLostClaim
	}
	return nil
}

// ReleaseJob puts a running job back on the queue without consuming an attempt,
// for when the worker is interrupted rather than the job failing.
func (s *Store) ReleaseJob(ctx context.Context, id uuid.UUID, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'queued',
			attempts = GREATEST(attempts - 1, 0),
			started_at = NULL,
			claimed_by = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND claimed_by = $2`,
		id, workerID)
	if err != nil {
		return fmt.Errorf("release job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLostClaim
	}
	return nil
}

// RequeueJob gives a terminally failed job a fresh attempt budget.
// Returns ErrNotFound if the job doesn't exist or isn't in 'failed'.
func (s *Store) RequeueJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET
			status = 'queued',
			attempts = 0,
			started_at = NULL,
			finished_at = NULL,
			next_attempt_at = NULL,
			claimed_by = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'failed'
		RETURNING `+jobColumns,
		id)
	return scanJob(row)
}

// PromoteDueScheduled moves scheduled jobs whose start time has arrived into
// 'queued' and returns them so the caller can publish them.
func (s *Store) PromoteDueScheduled(ctx context.Context, limit int) ([]PendingJob, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET
			status = 'queued',
			next_attempt_at = NULL,
			updated_at = now()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'scheduled'
			  AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
		)
		RETURNING id, priority`,
		limit)
	if err != nil {
		return nil, fmt.Errorf("promote due scheduled: %w", err)
	}
	defer rows.Close()

	return scanPending(rows, "promoted scheduled")
}

func scanPending(rows pgx.Rows, what string) ([]PendingJob, error) {
	var out []PendingJob
	for rows.Next() {
		var p PendingJob
		if err := rows.Scan(&p.ID, &p.Priority); err != nil {
			return nil, fmt.Errorf("scan %s: %w", what, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// FindStaleQueued returns IDs of jobs that have sat in 'queued' longer than olderThan
func (s *Store) FindStaleQueued(ctx context.Context, olderThan time.Duration, limit int) ([]PendingJob, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, priority FROM jobs
		WHERE status = 'queued'
		  AND updated_at < now() - make_interval(secs => $1)
		  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		ORDER BY priority DESC, updated_at
		LIMIT $2`,
		olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("find stale queued: %w", err)
	}
	defer rows.Close()

	return scanPending(rows, "stale queued")
}

// ReleaseStaleRunning rescues jobs whose worker vanished without recording a
// result - SIGKILL, OOM, or a node dying, none of which run the graceful path.
//
// Expiry is the lease, not a guess from started_at: a live worker renews its
// lease, so an expired one means the worker really is gone. Rows with a NULL
// lease are never touched (NULL < now() is NULL, so they don't match), which
// keeps jobs claimed by a pre-lease binary safe during a rolling upgrade.
func (s *Store) ReleaseStaleRunning(ctx context.Context, limit int) ([]PendingJob, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET
			status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END::job_status,
			finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END,
			last_error = 'lease expired; worker vanished before recording a result',
			next_attempt_at = NULL,
			claimed_by = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'running'
			  AND lease_expires_at < now()
			ORDER BY lease_expires_at
			LIMIT $1
		)
		RETURNING id, status, priority`,
		limit)
	if err != nil {
		return nil, fmt.Errorf("release stale running: %w", err)
	}
	defer rows.Close()

	var requeued []PendingJob
	for rows.Next() {
		var p PendingJob
		var status jobs.Status
		if err := rows.Scan(&p.ID, &status, &p.Priority); err != nil {
			return nil, fmt.Errorf("scan stale running: %w", err)
		}
		if status == jobs.StatusQueued {
			requeued = append(requeued, p)
		}
	}
	return requeued, rows.Err()
}

// FailJob atomically records a failed attempt. Returns ErrLostClaim if the lease
// expired and the job now belongs to someone else.
func (s *Store) FailJob(ctx context.Context, id uuid.UUID, workerID, errMsg string, retryAfter time.Duration) (*jobs.Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET
			status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END::job_status,
		    finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END,
			next_attempt_at = CASE WHEN attempts < max_attempts
			                       THEN now() + make_interval(secs => $4) END,
			last_error = $3,
			claimed_by = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND claimed_by = $2
		RETURNING `+jobColumns,
		id, workerID, errMsg, retryAfter.Seconds())

	job, err := scanJob(row)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrLostClaim
	}
	return job, err
}
