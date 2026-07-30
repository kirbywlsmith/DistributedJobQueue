CREATE TYPE run_status AS ENUM ('completed', 'failed', 'released', 'abandoned');

CREATE TABLE job_runs (
    id          BIGSERIAL   PRIMARY KEY,
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    worker_id   TEXT        NOT NULL,
    status      run_status  NOT NULL,
    error       TEXT,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_runs_job ON job_runs (job_id, attempt);
CREATE INDEX idx_job_runs_finished ON job_runs (finished_at);
