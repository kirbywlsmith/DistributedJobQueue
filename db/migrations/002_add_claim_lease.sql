ALTER TABLE jobs
    ADD COLUMN claimed_by       TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at) WHERE status = 'running';
