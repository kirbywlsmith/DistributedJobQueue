CREATE INDEX idx_jobs_scheduled ON jobs (next_attempt_at) WHERE status = 'scheduled';
