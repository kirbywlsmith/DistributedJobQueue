-- A retrying job is 'queued' in Postgres while its message waits in a TTL queue,
-- so the reconciler would otherwise treat it as an orphan and republish it early,
-- collapsing the backoff. next_attempt_at records when it is genuinely eligible.
ALTER TABLE jobs
    ADD COLUMN next_attempt_at TIMESTAMPTZ;
