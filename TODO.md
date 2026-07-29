# TODO

## Stretch (robustness, post-demo)
- Retries: exponential backoff via TTL queues + DLX (retries currently immediate)

## Stretch
- Apply a real use-case for jobs
- 'scheduled' status: delayed/scheduled job start
- 'cancelled' status: cancel a queued or running job via API
- 'paused' status: pause/resume jobs
- Runs table: one row per attempt (worker, timings, error) + retention policy
- Priority queues
- Per-type queues: workers bind only to job types they handle

## Final
- Write tests
- Update README.md
