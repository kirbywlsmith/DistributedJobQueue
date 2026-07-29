# TODO

## Demo
- Burst-submit CPU jobs, watch workers scale 1 => 10 in Grafana
- Kill worker mid-job, show job still completes

## Stretch (robustness, post-demo)
- Reconciler: periodically republish stale 'queued' jobs (covers lost publishes)
- Retries: exponential backoff via TTL queues + DLX (retries currently immediate)
- API requeue endpoint for terminally failed jobs

## Stretch
- Apply a real use-case for jobs
- 'scheduled' status: delayed/scheduled job start
- 'cancelled' status: cancel a queued or running job via API
- 'paused' status: pause/resume jobs
- Runs table: one row per attempt (worker, timings, error) + retention policy
- Priority queues
- Per-type queues: workers bind only to job types they handle
