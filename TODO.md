# TODO

## Scaling + Observability
- KEDA autoscaling on queue depth
- Prometheus metrics + Grafana dashboard
- Zero-loss rolling deploys / graceful shutdown:
  on SIGTERM the handler returns ctx.Err(), then FailJob is called with that SAME
  cancelled ctx -> DB write fails -> Nack(requeue), but the row stays 'running'.
  Next worker's StartJob wants status='queued', gets ErrNotFound, acks + drops it =>
  job stuck 'running' forever. Fix: bookkeeping writes (FailJob/CompleteJob) need an
  uncancelled ctx (context.WithTimeout(context.Background(), 5s)) +
  terminationGracePeriodSeconds on the worker Deployment.
  Fires on EVERY KEDA scale-down (10 -> 1 kills 9 mid-job), not just scale-to-zero.
  Blocks setting minReplicaCount: 0 on the ScaledObject.

## Demo
- Burst-submit CPU jobs, watch workers scale 1 => 10 in Grafana
- Kill worker mid-job, show job still completes

## Stretch (robustness, post-demo)
- Reconciler: periodically republish stale 'queued' jobs (covers lost publishes)
- AMQP reconnect: Publisher opens conn once at startup + never recovers if conn drops
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
