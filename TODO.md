# TODO

## Phase 1: Core + Docker Compose
- Refactor: move Handler type + registry + sleep/cpu/flaky into internal/handlers
- Graceful shutdown: verify/polish during k8s rolling-deploy work (basic SIGTERM handling done)

## Phase 2: Kubernetes
- k3s Deployments for api + worker
- Liveness/readiness probes, resource limits
- ConfigMaps + Secrets

## Phase 3: Scaling + Observability
- KEDA autoscaling on queue depth
- Prometheus metrics + Grafana dashboard
- Zero-loss rolling deploys

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
