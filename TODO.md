# TODO

## Phase 1: Core + Docker Compose
- API: POST /jobs (write row, publish ID to RabbitMQ), GET /jobs/{id}
- Worker: consume ID, load job, run handler, upsert result, ack after write
- Job handlers: sleep, CPU-heavy, flaky
- Retries: exponential backoff via TTL queues + DLX
- Dead-letter after max retries + API requeue endpoint
- Graceful shutdown: SIGTERM, stop consuming, finish in-flight, exit
- Dockerfiles (api, worker) + docker-compose with healthchecks

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

## Stretch
- Apply a real use-case for jobs
- 'scheduled' status: delayed/scheduled job start
- 'cancelled' status: cancel a queued or running job via API
- 'paused' status: pause/resume jobs
- Runs table: one row per attempt (worker, timings, error) + retention policy
- Priority queues
