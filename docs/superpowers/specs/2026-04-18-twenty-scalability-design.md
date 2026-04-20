# Twenty CRM Self-Hosted Scalability Design

## Overview

Production-grade scalability for self-hosted Twenty CRM on GKE using only infrastructure-level changes (Helm charts, K8s manifests, SQL scripts). No application code changes. All open-source components. Works locally on Docker Desktop K8s and in production with ArgoCD/GitOps.

**Scale target:** 50-500 users, 450K companies, 450K contacts, moderate deals/workflows. Custom objects (e.g., store data with 25+ fields) added via Twenty's custom apps feature — these create separate workspace tables with relations to companies/contacts, not extra columns on existing tables.

**Constraint:** No source code changes to Twenty. All changes are Helm values, K8s manifests, SQL init scripts, and monitoring config.

---

## 1. Architecture

### Deployment Topology (Multi-Chart Orchestration)

Each component is a separate Helm release / ArgoCD Application. Sync waves ensure correct startup order; ArgoCD health checks gate wave progression. Locally: `helm install` in order with `--wait`.

```
ArgoCD App-of-Apps (prod) / shell script (local)
|
+-- Wave 0: Operators
|   +-- CloudNativePG Operator
|   +-- Prometheus Operator (local only; exists in prod)
|
+-- Wave 1: Data Layer
|   +-- CloudNativePG Cluster (1 primary + 2 read replicas)
|   |   +-- PgBouncer Pooler (RW + RO)
|   +-- KeyDB StatefulSet (2 nodes, active replication)
|   +-- MinIO (local only; GCS in prod) — S3-compatible object storage for backups + file uploads
|
+-- Wave 2: Application Layer
|   +-- Twenty Server (HPA: 2-10 pods)
|   +-- Twenty Worker (HPA: 2-6 pods)
|
+-- Wave 3: Observability
    +-- ServiceMonitors (server, worker, PG, KeyDB)
    +-- Grafana Dashboards
    +-- PrometheusRules (alerts)
```

### Component Summary

| Component | Chart / Manifest | Purpose |
|-----------|-----------------|---------|
| CloudNativePG Operator | `cnpg/cloudnative-pg` (latest) | Manages PG clusters as CRDs |
| PostgreSQL Cluster | CloudNativePG `Cluster` CRD | 1 primary + 2 replicas + PgBouncer |
| KeyDB | Custom StatefulSet + headless Service | Multi-threaded Redis replacement |
| Twenty Server | Patched upstream Helm chart | Frontend + API with HPA/PDB |
| Twenty Worker | Patched upstream Helm chart | BullMQ processor with HPA/PDB |
| Monitoring | `kube-prometheus-stack` (local) / manifests only (prod) | Dashboards + ServiceMonitors |
| MinIO | `minio/minio` Helm chart (local only) | S3-compatible storage for backups + file uploads |

### Environment Differences

| Aspect | Local (Docker Desktop K8s) | Production (GKE) |
|--------|---------------------------|-------------------|
| Deployment | `helm install` with `--wait` | ArgoCD App-of-Apps with sync waves |
| Monitoring | Full `kube-prometheus-stack` | ServiceMonitors + dashboards only (existing stack) |
| Backup storage | MinIO (in-cluster, `minio/minio` Helm chart) | GCS bucket (Workload Identity) |
| File uploads | MinIO (same instance, separate bucket) | GCS bucket (HMAC keys via S3 interop API) |
| Storage class | Default | `premium-rwo` (SSD) |
| Value overlay | `values-local.yaml` | `values-prod.yaml` |

---

## 2. Database Design (CloudNativePG + PostgreSQL)

### Cluster Topology

- **Primary:** 1 instance (read-write)
- **Replicas:** 2 instances (read-only, streaming replication)
- **PgBouncer:** Deployed via CloudNativePG `Pooler` CRD — separate RW and RO pooler services
- **Image:** `ghcr.io/cloudnative-pg/postgresql` (latest stable, PG 17, includes postgresql-contrib)

### Cluster CRD Configuration

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: twenty-db
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:17.4
  storage:
    storageClass: premium-rwo  # default for local
    size: 20Gi
  walStorage:
    storageClass: premium-rwo
    size: 5Gi
  postgresql:
    parameters:
      shared_buffers: 256MB
      effective_cache_size: 768MB
      work_mem: 16MB
      maintenance_work_mem: 128MB
      random_page_cost: "1.1"
      max_connections: "200"
      pg_stat_statements.max: "10000"
      pg_stat_statements.track: all
      auto_explain.log_min_duration: "1000"
      auto_explain.log_analyze: "true"
      auto_explain.log_buffers: "true"
      log_min_duration_statement: "500"
  bootstrap:
    initdb:
      database: twenty
      owner: twenty
      postInitTemplateSQL:
        - CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"
        - CREATE EXTENSION IF NOT EXISTS "pg_trgm"
        - CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
        - CREATE EXTENSION IF NOT EXISTS "unaccent"
      postInitApplicationSQLRefs:
        configMapRefs:
          - name: twenty-db-init-sql
            key: optimizations.sql
  enableSuperuserAccess: true
  resources:
    requests:
      memory: 512Mi
      cpu: 500m
    limits:
      memory: 2Gi
      cpu: "2"
  affinity:
    enablePodAntiAffinity: true
    topologyKey: kubernetes.io/hostname
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: twenty-backup-store
  serviceAccountTemplate:
    metadata:
      annotations:
        iam.gke.io/gcp-service-account: twenty-cnpg@<project>.iam.gserviceaccount.com
```

### PgBouncer Pooler CRDs

```yaml
# Read-write pooler (routes to primary)
apiVersion: postgresql.cnpg.io/v1
kind: Pooler
metadata:
  name: twenty-db-pooler-rw
spec:
  cluster:
    name: twenty-db
  instances: 2
  type: rw
  pgbouncer:
    poolMode: transaction
    parameters:
      max_client_conn: "1000"
      default_pool_size: "20"
      min_pool_size: "5"
      reserve_pool_size: "5"
---
# Read-only pooler (load-balanced across replicas)
apiVersion: postgresql.cnpg.io/v1
kind: Pooler
metadata:
  name: twenty-db-pooler-ro
spec:
  cluster:
    name: twenty-db
  instances: 2
  type: ro
  pgbouncer:
    poolMode: transaction
    parameters:
      max_client_conn: "1000"
      default_pool_size: "20"
      min_pool_size: "5"
      reserve_pool_size: "5"
```

### Backup Configuration (Barman Cloud Plugin)

Production (GCS with GKE Workload Identity):

```yaml
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: twenty-backup-store
spec:
  configuration:
    destinationPath: "gs://twenty-backups/prod"
    googleCredentials:
      gkeEnvironment: true
---
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: twenty-daily-backup
spec:
  schedule: "0 0 2 * * *"
  backupOwnerReference: self
  cluster:
    name: twenty-db
  method: plugin
```

Local (MinIO):

```yaml
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: twenty-backup-store
spec:
  configuration:
    destinationPath: "s3://twenty-backups/"
    endpointURL: "http://minio.minio:9000"
    s3Credentials:
      accessKeyId:
        name: minio-creds
        key: ACCESS_KEY_ID
      secretAccessKey:
        name: minio-creds
        key: SECRET_ACCESS_KEY
```

### Application Connectivity

| Env Var | Value | Purpose |
|---------|-------|---------|
| `PG_DATABASE_URL` | `postgres://twenty:<pwd>@twenty-db-pooler-rw:5432/twenty` | Primary DB via PgBouncer RW |
| `PG_DATABASE_REPLICA_URL` | `postgres://twenty:<pwd>@twenty-db-pooler-ro:5432/twenty` | Read replicas via PgBouncer RO |

### High Availability

- Automatic failover managed by CloudNativePG (promotes replica if primary dies)
- PDB: minAvailable 1

---

## 3. KeyDB (Cache & Queue Layer)

### Why KeyDB

- Drop-in Redis replacement (same protocol, same commands)
- Multi-threaded — single instance handles what multiple Redis instances would
- Active replication for simplified failover
- Apache 2.0 (via Snap Inc.)

### Topology

- 2 KeyDB nodes as StatefulSet with headless service for peer discovery
- `active-replica yes` — both nodes accept reads; both can promote on failure
- K8s Service with readiness probes for health-based routing
- BullMQ queue writes pinned to one node via dedicated Service

### Configuration

```yaml
# keydb.conf
server-threads 4
active-replica yes
maxmemory 75%
maxmemory-policy noeviction
save 900 1 300 10
appendonly yes
```

### Failover Mechanism

KeyDB active-replica does NOT provide automatic failover (verified against docs). Failover is handled by:

- K8s readiness probe: `keydb-cli ping` — unhealthy node gets removed from Service endpoints
- K8s Service routes traffic only to healthy pods
- Active replication keeps both nodes in sync, so either can serve after failure

No Sentinel or HAProxy needed — K8s Service + readiness probes is sufficient and simpler.

### BullMQ Compatibility

BullMQ does NOT officially test against KeyDB (only AWS MemoryDB and ElastiCache are listed as tested). KeyDB claims full Redis protocol compatibility, but this requires explicit validation. See Section 7c.

### Application Connectivity

| Env Var | Value | Purpose |
|---------|-------|---------|
| `REDIS_URL` | `redis://twenty-keydb:6379` | Cache + pub/sub |
| `REDIS_QUEUE_URL` | `redis://twenty-keydb:6379` | BullMQ queues |

### PDB

minAvailable: 1

---

## 4. Application Layer (Server + Worker Scaling)

### Twenty Server (API + Frontend)

- Base replicas: 2 (min for HA)
- HPA: scale 2-10 based on CPU (70%) and memory (80%)
- PDB: minAvailable 1
- Strategy: RollingUpdate (maxSurge 1, maxUnavailable 1)
- Readiness probe: GET `/healthz` (existing endpoint)
- Liveness probe: GET `/healthz` (longer timeout)

### Twenty Worker (BullMQ Processor)

- Base replicas: 2
- HPA: scale 2-6 based on custom metric (`twenty_queue_jobs_waiting_total` exposed on Prometheus port 9464)
- PDB: minAvailable 1
- 17 queues with priority levels 1-7 (billing highest, cron lowest)
- Worker concurrency is hardcoded (not configurable via env vars without code changes)

### Storage

Multi-pod requires shared storage. Twenty supports S3-compatible storage via env vars:

| Env Var | Value | Purpose |
|---------|-------|---------|
| `STORAGE_TYPE` | `S_3` | Enable S3-compatible storage (note: uppercase `S_3`, not `s3`) |
| `STORAGE_S3_ENDPOINT` | `storage.googleapis.com` | GCS interoperability API |
| `STORAGE_S3_REGION` | `<gcp-region>` | GCS region |
| `STORAGE_S3_NAME` | `twenty-files-<env>` | GCS bucket name |
| `STORAGE_S3_ACCESS_KEY_ID` | HMAC key | GCS HMAC access key |
| `STORAGE_S3_SECRET_ACCESS_KEY` | HMAC secret | GCS HMAC secret key |

### Metrics

| Env Var | Value | Purpose |
|---------|-------|---------|
| `METER_DRIVER` | `prometheus` | Enable Prometheus metrics on port 9464 |

---

## 5. Observability

### What Twenty Exposes Natively (Verified)

All metrics exposed on Prometheus port 9464 via `METER_DRIVER=prometheus`.

**Counters:**

| Metric | Attributes | Source |
|--------|-----------|--------|
| `graphql-operation/200` through `graphql-operation/500`, `graphql-operation/unknown` | — | GraphQL error handler hook |
| `workflow-run/started/{trigger-type}` (4 variants: database-event, cron, webhook, manual) | — | Workflow executor |
| `workflow-run/completed`, `workflow-run/failed`, `workflow-run/stopped`, `workflow-run/throttled` | — | Workflow executor |
| `workflow-run/failed/to-enqueue`, `workflow-run/system-error` | — | Workflow executor |
| `message-channel-sync-job/active`, `message-channel-sync-job/failed-insufficient-permissions`, `message-channel-sync-job/failed-unknown` | — | Message sync |
| `calendar-event-sync-job/active`, `calendar-event-sync-job/failed-insufficient-permissions`, `calendar-event-sync-job/failed-unknown` | — | Calendar sync |
| `job/completed` | queue, job_name | BullMQ driver |
| `job/failed` | queue, job_name, error_type | BullMQ driver |
| `sign-up/completed`, `captcha/validation-failed`, `webhook/sent` | — | Various |

**Gauges:**

| Metric | Description |
|--------|------------|
| `twenty_queue_jobs_waiting_total` | Current waiting jobs across all 17 queues |

**NOT available (no code change):**
- HTTP request-level metrics (RPS, latency histograms) — Twenty only tracks GraphQL operation counts, not generic HTTP metrics
- No OpenTelemetry auto-instrumentation initialized despite dependency being present

### Infrastructure Metrics (Added by Us)

| Component | Source | Port | Metrics |
|-----------|--------|------|---------|
| CloudNativePG | Built-in exporter | 9187 | PG connections, replication lag, WAL size, tx/s, cache hit ratio |
| PgBouncer | CloudNativePG pooler metrics | 9187 | Pool utilization, wait times, client connections |
| KeyDB | `oliver006/redis_exporter` sidecar | 9121 | Memory, ops/sec, connections, replication offset |
| Twenty pods | cAdvisor (kubelet) | — | CPU, memory, network per pod |
| pg_stat_statements | Custom CloudNativePG monitoring query (ConfigMap) | 9187 | Top slow queries, call counts, mean exec time |

### PostgreSQL Query Planner Visibility

Enabled via CloudNativePG `postgresql.parameters`:

```yaml
pg_stat_statements.max: "10000"
pg_stat_statements.track: all
auto_explain.log_min_duration: "1000"  # log plans for queries > 1s
auto_explain.log_analyze: "true"       # include actual row counts
auto_explain.log_buffers: "true"       # include buffer usage
log_min_duration_statement: "500"      # log all queries > 500ms
```

Custom monitoring query (deployed as ConfigMap for CloudNativePG):

```sql
SELECT query, calls, mean_exec_time, total_exec_time, rows,
       shared_blks_hit, shared_blks_read
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 50
```

### Grafana Dashboards (5)

1. **Twenty Overview** — GraphQL operations by status code, error rate, active pods, queue depth
2. **PostgreSQL Health** — connections, replication lag, transaction rate, cache hit ratio, table sizes, top slow queries (pg_stat_statements)
3. **KeyDB Health** — memory, ops/sec, connected clients, replication offset, evictions
4. **BullMQ Queues** — jobs waiting/active/completed/failed per queue, processing time
5. **Workflow & Sync** — message sync metrics, calendar sync metrics, workflow execution rates

### Alerting Rules (PrometheusRules)

- PG replication lag > 30s
- PG connection pool > 80% utilization
- KeyDB memory > 90%
- Queue depth > 1000 for > 5 minutes
- Pod restart count > 3 in 10 minutes
- Zero healthy server pods

### Environment Differences

- **Local:** Full `kube-prometheus-stack` (Prometheus + Grafana + node-exporter + kube-state-metrics)
- **Production:** Deploy only ServiceMonitors, PrometheusRules, and Grafana dashboard ConfigMaps

---

## 6. PostgreSQL Optimization Inventory

All optimizations are pure SQL/config — zero application code changes. Applied in two layers:

- **Core schema optimizations** (6a, 6g for app_token): Applied via CloudNativePG `postInitApplicationSQLRefs` (ConfigMap with SQL) at cluster bootstrap.
- **Workspace schema optimizations** (6b-6f, 6g for workspace tables, 6h): Applied via a K8s Job that iterates over all workspace schemas (queried from `core."workspace"` table). Must run after Twenty creates workspace schemas. Re-runnable for new workspaces.
- **For existing clusters:** Same SQL scripts run as a one-time K8s Job.

### 6a. Missing FK Indexes (Core Entities)

Workspace entities are well-indexed (verified). Core entities have gaps:

| Table (core schema) | Column | Impact |
|---------------------|--------|--------|
| app_token | userId | Token lookups by user — currently full table scan |
| app_token | workspaceId | Token lookups by workspace — currently full table scan |

Additional missing indexes to be discovered via `pg_stat_user_indexes` after deployment and load testing.

### 6b. BRIN Indexes on Timestamps

For append-only, time-ordered tables. 10-100x smaller than B-tree.

| Table | Column | pages_per_range |
|-------|--------|-----------------|
| message | createdAt | 128 |
| workflow_run | createdAt | 128 |
| timeline_activity | createdAt | 128 |
| calendar_event | createdAt | 128 |

```sql
CREATE INDEX CONCURRENTLY idx_message_created_brin
  ON <workspace>."message" USING BRIN ("createdAt") WITH (pages_per_range = 128);
```

### 6c. pg_trgm GIN Indexes (ILIKE Fallback Acceleration)

Prerequisite: `CREATE EXTENSION pg_trgm` (in postInitTemplateSQL).

Twenty's search service falls back to `ILIKE` with `unaccent_immutable()` when tsvector returns no results. Without pg_trgm GIN indexes, this does a sequential scan.

| Table | Column | Rationale |
|-------|--------|-----------|
| company | searchVector (cast to text) | ILIKE fallback on 450K companies |
| person | searchVector (cast to text) | ILIKE fallback on 450K contacts |

```sql
CREATE INDEX CONCURRENTLY idx_company_search_trgm
  ON <workspace>."company" USING GIN (("searchVector"::text) gin_trgm_ops);
```

### 6d. Expression Indexes for unaccent_immutable

| Table | Expression | Rationale |
|-------|-----------|-----------|
| company | `unaccent_immutable(name)` | Accent-insensitive name search |

```sql
CREATE INDEX CONCURRENTLY idx_company_name_unaccent
  ON <workspace>."company" USING btree (public.unaccent_immutable(name));
```

### 6e. FILLFACTOR Tuning (HOT Updates)

For tables with frequent status/cursor updates. Lower FILLFACTOR leaves room for heap-only tuple updates, reducing VACUUM pressure.

| Table | FILLFACTOR | Rationale |
|-------|-----------|-----------|
| workflow_run | 70 | Frequent state field updates |
| message_channel | 75 | Frequent syncStatus/syncCursor updates |

```sql
ALTER TABLE <workspace>."workflowRun" SET (fillfactor = 70);
ALTER TABLE <workspace>."messageChannel" SET (fillfactor = 75);
```

Note: FILLFACTOR is NOT supported by YugabyteDB. Skip when running on YugabyteDB (Section 8).

### 6f. JSONB GIN Indexes

| Table | Column | Rationale |
|-------|--------|-----------|
| workflow_run | state | Workflow filtering (RAW_JSON/JSONB type) |

```sql
CREATE INDEX CONCURRENTLY idx_workflow_run_state_gin
  ON <workspace>."workflowRun" USING GIN (state jsonb_path_ops);
```

### 6g. Autovacuum Tuning

For high-churn tables. More aggressive thresholds than default 10%.

| Table | vacuum_scale_factor | analyze_scale_factor | Rationale |
|-------|--------------------|--------------------|-----------|
| message | 0.01 | 0.005 | High insert churn from email sync |
| workflow_run | 0.02 | 0.01 | Frequent state updates |
| app_token (core) | 0.02 | 0.01 | Expiry-based cleanup churn |

```sql
ALTER TABLE <workspace>."message" SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.005
);
```

### 6h. Materialized Views for Dashboards

| View | Source | Refresh |
|------|--------|---------|
| mv_opportunity_pipeline | opportunity (GROUP BY stage, SUM/COUNT/AVG) | Every 15 min via K8s CronJob |
| mv_company_stats | company JOIN custom object tables (COUNT, GROUP BY) | Every 15 min via K8s CronJob |

### NOT Included (Validated as Risky)

- **Partial indexes (WHERE deletedAt IS NULL)** — Twenty has active `withDeleted()` query paths for restore operations, email collision detection, and trash cleanup. Partial indexes would break or slow these operations.
- **Table partitioning** — Not needed at 450K scale. Revisit at 5M+ rows.

---

## 7. Validation & Testing Strategy

### 7a. Integration Test Validation (Existing — 368 Suites)

Run the full integration test suite against the new infrastructure:

```bash
npx nx run twenty-server:test:integration:with-db-reset
```

This resets the database (truncates schemas, runs migrations, seeds data), then runs Jest integration tests covering: GraphQL operations, REST API, metadata, permissions, search, workflows, migrations.

### 7b. E2E Validation (Existing — Playwright)

```bash
npx nx run twenty-e2e-testing:test
```

Runs against `http://localhost:3001`. Covers: login, record creation, workflow execution/visualization, kanban views.

### 7c. BullMQ + KeyDB Compatibility Validation (New)

BullMQ does NOT officially support KeyDB. Must validate before production use.

Test script exercising all BullMQ patterns Twenty uses (17 queues):

1. Queue creation and job enqueue (all 17 queue names)
2. Job processing with worker
3. Job scheduling via `upsertJobScheduler` (cron patterns)
4. Job priority ordering (7 priority levels)
5. Job retry on failure (`attempts` configuration)
6. Concurrent job processing
7. GraphQL subscriptions via Redis pub/sub (`graphql-redis-subscriptions` library)
8. Cache get/set/mget/mset operations
9. Redis SET operations (SADD, SREM, SPOP) via `cache-storage.service.ts`

Procedure: Run against Redis first (baseline), then KeyDB. Compare behavior and results. Any divergence is a blocker.

### 7d. PostgreSQL Optimization Validation (New)

SQL validation script run after bootstrap:

1. Verify extensions installed: `SELECT * FROM pg_extension`
2. Verify indexes exist: query `pg_indexes` for all expected index names
3. Run `EXPLAIN ANALYZE` on critical queries (search, company lookup, message sync joins) — confirm indexes are used
4. Check `pg_stat_statements` for top slow queries after integration tests
5. Validate FILLFACTOR: `SELECT reloptions FROM pg_class WHERE relname = 'workflowRun'`
6. Validate autovacuum: `SELECT reloptions FROM pg_class WHERE relname = 'message'`

### 7e. Load Testing (New — k6)

Install k6 locally via `brew install k6` (macOS).

k6 scripts targeting:
1. GraphQL query throughput (company list, person search, opportunity pipeline)
2. Search endpoint under concurrent load
3. Message sync simulation (batch inserts via API)
4. Workflow trigger throughput

k6 outputs metrics to Prometheus via `k6 run --out experimental-prometheus-rw` (remote write to local Prometheus). Grafana shows k6 metrics (request rate, latency p50/p95/p99, error rate) alongside PG, KeyDB, and Twenty app metrics in real-time — single pane of glass for bottleneck identification.

All load tests run on macOS with Docker Desktop K8s for initial validation.

### 7f. Test Execution Order

1. Deploy infrastructure (CloudNativePG + KeyDB + monitoring)
2. Run SQL optimization scripts
3. Run 7d (PostgreSQL optimization validation)
4. Run 7c (BullMQ + KeyDB compatibility) — blocker if fails
5. Deploy Twenty (server + worker)
6. Run 7a (integration tests)
7. Run 7b (E2E tests)
8. Run 7e (k6 load tests with monitoring)
9. Analyze results, tune, repeat

---

## 8. Distributed DB Validation Path (Hybrid — Future)

NOT part of initial deployment. Runs in a separate test namespace alongside the production CloudNativePG setup. Goal: determine with high confidence whether Twenty works on a distributed PG-compatible DB.

### Primary Candidate: YugabyteDB

- **License:** Apache 2.0 (100% open source, no restrictions)
- **Architecture:** Distributed SQL on DocDB storage engine, Raft consensus
- **PG compatibility:** High — supports CREATE SCHEMA, ALTER TABLE ADD COLUMN, pg_trgm, tsvector + GIN, unaccent
- **Known incompatibility:** FILLFACTOR not supported (skip Section 6e optimizations)
- **GIN deviations:** No bitmap index scan (uses index scan), no fast update, explicit deletes to index

### Secondary Candidate: Neon

- **License:** Apache 2.0
- **Language:** Rust, Paxos consensus
- **Architecture:** Separated storage + compute. IS PostgreSQL (runs unmodified PG compute nodes)
- **PG compatibility:** Full — supports all extensions (80+), all TypeORM patterns
- **Limitation:** Single-writer architecture. Horizontal READ scaling only (compute autoscaling + read replicas). No horizontal write scaling.
- **When it fits:** If bottleneck analysis shows reads are the constraint (likely at 450K scale), Neon may be sufficient

### NOT Considered: CockroachDB

- **License:** BSL (Business Source License) — revenue caps, mandatory telemetry, cannot use as-a-service
- **Blocker:** `CREATE EXTENSION` returns unimplemented for `unaccent`, `uuid-ossp`, `pg_trgm`. Twenty REQUIRES these at startup. Hard blocker without code changes.

### Phase 1 — Compatibility (Pass/Fail)

1. Deploy YugabyteDB (and optionally Neon) in a test K8s namespace
2. Point Twenty's `PG_DATABASE_URL` at it
3. Run `database:reset` — does schema creation + migration succeed?
4. Run full 368 integration test suites — record pass/fail per suite
5. Run Playwright E2E tests — record pass/fail
6. Document: which features work, which break, why

**Known risk areas to test:**
- Schema-per-workspace DDL (CREATE SCHEMA, ALTER TABLE ADD COLUMN at runtime)
- `unaccent_immutable()` custom function creation and usage
- tsvector + GIN full-text search behavior
- pg_trgm extension availability and ILIKE acceleration
- TypeORM-generated SQL (implicit casts, PG-specific syntax)
- Composite types (emails, links, currencies)

### Phase 2 — Performance (Bottleneck Identification)

Write-heavy paths to benchmark (identified from code analysis):

1. **Message sync batch inserts** — 100s-1000s of messages per sync cycle into message + message_participant + message_channel_message_association
2. **Search vector regeneration** — tsvector + GIN index maintenance on every record update
3. **Workspace schema DDL** — CREATE SCHEMA + all tables/indexes for new workspace
4. **Workflow status updates** — frequent UPDATE on workflow_run.state (JSONB)
5. **Calendar event sync** — similar pattern to message sync
6. **Dashboard aggregations** — GROUP BY on 450K companies with JOINs to custom object tables

k6 scripts target these paths, run against CloudNativePG, YugabyteDB, and Neon. Compare:
- Throughput (operations/sec)
- Latency (p50, p95, p99)
- Resource consumption (CPU, memory, disk I/O)

Monitor via Grafana during load tests for bottleneck identification.

### Phase 3 — Decision

- All tests pass + performance acceptable: plan migration to distributed DB
- Tests pass but performance worse: stay on CloudNativePG (read replicas sufficient for medium scale)
- Tests fail: document incompatibilities, stay on CloudNativePG

### Future Watch: OrioleDB

Apache 2.0, Raft-designed, IS PostgreSQL (table access method extension). Currently beta and single-node. Could be the ideal solution in 1-2 years — monitors for GA release.

---

## 9. Deliverables

The implementation produces:

1. **Helm value overlays** — `values-local.yaml` and `values-prod.yaml` for each component
2. **CloudNativePG manifests** — Cluster, Pooler (RW + RO), ObjectStore, ScheduledBackup CRDs
3. **KeyDB StatefulSet** — with active replication, readiness probes, redis_exporter sidecar
4. **HPA manifests** — for server and worker deployments
5. **PDB manifests** — for all stateful and application components
6. **SQL optimization ConfigMap** — indexes, BRIN, pg_trgm, FILLFACTOR, autovacuum tuning
7. **Grafana dashboard JSON** — 5 dashboards
8. **PrometheusRules** — alerting rules
9. **ServiceMonitor manifests** — for all components
10. **ArgoCD Application manifests** — App-of-Apps with sync waves
11. **Local setup script** — `helm install` in order with `--wait`
12. **BullMQ + KeyDB test suite** — compatibility validation
13. **SQL validation script** — index/extension verification
14. **k6 load test scripts** — targeting critical paths
15. **YugabyteDB/Neon test manifests** — for Phase 1-2 validation
