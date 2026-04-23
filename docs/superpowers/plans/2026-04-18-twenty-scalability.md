# Twenty CRM Scalability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy production-grade, horizontally scalable infrastructure for self-hosted Twenty CRM on Kubernetes using only infrastructure-level changes — no application source code modifications.

**Architecture:** Multi-chart orchestration with ArgoCD sync waves (prod) or sequential `helm install` (local). CloudNativePG manages PostgreSQL (1 primary + 2 replicas + PgBouncer poolers). KeyDB replaces Redis with active replication. Twenty server and worker scale via HPA. All components are open-source with no cloud lock-in.

**Tech Stack:** CloudNativePG, PgBouncer, KeyDB, MinIO (local), GCS (prod), Prometheus, Grafana, ArgoCD, k6, Helm 3

---

## Scope Decision

This spec covers multiple interdependent subsystems — the application layer requires the data layer, monitoring requires all components, and validation requires everything deployed. A single phased plan organized by ArgoCD sync wave ordering (Wave 0 → 3), then optimizations, then validation, is the correct approach.

### Deviation from Design Spec

The spec's `postInitApplicationSQLRefs` in the Cluster CRD is replaced by a post-deployment K8s Job. Reason: the referenced SQL creates indexes on tables (`app_token`, workspace entities) that don't exist at CNPG bootstrap time — they're created by Twenty's database migration. All SQL optimizations run via a Job after Twenty has initialized.

---

## File Structure

```
infra/
├── operators/
│   └── cnpg-values.yaml
├── database/
│   ├── cluster.yaml
│   ├── pooler-rw.yaml
│   ├── pooler-ro.yaml
│   ├── objectstore-local.yaml
│   ├── objectstore-prod.yaml
│   ├── scheduled-backup.yaml
│   └── monitoring-queries.yaml
├── keydb/
│   ├── configmap.yaml
│   ├── statefulset.yaml
│   ├── service.yaml
│   ├── service-headless.yaml
│   └── pdb.yaml
├── minio/
│   └── values-local.yaml
├── twenty/
│   ├── values-local.yaml
│   ├── values-prod.yaml
│   ├── service-server-metrics.yaml
│   ├── service-worker-metrics.yaml
│   ├── hpa-server.yaml
│   ├── hpa-worker.yaml
│   ├── pdb-server.yaml
│   └── pdb-worker.yaml
├── monitoring/
│   ├── kube-prometheus-stack-values.yaml
│   ├── servicemonitor-twenty-server.yaml
│   ├── servicemonitor-twenty-worker.yaml
│   ├── servicemonitor-keydb.yaml
│   ├── prometheusrules.yaml
│   └── dashboards/
│       ├── twenty-overview.yaml
│       ├── postgresql-health.yaml
│       ├── keydb-health.yaml
│       ├── bullmq-queues.yaml
│       └── workflow-sync.yaml
├── sql/
│   ├── core-optimizations.sql
│   ├── workspace-optimizations.sql
│   ├── optimization-job.yaml
│   └── mv-refresh-cronjob.yaml
├── argocd/
│   ├── app-of-apps.yaml
│   └── applications/
│       ├── cnpg-operator.yaml
│       ├── database.yaml
│       ├── keydb.yaml
│       ├── minio.yaml
│       ├── twenty.yaml
│       └── monitoring.yaml
├── validation/
│   ├── bullmq-keydb-test/
│   │   ├── package.json
│   │   └── test.js
│   ├── pg-validation.sql
│   └── k6/
│       ├── lib/
│       │   └── config.js
│       ├── graphql-throughput.js
│       ├── search-load.js
│       ├── message-sync.js
│       └── workflow-load.js
├── distributed-db/
│   ├── yugabytedb/
│   │   └── values.yaml
│   └── neon/
│       └── values.yaml
└── scripts/
    └── local-setup.sh
```

---

## Phase 1: Foundation (Wave 0)

### Task 1: Project Scaffolding

**Files:**
- Create: `infra/` directory tree

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p infra/{operators,database,keydb,minio,twenty,monitoring/dashboards,sql,argocd/applications,validation/bullmq-keydb-test,validation/k6/lib,distributed-db/yugabytedb,distributed-db/neon,scripts}
```

- [ ] **Step 2: Commit**

```bash
git add infra/
git commit -m "feat(infra): scaffold directory structure for scalability infrastructure"
```

---

### Task 2: CloudNativePG Operator Helm Values

**Files:**
- Create: `infra/operators/cnpg-values.yaml`

- [ ] **Step 1: Create CNPG operator Helm values**

```yaml
# infra/operators/cnpg-values.yaml
# CloudNativePG operator Helm values
# Chart: cnpg/cloudnative-pg
# Repo: https://cloudnative-pg.github.io/charts
#
# Install:
#   helm repo add cnpg https://cloudnative-pg.github.io/charts
#   helm install cnpg-operator cnpg/cloudnative-pg -n cnpg-system --create-namespace -f infra/operators/cnpg-values.yaml --wait

replicaCount: 1

resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 500m
    memory: 512Mi

monitoring:
  podMonitorEnabled: false
  grafanaDashboard:
    create: false
```

- [ ] **Step 2: Validate syntax**

```bash
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm template cnpg-operator cnpg/cloudnative-pg -f infra/operators/cnpg-values.yaml > /dev/null
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add infra/operators/cnpg-values.yaml
git commit -m "feat(infra): add CloudNativePG operator Helm values"
```

---

## Phase 2: Data Layer (Wave 1)

### Task 3: PostgreSQL Cluster CRD

**Files:**
- Create: `infra/database/cluster.yaml`
- Create: `infra/database/monitoring-queries.yaml`

- [ ] **Step 1: Create CNPG Cluster manifest**

```yaml
# infra/database/cluster.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: twenty-db
  namespace: twenty
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:17.4

  storage:
    storageClass: ""  # override in values-prod.yaml: premium-rwo
    size: 20Gi

  walStorage:
    storageClass: ""
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
    shared_preload_libraries:
      - pg_stat_statements
      - auto_explain

  bootstrap:
    initdb:
      database: twenty
      owner: twenty
      postInitTemplateSQL:
        - CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"
        - CREATE EXTENSION IF NOT EXISTS "pg_trgm"
        - CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
        - CREATE EXTENSION IF NOT EXISTS "unaccent"

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

  monitoring:
    enablePodMonitor: true
    customQueriesConfigMap:
      - name: twenty-db-monitoring
        key: queries

  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: twenty-backup-store

  # Production override: uncomment for GKE Workload Identity
  # serviceAccountTemplate:
  #   metadata:
  #     annotations:
  #       iam.gke.io/gcp-service-account: twenty-cnpg@<project>.iam.gserviceaccount.com
```

- [ ] **Step 2: Create CNPG monitoring queries ConfigMap**

```yaml
# infra/database/monitoring-queries.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: twenty-db-monitoring
  namespace: twenty
data:
  queries: |
    pg_stat_statements_top:
      query: |
        SELECT
          queryid,
          substr(query, 1, 100) as query_short,
          calls,
          mean_exec_time,
          total_exec_time,
          rows,
          shared_blks_hit,
          shared_blks_read
        FROM pg_stat_statements
        WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = 'twenty')
        ORDER BY mean_exec_time DESC
        LIMIT 20
      metrics:
        - queryid:
            usage: "LABEL"
            description: "Query ID"
        - query_short:
            usage: "LABEL"
            description: "Truncated query text"
        - calls:
            usage: "COUNTER"
            description: "Number of times executed"
        - mean_exec_time:
            usage: "GAUGE"
            description: "Mean execution time in ms"
        - total_exec_time:
            usage: "COUNTER"
            description: "Total execution time in ms"
        - rows:
            usage: "COUNTER"
            description: "Total rows returned"
        - shared_blks_hit:
            usage: "COUNTER"
            description: "Shared buffer hits"
        - shared_blks_read:
            usage: "COUNTER"
            description: "Shared blocks read from disk"
```

- [ ] **Step 3: Validate manifests**

```bash
kubectl apply --dry-run=client -f infra/database/monitoring-queries.yaml
```

Expected: `configmap/twenty-db-monitoring created (dry run)`

- [ ] **Step 4: Commit**

```bash
git add infra/database/cluster.yaml infra/database/monitoring-queries.yaml
git commit -m "feat(infra): add CloudNativePG Cluster CRD and monitoring queries"
```

---

### Task 4: PgBouncer Poolers (RW + RO)

**Files:**
- Create: `infra/database/pooler-rw.yaml`
- Create: `infra/database/pooler-ro.yaml`

- [ ] **Step 1: Create RW Pooler manifest**

```yaml
# infra/database/pooler-rw.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Pooler
metadata:
  name: twenty-db-pooler-rw
  namespace: twenty
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
  monitoring:
    enablePodMonitor: true
```

- [ ] **Step 2: Create RO Pooler manifest**

```yaml
# infra/database/pooler-ro.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Pooler
metadata:
  name: twenty-db-pooler-ro
  namespace: twenty
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
  monitoring:
    enablePodMonitor: true
```

- [ ] **Step 3: Validate manifests**

```bash
kubectl apply --dry-run=client -f infra/database/pooler-rw.yaml
kubectl apply --dry-run=client -f infra/database/pooler-ro.yaml
```

Expected: both created (dry run).

- [ ] **Step 4: Commit**

```bash
git add infra/database/pooler-rw.yaml infra/database/pooler-ro.yaml
git commit -m "feat(infra): add PgBouncer Pooler CRDs (RW + RO)"
```

---

### Task 5: Backup Configuration

**Files:**
- Create: `infra/database/objectstore-local.yaml`
- Create: `infra/database/objectstore-prod.yaml`
- Create: `infra/database/scheduled-backup.yaml`

- [ ] **Step 1: Create local ObjectStore (MinIO)**

```yaml
# infra/database/objectstore-local.yaml
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: twenty-backup-store
  namespace: twenty
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

- [ ] **Step 2: Create production ObjectStore (GCS)**

```yaml
# infra/database/objectstore-prod.yaml
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: twenty-backup-store
  namespace: twenty
spec:
  configuration:
    destinationPath: "gs://twenty-backups/prod"
    googleCredentials:
      gkeEnvironment: true
```

- [ ] **Step 3: Create ScheduledBackup**

```yaml
# infra/database/scheduled-backup.yaml
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: twenty-daily-backup
  namespace: twenty
spec:
  schedule: "0 0 2 * * *"
  backupOwnerReference: self
  cluster:
    name: twenty-db
  method: plugin
  pluginConfiguration:
    name: barman-cloud.cloudnative-pg.io
```

- [ ] **Step 4: Commit**

```bash
git add infra/database/objectstore-local.yaml infra/database/objectstore-prod.yaml infra/database/scheduled-backup.yaml
git commit -m "feat(infra): add backup config (ObjectStore for local/prod + ScheduledBackup)"
```

---

### Task 6: KeyDB StatefulSet

**Files:**
- Create: `infra/keydb/configmap.yaml`
- Create: `infra/keydb/statefulset.yaml`
- Create: `infra/keydb/service.yaml`
- Create: `infra/keydb/service-headless.yaml`
- Create: `infra/keydb/pdb.yaml`

- [ ] **Step 1: Create KeyDB ConfigMap**

```yaml
# infra/keydb/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: twenty-keydb-config
  namespace: twenty
data:
  keydb.conf: |
    server-threads 4
    active-replica yes
    multi-master yes
    repl-backlog-size 64mb
    maxmemory-policy noeviction
    save 900 1 300 10
    appendonly yes
    appendfsync everysec
    hz 25
    tcp-keepalive 60
    timeout 0
```

- [ ] **Step 2: Create KeyDB StatefulSet**

```yaml
# infra/keydb/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: twenty-keydb
  namespace: twenty
  labels:
    app.kubernetes.io/name: twenty-keydb
    app.kubernetes.io/component: cache
spec:
  serviceName: twenty-keydb-headless
  replicas: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: twenty-keydb
  template:
    metadata:
      labels:
        app.kubernetes.io/name: twenty-keydb
        app.kubernetes.io/component: cache
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app.kubernetes.io/name: twenty-keydb
                topologyKey: kubernetes.io/hostname
      initContainers:
        - name: init-config
          image: busybox:1.37
          command:
            - sh
            - -c
            - |
              cp /config/keydb.conf /data/keydb.conf
              HOSTNAME=$(hostname)
              ORDINAL=${HOSTNAME##*-}
              if [ "$ORDINAL" != "0" ]; then
                echo "replicaof twenty-keydb-0.twenty-keydb-headless 6379" >> /data/keydb.conf
              fi
          volumeMounts:
            - name: config
              mountPath: /config
            - name: data
              mountPath: /data
      containers:
        - name: keydb
          image: eqalpha/keydb:latest
          command:
            - keydb-server
            - /data/keydb.conf
          ports:
            - name: keydb
              containerPort: 6379
              protocol: TCP
          readinessProbe:
            exec:
              command:
                - keydb-cli
                - ping
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 3
          livenessProbe:
            exec:
              command:
                - keydb-cli
                - ping
            initialDelaySeconds: 15
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /config
        - name: redis-exporter
          image: oliver006/redis_exporter:latest
          ports:
            - name: metrics
              containerPort: 9121
              protocol: TCP
          env:
            - name: REDIS_ADDR
              value: "redis://localhost:6379"
          resources:
            requests:
              cpu: 50m
              memory: 32Mi
            limits:
              cpu: 100m
              memory: 64Mi
      volumes:
        - name: config
          configMap:
            name: twenty-keydb-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 5Gi
```

- [ ] **Step 3: Create KeyDB Services**

```yaml
# infra/keydb/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: twenty-keydb
  namespace: twenty
  labels:
    app.kubernetes.io/name: twenty-keydb
    app.kubernetes.io/component: cache
spec:
  type: ClusterIP
  ports:
    - name: keydb
      port: 6379
      targetPort: keydb
      protocol: TCP
    - name: metrics
      port: 9121
      targetPort: metrics
      protocol: TCP
  selector:
    app.kubernetes.io/name: twenty-keydb
```

```yaml
# infra/keydb/service-headless.yaml
apiVersion: v1
kind: Service
metadata:
  name: twenty-keydb-headless
  namespace: twenty
  labels:
    app.kubernetes.io/name: twenty-keydb
spec:
  type: ClusterIP
  clusterIP: None
  ports:
    - name: keydb
      port: 6379
      targetPort: keydb
      protocol: TCP
  selector:
    app.kubernetes.io/name: twenty-keydb
```

- [ ] **Step 4: Create KeyDB PDB**

```yaml
# infra/keydb/pdb.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: twenty-keydb
  namespace: twenty
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: twenty-keydb
```

- [ ] **Step 5: Validate all manifests**

```bash
kubectl apply --dry-run=client -f infra/keydb/configmap.yaml
kubectl apply --dry-run=client -f infra/keydb/statefulset.yaml
kubectl apply --dry-run=client -f infra/keydb/service.yaml
kubectl apply --dry-run=client -f infra/keydb/service-headless.yaml
kubectl apply --dry-run=client -f infra/keydb/pdb.yaml
```

Expected: all created (dry run).

- [ ] **Step 6: Commit**

```bash
git add infra/keydb/
git commit -m "feat(infra): add KeyDB StatefulSet with active replication and redis_exporter"
```

---

### Task 7: MinIO for Local Storage

**Files:**
- Create: `infra/minio/values-local.yaml`

- [ ] **Step 1: Create MinIO Helm values for local dev**

```yaml
# infra/minio/values-local.yaml
# MinIO standalone for local development
# Chart: minio/minio
# Repo: https://charts.min.io/
#
# Install:
#   helm repo add minio https://charts.min.io/
#   helm install minio minio/minio -n minio --create-namespace -f infra/minio/values-local.yaml --wait

mode: standalone

replicas: 1

resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 500m
    memory: 512Mi

persistence:
  enabled: true
  size: 10Gi

rootUser: minioadmin
rootPassword: minioadmin123

buckets:
  - name: twenty-backups
    policy: none
    purge: false
  - name: twenty-files
    policy: none
    purge: false

consoleIngress:
  enabled: false
```

- [ ] **Step 2: Create MinIO credentials secret for CNPG**

This secret is referenced by `infra/database/objectstore-local.yaml`:

```bash
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace twenty --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic minio-creds \
  --namespace=twenty \
  --from-literal=ACCESS_KEY_ID=minioadmin \
  --from-literal=SECRET_ACCESS_KEY=minioadmin123 \
  --dry-run=client -o yaml > /tmp/minio-creds-check.yaml
cat /tmp/minio-creds-check.yaml
```

Expected: valid Secret YAML output. The actual secret creation is done by the local setup script.

- [ ] **Step 3: Commit**

```bash
git add infra/minio/values-local.yaml
git commit -m "feat(infra): add MinIO Helm values for local S3-compatible storage"
```

---

## Phase 3: Application Layer (Wave 2)

### Task 8: Twenty Helm Value Overlays

**Files:**
- Create: `infra/twenty/values-local.yaml`
- Create: `infra/twenty/values-prod.yaml`

- [ ] **Step 1: Create local values overlay**

```yaml
# infra/twenty/values-local.yaml
# Twenty Helm chart values for local development (Docker Desktop K8s)
# Uses external CloudNativePG + KeyDB + MinIO
#
# Install:
#   helm install twenty packages/twenty-docker/helm/twenty -n twenty -f infra/twenty/values-local.yaml --wait

fullnameOverride: twenty

image:
  repository: twentycrm/twenty
  tag: ""
  pullPolicy: IfNotPresent

# Disable internal DB — using CloudNativePG
db:
  enabled: false
  external:
    host: twenty-db-pooler-rw
    port: 5432
    user: twenty
    database: twenty
    secretName: twenty-db-app
    passwordKey: password

# Disable internal Redis — using KeyDB
redisInternal:
  enabled: false
redis:
  external:
    host: twenty-keydb
    port: 6379

# Disable PVC persistence — using S3 storage (MinIO)
server:
  enabled: true
  replicaCount: 2

  persistence:
    enabled: false
  dockerDataPersistence:
    enabled: false

  resources:
    requests:
      cpu: 250m
      memory: 256Mi
    limits:
      cpu: "1"
      memory: 1Gi

  env:
    SIGN_IN_PREFILLED: "true"

  extraEnv:
    - name: PG_DATABASE_REPLICA_URL
      value: "postgres://twenty:$(DB_PASSWORD)@twenty-db-pooler-ro:5432/twenty"
    - name: METER_DRIVER
      value: prometheus
    - name: STORAGE_TYPE
      value: "S_3"
    - name: STORAGE_S3_ENDPOINT
      value: "http://minio.minio:9000"
    - name: STORAGE_S3_REGION
      value: "us-east-1"
    - name: STORAGE_S3_NAME
      value: "twenty-files"
    - name: STORAGE_S3_ACCESS_KEY_ID
      value: "minioadmin"
    - name: STORAGE_S3_SECRET_ACCESS_KEY
      value: "minioadmin123"

  ingress:
    enabled: false

worker:
  enabled: true
  replicaCount: 2

  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 2Gi

  extraEnv:
    - name: PG_DATABASE_REPLICA_URL
      value: "postgres://twenty:$(DB_PASSWORD)@twenty-db-pooler-ro:5432/twenty"
    - name: METER_DRIVER
      value: prometheus
    - name: STORAGE_TYPE
      value: "S_3"
    - name: STORAGE_S3_ENDPOINT
      value: "http://minio.minio:9000"
    - name: STORAGE_S3_REGION
      value: "us-east-1"
    - name: STORAGE_S3_NAME
      value: "twenty-files"
    - name: STORAGE_S3_ACCESS_KEY_ID
      value: "minioadmin"
    - name: STORAGE_S3_SECRET_ACCESS_KEY
      value: "minioadmin123"

# Override storage.type to prevent broken Helm helper (checks lowercase "s3")
storage:
  type: local
```

- [ ] **Step 2: Create production values overlay**

```yaml
# infra/twenty/values-prod.yaml
# Twenty Helm chart values for production (GKE)
# Uses external CloudNativePG + KeyDB + GCS

fullnameOverride: twenty

image:
  repository: twentycrm/twenty
  tag: ""
  pullPolicy: IfNotPresent

db:
  enabled: false
  external:
    host: twenty-db-pooler-rw
    port: 5432
    user: twenty
    database: twenty
    secretName: twenty-db-app
    passwordKey: password

redisInternal:
  enabled: false
redis:
  external:
    host: twenty-keydb
    port: 6379

server:
  enabled: true
  replicaCount: 2

  persistence:
    enabled: false
  dockerDataPersistence:
    enabled: false

  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: "2"
      memory: 2Gi

  env:
    SIGN_IN_PREFILLED: "false"

  extraEnv:
    - name: PG_DATABASE_REPLICA_URL
      value: "postgres://twenty:$(DB_PASSWORD)@twenty-db-pooler-ro:5432/twenty"
    - name: METER_DRIVER
      value: prometheus
    - name: STORAGE_TYPE
      value: "S_3"
    - name: STORAGE_S3_ENDPOINT
      value: "storage.googleapis.com"
    - name: STORAGE_S3_REGION
      valueFrom:
        configMapKeyRef:
          name: twenty-config
          key: GCS_REGION
    - name: STORAGE_S3_NAME
      valueFrom:
        configMapKeyRef:
          name: twenty-config
          key: GCS_BUCKET
    - name: STORAGE_S3_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: twenty-gcs-hmac
          key: accessKeyId
    - name: STORAGE_S3_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: twenty-gcs-hmac
          key: secretAccessKey

  ingress:
    enabled: true
    className: nginx
    acme: true
    hosts:
      - host: crm.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: twenty-tls
        hosts:
          - crm.example.com

worker:
  enabled: true
  replicaCount: 2

  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: "2"
      memory: 4Gi

  extraEnv:
    - name: PG_DATABASE_REPLICA_URL
      value: "postgres://twenty:$(DB_PASSWORD)@twenty-db-pooler-ro:5432/twenty"
    - name: METER_DRIVER
      value: prometheus
    - name: STORAGE_TYPE
      value: "S_3"
    - name: STORAGE_S3_ENDPOINT
      value: "storage.googleapis.com"
    - name: STORAGE_S3_REGION
      valueFrom:
        configMapKeyRef:
          name: twenty-config
          key: GCS_REGION
    - name: STORAGE_S3_NAME
      valueFrom:
        configMapKeyRef:
          name: twenty-config
          key: GCS_BUCKET
    - name: STORAGE_S3_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: twenty-gcs-hmac
          key: accessKeyId
    - name: STORAGE_S3_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: twenty-gcs-hmac
          key: secretAccessKey

storage:
  type: local
```

- [ ] **Step 3: Validate both overlays render correctly**

```bash
helm template twenty packages/twenty-docker/helm/twenty -f infra/twenty/values-local.yaml > /dev/null
helm template twenty packages/twenty-docker/helm/twenty -f infra/twenty/values-prod.yaml > /dev/null
```

Expected: no errors from either template.

- [ ] **Step 4: Commit**

```bash
git add infra/twenty/values-local.yaml infra/twenty/values-prod.yaml
git commit -m "feat(infra): add Twenty Helm value overlays for local and production"
```

---

### Task 9: HPA Manifests

**Files:**
- Create: `infra/twenty/hpa-server.yaml`
- Create: `infra/twenty/hpa-worker.yaml`

- [ ] **Step 1: Create server HPA**

```yaml
# infra/twenty/hpa-server.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: twenty-server
  namespace: twenty
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: twenty-server
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60
```

- [ ] **Step 2: Create worker HPA**

```yaml
# infra/twenty/hpa-worker.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: twenty-worker
  namespace: twenty
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: twenty-worker
  minReplicas: 2
  maxReplicas: 6
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: twenty_queue_jobs_waiting_total
        target:
          type: AverageValue
          averageValue: "100"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 30
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60
```

- [ ] **Step 3: Validate manifests**

```bash
kubectl apply --dry-run=client -f infra/twenty/hpa-server.yaml
kubectl apply --dry-run=client -f infra/twenty/hpa-worker.yaml
```

Expected: both created (dry run).

- [ ] **Step 4: Commit**

```bash
git add infra/twenty/hpa-server.yaml infra/twenty/hpa-worker.yaml
git commit -m "feat(infra): add HPA for server (CPU/mem) and worker (CPU/queue depth)"
```

---

### Task 10: PodDisruptionBudgets

**Files:**
- Create: `infra/twenty/pdb-server.yaml`
- Create: `infra/twenty/pdb-worker.yaml`

- [ ] **Step 1: Create server PDB**

```yaml
# infra/twenty/pdb-server.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: twenty-server
  namespace: twenty
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: twenty
      app.kubernetes.io/component: server
```

- [ ] **Step 2: Create worker PDB**

```yaml
# infra/twenty/pdb-worker.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: twenty-worker
  namespace: twenty
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: twenty
      app.kubernetes.io/component: worker
```

- [ ] **Step 3: Validate and commit**

```bash
kubectl apply --dry-run=client -f infra/twenty/pdb-server.yaml
kubectl apply --dry-run=client -f infra/twenty/pdb-worker.yaml
git add infra/twenty/pdb-server.yaml infra/twenty/pdb-worker.yaml
git commit -m "feat(infra): add PDBs for server and worker (minAvailable: 1)"
```

---

## Phase 4: Observability (Wave 3)

### Task 11: Metrics Services and ServiceMonitors

**Files:**
- Create: `infra/twenty/service-server-metrics.yaml`
- Create: `infra/twenty/service-worker-metrics.yaml`
- Create: `infra/monitoring/servicemonitor-twenty-server.yaml`
- Create: `infra/monitoring/servicemonitor-twenty-worker.yaml`
- Create: `infra/monitoring/servicemonitor-keydb.yaml`

- [ ] **Step 1: Create metrics Services for Twenty pods**

The upstream Helm chart Service doesn't expose port 9464 (Prometheus metrics). Create dedicated metrics Services that select the same pods.

```yaml
# infra/twenty/service-server-metrics.yaml
apiVersion: v1
kind: Service
metadata:
  name: twenty-server-metrics
  namespace: twenty
  labels:
    app.kubernetes.io/name: twenty
    app.kubernetes.io/component: server-metrics
spec:
  type: ClusterIP
  ports:
    - name: metrics
      port: 9464
      targetPort: 9464
      protocol: TCP
  selector:
    app.kubernetes.io/name: twenty
    app.kubernetes.io/component: server
```

```yaml
# infra/twenty/service-worker-metrics.yaml
apiVersion: v1
kind: Service
metadata:
  name: twenty-worker-metrics
  namespace: twenty
  labels:
    app.kubernetes.io/name: twenty
    app.kubernetes.io/component: worker-metrics
spec:
  type: ClusterIP
  ports:
    - name: metrics
      port: 9464
      targetPort: 9464
      protocol: TCP
  selector:
    app.kubernetes.io/name: twenty
    app.kubernetes.io/component: worker
```

- [ ] **Step 2: Create ServiceMonitors**

```yaml
# infra/monitoring/servicemonitor-twenty-server.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: twenty-server
  namespace: twenty
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: server-metrics
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

```yaml
# infra/monitoring/servicemonitor-twenty-worker.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: twenty-worker
  namespace: twenty
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: worker-metrics
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

```yaml
# infra/monitoring/servicemonitor-keydb.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: twenty-keydb
  namespace: twenty
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: twenty-keydb
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

- [ ] **Step 3: Validate all manifests**

```bash
kubectl apply --dry-run=client -f infra/twenty/service-server-metrics.yaml
kubectl apply --dry-run=client -f infra/twenty/service-worker-metrics.yaml
kubectl apply --dry-run=client -f infra/monitoring/servicemonitor-twenty-server.yaml
kubectl apply --dry-run=client -f infra/monitoring/servicemonitor-twenty-worker.yaml
kubectl apply --dry-run=client -f infra/monitoring/servicemonitor-keydb.yaml
```

Expected: all created (dry run).

- [ ] **Step 4: Commit**

```bash
git add infra/twenty/service-server-metrics.yaml infra/twenty/service-worker-metrics.yaml infra/monitoring/servicemonitor-*.yaml
git commit -m "feat(infra): add metrics Services and ServiceMonitors for server, worker, KeyDB"
```

---

### Task 12: Grafana Dashboard ConfigMaps

**Files:**
- Create: `infra/monitoring/dashboards/twenty-overview.yaml`
- Create: `infra/monitoring/dashboards/postgresql-health.yaml`
- Create: `infra/monitoring/dashboards/keydb-health.yaml`
- Create: `infra/monitoring/dashboards/bullmq-queues.yaml`
- Create: `infra/monitoring/dashboards/workflow-sync.yaml`

Dashboards are deployed as ConfigMaps with label `grafana_dashboard: "1"`. The Grafana sidecar (from kube-prometheus-stack) auto-discovers them.

- [ ] **Step 1: Create Twenty Overview dashboard**

```yaml
# infra/monitoring/dashboards/twenty-overview.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-twenty-overview
  namespace: twenty
  labels:
    grafana_dashboard: "1"
data:
  twenty-overview.json: |
    {
      "title": "Twenty Overview",
      "uid": "twenty-overview",
      "tags": ["twenty"],
      "timezone": "browser",
      "refresh": "30s",
      "time": {"from": "now-1h", "to": "now"},
      "templating": {
        "list": [
          {"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "Prometheus"}}
        ]
      },
      "panels": [
        {
          "type": "stat",
          "title": "Server Pods",
          "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "count(up{job=\"twenty-server\"} == 1)", "refId": "A"}]
        },
        {
          "type": "stat",
          "title": "Worker Pods",
          "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "count(up{job=\"twenty-worker\"} == 1)", "refId": "A"}]
        },
        {
          "type": "stat",
          "title": "Queue Depth",
          "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "sum(twenty_queue_jobs_waiting_total)", "refId": "A"}]
        },
        {
          "type": "stat",
          "title": "GraphQL Error Rate",
          "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "sum(rate(graphql_operation_500_total[5m])) / (sum(rate(graphql_operation_200_total[5m])) + sum(rate(graphql_operation_400_total[5m])) + sum(rate(graphql_operation_500_total[5m])) + 0.001)", "refId": "A"}],
          "fieldConfig": {"defaults": {"unit": "percentunit", "thresholds": {"steps": [{"value": 0, "color": "green"}, {"value": 0.01, "color": "yellow"}, {"value": 0.05, "color": "red"}]}}}
        },
        {
          "type": "timeseries",
          "title": "GraphQL Operations by Status",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "sum(rate(graphql_operation_200_total[5m]))", "legendFormat": "200 OK", "refId": "A"},
            {"expr": "sum(rate(graphql_operation_400_total[5m]))", "legendFormat": "400 Error", "refId": "B"},
            {"expr": "sum(rate(graphql_operation_500_total[5m]))", "legendFormat": "500 Error", "refId": "C"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Jobs Completed/Failed Rate",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "sum(rate(job_completed_total[5m]))", "legendFormat": "Completed", "refId": "A"},
            {"expr": "sum(rate(job_failed_total[5m]))", "legendFormat": "Failed", "refId": "B"}
          ]
        }
      ]
    }
```

- [ ] **Step 2: Create PostgreSQL Health dashboard**

```yaml
# infra/monitoring/dashboards/postgresql-health.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-postgresql-health
  namespace: twenty
  labels:
    grafana_dashboard: "1"
data:
  postgresql-health.json: |
    {
      "title": "PostgreSQL Health",
      "uid": "twenty-pg-health",
      "tags": ["twenty", "postgresql"],
      "timezone": "browser",
      "refresh": "30s",
      "time": {"from": "now-1h", "to": "now"},
      "templating": {
        "list": [
          {"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "Prometheus"}}
        ]
      },
      "panels": [
        {
          "type": "timeseries",
          "title": "Active Connections",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "cnpg_pg_stat_activity_count{datname=\"twenty\"}", "legendFormat": "{{pod}} - {{state}}", "refId": "A"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Replication Lag (seconds)",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "cnpg_pg_replication_lag{datname=\"twenty\"}", "legendFormat": "{{pod}}", "refId": "A"}
          ],
          "fieldConfig": {"defaults": {"unit": "s", "thresholds": {"steps": [{"value": 0, "color": "green"}, {"value": 10, "color": "yellow"}, {"value": 30, "color": "red"}]}}}
        },
        {
          "type": "timeseries",
          "title": "Transactions per Second",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(cnpg_pg_stat_database_xact_commit_total{datname=\"twenty\"}[5m])", "legendFormat": "{{pod}} commits/s", "refId": "A"},
            {"expr": "rate(cnpg_pg_stat_database_xact_rollback_total{datname=\"twenty\"}[5m])", "legendFormat": "{{pod}} rollbacks/s", "refId": "B"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Cache Hit Ratio",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(cnpg_pg_stat_database_blks_hit_total{datname=\"twenty\"}[5m]) / (rate(cnpg_pg_stat_database_blks_hit_total{datname=\"twenty\"}[5m]) + rate(cnpg_pg_stat_database_blks_read_total{datname=\"twenty\"}[5m]) + 0.001)", "legendFormat": "{{pod}}", "refId": "A"}
          ],
          "fieldConfig": {"defaults": {"unit": "percentunit", "min": 0, "max": 1}}
        },
        {
          "type": "stat",
          "title": "Database Size",
          "gridPos": {"h": 4, "w": 6, "x": 0, "y": 16},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "cnpg_pg_database_size_bytes{datname=\"twenty\"}", "refId": "A"}],
          "fieldConfig": {"defaults": {"unit": "bytes"}}
        },
        {
          "type": "table",
          "title": "Top Slow Queries (pg_stat_statements)",
          "gridPos": {"h": 8, "w": 24, "x": 0, "y": 20},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "topk(10, cnpg_pg_stat_statements_top_mean_exec_time{datname=\"twenty\"})", "legendFormat": "{{query_short}}", "refId": "A", "format": "table", "instant": true}
          ]
        }
      ]
    }
```

- [ ] **Step 3: Create KeyDB Health dashboard**

```yaml
# infra/monitoring/dashboards/keydb-health.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-keydb-health
  namespace: twenty
  labels:
    grafana_dashboard: "1"
data:
  keydb-health.json: |
    {
      "title": "KeyDB Health",
      "uid": "twenty-keydb-health",
      "tags": ["twenty", "keydb"],
      "timezone": "browser",
      "refresh": "30s",
      "time": {"from": "now-1h", "to": "now"},
      "templating": {
        "list": [
          {"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "Prometheus"}}
        ]
      },
      "panels": [
        {
          "type": "timeseries",
          "title": "Memory Usage",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "redis_memory_used_bytes{job=\"twenty-keydb\"}", "legendFormat": "{{pod}} used", "refId": "A"},
            {"expr": "redis_memory_max_bytes{job=\"twenty-keydb\"}", "legendFormat": "{{pod}} max", "refId": "B"}
          ],
          "fieldConfig": {"defaults": {"unit": "bytes"}}
        },
        {
          "type": "timeseries",
          "title": "Operations per Second",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(redis_commands_processed_total{job=\"twenty-keydb\"}[5m])", "legendFormat": "{{pod}}", "refId": "A"}
          ],
          "fieldConfig": {"defaults": {"unit": "ops"}}
        },
        {
          "type": "timeseries",
          "title": "Connected Clients",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "redis_connected_clients{job=\"twenty-keydb\"}", "legendFormat": "{{pod}}", "refId": "A"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Cache Hit Rate",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(redis_keyspace_hits_total{job=\"twenty-keydb\"}[5m]) / (rate(redis_keyspace_hits_total{job=\"twenty-keydb\"}[5m]) + rate(redis_keyspace_misses_total{job=\"twenty-keydb\"}[5m]) + 0.001)", "legendFormat": "{{pod}}", "refId": "A"}
          ],
          "fieldConfig": {"defaults": {"unit": "percentunit"}}
        }
      ]
    }
```

- [ ] **Step 4: Create BullMQ Queues dashboard**

```yaml
# infra/monitoring/dashboards/bullmq-queues.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-bullmq-queues
  namespace: twenty
  labels:
    grafana_dashboard: "1"
data:
  bullmq-queues.json: |
    {
      "title": "BullMQ Queues",
      "uid": "twenty-bullmq-queues",
      "tags": ["twenty", "bullmq"],
      "timezone": "browser",
      "refresh": "30s",
      "time": {"from": "now-1h", "to": "now"},
      "templating": {
        "list": [
          {"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "Prometheus"}},
          {"name": "queue", "type": "query", "query": "label_values(job_completed_total, queue)", "datasource": {"type": "prometheus", "uid": "${datasource}"}, "multi": true, "includeAll": true}
        ]
      },
      "panels": [
        {
          "type": "stat",
          "title": "Total Queue Depth",
          "gridPos": {"h": 4, "w": 8, "x": 0, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "sum(twenty_queue_jobs_waiting_total)", "refId": "A"}],
          "fieldConfig": {"defaults": {"thresholds": {"steps": [{"value": 0, "color": "green"}, {"value": 500, "color": "yellow"}, {"value": 1000, "color": "red"}]}}}
        },
        {
          "type": "stat",
          "title": "Jobs Completed/min",
          "gridPos": {"h": 4, "w": 8, "x": 8, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "sum(rate(job_completed_total[5m])) * 60", "refId": "A"}]
        },
        {
          "type": "stat",
          "title": "Jobs Failed/min",
          "gridPos": {"h": 4, "w": 8, "x": 16, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [{"expr": "sum(rate(job_failed_total[5m])) * 60", "refId": "A"}],
          "fieldConfig": {"defaults": {"thresholds": {"steps": [{"value": 0, "color": "green"}, {"value": 1, "color": "yellow"}, {"value": 10, "color": "red"}]}}}
        },
        {
          "type": "timeseries",
          "title": "Jobs Completed by Queue",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "sum by (queue) (rate(job_completed_total{queue=~\"$queue\"}[5m]))", "legendFormat": "{{queue}}", "refId": "A"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Jobs Failed by Queue",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "sum by (queue) (rate(job_failed_total{queue=~\"$queue\"}[5m]))", "legendFormat": "{{queue}}", "refId": "A"}
          ]
        }
      ]
    }
```

- [ ] **Step 5: Create Workflow & Sync dashboard**

```yaml
# infra/monitoring/dashboards/workflow-sync.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-workflow-sync
  namespace: twenty
  labels:
    grafana_dashboard: "1"
data:
  workflow-sync.json: |
    {
      "title": "Workflow & Sync",
      "uid": "twenty-workflow-sync",
      "tags": ["twenty", "workflow"],
      "timezone": "browser",
      "refresh": "30s",
      "time": {"from": "now-1h", "to": "now"},
      "templating": {
        "list": [
          {"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "Prometheus"}}
        ]
      },
      "panels": [
        {
          "type": "timeseries",
          "title": "Workflow Run Rate",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "sum(rate(workflow_run_completed_total[5m]))", "legendFormat": "Completed", "refId": "A"},
            {"expr": "sum(rate(workflow_run_failed_total[5m]))", "legendFormat": "Failed", "refId": "B"},
            {"expr": "sum(rate(workflow_run_stopped_total[5m]))", "legendFormat": "Stopped", "refId": "C"},
            {"expr": "sum(rate(workflow_run_throttled_total[5m]))", "legendFormat": "Throttled", "refId": "D"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Workflow Starts by Trigger",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(workflow_run_started_database_event_total[5m])", "legendFormat": "Database Event", "refId": "A"},
            {"expr": "rate(workflow_run_started_cron_total[5m])", "legendFormat": "Cron", "refId": "B"},
            {"expr": "rate(workflow_run_started_webhook_total[5m])", "legendFormat": "Webhook", "refId": "C"},
            {"expr": "rate(workflow_run_started_manual_total[5m])", "legendFormat": "Manual", "refId": "D"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Message Sync Activity",
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(message_channel_sync_job_active_total[5m])", "legendFormat": "Active", "refId": "A"},
            {"expr": "rate(message_channel_sync_job_failed_unknown_total[5m])", "legendFormat": "Failed", "refId": "B"}
          ]
        },
        {
          "type": "timeseries",
          "title": "Calendar Sync Activity",
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
          "datasource": {"type": "prometheus", "uid": "${datasource}"},
          "targets": [
            {"expr": "rate(calendar_event_sync_job_active_total[5m])", "legendFormat": "Active", "refId": "A"},
            {"expr": "rate(calendar_event_sync_job_failed_unknown_total[5m])", "legendFormat": "Failed", "refId": "B"}
          ]
        }
      ]
    }
```

- [ ] **Step 6: Validate all ConfigMaps**

```bash
for f in infra/monitoring/dashboards/*.yaml; do kubectl apply --dry-run=client -f "$f"; done
```

Expected: all created (dry run).

- [ ] **Step 7: Commit**

```bash
git add infra/monitoring/dashboards/
git commit -m "feat(infra): add 5 Grafana dashboard ConfigMaps (overview, PG, KeyDB, BullMQ, workflow)"
```

---

### Task 13: PrometheusRules (Alerting)

**Files:**
- Create: `infra/monitoring/prometheusrules.yaml`

- [ ] **Step 1: Create alerting rules**

```yaml
# infra/monitoring/prometheusrules.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: twenty-alerts
  namespace: twenty
  labels:
    release: kube-prometheus-stack
spec:
  groups:
    - name: twenty.database
      rules:
        - alert: PostgreSQLReplicationLagHigh
          expr: cnpg_pg_replication_lag > 30
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "PG replication lag > 30s on {{ $labels.pod }}"
            description: "Replication lag is {{ $value }}s. Check network and replica health."

        - alert: PgBouncerPoolUtilizationHigh
          expr: cnpg_pgbouncer_pools_server_active / cnpg_pgbouncer_pools_server_idle_timeout > 0.8
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "PgBouncer pool > 80% utilized on {{ $labels.pod }}"

    - name: twenty.keydb
      rules:
        - alert: KeyDBMemoryHigh
          expr: redis_memory_used_bytes{job="twenty-keydb"} / redis_memory_max_bytes{job="twenty-keydb"} > 0.9
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "KeyDB memory > 90% on {{ $labels.pod }}"
            description: "Memory usage is {{ $value | humanizePercentage }}. Check for memory leaks or increase limits."

    - name: twenty.application
      rules:
        - alert: QueueDepthHigh
          expr: sum(twenty_queue_jobs_waiting_total) > 1000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Queue depth > 1000 for 5 minutes"
            description: "{{ $value }} jobs waiting. Workers may be overwhelmed."

        - alert: PodRestartsTooFrequent
          expr: increase(kube_pod_container_status_restarts_total{namespace="twenty"}[10m]) > 3
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "{{ $labels.pod }} restarted > 3 times in 10 minutes"

        - alert: NoHealthyServerPods
          expr: count(up{job="twenty-server"} == 1) == 0
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "Zero healthy Twenty server pods"
            description: "All server pods are down. Immediate attention required."

        - alert: GraphQLErrorRateHigh
          expr: sum(rate(graphql_operation_500_total[5m])) / (sum(rate(graphql_operation_200_total[5m])) + sum(rate(graphql_operation_400_total[5m])) + sum(rate(graphql_operation_500_total[5m])) + 0.001) > 0.05
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "GraphQL 500 error rate > 5%"
```

- [ ] **Step 2: Validate and commit**

```bash
kubectl apply --dry-run=client -f infra/monitoring/prometheusrules.yaml
git add infra/monitoring/prometheusrules.yaml
git commit -m "feat(infra): add PrometheusRules for DB, KeyDB, queue, and application alerts"
```

---

### Task 14: Local Monitoring Stack

**Files:**
- Create: `infra/monitoring/kube-prometheus-stack-values.yaml`

- [ ] **Step 1: Create kube-prometheus-stack values for local dev**

```yaml
# infra/monitoring/kube-prometheus-stack-values.yaml
# kube-prometheus-stack Helm values for local development
# Chart: prometheus-community/kube-prometheus-stack
# Repo: https://prometheus-community.github.io/helm-charts
#
# Install:
#   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
#   helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring --create-namespace -f infra/monitoring/kube-prometheus-stack-values.yaml --wait

prometheus:
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false
    podMonitorSelectorNilUsesHelmValues: false
    ruleSelectorNilUsesHelmValues: false
    serviceMonitorNamespaceSelector: {}
    podMonitorNamespaceSelector: {}
    ruleNamespaceSelector: {}
    retention: 7d
    storageSpec:
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 10Gi
    remoteWrite: []
    enableRemoteWriteReceiver: true
    resources:
      requests:
        cpu: 200m
        memory: 512Mi
      limits:
        cpu: "1"
        memory: 2Gi

grafana:
  enabled: true
  adminPassword: admin
  sidecar:
    dashboards:
      enabled: true
      searchNamespace: ALL
      label: grafana_dashboard
      folderAnnotation: grafana_folder
      provider:
        foldersFromFilesStructure: false
  service:
    type: NodePort
    nodePort: 30300
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi

alertmanager:
  enabled: true

nodeExporter:
  enabled: true

kubeStateMetrics:
  enabled: true
```

- [ ] **Step 2: Validate and commit**

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm template monitoring prometheus-community/kube-prometheus-stack -f infra/monitoring/kube-prometheus-stack-values.yaml > /dev/null
git add infra/monitoring/kube-prometheus-stack-values.yaml
git commit -m "feat(infra): add kube-prometheus-stack values for local monitoring"
```

---

## Phase 5: SQL Optimizations

### Task 15: SQL Optimization Scripts and K8s Job

**Files:**
- Create: `infra/sql/core-optimizations.sql`
- Create: `infra/sql/workspace-optimizations.sql`
- Create: `infra/sql/optimization-job.yaml`

- [ ] **Step 1: Create core schema optimization SQL**

```sql
-- infra/sql/core-optimizations.sql
-- Core schema optimizations (run against "twenty" database after Twenty migrations)
-- Idempotent: safe to re-run

-- Missing FK indexes on app_token
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_token_user_id
  ON core."appToken" ("userId");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_token_workspace_id
  ON core."appToken" ("workspaceId");

-- Autovacuum tuning for high-churn core tables
ALTER TABLE core."appToken" SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
```

- [ ] **Step 2: Create workspace schema optimization SQL**

This SQL uses `__SCHEMA__` as a placeholder that the Job replaces with the actual workspace schema name.

```sql
-- infra/sql/workspace-optimizations.sql
-- Workspace schema optimizations (run per workspace schema)
-- Uses __SCHEMA__ placeholder — replaced at runtime by the Job
-- Idempotent: safe to re-run

-- 6b. BRIN indexes on timestamps (append-only tables)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_message_created_brin
  ON __SCHEMA__."message" USING BRIN ("createdAt") WITH (pages_per_range = 128);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_created_brin
  ON __SCHEMA__."workflowRun" USING BRIN ("createdAt") WITH (pages_per_range = 128);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_timeline_activity_created_brin
  ON __SCHEMA__."timelineActivity" USING BRIN ("createdAt") WITH (pages_per_range = 128);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_calendar_event_created_brin
  ON __SCHEMA__."calendarEvent" USING BRIN ("createdAt") WITH (pages_per_range = 128);

-- 6c. pg_trgm GIN indexes for ILIKE fallback
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_search_trgm
  ON __SCHEMA__."company" USING GIN (("searchVector"::text) gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_person_search_trgm
  ON __SCHEMA__."person" USING GIN (("searchVector"::text) gin_trgm_ops);

-- 6d. Expression index for unaccent_immutable
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_name_unaccent
  ON __SCHEMA__."company" USING btree (public.unaccent_immutable(name));

-- 6e. FILLFACTOR tuning (NOT supported on YugabyteDB — skip if running distributed DB)
ALTER TABLE __SCHEMA__."workflowRun" SET (fillfactor = 70);
ALTER TABLE __SCHEMA__."messageChannel" SET (fillfactor = 75);

-- 6f. JSONB GIN index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_state_gin
  ON __SCHEMA__."workflowRun" USING GIN (state jsonb_path_ops);

-- 6g. Autovacuum tuning
ALTER TABLE __SCHEMA__."message" SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.005
);

ALTER TABLE __SCHEMA__."workflowRun" SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
```

- [ ] **Step 3: Create K8s Job that applies optimizations**

```yaml
# infra/sql/optimization-job.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: twenty-sql-optimizations
  namespace: twenty
data:
  core-optimizations.sql: |
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_token_user_id
      ON core."appToken" ("userId");
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_token_workspace_id
      ON core."appToken" ("workspaceId");
    ALTER TABLE core."appToken" SET (
      autovacuum_vacuum_scale_factor = 0.02,
      autovacuum_analyze_scale_factor = 0.01
    );
  workspace-optimizations.sql: |
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_message_created_brin
      ON __SCHEMA__."message" USING BRIN ("createdAt") WITH (pages_per_range = 128);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_created_brin
      ON __SCHEMA__."workflowRun" USING BRIN ("createdAt") WITH (pages_per_range = 128);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_timeline_activity_created_brin
      ON __SCHEMA__."timelineActivity" USING BRIN ("createdAt") WITH (pages_per_range = 128);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_calendar_event_created_brin
      ON __SCHEMA__."calendarEvent" USING BRIN ("createdAt") WITH (pages_per_range = 128);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_search_trgm
      ON __SCHEMA__."company" USING GIN (("searchVector"::text) gin_trgm_ops);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_person_search_trgm
      ON __SCHEMA__."person" USING GIN (("searchVector"::text) gin_trgm_ops);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_name_unaccent
      ON __SCHEMA__."company" USING btree (public.unaccent_immutable(name));
    ALTER TABLE __SCHEMA__."workflowRun" SET (fillfactor = 70);
    ALTER TABLE __SCHEMA__."messageChannel" SET (fillfactor = 75);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_state_gin
      ON __SCHEMA__."workflowRun" USING GIN (state jsonb_path_ops);
    ALTER TABLE __SCHEMA__."message" SET (
      autovacuum_vacuum_scale_factor = 0.01,
      autovacuum_analyze_scale_factor = 0.005
    );
    ALTER TABLE __SCHEMA__."workflowRun" SET (
      autovacuum_vacuum_scale_factor = 0.02,
      autovacuum_analyze_scale_factor = 0.01
    );
  run.sh: |
    #!/bin/bash
    set -e

    echo "=== Twenty SQL Optimization Job ==="
    echo "Connecting to: $PGHOST:$PGPORT/$PGDATABASE"

    echo "--- Running core optimizations ---"
    psql -f /sql/core-optimizations.sql 2>&1 || echo "WARN: Some core optimizations may have failed (non-fatal)"

    echo "--- Discovering workspace schemas ---"
    SCHEMAS=$(psql -Atc "SELECT \"dataSourceMetadata\"->'schema' FROM core.\"dataSourceMetadata\" WHERE \"type\" = 'workspace'" | tr -d '"' | grep -v '^$')

    if [ -z "$SCHEMAS" ]; then
      echo "No workspace schemas found. Skipping workspace optimizations."
      exit 0
    fi

    echo "Found workspace schemas: $SCHEMAS"

    for SCHEMA in $SCHEMAS; do
      echo "--- Optimizing workspace: $SCHEMA ---"
      sed "s/__SCHEMA__/$SCHEMA/g" /sql/workspace-optimizations.sql | psql 2>&1 || echo "WARN: Some optimizations failed for $SCHEMA (non-fatal)"
    done

    echo "=== Optimization Job Complete ==="
---
apiVersion: batch/v1
kind: Job
metadata:
  name: twenty-sql-optimizations
  namespace: twenty
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: optimize
          image: postgres:17-alpine
          command: ["bash", "/sql/run.sh"]
          env:
            - name: PGHOST
              value: twenty-db-rw
            - name: PGPORT
              value: "5432"
            - name: PGDATABASE
              value: twenty
            - name: PGUSER
              valueFrom:
                secretKeyRef:
                  name: twenty-db-superuser
                  key: username
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: twenty-db-superuser
                  key: password
          volumeMounts:
            - name: sql
              mountPath: /sql
      volumes:
        - name: sql
          configMap:
            name: twenty-sql-optimizations
            defaultMode: 0755
```

- [ ] **Step 4: Validate and commit**

```bash
kubectl apply --dry-run=client -f infra/sql/optimization-job.yaml
git add infra/sql/
git commit -m "feat(infra): add SQL optimization scripts and K8s Job (core + workspace)"
```

---

### Task 16: Materialized View Refresh CronJob

**Files:**
- Create: `infra/sql/mv-refresh-cronjob.yaml`

- [ ] **Step 1: Create MV refresh CronJob**

```yaml
# infra/sql/mv-refresh-cronjob.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: twenty-mv-refresh
  namespace: twenty
data:
  refresh.sh: |
    #!/bin/bash
    set -e

    echo "=== Materialized View Refresh ==="

    SCHEMAS=$(psql -Atc "SELECT \"dataSourceMetadata\"->'schema' FROM core.\"dataSourceMetadata\" WHERE \"type\" = 'workspace'" | tr -d '"' | grep -v '^$')

    for SCHEMA in $SCHEMAS; do
      echo "--- Refreshing MVs for $SCHEMA ---"

      # Create MVs if they don't exist, then refresh
      psql <<EOSQL
      CREATE MATERIALIZED VIEW IF NOT EXISTS ${SCHEMA}."mv_opportunity_pipeline" AS
        SELECT
          stage,
          COUNT(*) as deal_count,
          SUM(CAST(amount AS numeric)) as total_value,
          AVG(CAST(amount AS numeric)) as avg_value
        FROM ${SCHEMA}."opportunity"
        WHERE "deletedAt" IS NULL
        GROUP BY stage;

      CREATE MATERIALIZED VIEW IF NOT EXISTS ${SCHEMA}."mv_company_stats" AS
        SELECT
          COUNT(*) as total_companies,
          COUNT(CASE WHEN "deletedAt" IS NULL THEN 1 END) as active_companies,
          DATE_TRUNC('month', "createdAt") as month
        FROM ${SCHEMA}."company"
        GROUP BY DATE_TRUNC('month', "createdAt");

      REFRESH MATERIALIZED VIEW ${SCHEMA}."mv_opportunity_pipeline";
      REFRESH MATERIALIZED VIEW ${SCHEMA}."mv_company_stats";
    EOSQL
      echo "Done: $SCHEMA"
    done

    echo "=== MV Refresh Complete ==="
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: twenty-mv-refresh
  namespace: twenty
spec:
  schedule: "*/15 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: refresh
              image: postgres:17-alpine
              command: ["bash", "/scripts/refresh.sh"]
              env:
                - name: PGHOST
                  value: twenty-db-rw
                - name: PGPORT
                  value: "5432"
                - name: PGDATABASE
                  value: twenty
                - name: PGUSER
                  valueFrom:
                    secretKeyRef:
                      name: twenty-db-superuser
                      key: username
                - name: PGPASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: twenty-db-superuser
                      key: password
              volumeMounts:
                - name: scripts
                  mountPath: /scripts
          volumes:
            - name: scripts
              configMap:
                name: twenty-mv-refresh
                defaultMode: 0755
```

- [ ] **Step 2: Validate and commit**

```bash
kubectl apply --dry-run=client -f infra/sql/mv-refresh-cronjob.yaml
git add infra/sql/mv-refresh-cronjob.yaml
git commit -m "feat(infra): add materialized view refresh CronJob (every 15 min)"
```

---

## Phase 6: Deployment Orchestration

### Task 17: ArgoCD App-of-Apps

**Files:**
- Create: `infra/argocd/app-of-apps.yaml`
- Create: `infra/argocd/applications/cnpg-operator.yaml`
- Create: `infra/argocd/applications/database.yaml`
- Create: `infra/argocd/applications/keydb.yaml`
- Create: `infra/argocd/applications/minio.yaml`
- Create: `infra/argocd/applications/twenty.yaml`
- Create: `infra/argocd/applications/monitoring.yaml`

- [ ] **Step 1: Create App-of-Apps root**

```yaml
# infra/argocd/app-of-apps.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty-infra
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/twenty.git  # REPLACE with your repo
    targetRevision: main
    path: infra/argocd/applications
  destination:
    server: https://kubernetes.default.svc
    namespace: argocd
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

- [ ] **Step 2: Create Wave 0 — CNPG Operator**

```yaml
# infra/argocd/applications/cnpg-operator.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: cnpg-operator
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "0"
spec:
  project: default
  source:
    repoURL: https://cloudnative-pg.github.io/charts
    chart: cloudnative-pg
    targetRevision: "*"
    helm:
      valueFiles: []
      values: |
        replicaCount: 1
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
  destination:
    server: https://kubernetes.default.svc
    namespace: cnpg-system
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 3: Create Wave 1 — Database**

```yaml
# infra/argocd/applications/database.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty-database
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/twenty.git
    targetRevision: main
    path: infra/database
  destination:
    server: https://kubernetes.default.svc
    namespace: twenty
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 4: Create Wave 1 — KeyDB**

```yaml
# infra/argocd/applications/keydb.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty-keydb
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/twenty.git
    targetRevision: main
    path: infra/keydb
  destination:
    server: https://kubernetes.default.svc
    namespace: twenty
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 5: Create Wave 1 — MinIO (local only, not included in prod app-of-apps)**

```yaml
# infra/argocd/applications/minio.yaml
# Only include this Application in local/dev environments
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty-minio
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://charts.min.io/
    chart: minio
    targetRevision: "*"
    helm:
      valueFiles: []
      valuesObject:
        mode: standalone
        replicas: 1
        persistence:
          enabled: true
          size: 10Gi
        rootUser: minioadmin
        rootPassword: minioadmin123
        buckets:
          - name: twenty-backups
            policy: none
          - name: twenty-files
            policy: none
  destination:
    server: https://kubernetes.default.svc
    namespace: minio
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 6: Create Wave 2 — Twenty**

```yaml
# infra/argocd/applications/twenty.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "2"
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/twenty.git
    targetRevision: main
    path: packages/twenty-docker/helm/twenty
    helm:
      valueFiles:
        - ../../../../infra/twenty/values-prod.yaml  # Change to values-local.yaml for dev
  destination:
    server: https://kubernetes.default.svc
    namespace: twenty
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 7: Create Wave 3 — Monitoring**

```yaml
# infra/argocd/applications/monitoring.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: twenty-monitoring
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "3"
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/twenty.git
    targetRevision: main
    path: infra/monitoring
    directory:
      include: "{servicemonitor-*.yaml,prometheusrules.yaml,dashboards/*.yaml}"
  destination:
    server: https://kubernetes.default.svc
    namespace: twenty
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

- [ ] **Step 8: Commit**

```bash
git add infra/argocd/
git commit -m "feat(infra): add ArgoCD App-of-Apps with sync waves 0-3"
```

---

### Task 18: Local Setup Script

**Files:**
- Create: `infra/scripts/local-setup.sh`

- [ ] **Step 1: Create local setup script**

```bash
#!/usr/bin/env bash
# infra/scripts/local-setup.sh
# Deploys the full Twenty scalability stack on local Kubernetes (Docker Desktop)
#
# Usage:
#   bash infra/scripts/local-setup.sh          # Deploy everything
#   bash infra/scripts/local-setup.sh --down    # Tear down everything

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$INFRA_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err() { echo -e "${RED}[x]${NC} $1"; exit 1; }

wait_for_pods() {
  local namespace=$1
  local label=$2
  local timeout=${3:-300}
  log "Waiting for pods ($label) in $namespace (timeout: ${timeout}s)..."
  kubectl wait --for=condition=ready pod -l "$label" -n "$namespace" --timeout="${timeout}s" 2>/dev/null || {
    warn "Pods not ready after ${timeout}s. Checking status..."
    kubectl get pods -l "$label" -n "$namespace"
    return 1
  }
}

teardown() {
  log "Tearing down Twenty infrastructure..."
  helm uninstall twenty -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/sql/optimization-job.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/sql/mv-refresh-cronjob.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/prometheusrules.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/dashboards/" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-twenty-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-twenty-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-keydb.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/service-server-metrics.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/service-worker-metrics.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/hpa-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/hpa-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/pdb-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/pdb-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/keydb/" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/scheduled-backup.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/objectstore-local.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/pooler-rw.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/pooler-ro.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/monitoring-queries.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/cluster.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  helm uninstall minio -n minio 2>/dev/null || true
  helm uninstall monitoring -n monitoring 2>/dev/null || true
  helm uninstall cnpg-operator -n cnpg-system 2>/dev/null || true
  log "Teardown complete."
  exit 0
}

if [[ "${1:-}" == "--down" ]]; then
  teardown
fi

# Preflight
command -v kubectl >/dev/null || err "kubectl not found"
command -v helm >/dev/null || err "helm not found"
kubectl cluster-info >/dev/null 2>&1 || err "No Kubernetes cluster available"

# Add Helm repos
log "Adding Helm repositories..."
helm repo add cnpg https://cloudnative-pg.github.io/charts 2>/dev/null || true
helm repo add minio https://charts.min.io/ 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update

# Create namespaces
log "Creating namespaces..."
kubectl create namespace twenty --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace cnpg-system --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# === Wave 0: Operators ===
log "=== Wave 0: Installing operators ==="

log "Installing CloudNativePG operator..."
helm upgrade --install cnpg-operator cnpg/cloudnative-pg \
  -n cnpg-system \
  -f "$INFRA_DIR/operators/cnpg-values.yaml" \
  --wait --timeout 5m

log "Installing kube-prometheus-stack..."
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring \
  -f "$INFRA_DIR/monitoring/kube-prometheus-stack-values.yaml" \
  --wait --timeout 10m

# === Wave 1: Data Layer ===
log "=== Wave 1: Deploying data layer ==="

log "Installing MinIO..."
helm upgrade --install minio minio/minio \
  -n minio \
  -f "$INFRA_DIR/minio/values-local.yaml" \
  --wait --timeout 5m

log "Creating MinIO credentials secret for CNPG..."
kubectl create secret generic minio-creds \
  --namespace=twenty \
  --from-literal=ACCESS_KEY_ID=minioadmin \
  --from-literal=SECRET_ACCESS_KEY=minioadmin123 \
  --dry-run=client -o yaml | kubectl apply -f -

log "Deploying CloudNativePG Cluster..."
kubectl apply -f "$INFRA_DIR/database/monitoring-queries.yaml"
kubectl apply -f "$INFRA_DIR/database/cluster.yaml"

log "Waiting for CNPG cluster to be ready (this takes 2-5 minutes)..."
kubectl wait --for=condition=Ready cluster/twenty-db -n twenty --timeout=600s 2>/dev/null || {
  warn "Cluster not ready via condition check. Checking pods..."
  sleep 30
  kubectl get pods -n twenty -l cnpg.io/cluster=twenty-db
}

log "Deploying PgBouncer poolers..."
kubectl apply -f "$INFRA_DIR/database/pooler-rw.yaml"
kubectl apply -f "$INFRA_DIR/database/pooler-ro.yaml"
sleep 10

log "Deploying backup configuration..."
kubectl apply -f "$INFRA_DIR/database/objectstore-local.yaml"
kubectl apply -f "$INFRA_DIR/database/scheduled-backup.yaml"

log "Deploying KeyDB..."
kubectl apply -f "$INFRA_DIR/keydb/configmap.yaml"
kubectl apply -f "$INFRA_DIR/keydb/service-headless.yaml"
kubectl apply -f "$INFRA_DIR/keydb/service.yaml"
kubectl apply -f "$INFRA_DIR/keydb/statefulset.yaml"
kubectl apply -f "$INFRA_DIR/keydb/pdb.yaml"
wait_for_pods twenty "app.kubernetes.io/name=twenty-keydb" 120

# === Wave 2: Application Layer ===
log "=== Wave 2: Deploying Twenty ==="

helm upgrade --install twenty "$REPO_ROOT/packages/twenty-docker/helm/twenty" \
  -n twenty \
  -f "$INFRA_DIR/twenty/values-local.yaml" \
  --wait --timeout 10m

log "Applying HPA and PDB manifests..."
kubectl apply -f "$INFRA_DIR/twenty/hpa-server.yaml"
kubectl apply -f "$INFRA_DIR/twenty/hpa-worker.yaml"
kubectl apply -f "$INFRA_DIR/twenty/pdb-server.yaml"
kubectl apply -f "$INFRA_DIR/twenty/pdb-worker.yaml"

# === Wave 3: Observability ===
log "=== Wave 3: Deploying observability ==="

kubectl apply -f "$INFRA_DIR/twenty/service-server-metrics.yaml"
kubectl apply -f "$INFRA_DIR/twenty/service-worker-metrics.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-twenty-server.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-twenty-worker.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-keydb.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/prometheusrules.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/dashboards/"

# === Post-deployment ===
log "=== Running SQL optimizations ==="

log "Waiting for Twenty to initialize database..."
sleep 30

kubectl delete job twenty-sql-optimizations -n twenty --ignore-not-found 2>/dev/null || true
kubectl apply -f "$INFRA_DIR/sql/optimization-job.yaml"
kubectl wait --for=condition=complete job/twenty-sql-optimizations -n twenty --timeout=300s || {
  warn "SQL optimization job may have issues. Check logs:"
  echo "  kubectl logs job/twenty-sql-optimizations -n twenty"
}

kubectl apply -f "$INFRA_DIR/sql/mv-refresh-cronjob.yaml"

# === Summary ===
log "=== Deployment Complete ==="
echo ""
echo "Services:"
echo "  Twenty:    http://localhost:$(kubectl get svc twenty-server -n twenty -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo '3000')"
echo "  Grafana:   http://localhost:30300 (admin/admin)"
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n twenty"
echo "  kubectl get cluster -n twenty"
echo "  kubectl logs -f deploy/twenty-server -n twenty"
echo "  kubectl logs job/twenty-sql-optimizations -n twenty"
```

- [ ] **Step 2: Make script executable and commit**

```bash
chmod +x infra/scripts/local-setup.sh
git add infra/scripts/local-setup.sh
git commit -m "feat(infra): add local setup script for Docker Desktop K8s deployment"
```

---

## Phase 7: Validation

### Task 19: BullMQ + KeyDB Compatibility Validation

**Files:**
- Create: `infra/validation/bullmq-keydb-test/package.json`
- Create: `infra/validation/bullmq-keydb-test/test.js`

- [ ] **Step 1: Create package.json**

```json
{
  "name": "bullmq-keydb-validation",
  "version": "1.0.0",
  "private": true,
  "description": "Validates BullMQ compatibility with KeyDB",
  "scripts": {
    "test": "node test.js"
  },
  "dependencies": {
    "bullmq": "^5.0.0",
    "ioredis": "^5.0.0"
  }
}
```

- [ ] **Step 2: Create validation test script**

```javascript
// infra/validation/bullmq-keydb-test/test.js
// BullMQ + KeyDB compatibility validation
// Tests all patterns used by Twenty's 17 queues
//
// Usage:
//   REDIS_URL=redis://localhost:6379 node test.js
//   REDIS_URL=redis://twenty-keydb:6379 node test.js

const { Queue, Worker, QueueScheduler } = require('bullmq');
const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const connection = { url: REDIS_URL };

const TWENTY_QUEUES = [
  'calendar-queue', 'email-queue', 'message-queue',
  'webhook-queue', 'workflow-queue', 'contact-queue',
  'billing-queue', 'connected-account-queue', 'cron-queue',
  'data-seed-demo-workspace-queue', 'record-crud-queue',
  'search-queue', 'timeline-queue', 'workspace-queue',
  'duplicate-queue', 'company-queue', 'field-mapping-queue'
];

let passed = 0;
let failed = 0;

async function assert(name, fn) {
  try {
    await fn();
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (err) {
    console.log(`  ✗ ${name}: ${err.message}`);
    failed++;
  }
}

async function testQueueCreation() {
  console.log('\n1. Queue Creation (all 17 queues)');
  const queues = [];
  for (const name of TWENTY_QUEUES) {
    const q = new Queue(name, { connection });
    queues.push(q);
  }
  await assert('Create all 17 queues', async () => {
    if (queues.length !== 17) throw new Error(`Expected 17, got ${queues.length}`);
  });
  for (const q of queues) await q.close();
}

async function testJobProcessing() {
  console.log('\n2. Job Processing');
  const queue = new Queue('test-processing', { connection });
  let processed = false;

  const worker = new Worker('test-processing', async (job) => {
    if (job.data.key === 'test-value') processed = true;
    return { result: 'ok' };
  }, { connection });

  await queue.add('test-job', { key: 'test-value' });
  await new Promise(r => setTimeout(r, 2000));

  await assert('Process a job', async () => {
    if (!processed) throw new Error('Job was not processed');
  });

  await worker.close();
  await queue.close();
}

async function testJobPriority() {
  console.log('\n3. Job Priority (7 levels)');
  const queue = new Queue('test-priority', { connection });
  const order = [];

  const worker = new Worker('test-priority', async (job) => {
    order.push(job.data.priority);
    return {};
  }, { connection });

  await worker.pause();
  for (let p = 7; p >= 1; p--) {
    await queue.add('p-job', { priority: p }, { priority: p });
  }
  await worker.resume();
  await new Promise(r => setTimeout(r, 3000));

  await assert('Process jobs in priority order', async () => {
    if (order.length < 5) throw new Error(`Only processed ${order.length} of 7 jobs`);
    // Priority 1 is highest, should come first
    if (order[0] !== 1) throw new Error(`Expected priority 1 first, got ${order[0]}`);
  });

  await worker.close();
  await queue.close();
}

async function testJobScheduling() {
  console.log('\n4. Job Scheduling (upsertJobScheduler)');
  const queue = new Queue('test-scheduling', { connection });
  let scheduledRun = false;

  const worker = new Worker('test-scheduling', async () => {
    scheduledRun = true;
    return {};
  }, { connection });

  await queue.upsertJobScheduler('test-scheduler', { every: 1000 }, { data: {} });
  await new Promise(r => setTimeout(r, 3000));

  await assert('Scheduled job executes', async () => {
    if (!scheduledRun) throw new Error('Scheduled job did not execute');
  });

  await queue.removeJobScheduler('test-scheduler');
  await worker.close();
  await queue.close();
}

async function testRetry() {
  console.log('\n5. Job Retry');
  const queue = new Queue('test-retry', { connection });
  let attempts = 0;

  const worker = new Worker('test-retry', async () => {
    attempts++;
    if (attempts < 3) throw new Error('Simulated failure');
    return {};
  }, { connection });

  await queue.add('retry-job', {}, { attempts: 5, backoff: { type: 'fixed', delay: 500 } });
  await new Promise(r => setTimeout(r, 5000));

  await assert('Job retries and succeeds', async () => {
    if (attempts < 3) throw new Error(`Only ${attempts} attempts, expected >=3`);
  });

  await worker.close();
  await queue.close();
}

async function testPubSub() {
  console.log('\n6. Redis Pub/Sub');
  const sub = new Redis(REDIS_URL);
  const pub = new Redis(REDIS_URL);
  let received = false;

  await new Promise((resolve) => {
    sub.subscribe('test-channel', () => {
      pub.publish('test-channel', 'hello');
    });
    sub.on('message', (channel, message) => {
      if (channel === 'test-channel' && message === 'hello') {
        received = true;
        resolve();
      }
    });
    setTimeout(resolve, 3000);
  });

  await assert('Pub/sub message received', async () => {
    if (!received) throw new Error('Message not received');
  });

  await sub.quit();
  await pub.quit();
}

async function testCacheOps() {
  console.log('\n7. Cache Operations (get/set/mget/mset)');
  const redis = new Redis(REDIS_URL);

  await redis.set('test:key1', 'value1');
  const v1 = await redis.get('test:key1');
  await assert('SET/GET', async () => {
    if (v1 !== 'value1') throw new Error(`Expected 'value1', got '${v1}'`);
  });

  await redis.mset('test:mk1', 'mv1', 'test:mk2', 'mv2');
  const mv = await redis.mget('test:mk1', 'test:mk2');
  await assert('MSET/MGET', async () => {
    if (mv[0] !== 'mv1' || mv[1] !== 'mv2') throw new Error(`Unexpected: ${mv}`);
  });

  await redis.quit();
}

async function testSetOps() {
  console.log('\n8. SET Operations (SADD/SREM/SPOP/SMEMBERS)');
  const redis = new Redis(REDIS_URL);

  await redis.sadd('test:set', 'a', 'b', 'c');
  const members = await redis.smembers('test:set');
  await assert('SADD/SMEMBERS', async () => {
    if (members.length !== 3) throw new Error(`Expected 3, got ${members.length}`);
  });

  await redis.srem('test:set', 'b');
  const after = await redis.smembers('test:set');
  await assert('SREM', async () => {
    if (after.includes('b')) throw new Error('b should have been removed');
  });

  const popped = await redis.spop('test:set');
  await assert('SPOP', async () => {
    if (!['a', 'c'].includes(popped)) throw new Error(`Unexpected pop: ${popped}`);
  });

  await redis.quit();
}

async function cleanup() {
  const redis = new Redis(REDIS_URL);
  const keys = await redis.keys('test:*');
  if (keys.length > 0) await redis.del(...keys);
  for (const q of [...TWENTY_QUEUES, 'test-processing', 'test-priority', 'test-scheduling', 'test-retry']) {
    const bkeys = await redis.keys(`bull:${q}:*`);
    if (bkeys.length > 0) await redis.del(...bkeys);
  }
  await redis.quit();
}

async function main() {
  console.log(`BullMQ + KeyDB Compatibility Test`);
  console.log(`Target: ${REDIS_URL}`);

  try {
    await testQueueCreation();
    await testJobProcessing();
    await testJobPriority();
    await testJobScheduling();
    await testRetry();
    await testPubSub();
    await testCacheOps();
    await testSetOps();
  } finally {
    await cleanup();
  }

  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
```

- [ ] **Step 3: Commit**

```bash
git add infra/validation/bullmq-keydb-test/
git commit -m "feat(infra): add BullMQ + KeyDB compatibility validation script"
```

---

### Task 20: PostgreSQL Optimization Validation Script

**Files:**
- Create: `infra/validation/pg-validation.sql`

- [ ] **Step 1: Create PG validation SQL**

```sql
-- infra/validation/pg-validation.sql
-- Run after SQL optimization job to verify all optimizations applied correctly
-- Usage: psql -h <host> -U <user> -d twenty -f infra/validation/pg-validation.sql

\echo '=== 1. Extension Verification ==='
SELECT extname, extversion FROM pg_extension
WHERE extname IN ('pg_stat_statements', 'pg_trgm', 'uuid-ossp', 'unaccent')
ORDER BY extname;

\echo '=== 2. Core Index Verification ==='
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'core'
  AND indexname IN ('idx_app_token_user_id', 'idx_app_token_workspace_id')
ORDER BY indexname;

\echo '=== 3. Workspace Schema Discovery ==='
SELECT "dataSourceMetadata"->'schema' as schema_name
FROM core."dataSourceMetadata"
WHERE "type" = 'workspace';

\echo '=== 4. Workspace Index Verification (first workspace) ==='
DO $$
DECLARE
  ws_schema text;
BEGIN
  SELECT "dataSourceMetadata"->'schema' INTO ws_schema
  FROM core."dataSourceMetadata"
  WHERE "type" = 'workspace'
  LIMIT 1;

  IF ws_schema IS NULL THEN
    RAISE NOTICE 'No workspace schemas found. Skipping workspace checks.';
    RETURN;
  END IF;

  ws_schema := trim(both '"' from ws_schema);

  RAISE NOTICE 'Checking workspace schema: %', ws_schema;

  -- Check BRIN indexes
  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_message_created_brin';
  IF FOUND THEN RAISE NOTICE '  ✓ BRIN index on message.createdAt'; ELSE RAISE WARNING '  ✗ MISSING: BRIN index on message.createdAt'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_workflow_run_created_brin';
  IF FOUND THEN RAISE NOTICE '  ✓ BRIN index on workflowRun.createdAt'; ELSE RAISE WARNING '  ✗ MISSING: BRIN index on workflowRun.createdAt'; END IF;

  -- Check GIN trgm indexes
  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_company_search_trgm';
  IF FOUND THEN RAISE NOTICE '  ✓ GIN trgm index on company.searchVector'; ELSE RAISE WARNING '  ✗ MISSING: GIN trgm index on company.searchVector'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_person_search_trgm';
  IF FOUND THEN RAISE NOTICE '  ✓ GIN trgm index on person.searchVector'; ELSE RAISE WARNING '  ✗ MISSING: GIN trgm index on person.searchVector'; END IF;

  -- Check expression index
  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_company_name_unaccent';
  IF FOUND THEN RAISE NOTICE '  ✓ Expression index on company.name (unaccent)'; ELSE RAISE WARNING '  ✗ MISSING: Expression index on company.name'; END IF;

  -- Check JSONB GIN index
  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_workflow_run_state_gin';
  IF FOUND THEN RAISE NOTICE '  ✓ GIN index on workflowRun.state'; ELSE RAISE WARNING '  ✗ MISSING: GIN index on workflowRun.state'; END IF;
END $$;

\echo '=== 5. FILLFACTOR Verification ==='
DO $$
DECLARE
  ws_schema text;
  opts text[];
BEGIN
  SELECT "dataSourceMetadata"->'schema' INTO ws_schema
  FROM core."dataSourceMetadata"
  WHERE "type" = 'workspace'
  LIMIT 1;

  IF ws_schema IS NULL THEN RETURN; END IF;
  ws_schema := trim(both '"' from ws_schema);

  SELECT reloptions INTO opts FROM pg_class
  WHERE relname = 'workflowRun' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ws_schema);

  IF opts IS NOT NULL AND 'fillfactor=70' = ANY(opts) THEN
    RAISE NOTICE '  ✓ workflowRun FILLFACTOR = 70';
  ELSE
    RAISE WARNING '  ✗ workflowRun FILLFACTOR not set to 70 (got: %)', opts;
  END IF;
END $$;

\echo '=== 6. Autovacuum Verification ==='
DO $$
DECLARE
  ws_schema text;
  opts text[];
BEGIN
  SELECT "dataSourceMetadata"->'schema' INTO ws_schema
  FROM core."dataSourceMetadata"
  WHERE "type" = 'workspace'
  LIMIT 1;

  IF ws_schema IS NULL THEN RETURN; END IF;
  ws_schema := trim(both '"' from ws_schema);

  SELECT reloptions INTO opts FROM pg_class
  WHERE relname = 'message' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ws_schema);

  IF opts IS NOT NULL AND 'autovacuum_vacuum_scale_factor=0.01' = ANY(opts) THEN
    RAISE NOTICE '  ✓ message autovacuum_vacuum_scale_factor = 0.01';
  ELSE
    RAISE WARNING '  ✗ message autovacuum tuning not applied (got: %)', opts;
  END IF;
END $$;

\echo '=== 7. pg_stat_statements Status ==='
SELECT count(*) as tracked_queries FROM pg_stat_statements;

\echo '=== Validation Complete ==='
```

- [ ] **Step 2: Commit**

```bash
git add infra/validation/pg-validation.sql
git commit -m "feat(infra): add PostgreSQL optimization validation script"
```

---

### Task 21: k6 Load Test Scripts

**Files:**
- Create: `infra/validation/k6/lib/config.js`
- Create: `infra/validation/k6/graphql-throughput.js`
- Create: `infra/validation/k6/search-load.js`
- Create: `infra/validation/k6/message-sync.js`
- Create: `infra/validation/k6/workflow-load.js`

- [ ] **Step 1: Create shared config**

```javascript
// infra/validation/k6/lib/config.js
// Shared configuration for k6 load tests
// Override via env vars: K6_BASE_URL, K6_AUTH_TOKEN

export const BASE_URL = __ENV.K6_BASE_URL || 'http://localhost:3000';
export const AUTH_TOKEN = __ENV.K6_AUTH_TOKEN || '';

export const headers = {
  'Content-Type': 'application/json',
  ...(AUTH_TOKEN ? { 'Authorization': `Bearer ${AUTH_TOKEN}` } : {}),
};

export function graphql(query, variables = {}) {
  return JSON.stringify({ query, variables });
}
```

- [ ] **Step 2: Create GraphQL throughput test**

```javascript
// infra/validation/k6/graphql-throughput.js
// Tests GraphQL query throughput for core operations
//
// Usage:
//   k6 run infra/validation/k6/graphql-throughput.js
//   k6 run --out experimental-prometheus-rw infra/validation/k6/graphql-throughput.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('errors');
const queryDuration = new Trend('query_duration', true);

export const options = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '2m', target: 50 },
    { duration: '1m', target: 100 },
    { duration: '2m', target: 100 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],
    errors: ['rate<0.05'],
  },
};

const QUERIES = {
  companyList: graphql(`
    query Companies($first: Int) {
      companies(first: $first) {
        edges {
          node { id name }
        }
      }
    }
  `, { first: 20 }),

  personList: graphql(`
    query People($first: Int) {
      people(first: $first) {
        edges {
          node { id name { firstName lastName } }
        }
      }
    }
  `, { first: 20 }),

  opportunityPipeline: graphql(`
    query Opportunities($first: Int) {
      opportunities(first: $first) {
        edges {
          node { id name stage amount }
        }
      }
    }
  `, { first: 50 }),
};

export default function () {
  const queryNames = Object.keys(QUERIES);
  const queryName = queryNames[Math.floor(Math.random() * queryNames.length)];
  const body = QUERIES[queryName];

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api`, body, { headers });
  queryDuration.add(Date.now() - start);

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'no GraphQL errors': (r) => {
      const json = r.json();
      return !json.errors || json.errors.length === 0;
    },
  });

  errorRate.add(!success);
  sleep(0.1);
}
```

- [ ] **Step 3: Create search load test**

```javascript
// infra/validation/k6/search-load.js
// Tests search endpoint under concurrent load

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('search_errors');

export const options = {
  stages: [
    { duration: '30s', target: 5 },
    { duration: '2m', target: 30 },
    { duration: '2m', target: 30 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<3000'],
    search_errors: ['rate<0.05'],
  },
};

const SEARCH_TERMS = [
  'acme', 'global', 'tech', 'consulting', 'john',
  'smith', 'engineering', 'marketing', 'sales', 'support',
];

export default function () {
  const term = SEARCH_TERMS[Math.floor(Math.random() * SEARCH_TERMS.length)];

  const body = graphql(`
    query SearchCompanies($filter: CompanyFilterInput) {
      companies(filter: $filter, first: 10) {
        edges {
          node { id name }
        }
      }
    }
  `, {
    filter: {
      name: { like: `%${term}%` }
    }
  });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  errorRate.add(!success);
  sleep(0.2);
}
```

- [ ] **Step 4: Create message sync simulation**

```javascript
// infra/validation/k6/message-sync.js
// Simulates message sync batch operations via GraphQL mutations

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('sync_errors');
const messagesCreated = new Counter('messages_created');

export const options = {
  stages: [
    { duration: '30s', target: 5 },
    { duration: '3m', target: 20 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'],
    sync_errors: ['rate<0.10'],
  },
};

export default function () {
  // Simulate creating message records (batch insert pattern)
  const body = graphql(`
    mutation CreateMessage($input: MessageCreateInput!) {
      createMessage(data: $input) {
        id
      }
    }
  `, {
    input: {
      subject: `Load test message ${Date.now()}`,
      body: 'This is a load test message for sync simulation.',
      direction: 'INCOMING',
    }
  });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (success) messagesCreated.add(1);
  errorRate.add(!success);
  sleep(0.5);
}
```

- [ ] **Step 5: Create workflow load test**

```javascript
// infra/validation/k6/workflow-load.js
// Tests workflow trigger throughput

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('workflow_errors');
const workflowsTriggered = new Counter('workflows_triggered');

export const options = {
  stages: [
    { duration: '30s', target: 3 },
    { duration: '2m', target: 15 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'],
    workflow_errors: ['rate<0.10'],
  },
};

export default function () {
  // Query workflow runs to assess throughput capacity
  const body = graphql(`
    query WorkflowRuns($first: Int) {
      workflowRuns(first: $first) {
        edges {
          node { id status }
        }
      }
    }
  `, { first: 20 });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (success) workflowsTriggered.add(1);
  errorRate.add(!success);
  sleep(0.3);
}
```

- [ ] **Step 6: Commit**

```bash
git add infra/validation/k6/
git commit -m "feat(infra): add k6 load test scripts (GraphQL, search, message sync, workflow)"
```

---

### Task 22: Deploy and Run Integration/E2E Tests

**Files:** No new files — execution task.

- [ ] **Step 1: Deploy locally**

```bash
bash infra/scripts/local-setup.sh
```

Expected: all components deployed successfully. Check with:

```bash
kubectl get pods -n twenty
kubectl get cluster -n twenty
kubectl get pooler -n twenty
```

All pods should be Running/Ready.

- [ ] **Step 2: Verify Twenty is accessible**

```bash
kubectl port-forward svc/twenty-server 3000:3000 -n twenty &
curl -s http://localhost:3000/healthz
```

Expected: HTTP 200 response.

- [ ] **Step 3: Run BullMQ + KeyDB validation**

```bash
cd infra/validation/bullmq-keydb-test
npm install
REDIS_URL=redis://$(kubectl get svc twenty-keydb -n twenty -o jsonpath='{.spec.clusterIP}'):6379 node test.js
```

Expected: all 8 test sections pass. If any fail, KeyDB compatibility is compromised — fall back to Redis.

- [ ] **Step 4: Run PostgreSQL validation**

```bash
kubectl port-forward svc/twenty-db-rw 5433:5432 -n twenty &
PGPASSWORD=$(kubectl get secret twenty-db-superuser -n twenty -o jsonpath='{.data.password}' | base64 -d) \
  psql -h localhost -p 5433 -U postgres -d twenty -f infra/validation/pg-validation.sql
```

Expected: all indexes verified present, FILLFACTOR and autovacuum settings confirmed.

- [ ] **Step 5: Run integration tests**

```bash
npx nx run twenty-server:test:integration:with-db-reset
```

Expected: all 368 suites pass. Any failures indicate incompatibility with the new infrastructure.

- [ ] **Step 6: Run E2E tests**

```bash
npx nx run twenty-e2e-testing:test
```

Expected: all Playwright tests pass.

---

### Task 23: Run k6 Load Tests

**Files:** No new files — execution task.

- [ ] **Step 1: Install k6**

```bash
brew install k6
```

- [ ] **Step 2: Obtain auth token**

Log into Twenty, create an API key, and export it:

```bash
export K6_BASE_URL=http://localhost:3000
export K6_AUTH_TOKEN=<your-api-key>
```

- [ ] **Step 3: Run GraphQL throughput test with Prometheus output**

```bash
k6 run --out experimental-prometheus-rw=http://localhost:9090/api/v1/write infra/validation/k6/graphql-throughput.js
```

Open Grafana at `http://localhost:30300` and observe the Twenty Overview and PostgreSQL Health dashboards during the test.

- [ ] **Step 4: Run remaining load tests**

```bash
k6 run --out experimental-prometheus-rw=http://localhost:9090/api/v1/write infra/validation/k6/search-load.js
k6 run --out experimental-prometheus-rw=http://localhost:9090/api/v1/write infra/validation/k6/message-sync.js
k6 run --out experimental-prometheus-rw=http://localhost:9090/api/v1/write infra/validation/k6/workflow-load.js
```

- [ ] **Step 5: Analyze results**

Check Grafana dashboards for:
- PG replication lag during load
- Cache hit ratio
- Queue depth during worker load
- KeyDB memory usage
- Top slow queries (pg_stat_statements dashboard)

Document any bottlenecks found.

---

## Phase 8: Distributed DB Validation Path

### Task 24: YugabyteDB + Neon Test Manifests

**Files:**
- Create: `infra/distributed-db/yugabytedb/values.yaml`
- Create: `infra/distributed-db/neon/values.yaml`

- [ ] **Step 1: Create YugabyteDB Helm values**

```yaml
# infra/distributed-db/yugabytedb/values.yaml
# YugabyteDB test deployment for compatibility validation
# Chart: yugabytedb/yugabyte
# Repo: https://charts.yugabyte.com
#
# Install in test namespace:
#   helm repo add yugabytedb https://charts.yugabyte.com
#   helm install yugabyte yugabytedb/yugabyte -n twenty-db-test --create-namespace -f infra/distributed-db/yugabytedb/values.yaml --wait

replicas:
  master: 1
  tserver: 3

resource:
  master:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 1Gi
  tserver:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: "2"
      memory: 2Gi

storage:
  master:
    size: 5Gi
  tserver:
    size: 20Gi

gflags:
  tserver:
    # Enable PostgreSQL compatibility
    ysql_enable_auth: true
    ysql_pg_conf_csv: "log_min_duration_statement=500"
  master: {}

# Expose YSQL on standard PG port
enableLoadBalancer: false
```

- [ ] **Step 2: Create Neon test values**

```yaml
# infra/distributed-db/neon/values.yaml
# Neon test deployment for compatibility validation
# Neon runs as a Docker container locally (no Helm chart — use docker-compose or K8s manifests)
#
# Option A: Use Neon's official Docker image
#   docker run -d --name neon-test \
#     -p 5434:5432 \
#     -e NEON_AUTH_TOKEN=test \
#     ghcr.io/neondatabase/neon:latest
#
# Option B: Use Neon cloud free tier for testing
#   Create project at https://neon.tech and use connection string
#
# Then point Twenty at it:
#   PG_DATABASE_URL=postgres://twenty:<pwd>@<neon-host>:5432/twenty

# If using K8s deployment for local testing:
apiVersion: apps/v1
kind: Deployment
metadata:
  name: neon-test
  namespace: twenty-db-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: neon-test
  template:
    metadata:
      labels:
        app: neon-test
    spec:
      containers:
        - name: postgres
          image: ghcr.io/neondatabase/compute-node-v17:latest
          ports:
            - containerPort: 5432
          env:
            - name: POSTGRES_USER
              value: twenty
            - name: POSTGRES_PASSWORD
              value: twenty-test
            - name: POSTGRES_DB
              value: twenty
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              cpu: "2"
              memory: 4Gi
---
apiVersion: v1
kind: Service
metadata:
  name: neon-test
  namespace: twenty-db-test
spec:
  type: ClusterIP
  ports:
    - port: 5432
      targetPort: 5432
  selector:
    app: neon-test
```

- [ ] **Step 3: Document distributed DB test procedure**

Test procedure (to run after Phase 7 is complete):

```bash
# 1. Create test namespace
kubectl create namespace twenty-db-test

# 2. Deploy YugabyteDB
helm repo add yugabytedb https://charts.yugabyte.com
helm install yugabyte yugabytedb/yugabyte -n twenty-db-test \
  -f infra/distributed-db/yugabytedb/values.yaml --wait --timeout 10m

# 3. Get YugabyteDB YSQL endpoint
YUGA_HOST=$(kubectl get svc yugabyte-tserver-service -n twenty-db-test -o jsonpath='{.spec.clusterIP}')
echo "YugabyteDB YSQL: postgres://twenty@${YUGA_HOST}:5433/twenty"

# 4. Create Twenty database on YugabyteDB
kubectl exec -it yugabyte-tserver-0 -n twenty-db-test -- ysqlsh -c "CREATE DATABASE twenty;"
kubectl exec -it yugabyte-tserver-0 -n twenty-db-test -- ysqlsh -d twenty -c "CREATE USER twenty WITH PASSWORD 'twenty-test';"
kubectl exec -it yugabyte-tserver-0 -n twenty-db-test -- ysqlsh -d twenty -c "GRANT ALL ON DATABASE twenty TO twenty;"

# 5. Point Twenty at YugabyteDB (temporary override)
# Edit values-local.yaml: db.external.host -> yugabyte-tserver-service.twenty-db-test
# Edit values-local.yaml: db.external.port -> 5433
# Redeploy Twenty

# 6. Run tests
npx nx run twenty-server:test:integration:with-db-reset
npx nx run twenty-e2e-testing:test

# 7. Record results: pass/fail per test suite
```

- [ ] **Step 4: Commit**

```bash
git add infra/distributed-db/
git commit -m "feat(infra): add YugabyteDB and Neon test manifests for distributed DB validation"
```
