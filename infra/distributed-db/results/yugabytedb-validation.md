# YugabyteDB Compatibility Validation — 2026-04-23

**Target:** Run Twenty CRM against YugabyteDB (PostgreSQL-wire-compatible distributed SQL) as a horizontal-scaling alternative to CNPG.

**YugabyteDB version:** 2.23+ (`yugabytedb/yugabyte` Helm chart, defaults)
**Topology:** 1 master + 3 tservers, YSQL port 5433
**Twenty version:** current main (1.14.0)

## Result: **NOT COMPATIBLE** for Twenty CRM

## What works

- Helm deployment (`yugabytedb/yugabyte`) on Docker Desktop Kind (single-node)
- YSQL connectivity via standard `postgres://` driver (no code changes)
- All four Twenty-required extensions install cleanly:
  - `pg_stat_statements`, `pg_trgm`, `uuid-ossp`, `unaccent`
- TypeORM initial schema sync via `CREATE TABLE`: `core` schema with ~100 tables created
- Twenty NestJS server and worker **start up successfully** against YugabyteDB
- **Signup mutation succeeds** — user created, JWT tokens issued
- Login/signIn mutations work

## What breaks

### 1. TypeORM migration history not recorded
- `core._typeorm_migrations` table exists but is **empty** after startup
- Only the current entity state is reflected in the schema; migration ALTER operations are silently skipped or not tracked
- Running server expects post-migration column names (`core.application.defaultRoleId`) while the schema still has the pre-rename name (`defaultServerlessFunctionRoleId`)

### 2. Runtime DDL/DML catalog version mismatches
- YugabyteDB raises `Catalog Version Mismatch: A DDL occurred while processing this query. Try again.` (YugabyteDB's `YBPrepareCacheRefreshIfNeeded` routine) whenever Twenty's workspace migrations run DDL in parallel with queries
- Twenty workers crash on simple SELECT queries because of stale catalog caches after a workspace-level migration

### 3. Specific query failures
- `SELECT ... FROM core.workspace WHERE activationStatus = 'ACTIVE'` fails with `column WorkspaceEntity.trashRetentionDays does not exist` — entity decorator expects a column added by a migration that didn't apply
- `signUpInNewWorkspace` mutation (which triggers per-workspace schema creation) fails with `column ApplicationEntity.defaultServerlessFunctionRoleId does not exist`

### 4. Collation difference (minor)
- YugabyteDB default: `Collate=C, Ctype=en_US.UTF-8`
- CNPG/Twenty expected: `Collate=en_US.utf8, Ctype=en_US.utf8`
- Affects sort order of case-insensitive queries; fixable via `CREATE DATABASE ... LC_COLLATE = 'en_US.utf8'` but not tested here

## Root causes

YugabyteDB is **mostly PostgreSQL-wire-compatible but not migration-chain-compatible**:
1. Some TypeORM migration DDL (particularly RENAME COLUMN) does not apply cleanly
2. YugabyteDB's distributed catalog cache has visibility issues with in-flight DDL — its "Catalog Version Mismatch" errors are expected per the docs but break Twenty's runtime assumptions
3. Twenty's per-workspace schema model triggers DDL frequently, compounding the problem

## Recommendation

**Do not use YugabyteDB for Twenty** — at least not without upstream migration-handling changes Twenty does not currently have.

**Stay on CNPG + PgBouncer** for Twenty production scaling:
- Vertical scaling of PG nodes (up to tens of TB)
- Read replicas via CNPG (already configured in `infra/database/cluster.yaml`)
- Horizontal scaling of Twenty app tier is what we actually needed, and that works today (validated in Task 22: integration tests, Task 23: k6 load tests)

## Neon validation: skipped

Neon's `compute-node-v17` image requires an external Neon `pageserver` and storage broker to function — it is not a self-contained standalone Postgres image. Full Neon local deployment needs `neon` + `pageserver` + `safekeepers` + `compute-node`, which is out of scope for this validation. Neon's cloud offering is compatible with Twenty (it exposes a PG-wire endpoint), but self-hosted Neon on K8s is not a trivial substitution.

For teams wanting Neon-style storage/compute separation on OSS stack: **CNPG with WAL archiving to S3 + read replicas** gives a comparable architecture without the operational complexity.

## How to reproduce

```bash
# Deploy YugabyteDB
kubectl create namespace twenty-db-test
helm install yugabyte yugabytedb/yugabyte -n twenty-db-test \
  -f infra/distributed-db/yugabytedb/values.yaml --wait --timeout 8m

# Create twenty user + database (yugabyte user password defaults to "yugabyte" when ysql_enable_auth=true)
kubectl exec -n twenty-db-test yb-tserver-0 -c yb-tserver -- /bin/sh -c \
  "PGPASSWORD=yugabyte /home/yugabyte/bin/ysqlsh -h yb-tserver-0.yb-tservers -U yugabyte -d yugabyte \
   -c \"CREATE USER twenty WITH PASSWORD 'twenty-test';\" \
   -c \"CREATE DATABASE twenty OWNER twenty;\" \
   -c \"GRANT ALL ON DATABASE twenty TO twenty;\""

# Point Twenty at YugabyteDB
kubectl set env deployment/twenty-server deployment/twenty-worker -n twenty \
  PG_DATABASE_URL='postgres://twenty:twenty-test@yb-tservers.twenty-db-test.svc.cluster.local:5433/twenty' \
  PG_DATABASE_REPLICA_URL='postgres://twenty:twenty-test@yb-tservers.twenty-db-test.svc.cluster.local:5433/twenty'

# Observe: signUp works, signUpInNewWorkspace fails with missing column errors
```
