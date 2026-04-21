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
