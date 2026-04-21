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

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_message_created_brin';
  IF FOUND THEN RAISE NOTICE '  ✓ BRIN index on message.createdAt'; ELSE RAISE WARNING '  ✗ MISSING: BRIN index on message.createdAt'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_workflow_run_created_brin';
  IF FOUND THEN RAISE NOTICE '  ✓ BRIN index on workflowRun.createdAt'; ELSE RAISE WARNING '  ✗ MISSING: BRIN index on workflowRun.createdAt'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_company_search_trgm';
  IF FOUND THEN RAISE NOTICE '  ✓ GIN trgm index on company.searchVector'; ELSE RAISE WARNING '  ✗ MISSING: GIN trgm index on company.searchVector'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_person_search_trgm';
  IF FOUND THEN RAISE NOTICE '  ✓ GIN trgm index on person.searchVector'; ELSE RAISE WARNING '  ✗ MISSING: GIN trgm index on person.searchVector'; END IF;

  PERFORM 1 FROM pg_indexes WHERE schemaname = ws_schema AND indexname = 'idx_company_name_unaccent';
  IF FOUND THEN RAISE NOTICE '  ✓ Expression index on company.name (unaccent)'; ELSE RAISE WARNING '  ✗ MISSING: Expression index on company.name'; END IF;

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
