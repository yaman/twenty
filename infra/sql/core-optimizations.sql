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
