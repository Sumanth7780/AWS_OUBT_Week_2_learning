-- Audit schema
CREATE SCHEMA IF NOT EXISTS audit;

-- Pipeline run log (job-level audit)
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
  run_id               BIGSERIAL PRIMARY KEY,
  job_name             TEXT NOT NULL,
  job_run_id           TEXT,
  environment          TEXT DEFAULT 'dev',

  input_path           TEXT,
  output_path          TEXT,
  start_time_utc       TIMESTAMP,
  end_time_utc         TIMESTAMP,

  status               TEXT,         -- SUCCEEDED / FAILED
  error_message        TEXT,

  records_in           BIGINT,
  records_out          BIGINT,
  rejects_count        BIGINT,

  created_at_utc       TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Delta table history snapshot (for compliance evidence)
CREATE TABLE IF NOT EXISTS audit.delta_history_snapshot (
  snapshot_id          BIGSERIAL PRIMARY KEY,
  table_name           TEXT NOT NULL,
  table_path           TEXT NOT NULL,

  version              BIGINT,
  operation            TEXT,
  operation_parameters JSONB,
  operation_metrics    JSONB,
  user_metadata        TEXT,

  committed_at_utc     TIMESTAMP,
  captured_at_utc      TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_delta_history_table_name ON audit.delta_history_snapshot(table_name);
CREATE INDEX IF NOT EXISTS idx_delta_history_version ON audit.delta_history_snapshot(table_name, version);
