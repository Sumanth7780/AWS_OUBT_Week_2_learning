-- Master Data schema (MDM)
CREATE SCHEMA IF NOT EXISTS mdm;

-- Golden master table for Taxi Zones
CREATE TABLE IF NOT EXISTS mdm.master_taxi_zones (
  golden_id            BIGSERIAL PRIMARY KEY,
  location_id          INT,
  borough              TEXT,
  zone_name            TEXT,
  service_zone         TEXT,

  match_key            TEXT,
  match_confidence     NUMERIC(5,4),
  survivorship_rule    TEXT,

  mdm_state            TEXT NOT NULL DEFAULT 'ACTIVE',
  source_system        TEXT NOT NULL DEFAULT 'raw_reference',

  created_at_utc       TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
  updated_at_utc       TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),

  -- Governance metadata
  data_owner           TEXT,
  data_steward         TEXT,
  classification       TEXT,
  version_tag          TEXT
);

CREATE INDEX IF NOT EXISTS idx_master_taxi_zones_location_id ON mdm.master_taxi_zones(location_id);
CREATE INDEX IF NOT EXISTS idx_master_taxi_zones_match_key ON mdm.master_taxi_zones(match_key);

-- Steward review queue (records needing manual review)
CREATE TABLE IF NOT EXISTS mdm.steward_queue_taxi_zones (
  queue_id             BIGSERIAL PRIMARY KEY,
  location_id          INT,
  borough              TEXT,
  zone_name            TEXT,
  service_zone         TEXT,

  match_key            TEXT,
  match_confidence     NUMERIC(5,4),
  decision             TEXT NOT NULL DEFAULT 'STEWARD_REVIEW',

  review_reason        TEXT,
  status               TEXT NOT NULL DEFAULT 'OPEN', -- OPEN, IN_REVIEW, APPROVED, REJECTED
  assigned_to          TEXT,

  submitted_at_utc     TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
  updated_at_utc       TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_steward_queue_status ON mdm.steward_queue_taxi_zones(status);
CREATE INDEX IF NOT EXISTS idx_steward_queue_match_key ON mdm.steward_queue_taxi_zones(match_key);
