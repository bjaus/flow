CREATE TABLE IF NOT EXISTS checkpoints (
  id TEXT PRIMARY KEY,
  data BLOB NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  workflow TEXT NOT NULL,
  fingerprint TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  trigger_name TEXT NOT NULL DEFAULT '',
  parent_id TEXT NOT NULL DEFAULT '',
  input BLOB NOT NULL,
  result BLOB,
  error TEXT NOT NULL DEFAULT '',
  interrupt_id TEXT NOT NULL DEFAULT '',
  gate_prompt TEXT NOT NULL DEFAULT '',
  decision BLOB,
  cancel_pending INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_queue ON runs(status, created_at, id);
CREATE INDEX IF NOT EXISTS runs_workflow ON runs(workflow, status);
CREATE TABLE IF NOT EXISTS events (
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  at TIMESTAMP NOT NULL,
  data BLOB,
  PRIMARY KEY(run_id, seq)
);
CREATE INDEX IF NOT EXISTS events_at ON events(at, run_id, seq);
