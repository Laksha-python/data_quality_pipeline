BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.schema_version (
    version INT PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq.dq_datasets (
    dataset_id SERIAL PRIMARY KEY,
    dataset_name TEXT UNIQUE NOT NULL,
    owner TEXT,
    contract_path TEXT,
    onboarded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq.dq_run_history (
    run_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    run_date DATE NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT,
    failed_step TEXT
);

CREATE INDEX IF NOT EXISTS idx_dq_run_history_ds_date
ON dq.dq_run_history (dataset_name, run_date);

CREATE TABLE IF NOT EXISTS dq.dq_anomalies (
    run_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    dimension TEXT NOT NULL,
    observed_value DOUBLE PRECISION,
    expected_value DOUBLE PRECISION,
    deviation_percent DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    severity TEXT NOT NULL,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_dq_anomaly
        UNIQUE (run_date, dataset_id, table_name, column_name, dimension)
);

CREATE INDEX IF NOT EXISTS idx_dq_anomalies_lookup
ON dq.dq_anomalies (dataset_id, run_date, dimension);

CREATE TABLE IF NOT EXISTS dq.dq_aggregated_anomalies (
    run_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    dimension TEXT NOT NULL,
    table_name TEXT NOT NULL,
    dominant_severity TEXT,
    anomaly_count INT,
    affected_columns TEXT,
    penalty_multiplier INT,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (run_date, dataset_id, dimension, table_name)
);

CREATE TABLE IF NOT EXISTS dq.dq_score_history (
    run_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    dq_score INT,
    status TEXT,
    top_issue TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (run_date, dataset_id)
);

CREATE TABLE IF NOT EXISTS dq.dq_root_causes (
    run_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    dimension TEXT NOT NULL,
    table_name TEXT NOT NULL,
    columns TEXT,
    summary TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (run_date, dataset_id, dimension, table_name)
);

CREATE TABLE IF NOT EXISTS dq.dq_baseline_stats (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    window_start DATE,
    window_end DATE,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS dq.dq_current_stats (
    run_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (run_date, dataset_id, table_name, column_name, metric_name)
);

CREATE TABLE IF NOT EXISTS dq.dq_schema_snapshot (
    snapshot_date DATE NOT NULL,
    dataset_id INT NOT NULL REFERENCES dq.dq_datasets(dataset_id),
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (snapshot_date, dataset_id, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_baseline_lookup
ON dq.dq_baseline_stats (table_name, column_name, metric_name, window_end);

INSERT INTO dq.schema_version (version)
VALUES (1)
ON CONFLICT DO NOTHING;

COMMIT;
