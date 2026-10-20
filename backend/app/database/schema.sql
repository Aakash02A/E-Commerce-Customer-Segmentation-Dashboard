-- SQLite schema for the E-Commerce Customer Segmentation Dashboard.
-- Created with IF NOT EXISTS so it is safe to re-run.

CREATE TABLE IF NOT EXISTS uploads (
    id TEXT PRIMARY KEY,
    original_filename TEXT NOT NULL,
    stored_filename TEXT NOT NULL,
    file_path TEXT NOT NULL,
    mime_type TEXT,
    file_size INTEGER NOT NULL,
    extension TEXT NOT NULL,
    sheet_names TEXT,            -- JSON array (Excel workbooks)
    selected_sheet TEXT,
    columns_json TEXT,           -- JSON: detected column info (names, dtypes, samples, scores)
    mapping_json TEXT,           -- JSON: confirmed source-column -> standard-field mapping
    normalized_path TEXT,        -- processed CSV produced after mapping confirmation
    status TEXT NOT NULL,        -- uploaded | mapping_required | mapped | validation_failed | ready_for_analysis | failed | deleted
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads (status);
CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads (created_at);

CREATE TABLE IF NOT EXISTS data_quality_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id TEXT NOT NULL REFERENCES uploads (id) ON DELETE CASCADE,
    report_json TEXT NOT NULL,   -- JSON: full quality report incl. applied rules (audit summary)
    total_rows INTEGER NOT NULL,
    valid_rows INTEGER NOT NULL,
    invalid_rows INTEGER NOT NULL,
    passed INTEGER NOT NULL,     -- 1 if analysis may proceed
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dq_reports_upload ON data_quality_reports (upload_id);

CREATE TABLE IF NOT EXISTS analyses (
    id TEXT PRIMARY KEY,
    upload_id TEXT NOT NULL REFERENCES uploads (id) ON DELETE CASCADE,
    status TEXT NOT NULL,        -- pending | running | completed | failed
    reference_date TEXT,         -- explicit RFM anchor date
    settings_json TEXT,          -- JSON: scoring bins, segment rules, clustering options
    summary_json TEXT,           -- JSON: compact dashboard KPI summary
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS customer_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id TEXT NOT NULL REFERENCES analyses (id) ON DELETE CASCADE,
    customer_id TEXT NOT NULL,
    recency_days INTEGER,        -- RFM input (Phase 7)
    frequency INTEGER,           -- distinct order count (Phase 6 base aggregate)
    monetary REAL,               -- total spend (Phase 6 base aggregate)
    total_quantity REAL,         -- total units purchased (Phase 6 base aggregate)
    first_order_date TEXT,       -- earliest purchase date (Phase 6 base aggregate)
    last_order_date TEXT,        -- most recent purchase date (Phase 6 base aggregate)
    r_score INTEGER,             -- RFM scoring (Phase 7)
    f_score INTEGER,
    m_score INTEGER,
    segment TEXT,                -- segmentation (Phase 7)
    cluster_label TEXT,          -- clustering (Phase 8)
    country TEXT,
    UNIQUE (analysis_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_metrics_analysis ON customer_metrics (analysis_id);
CREATE INDEX IF NOT EXISTS idx_customer_metrics_segment ON customer_metrics (analysis_id, segment);
