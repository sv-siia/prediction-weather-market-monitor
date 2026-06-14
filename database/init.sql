-- ================================================================
-- Real-Time Prediction Market Accuracy Monitoring
-- PostgreSQL Schema
-- ================================================================

-- ── Extensions ──────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ================================================================
-- TABLE 1: Raw correlations
-- Stores every correlated prediction + weather record
-- ================================================================
CREATE TABLE IF NOT EXISTS market_correlations (
    id                    SERIAL PRIMARY KEY,
    condition_id          VARCHAR(255),
    question              TEXT,
    location_name         VARCHAR(255),
    market_type           VARCHAR(50),
    market_status         VARCHAR(50),

    -- Prediction data
    yes_price             DECIMAL(6,4),
    winner                BOOLEAN,
    closed                BOOLEAN,
    end_date_iso          TIMESTAMP,

    -- Weather data
    weather_type          VARCHAR(50),
    observation_date      DATE,
    actual_temp_c         DECIMAL(6,2),
    actual_temp_f         DECIMAL(6,2),
    actual_precip_mm      DECIMAL(8,2),
    actual_rain_mm        DECIMAL(8,2),
    actual_weather_code   INTEGER,
    actual_wind_kmh       DECIMAL(6,2),

    -- Correlation results
    actual_outcome        SMALLINT,
    prediction_error      DECIMAL(6,4),
    correlation_method    VARCHAR(50),
    correlation_latency   INTEGER,

    -- Timestamps
    poll_timestamp        TIMESTAMP,
    created_at            TIMESTAMP DEFAULT NOW(),

    -- Indexes for fast queries
    CONSTRAINT valid_outcome CHECK (actual_outcome IN (0, 1)),
    CONSTRAINT valid_error   CHECK (prediction_error >= 0 AND prediction_error <= 1)
);

-- ================================================================
-- TABLE 2: Accuracy aggregates
-- Hourly aggregated accuracy per location + market type
-- ================================================================
CREATE TABLE IF NOT EXISTS market_accuracy_aggregates (
    id                        SERIAL PRIMARY KEY,
    location_name             VARCHAR(255) NOT NULL,
    market_type               VARCHAR(50)  NOT NULL,
    window_start              TIMESTAMP    NOT NULL,
    window_end                TIMESTAMP    NOT NULL,

    -- Accuracy metrics
    total_predictions         INTEGER      DEFAULT 0,
    correct_predictions       INTEGER      DEFAULT 0,
    accuracy_rate             DECIMAL(6,4) DEFAULT 0,
    avg_prediction_error      DECIMAL(6,4) DEFAULT 0,
    min_prediction_error      DECIMAL(6,4) DEFAULT 0,
    max_prediction_error      DECIMAL(6,4) DEFAULT 0,

    -- Volume metrics
    total_volume              DECIMAL(12,2) DEFAULT 0,
    volume_weighted_accuracy  DECIMAL(6,4)  DEFAULT 0,

    -- Bias metrics
    bias_score                DECIMAL(6,4) DEFAULT 0,
    over_prediction_count     INTEGER      DEFAULT 0,
    under_prediction_count    INTEGER      DEFAULT 0,

    -- Timestamps
    created_at                TIMESTAMP DEFAULT NOW(),
    updated_at                TIMESTAMP DEFAULT NOW(),

    UNIQUE (location_name, market_type, window_start)
);

-- ================================================================
-- TABLE 3: Anomaly alerts
-- Stores detected anomalies and arbitrage opportunities
-- ================================================================
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id                SERIAL PRIMARY KEY,
    alert_type        VARCHAR(50)  NOT NULL,
    severity          VARCHAR(20)  NOT NULL DEFAULT 'medium',
    location_name     VARCHAR(255),
    market_type       VARCHAR(50),
    condition_id      VARCHAR(255),

    -- Alert details
    message           TEXT,
    metric_value      DECIMAL(10,4),
    threshold_value   DECIMAL(10,4),
    deviation         DECIMAL(10,4),

    -- Arbitrage specific
    price_sum         DECIMAL(8,4),
    arbitrage_margin  DECIMAL(8,4),

    -- Status
    is_resolved       BOOLEAN   DEFAULT FALSE,
    resolved_at       TIMESTAMP,

    -- Timestamps
    detected_at       TIMESTAMP DEFAULT NOW(),
    created_at        TIMESTAMP DEFAULT NOW(),

    CONSTRAINT valid_severity CHECK (
        severity IN ('low', 'medium', 'high', 'critical')
    ),
    CONSTRAINT valid_alert_type CHECK (
        alert_type IN (
            'accuracy_drop', 'arbitrage_opportunity',
            'producer_down', 'low_coverage', 'bias_detected',
            'producer_timeout'
        )
    )
);

-- ================================================================
-- TABLE 4: Pipeline health metrics
-- Monitors producer/consumer health
-- ================================================================
CREATE TABLE IF NOT EXISTS pipeline_health (
    id                  SERIAL PRIMARY KEY,
    component           VARCHAR(100) NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'healthy',
    messages_processed  INTEGER      DEFAULT 0,
    error_count         INTEGER      DEFAULT 0,
    last_message_at     TIMESTAMP,
    latency_ms          INTEGER,
    details             JSONB,
    recorded_at         TIMESTAMP    DEFAULT NOW(),

    CONSTRAINT valid_status CHECK (
        status IN ('healthy', 'degraded', 'down', 'unknown')
    )
);

-- ================================================================
-- INDEXES for Grafana query performance
-- ================================================================
CREATE INDEX IF NOT EXISTS idx_correlations_location
    ON market_correlations (location_name);

CREATE INDEX IF NOT EXISTS idx_correlations_created
    ON market_correlations (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_correlations_type
    ON market_correlations (market_type);

CREATE INDEX IF NOT EXISTS idx_correlations_method
    ON market_correlations (correlation_method);

CREATE INDEX IF NOT EXISTS idx_aggregates_location_type
    ON market_accuracy_aggregates (location_name, market_type);

CREATE INDEX IF NOT EXISTS idx_aggregates_window
    ON market_accuracy_aggregates (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_type
    ON anomaly_alerts (alert_type, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_unresolved
    ON anomaly_alerts (is_resolved, detected_at DESC)
    WHERE is_resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_health_component
    ON pipeline_health (component, recorded_at DESC);

-- ================================================================
-- VIEWS for Grafana dashboards
-- ================================================================

-- View 1: Current accuracy per location
CREATE OR REPLACE VIEW v_current_accuracy AS
SELECT
    location_name,
    market_type,
    ROUND(AVG(accuracy_rate) * 100, 2)        AS accuracy_pct,
    ROUND(AVG(avg_prediction_error), 4)        AS avg_error,
    SUM(total_predictions)                     AS total_predictions,
    MAX(window_end)                            AS last_updated
FROM market_accuracy_aggregates
WHERE window_start >= NOW() - INTERVAL '24 hours'
GROUP BY location_name, market_type
ORDER BY accuracy_pct DESC;

-- View 2: Recent correlations with accuracy
CREATE OR REPLACE VIEW v_recent_correlations AS
SELECT
    location_name,
    market_type,
    correlation_method,
    actual_outcome,
    prediction_error,
    yes_price,
    actual_temp_c,
    actual_precip_mm,
    created_at
FROM market_correlations
WHERE created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;

-- View 3: Active alerts
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT
    alert_type,
    severity,
    location_name,
    market_type,
    message,
    metric_value,
    threshold_value,
    detected_at
FROM anomaly_alerts
WHERE is_resolved = FALSE
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
    END,
    detected_at DESC;

-- View 4: Accuracy trend per hour
CREATE OR REPLACE VIEW v_accuracy_trend AS
SELECT
    DATE_TRUNC('hour', window_start) AS hour,
    location_name,
    market_type,
    ROUND(AVG(accuracy_rate) * 100, 2) AS accuracy_pct,
    SUM(total_predictions)             AS predictions
FROM market_accuracy_aggregates
WHERE window_start >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2, 3
ORDER BY 1 DESC;

-- View 5: Pipeline health summary
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT
    component,
    status,
    messages_processed,
    error_count,
    last_message_at,
    latency_ms,
    recorded_at
FROM pipeline_health
WHERE recorded_at = (
    SELECT MAX(recorded_at)
    FROM pipeline_health ph2
    WHERE ph2.component = pipeline_health.component
)
ORDER BY component;