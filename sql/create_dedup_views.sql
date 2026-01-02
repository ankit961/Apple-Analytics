-- ============================================================================
-- Apple Analytics - Deduplication Views for Handling Corrections
-- ============================================================================
--
-- Purpose: Handle Apple data corrections/re-sends by selecting latest dt
--          per logical key (metric_date + dimensions)
--
-- Problem: Apple can re-send corrected data in later processingDate batches.
--          This means the same logical row (metric_date=2025-11-15, territory=US)
--          might appear in multiple dt partitions (dt=2025-11-16, dt=2025-11-20).
--          The later dt contains the corrected/authoritative values.
--
-- Solution: Create views that use ROW_NUMBER() to pick the latest dt per
--           logical key, ensuring queries get deduplicated, corrected data.
--
-- Usage: Query these views instead of base tables for accurate metrics
--
-- Author: Apple Analytics ETL Pipeline
-- Date: January 2, 2026
-- ============================================================================

-- ============================================================================
-- VIEW: appstore.v_downloads_dedup
-- ============================================================================
-- Description: Deduplicated downloads - picks latest dt per logical key
-- Logical Key: metric_date, app_id, territory, download_type, source_type, 
--              device, platform_version
-- ============================================================================

CREATE OR REPLACE VIEW appstore.v_downloads_dedup AS
WITH ranked AS (
    SELECT 
        metric_date,
        app_name,
        app_id,
        territory,
        total_downloads,
        download_type,
        source_type,
        device,
        platform_version,
        app_id_part,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                metric_date,
                app_id,
                territory,
                COALESCE(download_type, ''),
                COALESCE(source_type, ''),
                COALESCE(device, ''),
                COALESCE(platform_version, '')
            ORDER BY dt DESC
        ) as rn
    FROM appstore.curated_downloads
)
SELECT 
    metric_date,
    app_name,
    app_id,
    territory,
    total_downloads,
    download_type,
    source_type,
    device,
    platform_version,
    app_id_part,
    dt as latest_dt
FROM ranked
WHERE rn = 1;


-- ============================================================================
-- VIEW: appstore.v_engagement_dedup
-- ============================================================================
-- Description: Deduplicated engagement - picks latest dt per logical key
-- Logical Key: metric_date, app_id, territory, source_type, device
-- ============================================================================

CREATE OR REPLACE VIEW appstore.v_engagement_dedup AS
WITH ranked AS (
    SELECT 
        metric_date,
        app_name,
        app_id,
        territory,
        impressions,
        source_type,
        device,
        app_id_part,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                metric_date,
                app_id,
                territory,
                COALESCE(source_type, ''),
                COALESCE(device, '')
            ORDER BY dt DESC
        ) as rn
    FROM appstore.curated_engagement
)
SELECT 
    metric_date,
    app_name,
    app_id,
    territory,
    impressions,
    source_type,
    device,
    app_id_part,
    dt as latest_dt
FROM ranked
WHERE rn = 1;


-- ============================================================================
-- VIEW: appstore.v_sessions_dedup
-- ============================================================================
-- Description: Deduplicated sessions - picks latest dt per logical key
-- Logical Key: metric_date, app_id, territory, device
-- ============================================================================

CREATE OR REPLACE VIEW appstore.v_sessions_dedup AS
WITH ranked AS (
    SELECT 
        metric_date,
        app_name,
        app_id,
        territory,
        sessions,
        device,
        app_id_part,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                metric_date,
                app_id,
                territory,
                COALESCE(device, '')
            ORDER BY dt DESC
        ) as rn
    FROM appstore.curated_sessions
)
SELECT 
    metric_date,
    app_name,
    app_id,
    territory,
    sessions,
    device,
    app_id_part,
    dt as latest_dt
FROM ranked
WHERE rn = 1;


-- ============================================================================
-- VIEW: appstore.v_installs_dedup
-- ============================================================================
-- Description: Deduplicated installs - picks latest dt per logical key
-- Logical Key: metric_date, app_id, territory, event, device
-- ============================================================================

CREATE OR REPLACE VIEW appstore.v_installs_dedup AS
WITH ranked AS (
    SELECT 
        metric_date,
        app_name,
        app_id,
        territory,
        counts,
        event,
        device,
        app_id_part,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                metric_date,
                app_id,
                territory,
                COALESCE(event, ''),
                COALESCE(device, '')
            ORDER BY dt DESC
        ) as rn
    FROM appstore.curated_installs
)
SELECT 
    metric_date,
    app_name,
    app_id,
    territory,
    counts,
    event,
    device,
    app_id_part,
    dt as latest_dt
FROM ranked
WHERE rn = 1;


-- ============================================================================
-- VIEW: appstore.v_purchases_dedup
-- ============================================================================
-- Description: Deduplicated purchases - picks latest dt per logical key
-- Logical Key: metric_date, app_id, territory, device
-- ============================================================================

CREATE OR REPLACE VIEW appstore.v_purchases_dedup AS
WITH ranked AS (
    SELECT 
        metric_date,
        app_name,
        app_id,
        territory,
        purchases,
        proceeds_usd,
        device,
        app_id_part,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                metric_date,
                app_id,
                territory,
                COALESCE(device, '')
            ORDER BY dt DESC
        ) as rn
    FROM appstore.curated_purchases
)
SELECT 
    metric_date,
    app_name,
    app_id,
    territory,
    purchases,
    proceeds_usd,
    device,
    app_id_part,
    dt as latest_dt
FROM ranked
WHERE rn = 1;


-- ============================================================================
-- AGGREGATED VIEWS FOR DASHBOARDS (using deduplicated data)
-- ============================================================================

-- Daily Downloads Summary (deduplicated)
CREATE OR REPLACE VIEW appstore.v_daily_downloads_summary AS
SELECT 
    metric_date,
    app_id,
    app_name,
    territory,
    SUM(total_downloads) as total_downloads
FROM appstore.v_downloads_dedup
GROUP BY metric_date, app_id, app_name, territory;


-- Daily Engagement Summary (deduplicated)
CREATE OR REPLACE VIEW appstore.v_daily_engagement_summary AS
SELECT 
    metric_date,
    app_id,
    app_name,
    territory,
    source_type,
    SUM(impressions) as total_impressions
FROM appstore.v_engagement_dedup
GROUP BY metric_date, app_id, app_name, territory, source_type;


-- Daily Sessions Summary (deduplicated)
CREATE OR REPLACE VIEW appstore.v_daily_sessions_summary AS
SELECT 
    metric_date,
    app_id,
    app_name,
    territory,
    SUM(sessions) as total_sessions
FROM appstore.v_sessions_dedup
GROUP BY metric_date, app_id, app_name, territory;


-- Daily Purchases Summary (deduplicated)
CREATE OR REPLACE VIEW appstore.v_daily_purchases_summary AS
SELECT 
    metric_date,
    app_id,
    app_name,
    territory,
    SUM(purchases) as total_purchases,
    SUM(proceeds_usd) as total_proceeds_usd
FROM appstore.v_purchases_dedup
GROUP BY metric_date, app_id, app_name, territory;
