# ETL Pipeline Issues & Fix Plan

> **Generated:** 2025-11-28
> **Status:** ⚠️ Issues Identified - Fixes Required

---

## 🔴 Critical Issues Found

### Issue 1: Raw Table Schema Mismatch

**Problem:** The `raw_downloads` table DDL does not match the actual CSV columns from Apple.

| Table Expects | CSV Actually Has |
|---------------|------------------|
| `date` | `Date` |
| `source_type` | `App Name` |
| `territory` | `App Apple Identifier` |
| `device` | `Download Type` |
| `total_downloads` | `App Version` |
| `first_time_downloads` | `Device` |
| `redownloads` | `Platform Version` |
| `app_name` | `Source Type` |
| - | `Source Info` |
| - | `Campaign` |
| - | `Page Type` |
| - | `Page Title` |
| - | `Pre-Order` |
| - | `Territory` |
| - | `Counts` |

**Impact:** Queries return garbage data because columns are misaligned.

---

### Issue 2: Curated Partition Structure Wrong

**Problem:** The `appstore_downloads` table expects:
```
s3://bucket/appstore/curated/downloads/dt=${dt}/app_id_part=${app_id_part}/
```

But actual S3 structure has:
```
s3://bucket/appstore/curated/downloads/dt=2025-11-27/app_id=1506886061/  ❌
s3://bucket/appstore/curated/downloads/year=2024/month=01/              ❌
```

**Impact:** Athena can't find the parquet files because partition format doesn't match.

---

### Issue 3: Mixed File Formats

**Problem:** 
- Raw folder has CSV files (tab-separated)
- Curated folder has some Parquet files
- Table expects specific SerDe but files don't match

---

## ✅ Recommended Fixes

### Fix 1: Recreate Raw Tables with Correct Schema

```sql
-- Drop and recreate raw_downloads with correct schema
DROP TABLE IF EXISTS appstore.raw_downloads;

CREATE EXTERNAL TABLE appstore.raw_downloads (
    `date` string,
    `app_name` string,
    `app_apple_identifier` string,
    `download_type` string,
    `app_version` string,
    `device` string,
    `platform_version` string,
    `source_type` string,
    `source_info` string,
    `campaign` string,
    `page_type` string,
    `page_title` string,
    `pre_order` string,
    `territory` string,
    `counts` bigint
)
PARTITIONED BY (dt string, app_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://skidos-apptrack/appstore/raw/downloads/'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.format'='yyyy-MM-dd',
    'projection.dt.range'='2024-01-01,NOW',
    'projection.app_id.type'='injected',
    'storage.location.template'='s3://skidos-apptrack/appstore/raw/downloads/dt=${dt}/app_id=${app_id}/'
);
```

### Fix 2: Recreate Curated Tables with Correct Partition

```sql
-- Option A: Use app_id instead of app_id_part
DROP TABLE IF EXISTS appstore.appstore_downloads;

CREATE EXTERNAL TABLE appstore.appstore_downloads (
    `report_date` string,
    `app_name` string,
    `app_apple_id` string,
    `download_type` string,
    `source_type` string,
    `territory` string,
    `device` string,
    `counts` bigint
)
PARTITIONED BY (dt string, app_id string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://skidos-apptrack/appstore/curated/downloads/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.format'='yyyy-MM-dd',
    'projection.dt.range'='2024-01-01,NOW',
    'projection.app_id.type'='injected',
    'storage.location.template'='s3://skidos-apptrack/appstore/curated/downloads/dt=${dt}/app_id=${app_id}/'
);
```

### Fix 3: Update Curation Pipeline

The curation pipeline needs to:
1. Read CSVs with correct column mapping
2. Write Parquet files to correct partition structure: `dt=YYYY-MM-DD/app_id=NNNN/`
3. Ensure column names match table schema

---

## 📊 Current State Summary

| Component | Status | Issue |
|-----------|--------|-------|
| **Raw S3 Data** | ✅ Good | Files present with correct partitions |
| **Raw Athena Tables** | ❌ Broken | Schema doesn't match CSV columns |
| **Curated S3 Data** | ⚠️ Mixed | Some correct, some wrong partition format |
| **Curated Athena Tables** | ❌ Broken | Partition template wrong (app_id_part vs app_id) |
| **Daily ETL Extract** | ✅ Works | Downloads files correctly |
| **Daily ETL Transform** | ⚠️ Issues | May not create correct partition structure |

---

## 🔧 Action Items

1. **[HIGH]** Fix raw table DDL to match actual CSV columns
2. **[HIGH]** Fix curated table DDL to match actual S3 partition structure  
3. **[MEDIUM]** Update curation code to output correct partition format
4. **[MEDIUM]** Re-run curation for existing data
5. **[LOW]** Add data validation tests

---

## Quick Test Queries (After Fixes)

```sql
-- Test raw table
SELECT * FROM appstore.raw_downloads 
WHERE dt = '2025-11-27' AND app_id = '1506886061'
LIMIT 5;

-- Test curated table
SELECT * FROM appstore.appstore_downloads
WHERE dt = '2025-11-27' AND app_id = '1506886061'
LIMIT 5;
```
