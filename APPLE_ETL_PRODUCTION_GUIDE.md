
# Apple ETL Pipeline - Production Usage Guide
Generated: 2025-11-27 22:43:37

## Quick Start Commands

### 1. Extract Fresh Data
```python
from src.extract.apple_analytics_client import AppleAnalyticsRequestor
from src.extract.focused_data_extractor import FocusedAppleDataExtractor

# Create Apple API request
client = AppleAnalyticsRequestor()
request_id = client.create_or_reuse_one_time_request(
    app_id="1506886061", 
    start_date="2025-11-25", 
    end_date="2025-11-27"
)

# Extract data when ready
extractor = FocusedAppleDataExtractor()
result = extractor.extract_app_business_data(app_id="1506886061")
```

### 2. Transform Data
```python
from src.transform.apple_analytics_data_curator_production import AppleAnalyticsDataCurator

curator = AppleAnalyticsDataCurator()

# Process downloads
downloads_result = curator.process_downloads_files("1506886061", ["2025-11-27"])

# Process engagement  
engagement_result = curator.process_engagement_files("1506886061", ["2025-11-27"])
```

### 3. Query Data
```sql
-- Raw downloads data (requires app_id in WHERE clause)
SELECT COUNT(*) as total_downloads,
       COUNT(DISTINCT territory) as countries
FROM appstore.raw_downloads 
WHERE app_id = '1506886061'
AND dt >= '2025-11-01';

-- Curated downloads data
SELECT app_id, metric_date, 
       SUM(total_downloads) as daily_downloads
FROM curated.downloads
WHERE app_id = 1506886061
AND dt >= '2025-11-01'
GROUP BY app_id, metric_date
ORDER BY metric_date DESC;
```

## Data Locations

- **Raw Data**: `s3://skidos-apptrack/appstore/raw/downloads/dt=YYYY-MM-DD/app_id=XXXXXXX/`
- **Curated Data**: `s3://skidos-apptrack/appstore/curated/downloads/dt=YYYY-MM-DD/app_id=XXXXXXX/`
- **Athena Results**: `s3://skidos-apptrack/athena-results/`

## Important Notes

1. **Partition Constraints**: Athena queries on `appstore.raw_*` tables MUST include `app_id = 'specific_value'` in WHERE clause
2. **Data Types**: Raw tables use `app_id` as STRING, curated tables use BIGINT
3. **File Format**: Raw data is CSV (tab-separated), curated data is Parquet
4. **Daily Schedule**: Run extraction daily at 4 PM for previous day's data

## Troubleshooting

- **409 Conflicts**: Use existing request IDs from registry files
- **Partition Errors**: Always include specific app_id in WHERE clauses  
- **Timeout Issues**: Use smaller date ranges for large apps
- **Transform Errors**: Check CSV delimiter (tab vs comma)

## Production Pipeline Status: ✅ FUNCTIONAL
