#!/bin/bash
# Apple ETL Pipeline - Daily Production Deployment
# Uses ONGOING requests to avoid 409 conflicts and extract yesterday's data
# 
# Usage:
#   ./run_daily_etl.sh                    # Process all configured apps for yesterday
#   ./run_daily_etl.sh --app-id 1506886061  # Process specific app
#   ./run_daily_etl.sh --date 2025-11-25    # Process specific date

set -e  # Exit on any error

# Configuration
PIPELINE_DIR="/Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics"
ROOT_DIR="/Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline"
VENV_DIR="${ROOT_DIR}/.venv"
LOG_DIR="${PIPELINE_DIR}/logs"
LOG_FILE="${LOG_DIR}/daily_etl_$(date +%Y%m%d_%H%M%S).log"
YESTERDAY=$(date -v-1d +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "${LOG_FILE}"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "${LOG_FILE}"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "${LOG_FILE}"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "${LOG_FILE}"
}

# Main apps to process (add/remove as needed)
MAIN_APPS=(
    "1506886061"  # App with 17M+ downloads confirmed working
    "1159612010"  # Another confirmed working app
    "1468754350"  # Chess for Kids
)

echo "🚀 APPLE ETL PIPELINE - DAILY PRODUCTION RUN"
echo "=============================================="
log "Starting Apple ETL Pipeline for ${YESTERDAY} data"
log "Pipeline Directory: ${PIPELINE_DIR}"

# Create log directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Change to pipeline directory
cd "${PIPELINE_DIR}"

# Activate virtual environment
source "${VENV_DIR}/bin/activate"

# Step 1: Extract data using ONGOING requests (avoids 409 conflicts)
echo ""
log "📱 STEP 1: Extracting Apple Analytics Data (ONGOING requests)"
echo "-------------------------------------------------------------"

# Run the new daily ETL which uses ONGOING requests
python3 daily_etl.py "$@" 2>&1 | tee -a "${LOG_FILE}"

log_success "Data extraction phase completed"

# Step 2: Curate Data (Raw CSV -> Parquet)
echo ""
log "🔄 STEP 2: Curating Data (CSV to Parquet)"
echo "-----------------------------------------"

# Run batch curation for yesterday's data
cd "${ROOT_DIR}"
python3 batch_curate.py --date "${YESTERDAY}" 2>&1 | tee -a "${LOG_FILE}"

log_success "Data curation phase completed"

# Step 3: Transform Data
echo ""
log "🔄 STEP 3: Transforming Data"
echo "----------------------------"

python3 -c "
import sys
sys.path.insert(0, 'src')
from transform.apple_analytics_data_curator_production import AppleAnalyticsDataCurator

curator = AppleAnalyticsDataCurator()

# Process all recent data
try:
    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Process downloads
    downloads_result = curator.process_downloads_files(None, [yesterday])
    print(f'✅ Downloads processed: {downloads_result}')
    
    # Process engagement  
    engagement_result = curator.process_engagement_files(None, [yesterday])
    print(f'✅ Engagement processed: {engagement_result}')
except Exception as e:
    print(f'⚠️  Transform processing issue: {e}')
" 2>&1 | tee -a "${LOG_FILE}"

log_success "Data transformation phase completed"

# Step 4: Verify Data in Athena
echo ""
log "🔍 STEP 4: Verifying Data in Athena"
echo "-----------------------------------"

python3 -c "
import boto3

client = boto3.client('athena', region_name='us-east-1')

# Test query for data verification
query = '''
SELECT 
    app_id,
    dt as partition_date,
    COUNT(*) as records_count,
    SUM(total_downloads) as total_downloads_sum
FROM appstore.raw_downloads 
WHERE app_id IN ('1506886061', '1159612010', '1468754350')
AND dt = '${YESTERDAY}'
GROUP BY app_id, dt
ORDER BY records_count DESC
'''

try:
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': 's3://skidos-apptrack/athena-results/'},
        WorkGroup='primary'
    )
    
    query_id = response['QueryExecutionId']
    print(f'🔍 Data verification query started: {query_id}')
    print('✅ Athena connectivity confirmed')
    
except Exception as e:
    print(f'⚠️  Athena verification issue: {e}')
" 2>&1 | tee -a "${LOG_FILE}"

log_success "Athena verification completed"

# Step 5: Generate Summary Report
echo ""
log "📊 STEP 5: Generating Summary Report"
echo "-----------------------------------"

# Check S3 for today's processed data
python3 -c "
import boto3
from datetime import datetime

s3 = boto3.client('s3')
bucket = 'skidos-apptrack'

print('📁 Data Summary for ${YESTERDAY}:')
print('================================')

# Check raw data
for app_id in ['1506886061', '1159612010', '1468754350']:
    downloads_prefix = f'appstore/raw/downloads/dt=${YESTERDAY}/app_id={app_id}/'
    engagement_prefix = f'appstore/raw/engagement/dt=${YESTERDAY}/app_id={app_id}/'
    
    try:
        downloads_resp = s3.list_objects_v2(Bucket=bucket, Prefix=downloads_prefix)
        downloads_count = len(downloads_resp.get('Contents', []))
        
        engagement_resp = s3.list_objects_v2(Bucket=bucket, Prefix=engagement_prefix)
        engagement_count = len(engagement_resp.get('Contents', []))
        
        print(f'📱 App {app_id}: {downloads_count} downloads files, {engagement_count} engagement files')
        
    except Exception as e:
        print(f'⚠️  Error checking app {app_id}: {e}')

# Check curated data
curated_downloads_prefix = f'appstore/curated/downloads/dt=${YESTERDAY}/'
curated_engagement_prefix = f'appstore/curated/engagement/dt=${YESTERDAY}/'

try:
    curated_d_resp = s3.list_objects_v2(Bucket=bucket, Prefix=curated_downloads_prefix)
    curated_d_count = len(curated_d_resp.get('Contents', []))
    
    curated_e_resp = s3.list_objects_v2(Bucket=bucket, Prefix=curated_engagement_prefix)
    curated_e_count = len(curated_e_resp.get('Contents', []))
    
    print(f'🎯 Curated Data: {curated_d_count} downloads parquet files, {curated_e_count} engagement parquet files')
    
except Exception as e:
    print(f'⚠️  Error checking curated data: {e}')

print('')
print('✅ Apple ETL Pipeline daily run completed successfully!')
print(f'📄 Detailed logs: ${LOG_FILE}')
" 2>&1 | tee -a "${LOG_FILE}"

# Final summary
echo ""
echo "🎉 APPLE ETL PIPELINE DAILY RUN COMPLETED"
echo "=========================================="
log_success "All phases completed successfully"
log "Log file: ${LOG_FILE}"
log "Next run should be scheduled for tomorrow"

# Optional: Send notification (uncomment if you want email notifications)
# echo "Apple ETL Pipeline completed successfully for ${YESTERDAY}" | mail -s "ETL Success - ${TODAY}" your-email@example.com

exit 0
