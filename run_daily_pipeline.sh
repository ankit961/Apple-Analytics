#!/bin/bash
# filepath: /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/run_daily_pipeline.sh
# Apple Analytics Daily Pipeline - Automated Execution
# This script runs daily via cron to extract and curate Apple Analytics data

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="${SCRIPT_DIR}/logs"
VENV_PATH="${SCRIPT_DIR}/.venv"
DATE=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)

# Setup logging
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/daily_pipeline_${DATE}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Starting Daily Apple Analytics Pipeline"
log "Target Date: $YESTERDAY"
log "=========================================="

# Activate virtual environment
source "${VENV_PATH}/bin/activate"

# Change to script directory
cd "$SCRIPT_DIR"

# Step 1: Extract data from Apple Analytics API
log "Step 1: Extracting data from Apple Analytics API..."
cd "${SCRIPT_DIR}/Apple-Analytics"
python3 daily_etl.py --date "$YESTERDAY" >> "$LOG_FILE" 2>&1
EXTRACT_STATUS=$?

if [ $EXTRACT_STATUS -eq 0 ]; then
    log "✅ Extraction completed successfully"
else
    log "⚠️ Extraction completed with warnings (exit code: $EXTRACT_STATUS)"
fi

# Step 2: Curate raw data to Parquet
log "Step 2: Curating raw data to Parquet..."
cd "$SCRIPT_DIR"
python3 batch_curate.py --date "$YESTERDAY" >> "$LOG_FILE" 2>&1
CURATE_STATUS=$?

if [ $CURATE_STATUS -eq 0 ]; then
    log "✅ Curation completed successfully"
else
    log "⚠️ Curation completed with warnings (exit code: $CURATE_STATUS)"
fi

# Step 3: Refresh Athena table partitions
log "Step 3: Refreshing Athena partitions..."
TABLES=("raw_downloads" "raw_engagement" "raw_sessions" "raw_installs" "raw_purchases" "curated_downloads" "curated_engagement" "curated_sessions" "curated_installs" "curated_purchases" "curated_reviews")

for TABLE in "${TABLES[@]}"; do
    aws athena start-query-execution \
        --query-string "MSCK REPAIR TABLE appstore.${TABLE}" \
        --result-configuration "OutputLocation=s3://skidos-apptrack/athena-results/" \
        --region us-east-1 >> "$LOG_FILE" 2>&1 || true
done

log "=========================================="
log "Daily Pipeline Complete"
log "Log file: $LOG_FILE"
log "=========================================="

# Cleanup old logs (keep last 30 days)
find "$LOG_DIR" -name "daily_pipeline_*.log" -mtime +30 -delete 2>/dev/null || true

exit 0
