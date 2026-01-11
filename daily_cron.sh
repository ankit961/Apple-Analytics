#!/bin/bash
# =============================================================================
# Apple Analytics ETL - Daily Cron Script
# =============================================================================
# Schedule this script to run daily at 6 AM:
#   crontab -e
#   0 6 * * * /path/to/Apple-Analytics/daily_cron.sh >> /path/to/logs/cron.log 2>&1
# =============================================================================

set -e

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${SCRIPT_DIR}/logs"
DATE=$(date +%Y-%m-%d)

# Create log directory
mkdir -p "$LOG_DIR"

echo "============================================================"
echo "Apple Analytics ETL - $(date)"
echo "============================================================"

# Activate virtual environment
if [ -f "${PARENT_DIR}/.venv/bin/activate" ]; then
    source "${PARENT_DIR}/.venv/bin/activate"
elif [ -f "${SCRIPT_DIR}/.venv/bin/activate" ]; then
    source "${SCRIPT_DIR}/.venv/bin/activate"
else
    echo "ERROR: Virtual environment not found"
    exit 1
fi

# Run unified ETL (extracts yesterday's data for all 92 apps)
cd "$SCRIPT_DIR"
python3 unified_etl.py

echo "============================================================"
echo "ETL Complete - $(date)"
echo "============================================================"
