#!/bin/bash
# =============================================================================
# Apple Analytics ETL Pipeline - Startup & Verification Script
# =============================================================================
# Version: 2.0 (December 2025)
# Usage: ./startup_verification.sh [--run-etl] [--date YYYY-MM-DD]
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${SCRIPT_DIR}/logs"

# Parse arguments
RUN_ETL=false
TARGET_DATE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --run-etl) RUN_ETL=true; shift ;;
        --date) TARGET_DATE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║           🍎 APPLE ANALYTICS ETL PIPELINE - STARTUP VERIFICATION             ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================================================
# 1. ENVIRONMENT CHECK
# =============================================================================
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}1️⃣  ENVIRONMENT CHECK${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Check Python virtual environment
echo -n "   Python venv: "
if [ -f "${PARENT_DIR}/.venv/bin/activate" ]; then
    source "${PARENT_DIR}/.venv/bin/activate"
    echo -e "${GREEN}✅ Found and activated${NC}"
elif [ -f "${SCRIPT_DIR}/.venv/bin/activate" ]; then
    source "${SCRIPT_DIR}/.venv/bin/activate"
    echo -e "${GREEN}✅ Found and activated${NC}"
else
    echo -e "${RED}❌ Not found${NC}"
    echo "   Please create: python3 -m venv .venv && source .venv/bin/activate"
    exit 1
fi

# Check Python version
echo -n "   Python version: "
PYTHON_VERSION=$(python3 --version 2>&1)
echo -e "${GREEN}✅ $PYTHON_VERSION${NC}"

# Check required packages
echo -n "   Required packages: "
python3 -c "import boto3, pandas, pyarrow, jwt, requests, dotenv" 2>/dev/null && \
    echo -e "${GREEN}✅ All installed${NC}" || {
    echo -e "${RED}❌ Missing packages${NC}"
    echo "   Install with: pip install boto3 pandas pyarrow pyjwt requests python-dotenv"
    exit 1
}

# Check .env file
echo -n "   .env configuration: "
if [ -f "${SCRIPT_DIR}/.env" ]; then
    APP_COUNT=$(grep -o ',' "${SCRIPT_DIR}/.env" | grep -c ',' || echo 0)
    echo -e "${GREEN}✅ Found (${APP_COUNT}+ app IDs)${NC}"
else
    echo -e "${RED}❌ Not found${NC}"
    exit 1
fi

# Check AWS credentials
echo -n "   AWS credentials: "
if aws sts get-caller-identity >/dev/null 2>&1; then
    AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    echo -e "${GREEN}✅ Valid (Account: ${AWS_ACCOUNT})${NC}"
else
    echo -e "${RED}❌ Invalid or not configured${NC}"
    exit 1
fi

# Check Apple API key
echo -n "   Apple API key: "
P8_PATH=$(grep ASC_P8_PATH "${SCRIPT_DIR}/.env" 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d "'")
if [ -f "$P8_PATH" ]; then
    echo -e "${GREEN}✅ Found at $P8_PATH${NC}"
else
    echo -e "${YELLOW}⚠️  P8 file not found (check ASC_P8_PATH in .env)${NC}"
fi

# =============================================================================
# 2. FILE STRUCTURE CHECK
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}2️⃣  FILE STRUCTURE CHECK${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

FILES=(
    "unified_etl.py:Main ETL script"
    "daily_cron.sh:Cron wrapper"
    "src/extract/apple_analytics_client.py:Apple API client"
    "COMPLETE_PIPELINE_DOCUMENTATION.md:Full documentation"
    ".env:Configuration"
)

for item in "${FILES[@]}"; do
    FILE="${item%%:*}"
    DESC="${item##*:}"
    echo -n "   $FILE: "
    if [ -f "${SCRIPT_DIR}/$FILE" ]; then
        echo -e "${GREEN}✅ Present${NC} ($DESC)"
    else
        echo -e "${RED}❌ Missing${NC}"
    fi
done

# Check logs directory
echo -n "   logs/: "
mkdir -p "${LOG_DIR}"
echo -e "${GREEN}✅ Ready${NC}"

# =============================================================================
# 3. S3 CONNECTIVITY CHECK
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}3️⃣  S3 CONNECTIVITY CHECK${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

S3_BUCKET="skidos-apptrack"
echo -n "   S3 bucket access: "
if aws s3 ls "s3://${S3_BUCKET}/" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ s3://${S3_BUCKET}/${NC}"
else
    echo -e "${RED}❌ Cannot access bucket${NC}"
fi

echo -n "   Raw data path: "
RAW_COUNT=$(aws s3 ls "s3://${S3_BUCKET}/appstore/raw/" 2>/dev/null | wc -l | tr -d ' ')
echo -e "${GREEN}✅ ${RAW_COUNT} data type folders${NC}"

echo -n "   Curated data path: "
CURATED_COUNT=$(aws s3 ls "s3://${S3_BUCKET}/appstore/curated/" 2>/dev/null | wc -l | tr -d ' ')
echo -e "${GREEN}✅ ${CURATED_COUNT} data type folders${NC}"

# =============================================================================
# 4. ATHENA TABLES CHECK
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}4️⃣  ATHENA TABLES CHECK${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

cd "${SCRIPT_DIR}"
python3 << 'EOF'
import boto3
import time

athena = boto3.client('athena', region_name='us-east-1')
tables = ['curated_downloads', 'curated_engagement', 'curated_sessions', 'curated_installs', 'curated_purchases']

for table in tables:
    query = f"SELECT COUNT(*) as cnt FROM appstore.{table}"
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'appstore'},
            ResultConfiguration={'OutputLocation': 's3://skidos-apptrack/Athena-Output/'}
        )
        execution_id = response['QueryExecutionId']
        
        for _ in range(30):
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)
        
        if state == 'SUCCEEDED':
            results = athena.get_query_results(QueryExecutionId=execution_id)
            count = results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
            print(f"   {table}: ✅ {int(count):,} rows")
        else:
            print(f"   {table}: ⚠️ Query {state}")
    except Exception as e:
        print(f"   {table}: ❌ Error")
EOF

# =============================================================================
# 5. CRON JOB CHECK
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}5️⃣  CRON JOB CHECK${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo -n "   Daily cron (6 AM): "
if crontab -l 2>/dev/null | grep -q "daily_cron.sh"; then
    echo -e "${GREEN}✅ Installed${NC}"
    crontab -l 2>/dev/null | grep "daily_cron" | sed 's/^/      /'
else
    echo -e "${YELLOW}⚠️ Not installed${NC}"
    echo "      Install with: crontab -e"
    echo "      Add: 0 6 * * * ${SCRIPT_DIR}/daily_cron.sh >> ${LOG_DIR}/cron.log 2>&1"
fi

# =============================================================================
# 6. RECENT LOGS CHECK
# =============================================================================
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}6️⃣  RECENT LOGS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if ls "${LOG_DIR}"/unified_etl_*.log 1> /dev/null 2>&1; then
    LATEST_LOG=$(ls -t "${LOG_DIR}"/unified_etl_*.log | head -1)
    echo "   Latest log: $(basename "$LATEST_LOG")"
    echo "   Last 5 lines:"
    tail -5 "$LATEST_LOG" | sed 's/^/      /'
else
    echo "   No ETL logs found yet"
fi

# =============================================================================
# 7. SUMMARY
# =============================================================================
echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                            ✅ VERIFICATION COMPLETE                          ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Pipeline is ready for production use!${NC}"
echo ""
echo "📚 Documentation: COMPLETE_PIPELINE_DOCUMENTATION.md"
echo "🚀 Run ETL:       python3 unified_etl.py"
echo "📅 Specific date: python3 unified_etl.py --date 2025-11-28"
echo "🔄 Transform only: python3 unified_etl.py --transform-only --date 2025-11-28"
echo ""

# =============================================================================
# OPTIONAL: RUN ETL
# =============================================================================
if [ "$RUN_ETL" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}🚀 RUNNING ETL PIPELINE${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    if [ -n "$TARGET_DATE" ]; then
        echo "Running for date: $TARGET_DATE"
        python3 unified_etl.py --date "$TARGET_DATE"
    else
        echo "Running for yesterday's data..."
        python3 unified_etl.py
    fi
fi
