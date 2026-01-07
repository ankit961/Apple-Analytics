#!/bin/bash
# =============================================================================
# Apple Analytics ETL - Daily Cron Script with Slack Notifications
# =============================================================================
# Schedule: 30 9 * * * (9:30 AM UTC = 3:00 PM IST)
#   crontab -e
#   30 9 * * * ~/apple-analytics/daily_cron.sh >> ~/apple-analytics/logs/cron.log 2>&1
# 
# Slack Setup:
#   1. Create Incoming Webhook at https://api.slack.com/apps
#   2. Save webhook URL: echo "https://hooks.slack.com/services/XXX" > ~/.slack_webhook
# =============================================================================

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="${SCRIPT_DIR}/logs"
DATE=$(date +%Y-%m-%d)
DATE_COMPACT=$(date +%Y%m%d)
START_TIME=$(date +%s)

# Create log directory
mkdir -p "$LOG_DIR"

# =============================================================================
# Slack Notification Function
# =============================================================================
send_slack_notification() {
    local status="$1"      # success, failure, started
    local message="$2"
    local details="$3"
    
    # Read webhook URL from file
    SLACK_WEBHOOK_FILE="${SCRIPT_DIR}/.slack_webhook"
    if [ ! -f "$SLACK_WEBHOOK_FILE" ]; then
        echo "Slack webhook not configured. Skipping notification."
        return 0
    fi
    
    SLACK_WEBHOOK=$(cat "$SLACK_WEBHOOK_FILE" | tr -d '[:space:]')
    if [ -z "$SLACK_WEBHOOK" ]; then
        echo "Slack webhook URL is empty. Skipping notification."
        return 0
    fi
    
    # Set color and emoji based on status
    case $status in
        success)
            COLOR="#36a64f"  # Green
            EMOJI=":white_check_mark:"
            ;;
        failure)
            COLOR="#ff0000"  # Red
            EMOJI=":x:"
            ;;
        started)
            COLOR="#ffcc00"  # Yellow
            EMOJI=":hourglass_flowing_sand:"
            ;;
        *)
            COLOR="#808080"  # Gray
            EMOJI=":information_source:"
            ;;
    esac
    
    # Build payload
    PAYLOAD=$(cat <<EOF
{
    "attachments": [
        {
            "color": "${COLOR}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "${EMOJI} *Apple Analytics ETL*\n${message}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "${details}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "Server: EC2 Production | Date: ${DATE} | Time: $(date '+%H:%M:%S UTC')"
                        }
                    ]
                }
            ]
        }
    ]
}
EOF
)
    
    # Send to Slack
    curl -s -X POST -H 'Content-type: application/json' --data "$PAYLOAD" "$SLACK_WEBHOOK" > /dev/null 2>&1
    echo "Slack notification sent: $status"
}

# =============================================================================
# Parse Results JSON - Find the latest results file for today
# =============================================================================
parse_results() {
    # Find the latest results file for today (pattern: unified_etl_results_YYYYMMDD_*.json)
    RESULTS_FILE=$(ls -t ${LOG_DIR}/unified_etl_results_${DATE_COMPACT}_*.json 2>/dev/null | head -1)
    
    if [ -z "$RESULTS_FILE" ] || [ ! -f "$RESULTS_FILE" ]; then
        echo "No results file found for today: ${DATE_COMPACT}"
        return 1
    fi
    
    echo "Parsing results from: $RESULTS_FILE"
    
    # Extract values using Python (more reliable than jq)
    PYTHON_CMD="/home/ec2-user/anaconda3/bin/python3"
    if [ ! -f "$PYTHON_CMD" ]; then
        PYTHON_CMD="python3"
    fi
    
    STATS=$($PYTHON_CMD -c "
import json
try:
    with open('$RESULTS_FILE', 'r') as f:
        data = json.load(f)
    # Keys are at root level, not under 'summary'
    print(f\"APPS_PROCESSED={data.get('apps_processed', 0)}\")
    print(f\"APPS_SUCCESS={data.get('apps_successful', 0)}\")
    print(f\"APPS_FAILED={data.get('apps_processed', 0) - data.get('apps_successful', 0)}\")
    print(f\"FILES_EXTRACTED={data.get('files_extracted', 0)}\")
    print(f\"FILES_CURATED={data.get('files_curated', 0)}\")
    print(f\"TOTAL_ROWS={data.get('total_rows', 0)}\")
    print(f\"ERRORS={len(data.get('errors', []))}\")
except Exception as e:
    print(f'PARSE_ERROR={e}')
" 2>&1)
    
    echo "Parse output: $STATS"
    eval "$STATS"
}

# =============================================================================
# Main Execution
# =============================================================================
echo "============================================================"
echo "Apple Analytics ETL - $(date)"
echo "============================================================"

# Send start notification
send_slack_notification "started" "ETL Job Started" "Daily data extraction initiated for all configured apps."

# Use Anaconda Python (EC2 production)
export PATH="/home/ec2-user/anaconda3/bin:$PATH"

# Run unified ETL
cd "$SCRIPT_DIR"
ETL_EXIT_CODE=0
python3 unified_etl.py || ETL_EXIT_CODE=$?

# Calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

# Parse results
APPS_PROCESSED=0
APPS_SUCCESS=0
APPS_FAILED=0
FILES_EXTRACTED=0
FILES_CURATED=0
TOTAL_ROWS=0
ERRORS=0
parse_results

# Build details string
DETAILS="• *Apps Processed:* ${APPS_PROCESSED} (${APPS_SUCCESS} successful, ${APPS_FAILED} failed)
• *Files Extracted:* ${FILES_EXTRACTED}
• *Files Curated:* ${FILES_CURATED}
• *Total Rows:* ${TOTAL_ROWS}
• *Duration:* ${DURATION_MIN}m ${DURATION_SEC}s"

if [ $ERRORS -gt 0 ]; then
    DETAILS="${DETAILS}
• *Errors:* ${ERRORS}"
fi

# Send completion notification
if [ $ETL_EXIT_CODE -eq 0 ]; then
    send_slack_notification "success" "ETL Job Completed Successfully" "$DETAILS"
    echo "============================================================"
    echo "ETL Complete - $(date)"
    echo "============================================================"
else
    DETAILS="${DETAILS}
• *Exit Code:* ${ETL_EXIT_CODE}
• Check logs at: ${LOG_DIR}/"
    send_slack_notification "failure" "ETL Job Failed" "$DETAILS"
    echo "============================================================"
    echo "ETL FAILED - $(date)"
    echo "============================================================"
    exit $ETL_EXIT_CODE
fi
