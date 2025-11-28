#!/bin/bash
# Quick Start Script for Apple Analytics Production ETL
# This script provides easy commands to run the complete production ETL pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project directory
PROJECT_DIR="/Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics"

echo -e "${BLUE}🚀 Apple Analytics Production ETL - Quick Start${NC}"
echo "=============================================="

# Function to print colored status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if we're in the right directory
if [[ ! -d "$PROJECT_DIR" ]]; then
    print_error "Project directory not found: $PROJECT_DIR"
    exit 1
fi

cd "$PROJECT_DIR"

# Check dependencies
echo -e "${BLUE}1️⃣ Checking dependencies...${NC}"
python3 -c "import boto3, pandas, pyarrow" 2>/dev/null && print_status "Python dependencies OK" || {
    print_error "Missing dependencies. Install with: pip install boto3 pandas pyarrow"
    exit 1
}

# Check AWS credentials
echo -e "${BLUE}2️⃣ Checking AWS credentials...${NC}"
aws sts get-caller-identity >/dev/null 2>&1 && print_status "AWS credentials OK" || {
    print_error "AWS credentials not configured. Run: aws configure"
    exit 1
}

# Create necessary directories
echo -e "${BLUE}3️⃣ Setting up project structure...${NC}"
mkdir -p logs reports config data/{raw,curated}
print_status "Directory structure ready"

# Show menu
echo -e "${BLUE}4️⃣ Choose operation:${NC}"
echo "1. Daily Production ETL (recommended)"
echo "2. Check Apple Analytics Request Status"  
echo "3. Schema Verification"
echo "4. Backfill Historical Data"
echo "5. View Recent Results"
echo "6. Full Status Report"

read -p "Enter choice [1-6]: " choice

case $choice in
    1)
        echo -e "${BLUE}🔄 Running Daily Production ETL...${NC}"
        python3 production_manager.py --operation daily_production
        ;;
    2)
        echo -e "${BLUE}📊 Checking Apple Analytics Request Status...${NC}"
        python3 production_manager.py --operation status_check
        ;;
    3)
        echo -e "${BLUE}🔍 Running Schema Verification...${NC}"
        python3 production_manager.py --operation schema_verify
        ;;
    4)
        echo -e "${BLUE}📂 Running Backfill Operation...${NC}"
        read -p "Enter app IDs (space-separated): " app_ids
        python3 production_manager.py --operation backfill --apps $app_ids
        ;;
    5)
        echo -e "${BLUE}📋 Recent Results:${NC}"
        ls -la reports/ | tail -10
        echo
        echo "View detailed results:"
        find reports/ -name "*.json" -mtime -1 | head -3 | while read file; do
            echo -e "${GREEN}📄 $file${NC}"
            python3 -c "import json; print(json.dumps(json.load(open('$file')), indent=2))" | head -20
            echo "..."
        done
        ;;
    6)
        echo -e "${BLUE}📊 Full Status Report${NC}"
        echo "=================="
        
        echo -e "${YELLOW}📁 Project Structure:${NC}"
        find . -type d -maxdepth 2 | sort
        
        echo -e "${YELLOW}📊 Recent Activity:${NC}"
        ls -la logs/ | tail -5
        
        echo -e "${YELLOW}🔍 Request Registry Status:${NC}"
        if [[ -f "config/production_request_registry.json" ]]; then
            python3 -c "
import json
registry = json.load(open('config/production_request_registry.json'))
stats = registry.get('statistics', {})
print(f\"Total requests created: {stats.get('total_requests_created', 0)}\")
print(f\"Duplicates prevented: {stats.get('total_duplicates_prevented', 0)}\")
print(f\"Apps with ready data: {len(registry.get('app_status', {}))}\")
"
        else
            echo "No registry file found (will be created on first run)"
        fi
        
        echo -e "${YELLOW}☁️  AWS Resources:${NC}"
        echo "S3 Bucket: apple-analytics-pipeline"
        echo "Athena Databases: curated, appstore"
        
        ;;
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

echo
echo -e "${GREEN}🏁 Operation completed!${NC}"
echo
echo "💡 Useful commands:"
echo "  • View logs: tail -f logs/production_etl_$(date +%Y%m%d).log"
echo "  • Check results: ls -la reports/"
echo "  • Run again: $0"
