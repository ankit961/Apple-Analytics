#!/usr/bin/env python3
"""
Production ETL Runner for Apple Analytics Pipeline

This script provides easy command-line execution of the unified ETL pipeline
with proper error handling, logging, and monitoring.

Usage Examples:

# Daily run (default mode)
./run_etl_production.py

# Daily run for specific apps
./run_etl_production.py --app-ids 1234567890 0987654321

# Backfill mode for date range
./run_etl_production.py --mode backfill --start-date 2024-11-01 --end-date 2024-11-25 --app-ids 1234567890

# Use custom config file
./run_etl_production.py --config-file /path/to/config.json

# Debug mode with verbose logging
./run_etl_production.py --log-level DEBUG

Environment Variables:
- APPLE_ANALYTICS_CONFIG: Path to config file
- APPLE_ANALYTICS_LOG_LEVEL: Default log level
- AWS_PROFILE: AWS profile to use
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add the source directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def check_dependencies():
    """Check if all required dependencies are available"""
    required_modules = [
        'boto3',
        'pandas', 
        'pyarrow',
        'requests',
        'gzip'
    ]
    
    missing_modules = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"❌ Missing required modules: {', '.join(missing_modules)}")
        print("Please install them with: pip install " + ' '.join(missing_modules))
        return False
    
    return True

def check_aws_credentials():
    """Check if AWS credentials are properly configured"""
    try:
        import boto3
        
        # Try to create a client and make a simple call
        s3_client = boto3.client('s3')
        s3_client.list_buckets()
        return True
        
    except Exception as e:
        print(f"❌ AWS credentials check failed: {e}")
        print("Please configure AWS credentials using:")
        print("  - aws configure")
        print("  - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        print("  - IAM role (if running on EC2)")
        return False

def setup_project_structure():
    """Ensure required project directories exist"""
    project_root = Path(__file__).parent
    
    required_dirs = [
        "logs",
        "config", 
        "data/raw",
        "data/curated",
        "reports"
    ]
    
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
    
    print(f"✅ Project structure ready at {project_root}")

def get_sample_app_ids():
    """Get sample app IDs from your successful extractions"""
    # These are the app IDs from your recent successful extraction
    successful_app_ids = [
        "6444833326",  # Successfully extracted 79 files, 12M+ rows
        "6680158159",  # Another app with good data
        "6416816479",  # Another successful app
    ]
    
    return successful_app_ids

def create_sample_config():
    """Create a sample configuration file if none exists"""
    config_file = Path(__file__).parent / "config" / "etl_config.json"
    
    if not config_file.exists():
        sample_config = {
            "mode": "daily",
            "app_ids": get_sample_app_ids()[:1],  # Start with just one app
            "start_date": "2024-11-18",
            "end_date": "2024-11-25", 
            "s3_bucket": "apple-analytics-pipeline",
            "registry_file": str(config_file.parent / "request_registry.json"),
            "log_level": "INFO",
            "log_to_file": True
        }
        
        with open(config_file, 'w') as f:
            json.dump(sample_config, f, indent=2)
        
        print(f"✅ Created sample config at {config_file}")
        
    return config_file

def main():
    """Main entry point with comprehensive checks and setup"""
    print("🚀 Apple Analytics ETL Production Runner")
    print("=" * 50)
    
    # 1. Check dependencies
    print("1️⃣ Checking dependencies...")
    if not check_dependencies():
        return 1
    print("✅ All dependencies available")
    
    # 2. Check AWS credentials  
    print("\n2️⃣ Checking AWS credentials...")
    if not check_aws_credentials():
        return 1
    print("✅ AWS credentials configured")
    
    # 3. Setup project structure
    print("\n3️⃣ Setting up project structure...")
    setup_project_structure()
    
    # 4. Create sample config if needed
    print("\n4️⃣ Preparing configuration...")
    config_file = create_sample_config()
    
    # 5. Import and run the ETL pipeline
    print("\n5️⃣ Starting ETL pipeline...")
    try:
        from orchestration.unified_production_etl import UnifiedProductionETL, ETLConfig
        
        # Load config
        with open(config_file, 'r') as f:
            config_dict = json.load(f)
        
        # Override with command line args if provided
        if len(sys.argv) > 1:
            import argparse
            
            parser = argparse.ArgumentParser(description="Apple Analytics ETL Pipeline")
            parser.add_argument('--mode', choices=['daily', 'backfill'], 
                               help='ETL mode')
            parser.add_argument('--app-ids', nargs='+',
                               help='App IDs to process')
            parser.add_argument('--start-date',
                               help='Start date (YYYY-MM-DD)')
            parser.add_argument('--end-date', 
                               help='End date (YYYY-MM-DD)')
            parser.add_argument('--config-file',
                               help='Config file path')
            parser.add_argument('--log-level', 
                               choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                               help='Log level')
            
            args = parser.parse_args()
            
            # Override config with args
            if args.mode:
                config_dict['mode'] = args.mode
            if args.app_ids:
                config_dict['app_ids'] = args.app_ids
            if args.start_date:
                config_dict['start_date'] = args.start_date
            if args.end_date:
                config_dict['end_date'] = args.end_date
            if args.log_level:
                config_dict['log_level'] = args.log_level
            if args.config_file:
                with open(args.config_file, 'r') as f:
                    config_dict = json.load(f)
        
        # Create config object
        config = ETLConfig(**config_dict)
        
        # Validate config
        if not config.app_ids:
            print("❌ No app IDs specified. Using sample app IDs.")
            config.app_ids = get_sample_app_ids()[:1]
        
        print(f"📋 Configuration:")
        print(f"   Mode: {config.mode}")
        print(f"   Apps: {config.app_ids}")
        print(f"   Date range: {config.start_date} to {config.end_date}")
        
        # Run the ETL pipeline
        etl = UnifiedProductionETL(config)
        results = etl.run_pipeline()
        
        # Print results
        print("\n" + "=" * 50)
        print("📊 PIPELINE RESULTS")
        print("=" * 50)
        
        if results['success']:
            print("✅ Pipeline completed successfully!")
            
            # Extract phase summary
            extract = results.get('extract_results', {})
            print(f"📥 Extract: {extract.get('total_files_downloaded', 0)} files, "
                  f"{extract.get('total_rows_extracted', 0):,} rows")
            
            # Transform phase summary
            transform = results.get('transform_results', {})
            print(f"🔄 Transform: {transform.get('successful_apps', 0)} apps processed")
            
            # Load phase summary
            load = results.get('load_results', {})
            print(f"📊 Load: {load.get('tables_created', 0)} tables created, "
                  f"{load.get('tables_verified', 0)} verified")
            
            # Athena verification
            verify = results.get('athena_verification', {})
            print(f"🔍 Verification: {verify.get('tests_passed', 0)}/{verify.get('tests_run', 0)} tests passed")
            
        else:
            print("❌ Pipeline failed!")
            if 'error' in results:
                print(f"Error: {results['error']}")
        
        # Save detailed results
        results_file = Path(__file__).parent / "reports" / f"etl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n📋 Detailed results saved to: {results_file}")
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        logging.exception("Pipeline execution error")
        return 1

if __name__ == "__main__":
    exit(main())
