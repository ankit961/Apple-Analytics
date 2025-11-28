#!/usr/bin/env python3
"""
Complete ETL Health Check
Verifies all components of the Apple Analytics ETL pipeline
"""

import os
import sys
import json
import boto3
import time
from datetime import datetime, date, timedelta
from pathlib import Path

# Add project modules to path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

class ETLHealthCheck:
    def __init__(self):
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "checks": {},
            "summary": {"passed": 0, "failed": 0, "warnings": 0}
        }
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.athena_client = boto3.client('athena', region_name='us-east-1')
        self.glue_client = boto3.client('glue', region_name='us-east-1')
        
    def check_passed(self, name, message, details=None):
        self.results["checks"][name] = {"status": "PASSED", "message": message, "details": details}
        self.results["summary"]["passed"] += 1
        print(f"✅ {name}: {message}")
        
    def check_failed(self, name, message, details=None):
        self.results["checks"][name] = {"status": "FAILED", "message": message, "details": details}
        self.results["summary"]["failed"] += 1
        print(f"❌ {name}: {message}")
        
    def check_warning(self, name, message, details=None):
        self.results["checks"][name] = {"status": "WARNING", "message": message, "details": details}
        self.results["summary"]["warnings"] += 1
        print(f"⚠️ {name}: {message}")

    def run_athena_query(self, query, database="appstore", timeout=30):
        """Execute Athena query and return results"""
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': 's3://skidos-apptrack/athena-results/'}
            )
            query_id = response['QueryExecutionId']
            
            waited = 0
            while waited < timeout:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    results = self.athena_client.get_query_results(QueryExecutionId=query_id)
                    rows = results['ResultSet']['Rows']
                    if len(rows) > 1:
                        columns = [col.get('VarCharValue', '') for col in rows[0]['Data']]
                        data = []
                        for row in rows[1:]:
                            row_data = {}
                            for i, cell in enumerate(row['Data']):
                                row_data[columns[i]] = cell.get('VarCharValue', '')
                            data.append(row_data)
                        return {"success": True, "data": data}
                    return {"success": True, "data": []}
                elif status in ['FAILED', 'CANCELLED']:
                    error = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                    return {"success": False, "error": error}
                    
                time.sleep(2)
                waited += 2
                
            return {"success": False, "error": "Query timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def check_1_environment(self):
        """Check environment configuration"""
        print("\n" + "="*60)
        print("1️⃣ ENVIRONMENT CONFIGURATION")
        print("="*60)
        
        env_file = project_root / ".env"
        if env_file.exists():
            with open(env_file) as f:
                content = f.read()
            
            required_vars = ['ASC_ISSUER_ID', 'ASC_KEY_ID', 'AWS_ACCESS_KEY_ID', 'S3_BUCKET']
            missing = [v for v in required_vars if v not in content]
            
            if not missing:
                self.check_passed("env_config", "All required environment variables present")
            else:
                self.check_failed("env_config", f"Missing variables: {missing}")
        else:
            self.check_failed("env_config", ".env file not found")
            
        # Check P8 key
        p8_paths = [
            project_root / "AuthKey_54G63QGUHT.p8",
            Path("/Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/AuthKey_54G63QGUHT.p8")
        ]
        p8_found = any(p.exists() for p in p8_paths)
        if p8_found:
            self.check_passed("p8_key", "Apple P8 key file found")
        else:
            self.check_failed("p8_key", "Apple P8 key file not found")

    def check_2_s3_connectivity(self):
        """Check S3 bucket accessibility"""
        print("\n" + "="*60)
        print("2️⃣ S3 CONNECTIVITY & DATA")
        print("="*60)
        
        bucket = "skidos-apptrack"
        
        try:
            # Check bucket access
            self.s3_client.head_bucket(Bucket=bucket)
            self.check_passed("s3_access", f"S3 bucket '{bucket}' accessible")
        except Exception as e:
            self.check_failed("s3_access", f"Cannot access S3 bucket: {e}")
            return
            
        # Check raw data directories
        prefixes = {
            "raw_downloads": "appstore/raw/downloads/",
            "raw_engagement": "appstore/raw/engagement/",
            "curated_downloads": "appstore/curated/downloads/",
            "curated_engagement": "appstore/curated/engagement/"
        }
        
        for name, prefix in prefixes.items():
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=10
                )
                count = response.get('KeyCount', 0)
                if count > 0:
                    self.check_passed(f"s3_{name}", f"{count}+ files found in {prefix}")
                else:
                    self.check_warning(f"s3_{name}", f"No files in {prefix}")
            except Exception as e:
                self.check_failed(f"s3_{name}", f"Error checking {prefix}: {e}")
        
        # Check recent data (last 7 days)
        today = date.today()
        recent_dates = []
        for i in range(7):
            dt = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            prefix = f"appstore/raw/downloads/dt={dt}/"
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=1
                )
                if response.get('KeyCount', 0) > 0:
                    recent_dates.append(dt)
            except:
                pass
                
        if recent_dates:
            self.check_passed("recent_data", f"Recent data found for: {recent_dates[:3]}...")
        else:
            self.check_warning("recent_data", "No recent data in last 7 days")

    def check_3_athena_databases(self):
        """Check Athena databases and tables"""
        print("\n" + "="*60)
        print("3️⃣ ATHENA DATABASES & TABLES")
        print("="*60)
        
        required_databases = ['appstore', 'curated']
        
        for db in required_databases:
            try:
                self.glue_client.get_database(Name=db)
                self.check_passed(f"db_{db}", f"Database '{db}' exists")
                
                # Get tables
                tables_response = self.glue_client.get_tables(DatabaseName=db)
                tables = [t['Name'] for t in tables_response['TableList']]
                self.check_passed(f"tables_{db}", f"{len(tables)} tables: {tables[:5]}...")
            except Exception as e:
                self.check_failed(f"db_{db}", f"Database '{db}' not accessible: {e}")

    def check_4_athena_queries(self):
        """Check Athena query functionality"""
        print("\n" + "="*60)
        print("4️⃣ ATHENA QUERY VERIFICATION")
        print("="*60)
        
        # Test queries
        queries = [
            {
                "name": "raw_downloads_count",
                "query": "SELECT COUNT(*) as cnt FROM appstore.raw_downloads LIMIT 1",
                "database": "appstore"
            },
            {
                "name": "raw_engagement_count", 
                "query": "SELECT COUNT(*) as cnt FROM appstore.raw_engagement LIMIT 1",
                "database": "appstore"
            },
            {
                "name": "curated_downloads_count",
                "query": "SELECT COUNT(*) as cnt FROM curated.downloads LIMIT 1",
                "database": "curated"
            },
            {
                "name": "curated_engagement_count",
                "query": "SELECT COUNT(*) as cnt FROM curated.engagement LIMIT 1",
                "database": "curated"
            }
        ]
        
        for q in queries:
            result = self.run_athena_query(q["query"], q["database"])
            if result["success"]:
                count = result["data"][0].get("cnt", "0") if result["data"] else "0"
                self.check_passed(q["name"], f"Query successful, count={count}")
            else:
                self.check_failed(q["name"], f"Query failed: {result['error'][:100]}")

    def check_5_data_freshness(self):
        """Check data freshness - most recent dates"""
        print("\n" + "="*60)
        print("5️⃣ DATA FRESHNESS")
        print("="*60)
        
        # Check raw_downloads freshness
        query = """
        SELECT MAX(dt) as latest_dt, COUNT(DISTINCT dt) as date_count 
        FROM appstore.raw_downloads
        """
        result = self.run_athena_query(query)
        if result["success"] and result["data"]:
            latest = result["data"][0].get("latest_dt", "unknown")
            count = result["data"][0].get("date_count", "0")
            
            if latest:
                latest_date = datetime.strptime(latest, "%Y-%m-%d").date()
                days_old = (date.today() - latest_date).days
                
                if days_old <= 2:
                    self.check_passed("freshness_downloads", f"Latest: {latest} ({days_old} days old), {count} dates total")
                elif days_old <= 7:
                    self.check_warning("freshness_downloads", f"Latest: {latest} ({days_old} days old)")
                else:
                    self.check_failed("freshness_downloads", f"Data is stale: {latest} ({days_old} days old)")
            else:
                self.check_warning("freshness_downloads", f"No date info available, {count} dates")
        else:
            self.check_failed("freshness_downloads", f"Cannot check freshness: {result.get('error', 'unknown')}")

    def check_6_apple_api_client(self):
        """Check Apple Analytics API client"""
        print("\n" + "="*60)
        print("6️⃣ APPLE ANALYTICS API CLIENT")
        print("="*60)
        
        try:
            from extract.apple_analytics_client import AppleAnalyticsRequestor
            client = AppleAnalyticsRequestor()
            self.check_passed("api_client_init", "AppleAnalyticsRequestor initialized successfully")
            
            # Check if we can generate JWT
            if hasattr(client, 'jwt_token') and client.jwt_token:
                self.check_passed("jwt_token", "JWT token generated successfully")
            else:
                self.check_warning("jwt_token", "JWT token not immediately available")
                
        except Exception as e:
            self.check_failed("api_client_init", f"Failed to initialize API client: {e}")

    def check_7_transform_module(self):
        """Check Transform module"""
        print("\n" + "="*60)
        print("7️⃣ TRANSFORM MODULE")
        print("="*60)
        
        try:
            from transform.apple_analytics_data_curator_production import AppleAnalyticsDataCurator
            curator = AppleAnalyticsDataCurator()
            self.check_passed("curator_init", "AppleAnalyticsDataCurator initialized")
            
            # Check bucket config
            if hasattr(curator, 's3_bucket') and curator.s3_bucket == "skidos-apptrack":
                self.check_passed("curator_config", f"Curator configured for bucket: {curator.s3_bucket}")
            else:
                self.check_warning("curator_config", "Curator bucket config may be incorrect")
                
        except Exception as e:
            self.check_failed("curator_init", f"Failed to initialize curator: {e}")

    def check_8_load_module(self):
        """Check Load module (Athena table manager)"""
        print("\n" + "="*60)
        print("8️⃣ LOAD MODULE (ATHENA TABLE MANAGER)")
        print("="*60)
        
        try:
            from load.athena_table_manager_production import AthenaTableManager
            manager = AthenaTableManager()
            self.check_passed("table_manager_init", "AthenaTableManager initialized")
            
            # Check required methods exist
            required_methods = ['create_engagement_table', 'create_downloads_table', 'create_raw_appstore_tables']
            missing_methods = [m for m in required_methods if not hasattr(manager, m)]
            
            if not missing_methods:
                self.check_passed("table_manager_methods", "All required methods available")
            else:
                self.check_warning("table_manager_methods", f"Missing methods: {missing_methods}")
                
        except Exception as e:
            self.check_failed("table_manager_init", f"Failed to initialize table manager: {e}")

    def check_9_ongoing_requests(self):
        """Check for existing ONGOING requests"""
        print("\n" + "="*60)
        print("9️⃣ ONGOING REQUESTS STATUS")
        print("="*60)
        
        try:
            from extract.apple_analytics_client import AppleAnalyticsRequestor
            client = AppleAnalyticsRequestor()
            
            # Check S3 registry for tracked requests
            bucket = "skidos-apptrack"
            prefix = "appstore/request_registry/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=100
                )
                
                registry_files = response.get('Contents', [])
                if registry_files:
                    self.check_passed("request_registry", f"{len(registry_files)} request registries found in S3")
                    
                    # Sample a few
                    sample_apps = []
                    for obj in registry_files[:3]:
                        key = obj['Key']
                        app_id = key.split('/')[-1].replace('.json', '') if '/' in key else key
                        sample_apps.append(app_id)
                    
                    self.check_passed("sample_registries", f"Sample apps with registries: {sample_apps}")
                else:
                    self.check_warning("request_registry", "No request registries found in S3")
                    
            except Exception as e:
                self.check_warning("request_registry", f"Cannot check registry: {e}")
                
        except Exception as e:
            self.check_failed("ongoing_requests", f"Cannot check ONGOING requests: {e}")

    def check_10_data_volume(self):
        """Check data volume statistics"""
        print("\n" + "="*60)
        print("🔟 DATA VOLUME STATISTICS")
        print("="*60)
        
        # Get row counts for key tables
        tables = [
            ("appstore.raw_downloads", "appstore"),
            ("appstore.raw_engagement", "appstore"),
            ("curated.downloads", "curated"),
            ("curated.engagement", "curated")
        ]
        
        for table, db in tables:
            query = f"SELECT COUNT(*) as total FROM {table}"
            result = self.run_athena_query(query, db, timeout=45)
            
            if result["success"] and result["data"]:
                total = result["data"][0].get("total", "0")
                total_int = int(total) if total.isdigit() else 0
                
                if total_int > 1000000:
                    self.check_passed(f"volume_{table}", f"{total_int:,} rows ✨")
                elif total_int > 0:
                    self.check_passed(f"volume_{table}", f"{total_int:,} rows")
                else:
                    self.check_warning(f"volume_{table}", "No rows found")
            else:
                self.check_warning(f"volume_{table}", f"Cannot count: {result.get('error', 'timeout')[:50]}")

    def generate_report(self):
        """Generate final health check report"""
        print("\n" + "="*60)
        print("📋 FINAL HEALTH CHECK REPORT")
        print("="*60)
        
        total = self.results["summary"]["passed"] + self.results["summary"]["failed"] + self.results["summary"]["warnings"]
        
        print(f"""
╔══════════════════════════════════════════════════════════╗
║           APPLE ETL PIPELINE HEALTH CHECK                 ║
╠══════════════════════════════════════════════════════════╣
║  ✅ Passed:   {self.results['summary']['passed']:>3}                                       ║
║  ❌ Failed:   {self.results['summary']['failed']:>3}                                       ║
║  ⚠️ Warnings: {self.results['summary']['warnings']:>3}                                       ║
║  📊 Total:    {total:>3}                                       ║
╠══════════════════════════════════════════════════════════╣
""")
        
        if self.results["summary"]["failed"] == 0:
            health_status = "🎉 HEALTHY - Pipeline is fully operational!"
        elif self.results["summary"]["failed"] <= 2:
            health_status = "⚠️ MOSTLY HEALTHY - Minor issues detected"
        else:
            health_status = "🚨 NEEDS ATTENTION - Multiple issues found"
            
        print(f"║  Status: {health_status:<49}║")
        print("╚══════════════════════════════════════════════════════════╝")
        
        # Save report
        report_file = project_root / f"etl_health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\n📄 Full report saved to: {report_file}")
        
        return self.results["summary"]["failed"] == 0

    def run_all_checks(self):
        """Run all health checks"""
        print("🔍 APPLE ETL PIPELINE HEALTH CHECK")
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.check_1_environment()
        self.check_2_s3_connectivity()
        self.check_3_athena_databases()
        self.check_4_athena_queries()
        self.check_5_data_freshness()
        self.check_6_apple_api_client()
        self.check_7_transform_module()
        self.check_8_load_module()
        self.check_9_ongoing_requests()
        self.check_10_data_volume()
        
        return self.generate_report()


if __name__ == "__main__":
    health_check = ETLHealthCheck()
    success = health_check.run_all_checks()
    sys.exit(0 if success else 1)
