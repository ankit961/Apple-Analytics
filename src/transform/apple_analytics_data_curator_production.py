#!/usr/bin/env python3
"""
Apple Analytics Data Curator - Production Version
Processes raw CSV files from Apple Analytics with proper column mapping and deduplication
"""

import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
import io
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AppleAnalyticsDataCurator:
    """Production-grade data curator with column mapping and deduplication"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.s3_bucket = "skidos-apptrack"  # Updated to correct bucket
        
        # NEW: Parquet output configuration
        self.parquet_bucket = "skidos-apptrack"  # Use correct bucket for production
        self.enable_parquet_conversion = True
        
        # Column mappings from Apple CSV headers to our curated schema
        self.engagement_column_map = {
            'Date': 'metric_date',
            'App Name': 'app_name',
            'App Apple Identifier': 'app_id',  # Corrected column name
            'Source Type': 'source_type', 
            'Page Type': 'page_type',
            'Territory': 'territory',
            'Country or Region': 'territory',  # Alternative header
            'Event': 'event_type',  # Apple uses 'Event' for engagement events
            'Engagement Type': 'engagement_type',  # New field
            'Counts': 'impressions',  # Apple uses 'Counts' for impression data
            'Unique Counts': 'impressions_unique',  # Apple uses 'Unique Counts'
            'Device': 'device',
            'Platform Version': 'platform_version'
        }
        
        self.downloads_column_map = {
            'Date': 'metric_date',
            'App Name': 'app_name', 
            'App Apple Identifier': 'app_id',  # Corrected column name
            'Territory': 'territory',
            'Country or Region': 'territory',  # Alternative header
            'Counts': 'total_downloads',  # Apple uses 'Counts' not 'Total Downloads'
            'Download Type': 'download_type',  # New field from Apple data
            'Source Type': 'source_type',
            'Device': 'device',
            'Platform Version': 'platform_version',
            'Page Type': 'page_type'
        }
        
        self.reviews_column_map = {
            'Date': 'review_date',
            'App Name': 'app_name', 
            'App Apple ID': 'app_id',
            'Territory': 'territory',
            'Country or Region': 'territory',
            'Review ID': 'review_id',
            'Rating': 'rating',
            'Title': 'title',
            'Review': 'review_text',
            'Version': 'version',
            'Developer Response': 'developer_response'
        }
    
    def process_engagement_files(self, app_id: str, date_range: List[str]) -> bool:
        """Process APP_STORE_DISCOVERY_AND_ENGAGEMENT files"""
        logger.info(f"🎯 Processing engagement files for app {app_id}")
        
        total_files_processed = 0
        total_records_processed = 0
        
        for date_str in date_range:
            # List files in S3 for this date
            prefix = f"appstore/raw/engagement/dt={date_str}/app_id={app_id}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=prefix
                )
                
                if 'Contents' not in response:
                    logger.info(f"No engagement files found for {date_str}")
                    continue
                
                # Process each CSV file
                combined_df = pd.DataFrame()
                
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        df = self._read_csv_from_s3(obj['Key'])
                        
                        if df is not None and not df.empty:
                            # Map columns
                            df_mapped = self._map_columns(df, self.engagement_column_map)
                            combined_df = pd.concat([combined_df, df_mapped], ignore_index=True)
                            total_files_processed += 1
                
                if not combined_df.empty:
                    # Clean and curate data
                    curated_df = self._curate_engagement_data(combined_df, app_id)
                    
                    # Save to curated location
                    if self._save_curated_data(curated_df, 'engagement', app_id, date_str):
                        total_records_processed += len(curated_df)
                        logger.info(f"✅ Processed {len(curated_df)} engagement records for {date_str}")
                
            except Exception as e:
                logger.error(f"❌ Error processing engagement for {date_str}: {e}")
        
        logger.info(f"✅ Engagement processing complete: {total_files_processed} files, {total_records_processed} records")
        return total_files_processed > 0
    
    def process_downloads_files(self, app_id: str, date_range: List[str]) -> bool:
        """Process APP_DOWNLOAD files"""
        logger.info(f"📱 Processing downloads files for app {app_id}")
        
        total_files_processed = 0
        total_records_processed = 0
        
        for date_str in date_range:
            prefix = f"appstore/raw/downloads/dt={date_str}/app_id={app_id}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=prefix
                )
                
                if 'Contents' not in response:
                    logger.info(f"No download files found for {date_str}")
                    continue
                
                combined_df = pd.DataFrame()
                
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        df = self._read_csv_from_s3(obj['Key'])
                        
                        if df is not None and not df.empty:
                            df_mapped = self._map_columns(df, self.downloads_column_map)
                            combined_df = pd.concat([combined_df, df_mapped], ignore_index=True)
                            total_files_processed += 1
                
                if not combined_df.empty:
                    curated_df = self._curate_downloads_data(combined_df, app_id)
                    
                    if self._save_curated_data(curated_df, 'downloads', app_id, date_str):
                        total_records_processed += len(curated_df)
                        logger.info(f"✅ Processed {len(curated_df)} download records for {date_str}")
                
            except Exception as e:
                logger.error(f"❌ Error processing downloads for {date_str}: {e}")
        
        logger.info(f"✅ Downloads processing complete: {total_files_processed} files, {total_records_processed} records")
        return total_files_processed > 0
    
    def process_reviews_files(self, app_id: str, date_range: List[str]) -> bool:
        """Process reviews files with proper deduplication"""
        logger.info(f"📝 Processing reviews files for app {app_id}")
        
        total_files_processed = 0
        total_records_processed = 0
        
        # Collect all reviews across date range for proper deduplication
        all_reviews_df = pd.DataFrame()
        
        for date_str in date_range:
            prefix = f"appstore/raw/reviews/dt={date_str}/app_id={app_id}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=prefix
                )
                
                if 'Contents' not in response:
                    continue
                
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        df = self._read_csv_from_s3(obj['Key'])
                        
                        if df is not None and not df.empty:
                            df_mapped = self._map_columns(df, self.reviews_column_map)
                            all_reviews_df = pd.concat([all_reviews_df, df_mapped], ignore_index=True)
                            total_files_processed += 1
                            
            except Exception as e:
                logger.error(f"❌ Error processing reviews for {date_str}: {e}")
        
        if not all_reviews_df.empty:
            # CRITICAL: Deduplicate reviews by review_id
            logger.info(f"📝 Deduplicating reviews: {len(all_reviews_df)} total")
            all_reviews_df.drop_duplicates(subset=['review_id'], inplace=True)
            logger.info(f"📝 After deduplication: {len(all_reviews_df)} unique reviews")
            
            # Curate reviews data
            curated_df = self._curate_reviews_data(all_reviews_df, app_id)
            
            # Save all deduplicated reviews
            if self._save_curated_reviews(curated_df, app_id):
                total_records_processed = len(curated_df)
                logger.info(f"✅ Processed {total_records_processed} unique reviews")
        
        logger.info(f"✅ Reviews processing complete: {total_files_processed} files, {total_records_processed} unique records")
        return total_files_processed > 0
    
    def _read_csv_from_s3(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Read CSV file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            csv_content = response['Body'].read().decode('utf-8')
            
            # Handle CSV with tab separator (Apple uses tab-separated files)
            df = pd.read_csv(io.StringIO(csv_content), sep='\t')
            
            logger.debug(f"📄 Read {len(df)} rows from {s3_key}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading {s3_key}: {e}")
            return None
    
    def _map_columns(self, df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
        """Map Apple CSV columns to our curated schema"""
        mapped_df = pd.DataFrame()
        
        for apple_col, our_col in column_map.items():
            if apple_col in df.columns:
                mapped_df[our_col] = df[apple_col]
            else:
                # Check for similar column names (case insensitive, space variations)
                found_col = None
                for col in df.columns:
                    if col.lower().replace(' ', '_') == apple_col.lower().replace(' ', '_'):
                        found_col = col
                        break
                
                if found_col:
                    mapped_df[our_col] = df[found_col]
                else:
                    logger.warning(f"⚠️  Column '{apple_col}' not found, setting as null")
                    mapped_df[our_col] = None
        
        return mapped_df
    
    def _curate_engagement_data(self, df: pd.DataFrame, app_id: str) -> pd.DataFrame:
        """Clean and curate engagement data - Updated for Apple's event-based structure"""
        # Ensure app_id is consistent (use app_id for partitioning, not app_id_part)
        df['app_id'] = str(app_id)  # Keep as string for consistent partitioning
        
        # Convert date
        df['metric_date'] = pd.to_datetime(df['metric_date']).dt.date
        
        # Convert numeric columns (Apple uses 'impressions' for Counts, 'impressions_unique' for Unique Counts)
        numeric_cols = ['impressions', 'impressions_unique']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Clean string columns
        string_cols = ['source_type', 'page_type', 'territory', 'event_type', 'engagement_type', 'device', 'platform_version']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Remove rows with no meaningful data
        df = df[
            (df['impressions'] > 0) | 
            (df['impressions_unique'] > 0)
        ]
        
        return df
    
    def _curate_downloads_data(self, df: pd.DataFrame, app_id: str) -> pd.DataFrame:
        """Clean and curate downloads data"""
        # Ensure app_id is consistent (use app_id for partitioning)
        df['app_id'] = str(app_id)
        
        # Convert date
        df['metric_date'] = pd.to_datetime(df['metric_date']).dt.date
        
        # Convert numeric columns
        numeric_cols = ['total_downloads', 'first_time_downloads', 'redownloads']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Clean string columns
        string_cols = ['territory', 'source_type', 'device']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Remove rows with no downloads
        df = df[df['total_downloads'] > 0]
        
        return df
    
    def _curate_reviews_data(self, df: pd.DataFrame, app_id: str) -> pd.DataFrame:
        """Clean and curate reviews data"""
        # Ensure app_id is consistent (use app_id for partitioning)
        df['app_id'] = str(app_id)
        
        # Convert date
        df['review_date'] = pd.to_datetime(df['review_date']).dt.date
        
        # Convert rating to numeric
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        
        # Clean text fields
        text_cols = ['title', 'review_text', 'developer_response']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', None)
        
        # Remove invalid reviews
        df = df[df['rating'].notna() & (df['rating'] >= 1) & (df['rating'] <= 5)]
        
        return df
    
    def _save_curated_data(self, df: pd.DataFrame, data_type: str, app_id: str, date_str: str) -> bool:
        """Save curated data to S3 as Parquet"""
        try:
            # Convert date column to string for Parquet compatibility
            if 'metric_date' in df.columns:
                df['dt'] = df['metric_date'].astype(str)
            elif 'review_date' in df.columns:
                df['dt'] = df['review_date'].astype(str)
            
            # Create S3 key with Hive partitioning
            s3_key = f"appstore/curated/{data_type}/dt={date_str}/app_id={app_id}/data.parquet"
            
            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"✅ Saved: s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving curated data: {e}")
            return False
    
    def _save_curated_reviews(self, df: pd.DataFrame, app_id: str) -> bool:
        """Save all curated reviews (not partitioned by date for deduplication)"""
        try:
            # Add partition column
            df['dt'] = df['review_date'].astype(str)
            
            # Group by date and save separately to maintain partitioning
            for date_str, group_df in df.groupby('dt'):
                s3_key = f"appstore/curated/reviews/dt={date_str}/app_id={app_id}/data.parquet"
                
                parquet_buffer = io.BytesIO()
                group_df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
                parquet_buffer.seek(0)
                
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=parquet_buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
                
                logger.info(f"✅ Saved reviews: s3://{self.s3_bucket}/{s3_key}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving curated reviews: {e}")
            return False

    def save_to_parquet(self, df: pd.DataFrame, data_type: str, app_id: str, date_str: Optional[str] = None) -> bool:
        """
        Save DataFrame to S3 as Parquet format for efficient querying
        
        Args:
            df: DataFrame to save
            data_type: Type of data (engagement, downloads, reviews)  
            app_id: Apple App Store ID
            date_str: Optional date for partitioning
            
        Returns:
            bool: Success status
        """
        if not self.enable_parquet_conversion or df.empty:
            return True
            
        try:
            # Create partitioned path structure
            if date_str:
                year = date_str[:4]
                month = date_str[5:7] 
                day = date_str[8:10]
                parquet_key = f"curated_parquet/{data_type}/app_id={app_id}/year={year}/month={month}/day={day}/data.parquet"
            else:
                parquet_key = f"curated_parquet/{data_type}/app_id={app_id}/data.parquet"
            
            # Convert to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.parquet_bucket,
                Key=parquet_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"✅ Saved {len(df)} records to parquet: s3://{self.parquet_bucket}/{parquet_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save parquet for {data_type}: {e}")
            return False

def main():
    """Main execution for Doctor app data curation"""
    print("🏥 APPLE ANALYTICS DATA CURATOR - PRODUCTION VERSION")
    print("=" * 60)
    
    curator = AppleAnalyticsDataCurator()
    
    # Doctor app configuration
    doctor_app_id = "1506886061"
    
    # Generate date range for last 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    print(f"🏥 App ID: {doctor_app_id}")
    print(f"📅 Date Range: {start_date} to {end_date} ({len(date_range)} days)")
    print()
    
    # Process each data type
    results = {}
    
    # 1. Process engagement data (impressions, product page views)
    print("🎯 Processing engagement data...")
    results['engagement'] = curator.process_engagement_files(doctor_app_id, date_range)
    
    # 2. Process downloads data (installs)
    print("\n📱 Processing downloads data...")
    results['downloads'] = curator.process_downloads_files(doctor_app_id, date_range)
    
    # 3. Process reviews data (with deduplication)
    print("\n📝 Processing reviews data...")
    results['reviews'] = curator.process_reviews_files(doctor_app_id, date_range)
    
    # Summary
    print(f"\n" + "=" * 60)
    print("📊 CURATION SUMMARY")
    print("=" * 60)
    
    for data_type, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{data_type.upper():<12} {status}")
    
    if all(results.values()):
        print(f"\n🎉 All data types processed successfully!")
        print(f"📊 Next step: Update Athena table schemas and run queries")
    else:
        print(f"\n⚠️  Some data types failed to process")

if __name__ == "__main__":
    main()
