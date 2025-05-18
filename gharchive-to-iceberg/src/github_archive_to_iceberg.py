#!/usr/bin/env python3

import os
import sys
import argparse
import tempfile
import requests
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_json

# Get configuration from environment variables
def get_config():
    """Get configuration from environment variables with fallbacks."""
    config = {
        "s3_bucket": os.environ.get("ICEBERG_WAREHOUSE", "s3a://tinybird-gharchive/iceberg/"),
        "database": os.environ.get("ICEBERG_DATABASE", "db"),
        "table_name": os.environ.get("ICEBERG_TABLE", "github_events"),
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.environ.get("AWS_REGION", "eu-west-1"),
        "tmp_dir": os.environ.get("TMP_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tmp"))
    }
    
    # Validate required environment variables
    if not config["aws_access_key"] or not config["aws_secret_key"]:
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set")
        sys.exit(1)
    
    # Create tmp directory if it doesn't exist
    os.makedirs(config["tmp_dir"], exist_ok=True)
    
    return config

# Configuration
config = get_config()
S3_BUCKET = config["s3_bucket"]
DATABASE = config["database"]
TABLE_NAME = config["table_name"]
AWS_ACCESS_KEY = config["aws_access_key"]
AWS_SECRET_KEY = config["aws_secret_key"]
AWS_REGION = config["aws_region"]
TMP_DIR = config["tmp_dir"]

def initialize_spark():
    """Initialize and return a Spark session configured for Iceberg."""
    warehouse_path = S3_BUCKET
    print(f"Using S3 warehouse: {warehouse_path}")
    
    # Base configuration
    builder = (SparkSession.builder
            .appName("GitHub Archive to Iceberg")
            .config("spark.jars.packages", 
                   "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", warehouse_path)
            .config("spark.sql.defaultCatalog", "iceberg")
            .config("spark.sql.parquet.columnarReaderBatchSize", 1024)
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.executor.memory", "16g")
            .config("spark.executor.cores", "4")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.default.parallelism", "100")
            .config("spark.driver.memory", "8g"))
    
    # Add S3 specific configuration
    builder = (builder
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.region", AWS_REGION))
    
    # Build the SparkSession
    spark = builder.getOrCreate()
    
    # Create the namespace if it doesn't exist
    try:
        print(f"Creating namespace iceberg.{DATABASE} if it doesn't exist...")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{DATABASE}")
        print(f"Namespace iceberg.{DATABASE} is ready")
    except Exception as e:
        print(f"Warning: Could not create namespace: {e}")
    
    return spark

def download_file(url, destination):
    """Download a file from a URL to a destination."""
    # Check if file already exists
    if os.path.exists(destination):
        file_size = os.path.getsize(destination)
        if file_size > 0:
            print(f"File {destination} already exists ({file_size} bytes), skipping download")
            return True
    
    print(f"Downloading {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            print(f"Downloaded {url} to {destination} ({os.path.getsize(destination)} bytes)")
            return True
        else:
            print(f"Failed to download {url}: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def process_file(spark, json_gz_file, date_str, hour):
    """Process a GitHub Archive JSON.gz file and load it into Iceberg."""
    try:
        print(f"Processing {date_str}-{hour}")
        
        # Read the JSON.gz file
        df = spark.read.json(json_gz_file)
        
        # Import required functions
        from pyspark.sql.functions import col, to_json, to_timestamp, lit, when
        
        print(f"Read {df.count()} records from file")
        
        # Ensure created_at is a timestamp
        if "created_at" in df.columns:
            df = df.withColumn("created_at", to_timestamp(col("created_at")))
        
        # We no longer add date and hour columns - we'll use created_at for all filters
        
        # Extract optional fields based on event type BEFORE converting payload to JSON
        if "payload" in df.columns and "type" in df.columns:
            # Extract event-specific properties from the payload struct
            df = df.withColumn("action", when(col("type").isin(
                "IssuesEvent", "PullRequestEvent", "IssueCommentEvent", "WatchEvent"), 
                col("payload.action")).otherwise(None))
                
            df = df.withColumn("ref", when(col("type").isin(
                "PushEvent", "CreateEvent", "DeleteEvent"), 
                col("payload.ref")).otherwise(None))
                
            df = df.withColumn("ref_type", when(col("type").isin(
                "CreateEvent", "DeleteEvent"), 
                col("payload.ref_type")).otherwise(None))
                
            df = df.withColumn("number", when(col("type").isin(
                "IssuesEvent", "PullRequestEvent"), 
                col("payload.number")).otherwise(None))
            
            # Convert payload struct to JSON string AFTER extracting specific fields
            print("Converting payload struct to JSON string")
            df = df.withColumn("payload", to_json(col("payload")))
        
        return df
        
    except Exception as e:
        print(f"Error processing {date_str}-{hour}: {e}")
        import traceback
        traceback.print_exc()
        return None

def ensure_database_exists(spark):
    """Ensure the Iceberg database exists."""
    try:
        print(f"Creating namespace iceberg.{DATABASE} if it doesn't exist...")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{DATABASE}")
        print(f"Namespace iceberg.{DATABASE} is ready")
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def save_to_iceberg(spark, df, date_str, hour=None):
    """Save DataFrame to Iceberg table."""
    try:
        # Ensure database exists
        ensure_database_exists(spark)
        
        # Register temp view
        df.createOrReplaceTempView("temp_events")
        
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN iceberg.{DATABASE}").collect()
        table_exists = any(row.tableName == TABLE_NAME for row in tables)
        
        if not table_exists:
            # Create new table with specific schema
            print(f"Creating table: iceberg.{DATABASE}.{TABLE_NAME}")
            
            # Define the table with the exact schema provided
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS iceberg.{DATABASE}.{TABLE_NAME} (
                    -- Common event properties
                    id STRING,
                    type STRING,
                    created_at TIMESTAMP,
                    public BOOLEAN,
                    
                    -- Actor properties
                    actor STRUCT<
                        id: STRING,
                        login: STRING,
                        display_login: STRING,
                        gravatar_id: STRING,
                        url: STRING,
                        avatar_url: STRING
                    >,
                    
                    -- Repository properties
                    repo STRUCT<
                        id: STRING,
                        name: STRING,
                        url: STRING
                    >,
                    
                    -- Organization properties (may be null)
                    org STRUCT<
                        id: STRING,
                        login: STRING, 
                        gravatar_id: STRING,
                        url: STRING,
                        avatar_url: STRING
                    >,
                    
                    -- Event-specific payload as STRING (JSON)
                    payload STRING,

                    -- Event-specific properties, should be optional
                    action STRING,
                    ref STRING,
                    ref_type STRING,
                    number INT
                )
                USING iceberg
                PARTITIONED BY (month(created_at), type)
            """)
            print("Table created successfully with the specified schema")
            
            # Select only the columns in the schema and insert data
            print("Inserting data with the specified schema...")
            spark.sql(f"""
                INSERT INTO iceberg.{DATABASE}.{TABLE_NAME}
                SELECT 
                    id, type, created_at, public,
                    actor, repo, org, 
                    payload,
                    action, ref, ref_type, number
                FROM temp_events
            """)
        else:
            # Delete existing data if specified hour(s)
            if hour is not None:
                # Single hour mode
                try:
                    # Use a date string formatted query based on created_at timestamp
                    existing = spark.sql(f"""
                        SELECT count(*) as count FROM iceberg.{DATABASE}.{TABLE_NAME} 
                        WHERE DATE_FORMAT(created_at, 'yyyy-MM-dd') = '{date_str}' 
                          AND HOUR(created_at) = {hour}
                    """).collect()
                    
                    if existing[0]['count'] > 0:
                        print(f"Deleting existing data for {date_str}-{hour}")
                        spark.sql(f"""
                            DELETE FROM iceberg.{DATABASE}.{TABLE_NAME} 
                            WHERE DATE_FORMAT(created_at, 'yyyy-MM-dd') = '{date_str}' 
                              AND HOUR(created_at) = {hour}
                        """)
                except Exception as count_err:
                    print(f"Error checking existing data: {count_err}")
            else:
                # Full day mode - delete all hours for this date
                try:
                    # Use date extraction from created_at
                    existing = spark.sql(f"""
                        SELECT count(*) as count FROM iceberg.{DATABASE}.{TABLE_NAME} 
                        WHERE DATE_FORMAT(created_at, 'yyyy-MM-dd') = '{date_str}'
                    """).collect()
                    
                    if existing[0]['count'] > 0:
                        print(f"Deleting existing data for all hours on {date_str}")
                        spark.sql(f"""
                            DELETE FROM iceberg.{DATABASE}.{TABLE_NAME} 
                            WHERE DATE_FORMAT(created_at, 'yyyy-MM-dd') = '{date_str}'
                        """)
                except Exception as count_err:
                    print(f"Error checking existing data: {count_err}")
            
            # Insert new data with only the schema columns
            print(f"Inserting data for {date_str}{' hour ' + str(hour) if hour is not None else ' (all hours)'}")
            spark.sql(f"""
                INSERT INTO iceberg.{DATABASE}.{TABLE_NAME}
                SELECT 
                    id, type, created_at, public,
                    actor, repo, org, 
                    payload,
                    action, ref, ref_type, number
                FROM temp_events
            """)
            print("Data inserted successfully")
        
        # Cleanup
        spark.catalog.dropTempView("temp_events")
        return True
        
    except Exception as e:
        print(f"Error saving to Iceberg: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_single_hour(spark, date_str, hour, temp_dir, force_download=False):
    """Process a single hour of GitHub Archive data."""
    hour_formatted = f"{hour:02d}"  # Ensure hour is properly zero-padded
    url = f"https://data.gharchive.org/{date_str}-{hour_formatted}.json.gz"
    download_path = os.path.join(temp_dir, f"{date_str}-{hour_formatted}.json.gz")
    
    # Delete file if force_download is True and file exists
    if force_download and os.path.exists(download_path):
        os.remove(download_path)
        print(f"Deleted existing file {download_path} for forced download")
    
    if download_file(url, download_path):
        df = process_file(spark, download_path, date_str, hour_formatted)
        
        if df is not None:
            success = save_to_iceberg(spark, df, date_str, hour)
            return success
    else:
        print(f"Failed to download {date_str}-{hour_formatted}")
    
    return False

def process_full_day(spark, date_str, temp_dir, force_download=False):
    """Process all 24 hours of a day and save as a single batch."""
    print(f"Processing all hours for {date_str}")
    
    # List to store downloaded file paths and their corresponding hours
    downloaded_files = []
    
    try:
        # First, download all files
        for hour in range(24):
            hour_formatted = f"{hour:02d}"  # Ensure hour is properly zero-padded
            url = f"https://data.gharchive.org/{date_str}-{hour_formatted}.json.gz"
            download_path = os.path.join(temp_dir, f"{date_str}-{hour_formatted}.json.gz")
            
            # Delete file if force_download is True and file exists
            if force_download and os.path.exists(download_path):
                os.remove(download_path)
                print(f"Deleted existing file {download_path} for forced download")
            
            if download_file(url, download_path):
                downloaded_files.append((download_path, hour))
            else:
                print(f"Skipping hour {hour_formatted} due to download error")
        
        if not downloaded_files:
            print("No files were successfully downloaded")
            return False
        
        print(f"Successfully downloaded {len(downloaded_files)} files")
        
        # Enable garbage collection to help with memory management
        import gc
        
        # Process in batches of 4 hours to reduce memory pressure
        batch_size = 4
        num_batches = (len(downloaded_files) + batch_size - 1) // batch_size
        
        print(f"Processing data in {num_batches} batches of up to {batch_size} hours each")
        
        # Set up for tracking total record count
        total_record_count = 0
        
        # Process each batch
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(downloaded_files))
            
            print(f"Processing batch {batch_idx+1}/{num_batches}, hours {start_idx} to {end_idx-1}")
            
            # Process files in this batch
            batch_dfs = []
            for idx in range(start_idx, end_idx):
                file_path, hour = downloaded_files[idx]
                df = process_file(spark, file_path, date_str, f"{hour:02d}")
                if df is not None:
                    # Cache DataFrame to avoid reloading from file
                    df = df.cache()
                    batch_dfs.append(df)
                else:
                    print(f"Skipping hour {hour:02d} due to processing error")
            
            if not batch_dfs:
                print(f"No data in batch {batch_idx+1}, skipping")
                continue
                
            # Combine DataFrames in this batch
            print(f"Combining data from batch {batch_idx+1}")
            batch_df = batch_dfs[0]
            for df in batch_dfs[1:]:
                batch_df = batch_df.union(df)
            
            # Cache and count
            batch_df = batch_df.cache()
            batch_count = batch_df.count()
            total_record_count += batch_count
            print(f"Batch {batch_idx+1} has {batch_count} records")
            
            # Save this batch
            print(f"Saving batch {batch_idx+1} to Iceberg")
            save_to_iceberg(spark, batch_df, date_str)
            
            # Unpersist all DataFrames in this batch to free memory
            batch_df.unpersist()
            for df in batch_dfs:
                df.unpersist()
            
            # Force garbage collection
            gc.collect()
        
        print(f"Successfully processed all batches, total {total_record_count} records")
        return True
        
    except Exception as e:
        print(f"Error in batch processing: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_date_range(from_date, to_date):
    """Generate a list of dates between from_date and to_date (inclusive)."""
    try:
        start_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").date()
        
        date_list = []
        current_date = start_date
        
        while current_date <= end_date:
            date_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += datetime.timedelta(days=1)
        
        return date_list
    except Exception as e:
        print(f"Error generating date range: {e}")
        return []

def process_date_range(spark, from_date, to_date, temp_dir, force_download=False):
    """Process a range of dates, reusing the same Spark session."""
    print(f"Processing date range from {from_date} to {to_date}")
    
    # Generate list of dates
    date_list = get_date_range(from_date, to_date)
    if not date_list:
        print("Failed to generate date list. Please check the date formats (YYYY-MM-DD).")
        return False
        
    print(f"Will process {len(date_list)} days: {', '.join(date_list)}")
    
    # Enable garbage collection to help with memory management
    import gc
    
    # Process each date
    total_days_processed = 0
    
    for date_str in date_list:
        print(f"\n{'='*80}\nProcessing date: {date_str}\n{'='*80}")
        
        # Process all hours for this day using the existing function
        success = process_full_day(spark, date_str, temp_dir, force_download)
        
        if success:
            total_days_processed += 1
        else:
            print(f"WARNING: Failed to process date {date_str}")
        
        # Force garbage collection after each day
        gc.collect()
    
    print(f"\nCompleted processing date range: {from_date} to {to_date}")
    print(f"Successfully processed {total_days_processed} out of {len(date_list)} days")
    
    return total_days_processed > 0

def main():
    # Parse command line args
    parser = argparse.ArgumentParser(description='Load GitHub Archive data to Iceberg')
    parser.add_argument('--date', help='Single date to process in YYYY-MM-DD format')
    parser.add_argument('--hour', type=int, help='Hour to process (0-23). Only used with --date')
    parser.add_argument('--from_date', help='Start date for range processing in YYYY-MM-DD format')
    parser.add_argument('--to_date', help='End date for range processing in YYYY-MM-DD format')
    parser.add_argument('--progress', action='store_true', help='Show detailed progress information')
    parser.add_argument('--force-download', action='store_true', help='Force download even if files exist')
    args = parser.parse_args()
    
    # Verify input parameters
    single_date = bool(args.date)
    date_range = bool(args.from_date and args.to_date)
    
    if not (single_date or date_range):
        print("ERROR: You must specify either --date or both --from_date and --to_date")
        parser.print_help()
        sys.exit(1)
    
    if single_date and date_range:
        print("ERROR: Cannot use both --date and --from_date/--to_date together")
        parser.print_help()
        sys.exit(1)
    
    if args.hour is not None and not single_date:
        print("ERROR: --hour can only be used with --date")
        parser.print_help()
        sys.exit(1)
    
    # Store parameters
    date_str = args.date
    hour = args.hour
    from_date = args.from_date
    to_date = args.to_date
    show_progress = args.progress
    force_download = args.force_download
    
    # Display configuration
    print("\nConfiguration:")
    print(f"S3 Bucket:  {S3_BUCKET}")
    print(f"Database:   {DATABASE}")
    print(f"Table:      {TABLE_NAME}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"AWS Access: {'Configured' if AWS_ACCESS_KEY else 'MISSING'}")
    print(f"AWS Secret: {'Configured' if AWS_SECRET_KEY else 'MISSING'}")
    print(f"Temp Dir:   {TMP_DIR}")
    
    if single_date:
        print(f"Mode:       Single date ({date_str}{' hour ' + str(hour) if hour is not None else ' all hours'})")
    else:
        print(f"Mode:       Date range ({from_date} to {to_date})")
    print()
    
    # Initialize Spark once for the entire process
    spark = initialize_spark()
    
    # Add progress listener if requested
    if show_progress:
        sc = spark.sparkContext
        spark._jsc.sc().addSparkListener(sc._jvm.ProgressListener())
    
    try:
        # We'll use our own tmp directory instead of tempfile.mkdtemp()
        temp_dir = TMP_DIR
        print(f"Using temporary directory: {temp_dir}")
        
        if single_date:
            if hour is not None:
                # Process a single hour
                process_single_hour(spark, date_str, hour, temp_dir, force_download)
            else:
                # Process all hours of the day
                process_full_day(spark, date_str, temp_dir, force_download)
        elif date_range:
            # Process a range of dates
            process_date_range(spark, from_date, to_date, temp_dir, force_download)
            
    except KeyboardInterrupt:
        print("\nScript interrupted.")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Stop Spark
        spark.stop()
        
        # Note: We're no longer cleaning up the temp directory since we want to keep files

if __name__ == "__main__":
    main() 