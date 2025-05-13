#!/usr/bin/env python3

import os
import sys
import argparse
import tempfile
import requests
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
        "aws_region": os.environ.get("AWS_REGION", "eu-west-1")
    }
    
    # Validate required environment variables
    if not config["aws_access_key"] or not config["aws_secret_key"]:
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set")
        sys.exit(1)
    
    return config

# Configuration
config = get_config()
S3_BUCKET = config["s3_bucket"]
DATABASE = config["database"]
TABLE_NAME = config["table_name"]
AWS_ACCESS_KEY = config["aws_access_key"]
AWS_SECRET_KEY = config["aws_secret_key"]
AWS_REGION = config["aws_region"]

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
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "2g"))
    
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
    print(f"Downloading {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
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
        
        # Convert payload struct to JSON string for flexibility
        if "payload" in df.columns:
            print("Converting payload struct to JSON string")
            df = df.withColumn("payload_json", to_json(col("payload")))
            df = df.drop("payload").withColumnRenamed("payload_json", "payload")
        
        print(f"Read {df.count()} records from file")
        
        # Add date and hour columns
        df = df.withColumn("date", lit(date_str))
        df = df.withColumn("hour", lit(hour))
        
        return df
        
    except Exception as e:
        print(f"Error processing {date_str}-{hour}: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_iceberg(spark, df, date_str, hour=None):
    """Save DataFrame to Iceberg table."""
    try:
        # Register temp view
        df.createOrReplaceTempView("temp_events")
        
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN iceberg.{DATABASE}").collect()
        table_exists = any(row.tableName == TABLE_NAME for row in tables)
        
        if not table_exists:
            # Create new table
            print(f"Creating table: iceberg.{DATABASE}.{TABLE_NAME}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS iceberg.{DATABASE}.{TABLE_NAME} 
                USING iceberg 
                AS SELECT * FROM temp_events
            """)
            print("Table created successfully")
        else:
            # Delete existing data if specified hour(s)
            if hour is not None:
                # Single hour mode
                try:
                    existing = spark.sql(f"""
                        SELECT count(*) as count FROM iceberg.{DATABASE}.{TABLE_NAME} 
                        WHERE date = '{date_str}' AND hour = {hour}
                    """).collect()
                    
                    if existing[0]['count'] > 0:
                        print(f"Deleting existing data for {date_str}-{hour}")
                        spark.sql(f"""
                            DELETE FROM iceberg.{DATABASE}.{TABLE_NAME} 
                            WHERE date = '{date_str}' AND hour = {hour}
                        """)
                except Exception as count_err:
                    print(f"Error checking existing data: {count_err}")
            else:
                # Full day mode - delete all hours for this date
                try:
                    existing = spark.sql(f"""
                        SELECT count(*) as count FROM iceberg.{DATABASE}.{TABLE_NAME} 
                        WHERE date = '{date_str}'
                    """).collect()
                    
                    if existing[0]['count'] > 0:
                        print(f"Deleting existing data for all hours on {date_str}")
                        spark.sql(f"""
                            DELETE FROM iceberg.{DATABASE}.{TABLE_NAME} 
                            WHERE date = '{date_str}'
                        """)
                except Exception as count_err:
                    print(f"Error checking existing data: {count_err}")
            
            # Insert new data
            print(f"Inserting data for {date_str}{' hour ' + str(hour) if hour is not None else ' (all hours)'}")
            spark.sql(f"""
                INSERT INTO iceberg.{DATABASE}.{TABLE_NAME}
                SELECT * FROM temp_events
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

def process_single_hour(spark, date_str, hour, temp_dir):
    """Process a single hour of GitHub Archive data."""
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    download_path = os.path.join(temp_dir, f"{date_str}-{hour}.json.gz")
    
    if download_file(url, download_path):
        df = process_file(spark, download_path, date_str, hour)
        
        if df is not None:
            success = save_to_iceberg(spark, df, date_str, hour)
            # Only remove file after processing is complete
            os.remove(download_path)
            return success
        else:
            # Remove file if processing failed
            os.remove(download_path)
    else:
        print(f"Failed to download {date_str}-{hour}")
    
    return False

def process_full_day(spark, date_str, temp_dir):
    """Process all 24 hours of a day and save as a single batch."""
    print(f"Processing all hours for {date_str}")
    
    # List to store downloaded file paths and their corresponding hours
    downloaded_files = []
    
    try:
        # First, download all files
        for hour in range(24):
            url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
            download_path = os.path.join(temp_dir, f"{date_str}-{hour}.json.gz")
            
            if download_file(url, download_path):
                downloaded_files.append((download_path, hour))
            else:
                print(f"Skipping hour {hour} due to download error")
        
        if not downloaded_files:
            print("No files were successfully downloaded")
            return False
        
        print(f"Successfully downloaded {len(downloaded_files)} files")
        
        # Now process all files and collect DataFrames
        all_dfs = []
        for file_path, hour in downloaded_files:
            df = process_file(spark, file_path, date_str, hour)
            if df is not None:
                # Cache DataFrame to avoid reloading from file
                df = df.cache()
                all_dfs.append(df)
            else:
                print(f"Skipping hour {hour} due to processing error")
        
        # If we have data, combine and save
        if all_dfs:
            print(f"Combining data from {len(all_dfs)} hours")
            # Use union instead of unionAll (which is deprecated)
            combined_df = all_dfs[0]
            for df in all_dfs[1:]:
                combined_df = combined_df.union(df)
            
            # Cache the combined DataFrame
            combined_df = combined_df.cache()
            print(f"Total combined records: {combined_df.count()}")
            
            # Save to Iceberg
            success = save_to_iceberg(spark, combined_df, date_str)
            
            # Unpersist cached DataFrames
            for df in all_dfs:
                df.unpersist()
            combined_df.unpersist()
            
            return success
        else:
            print("No data was successfully processed for any hour")
            return False
    
    finally:
        # Clean up all downloaded files
        for file_path, _ in downloaded_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Warning: Could not remove file {file_path}: {e}")

def main():
    # Parse command line args
    parser = argparse.ArgumentParser(description='Load GitHub Archive data to Iceberg')
    parser.add_argument('--date', required=True, help='Date to process in YYYY-MM-DD format')
    parser.add_argument('--hour', type=int, help='Hour to process (0-23). If not provided, all 24 hours will be processed.')
    args = parser.parse_args()
    
    date_str = args.date
    hour = args.hour
    
    # Display configuration
    print("\nConfiguration:")
    print(f"S3 Bucket:  {S3_BUCKET}")
    print(f"Database:   {DATABASE}")
    print(f"Table:      {TABLE_NAME}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"AWS Access: {'Configured' if AWS_ACCESS_KEY else 'MISSING'}")
    print(f"AWS Secret: {'Configured' if AWS_SECRET_KEY else 'MISSING'}")
    print()
    
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Create temp directory for downloads
        temp_dir = tempfile.mkdtemp(prefix="github_archive_")
        print(f"Using temporary directory: {temp_dir}")
        
        if hour is not None:
            # Process a single hour
            process_single_hour(spark, date_str, hour, temp_dir)
        else:
            # Process all hours of the day
            process_full_day(spark, date_str, temp_dir)
            
    except KeyboardInterrupt:
        print("\nScript interrupted.")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Stop Spark
        spark.stop()
        
        # Clean up temp directory
        try:
            os.rmdir(temp_dir)
        except OSError:
            print(f"Note: Temp directory {temp_dir} still contains files")

if __name__ == "__main__":
    main() 