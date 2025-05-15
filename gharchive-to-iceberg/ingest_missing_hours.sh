#!/bin/bash
# Script to ingest missing hours of GitHub Archive data
# Checks the last ingested date from Tinybird and processes any missing hours up to current time
#
# Usage:
#   ./ingest_missing_hours.sh             # Normal mode: process missing hours
#   ./ingest_missing_hours.sh --dry-run   # Dry run mode: show what would be processed without writing data

set -e

# Parse command line arguments
DRY_RUN=false
for arg in "$@"; do
  case $arg in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
  esac
done

if [ "$DRY_RUN" = true ]; then
  echo "Running in DRY RUN mode - no data will be written"
fi

# Change to the script's directory
cd "$(dirname "$0")"

# Function to handle both macOS and Linux date formats
next_hour() {
  local date_str="$1"
  local hour="$2"
  
  # Increment the hour
  hour=$((hour + 1))
  
  # If hour is 24, roll over to next day
  if [ $hour -eq 24 ]; then
    hour=0
    if [[ "$OSTYPE" == "darwin"* ]]; then
      # macOS
      date_str=$(date -j -v+1d -f "%Y-%m-%d" "$date_str" "+%Y-%m-%d")
    else
      # Linux
      date_str=$(date -d "$date_str + 1 day" "+%Y-%m-%d")
    fi
  fi
  
  echo "$date_str $hour"
}

# Function to convert date and hour to epoch seconds
to_epoch() {
  local date_str="$1"
  local hour="$2"
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -f "%Y-%m-%d %H" "$date_str $hour" +%s
  else
    # Linux
    date -d "$date_str $hour:00:00" +%s
  fi
}

echo "Checking last ingested GitHub event date from Tinybird..."

# Get the last ingested event date from Tinybird
LAST_INGESTED=$(tb --token $TINYBIRD_TOKEN --host $TINYBIRD_HOST --cloud --no-version-warning endpoint data last_github_event_date --format json)
echo "Tinybird response: $LAST_INGESTED"

# Extract the last date using grep and cut
LAST_DATE_TIME=$(echo "$LAST_INGESTED" | grep -o '"last_date": "[^"]*"' | cut -d'"' -f4)
echo "Last ingested date: $LAST_DATE_TIME"

# Parse the date and extract components
LAST_DATE=$(echo "$LAST_DATE_TIME" | cut -d' ' -f1)
LAST_TIME=$(echo "$LAST_DATE_TIME" | cut -d' ' -f2)
LAST_HOUR=$(echo "$LAST_TIME" | cut -d':' -f1)
echo "Last date: $LAST_DATE, Last hour: $LAST_HOUR"

# Get current UTC time (rounded to the latest completed hour)
CURRENT_UTC=$(date -u +"%Y-%m-%d %H")
CURRENT_DATE=$(echo "$CURRENT_UTC" | cut -d' ' -f1)
CURRENT_HOUR=$(echo "$CURRENT_UTC" | cut -d' ' -f2)
echo "Current date: $CURRENT_DATE, Current hour: $CURRENT_HOUR (UTC)"

# Convert to epoch seconds for comparison
LAST_EPOCH=$(to_epoch "$LAST_DATE" "$LAST_HOUR")
CURRENT_EPOCH=$(to_epoch "$CURRENT_DATE" "$CURRENT_HOUR")

echo "Last epoch: $LAST_EPOCH, Current epoch: $CURRENT_EPOCH"

# Start with the hour after the last ingested
NEXT_HOUR_DATA=$(next_hour "$LAST_DATE" "$LAST_HOUR")
PROCESSING_DATE=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f1)
PROCESSING_HOUR=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f2)

echo "Starting to process from: $PROCESSING_DATE hour $PROCESSING_HOUR"

# Process each hour until we reach current time
PROCESSED_COUNT=0
while true; do
  # Convert processing time to epoch for comparison
  PROCESSING_EPOCH=$(to_epoch "$PROCESSING_DATE" "$PROCESSING_HOUR")
  
  # Break if we've reached or exceeded current time
  if [ "$PROCESSING_EPOCH" -ge "$CURRENT_EPOCH" ]; then
    echo "Reached current time, stopping."
    break
  fi
  
  if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would process: $PROCESSING_DATE hour $PROCESSING_HOUR"
  else
    echo "Processing: $PROCESSING_DATE hour $PROCESSING_HOUR"
    python -m src.github_archive_to_iceberg --date "$PROCESSING_DATE" --hour "$PROCESSING_HOUR"
  fi
  
  # Move to next hour
  NEXT_HOUR_DATA=$(next_hour "$PROCESSING_DATE" "$PROCESSING_HOUR")
  PROCESSING_DATE=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f1)
  PROCESSING_HOUR=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f2)
  
  PROCESSED_COUNT=$((PROCESSED_COUNT + 1))
done

if [ $PROCESSED_COUNT -eq 0 ]; then
  echo "No new hours to process. Already up to date."
else
  if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would have processed $PROCESSED_COUNT hours of GitHub event data."
  else
    echo "Successfully processed $PROCESSED_COUNT hours of GitHub event data."
  fi
fi 