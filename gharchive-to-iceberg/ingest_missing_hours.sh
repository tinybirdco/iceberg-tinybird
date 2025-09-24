#!/bin/bash
# Script to ingest missing hours of GitHub Archive data
# Checks the last ingested date from Tinybird and processes any missing hours up to current time
#
# Usage:
#   ./ingest_missing_hours.sh             # Normal mode: process missing hours
#   ./ingest_missing_hours.sh --dry-run   # Dry run mode: show what would be processed without writing data
#   ./ingest_missing_hours.sh --from-date YYYY-MM-DD --from-hour HH [--to-date YYYY-MM-DD] [--to-hour HH]  # Manual date range

set -e

# Parse command line arguments
DRY_RUN=false
MANUAL_RANGE=false
FROM_DATE=""
FROM_HOUR=""
TO_DATE=""
TO_HOUR=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --from-date)
      FROM_DATE="$2"
      MANUAL_RANGE=true
      shift 2
      ;;
    --from-hour)
      FROM_HOUR="$2"
      shift 2
      ;;
    --to-date)
      TO_DATE="$2"
      shift 2
      ;;
    --to-hour)
      TO_HOUR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--dry-run] [--from-date YYYY-MM-DD --from-hour HH] [--to-date YYYY-MM-DD --to-hour HH]"
      exit 1
      ;;
  esac
done

# Validate manual range parameters
if [ "$MANUAL_RANGE" = true ]; then
  if [ -z "$FROM_DATE" ] || [ -z "$FROM_HOUR" ]; then
    echo "Error: --from-date and --from-hour are required when specifying manual range"
    exit 1
  fi
  
  # Validate date format (basic check)
  if ! [[ "$FROM_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: --from-date must be in YYYY-MM-DD format"
    exit 1
  fi
  
  # Validate hour format
  if ! [[ "$FROM_HOUR" =~ ^[0-9]{1,2}$ ]] || [ "$FROM_HOUR" -gt 23 ]; then
    echo "Error: --from-hour must be between 0 and 23"
    exit 1
  fi
  
  # Set default end time if not provided
  if [ -z "$TO_DATE" ] || [ -z "$TO_HOUR" ]; then
    TO_DATE=$(date -u +"%Y-%m-%d")
    TO_HOUR=$(date -u +"%H")
    echo "No end time specified, using current time: $TO_DATE $TO_HOUR"
  else
    # Validate end date format
    if ! [[ "$TO_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Error: --to-date must be in YYYY-MM-DD format"
      exit 1
    fi
    
    # Validate end hour format
    if ! [[ "$TO_HOUR" =~ ^[0-9]{1,2}$ ]] || [ "$TO_HOUR" -gt 23 ]; then
      echo "Error: --to-hour must be between 0 and 23"
      exit 1
    fi
  fi
fi

if [ "$DRY_RUN" = true ]; then
  echo "Running in DRY RUN mode - no data will be written"
fi

# Change to the script's directory
cd "$(dirname "$0")"

# Function to handle both macOS and Linux date formats
next_hour() {
  local date_str="$1"
  local hour="$2"
  
  # Remove leading zeros to avoid octal interpretation
  hour=$(echo "$hour" | sed 's/^0*//')
  
  # Increment the hour
  hour=$((hour + 1))
  
  # If hour is 24, roll over to next day
  if [ $hour -eq 24 ]; then
    hour=0
    if [[ "$OSTYPE" == "darwin"* ]]; then
      # macOS
      date_str=$(date -j -v+1d -f "%Y-%m-%d" "$date_str" "+%Y-%m-%d")
    else
      # Linux - fix: properly format the date string argument
      date_str=$(date -d "$date_str +1 day" "+%Y-%m-%d")
    fi
  fi
  
  # Format hour with leading zero if needed
  printf "%s %02d" "$date_str" "$hour"
}

# Function to convert date and hour to epoch seconds
to_epoch() {
  local date_str="$1"
  local hour="$2"
  
  # Remove leading zeros to avoid octal interpretation
  hour=$(echo "$hour" | sed 's/^0*//')
  
  # Format hour with leading zero
  hour=$(printf "%02d" "$hour")
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -f "%Y-%m-%d %H" "$date_str $hour" +%s
  else
    # Linux - ensure proper formatting
    date -d "$date_str $hour:00:00" +%s
  fi
}

if [ "$MANUAL_RANGE" = true ]; then
  echo "Using manual date range: $FROM_DATE $FROM_HOUR to $TO_DATE $TO_HOUR"
  PROCESSING_DATE="$FROM_DATE"
  PROCESSING_HOUR="$FROM_HOUR"
  CURRENT_DATE="$TO_DATE"
  CURRENT_HOUR="$TO_HOUR"
else
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

  # Start with the hour after the last ingested
  NEXT_HOUR_DATA=$(next_hour "$LAST_DATE" "$LAST_HOUR")
  PROCESSING_DATE=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f1)
  PROCESSING_HOUR=$(echo "$NEXT_HOUR_DATA" | cut -d' ' -f2)
fi

echo "Starting to process from: $PROCESSING_DATE hour $PROCESSING_HOUR"

# Process each hour until we reach end time
PROCESSED_COUNT=0
while true; do
  # Convert processing time to epoch for comparison
  PROCESSING_EPOCH=$(to_epoch "$PROCESSING_DATE" "$PROCESSING_HOUR")
  END_EPOCH=$(to_epoch "$CURRENT_DATE" "$CURRENT_HOUR")
  
  # Break if we've reached or exceeded end time
  if [ "$PROCESSING_EPOCH" -ge "$END_EPOCH" ]; then
    echo "Reached end time, stopping."
    break
  fi
  
  # Remove leading zeros from hour for URL formatting (GitHub Archive format)
  URL_HOUR=$(echo "$PROCESSING_HOUR" | sed 's/^0*//')
  
  # Ensure hour 0 is handled correctly (should be '0' not empty string)
  if [ -z "$URL_HOUR" ]; then
    URL_HOUR="0"
  fi
  
  # Format file URL with single-digit hour
  FILE_URL="https://data.gharchive.org/${PROCESSING_DATE}-${URL_HOUR}.json.gz"
  
  if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would process: $PROCESSING_DATE hour $PROCESSING_HOUR"
    echo "[DRY RUN] Would download: $FILE_URL"
  else
    echo "Processing: $PROCESSING_DATE hour $PROCESSING_HOUR"
    echo "Downloading: $FILE_URL"
    python -m src.github_archive_to_iceberg --date "$PROCESSING_DATE" --hour "$URL_HOUR"
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