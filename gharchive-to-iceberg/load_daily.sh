#!/bin/bash

# Simple script to load one GitHub Archive file per day for a date range
# Usage: ./load_daily.sh 2023-01-01 2023-01-31

START_DATE=$1
END_DATE=$2
HOUR=3  # Fixed hour to use for each day

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "Usage: $0 START_DATE END_DATE"
  echo "Example: $0 2023-01-01 2023-01-31"
  exit 1
fi

cd "$(dirname "$0")"

# Function to get next date that works on both macOS and Linux
next_date() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -v+1d -f "%Y-%m-%d" "$1" "+%Y-%m-%d"
  else
    # Linux
    date -d "$1 + 1 day" "+%Y-%m-%d"
  fi
}

# Function to add days to a date (for end condition)
add_days() {
  local date_str=$1
  local days=$2
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -v+"$days"d -f "%Y-%m-%d" "$date_str" "+%Y-%m-%d"
  else
    # Linux
    date -d "$date_str + $days day" "+%Y-%m-%d"
  fi
}

# Calculate end condition (day after END_DATE)
end_condition=$(add_days "$END_DATE" 1)

current_date=$START_DATE
while [ "$current_date" != "$end_condition" ]; do
  echo "Processing $current_date hour $HOUR"
  python -m src.github_archive_to_iceberg --date "$current_date" --hour "$HOUR"
  current_date=$(next_date "$current_date")
done

echo "Complete!" 