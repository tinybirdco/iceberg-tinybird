#!/bin/bash

# Simple script to load GitHub Archive files in reverse order, from the last day backwards
# Usage: ./load_daily.sh 2024-05-01 2025-05-15

START_DATE=$1
END_DATE=$2

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "Usage: $0 START_DATE END_DATE"
  echo "Example: $0 2024-05-01 2025-05-15"
  exit 1
fi

cd "$(dirname "$0")"

# Function to get previous date that works on both macOS and Linux
previous_date() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -v-1d -f "%Y-%m-%d" "$1" "+%Y-%m-%d"
  else
    # Linux
    date -d "$1 - 1 day" "+%Y-%m-%d"
  fi
}

# Function to subtract days from a date (for start condition)
subtract_days() {
  local date_str=$1
  local days=$2
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    date -j -v-"$days"d -f "%Y-%m-%d" "$date_str" "+%Y-%m-%d"
  else
    # Linux
    date -d "$date_str - $days day" "+%Y-%m-%d"
  fi
}

# Calculate start condition (day before START_DATE)
start_condition=$(subtract_days "$START_DATE" 1)

current_date=$END_DATE
while [ "$current_date" != "$start_condition" ]; do
  # Iterate through all hours for each day in reverse order
  for hour in {23..0}; do
    echo "Processing $current_date hour $hour"
    python -m src.github_archive_to_iceberg --date "$current_date" --hour "$hour"
  done
  current_date=$(previous_date "$current_date")
done

echo "Complete!" 