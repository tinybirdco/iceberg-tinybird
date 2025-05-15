#!/bin/bash

# Script to run GitHub events copy day by day from 2011-02-12 to 2025-05-14

# Convert dates to seconds since epoch for easy iteration
start_date=$(date -j -f "%Y-%m-%d %H:%M:%S" "2011-02-12 00:00:00" "+%s")
end_date=$(date -j -f "%Y-%m-%d %H:%M:%S" "2025-05-14 00:00:00" "+%s")

# Seconds in a day
day_seconds=$((60*60*24))

# Iterate through each day
current_date=$start_date
while [ $current_date -le $end_date ]; do
    # Format current date for display and parameters
    from_date=$(date -j -f "%s" "$current_date" "+%Y-%m-%d 00:00:00")
    
    # Calculate next day
    next_date=$((current_date + day_seconds))
    to_date=$(date -j -f "%s" "$next_date" "+%Y-%m-%d 00:00:00")
    
    echo "Processing data from $from_date to $to_date"
    
    # Run the Tinybird copy command for this day
    tb copy run github_events_copy --param from_date="$from_date" --param to_date="$to_date" --wait
    
    # Move to next day
    current_date=$next_date
    
    # Optional: add a small delay between API calls if needed
    sleep 1
done

echo "GitHub events copy completed for all days" 