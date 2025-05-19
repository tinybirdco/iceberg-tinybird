
## Tinybird GitHub Events Analysis

### Overview

This project analyzes GitHub events data to extract insights about repositories, users, and activity patterns. It demonstrates how to use Tinybird to process and analyze large volumes of GitHub event data.

### Data Sources

#### github_events
This data source contains GitHub events data including information about stars, forks, pull requests, issues, and more.

`cp_github_events_historic.pipe` backfills historical data from Iceberg into this Data Source

#### github_events_rt
This data source contains hourly syncs of the GitHub archive.

`cp_github_events_rt.pipe` syncs data from Iceberg into this Data Source.

#### github_events_by_type
This data source stores materialized counts of GitHub events by repository, type and date.

### Pipes

#### Materialize GitHub Events
This materialization counts GitHub events by repository, type and date, and stores the results in the `github_events_by_day` datasource.

#### Copy GitHub Events by Date Range
Filter GitHub events by a specified date range and copy them to the `github_events_rt` and `github_events` datasources.

#### Top Repositories
Get the most starred repositories for a given time period.

```bash
curl -X GET "http://127.0.0.1:8001/v0/pipes/top_repos.json?token=$TB_ADMIN_TOKEN&start_date=2023-01-01&end_date=2023-12-31&limit=10&event_type=WatchEvent"
```

Parameters:
- `start_date`: Start date for filtering stars (format: YYYY-MM-DD). Default: 2023-01-01
- `end_date`: End date for filtering stars (format: YYYY-MM-DD). Default: 2023-12-31
- `event_type`: The GitHub event type. Example: 'WatchEvent'
- `group_by`: Name of the columns to aggregate. Example: 'repo_name,event_type'
- `limit`: Number of top repositories to return. Default: 10

