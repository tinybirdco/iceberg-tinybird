
# Tinybird GitHub Events Analysis

## Tinybird

### Overview
This project analyzes GitHub events data to extract insights about repositories, users, and activity patterns. It demonstrates how to use Tinybird to process and analyze large volumes of GitHub event data.

### Data Sources

#### github_events
This data source contains GitHub events data including information about stars, forks, pull requests, issues, and more.

```bash
curl -X POST "http://127.0.0.1:8001/v0/events?name=github_events" \
     -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
     -d '{
       "file_time": "2023-01-01 12:00:00",
       "event_type": "WatchEvent",
       "actor_login": "user123",
       "repo_name": "organization/repository",
       "created_at": "2023-01-01 12:00:00",
       "updated_at": "2023-01-01 12:00:00",
       "action": "none",
       "comment_id": 0,
       "position": 0,
       "line": 0,
       "ref_type": "none",
       "number": 0,
       "state": "",
       "locked": 0,
       "comments": 0,
       "author_association": "NONE",
       "closed_at": "1970-01-01 00:00:00",
       "merged_at": "1970-01-01 00:00:00",
       "merge_commit_sha": "",
       "head_ref": "",
       "head_sha": "",
       "base_ref": "",
       "base_sha": "",
       "merged": 0,
       "mergeable": 0,
       "rebaseable": 0,
       "mergeable_state": "unknown",
       "merged_by": "",
       "review_comments": 0,
       "maintainer_can_modify": 0,
       "commits": 0,
       "additions": 0,
       "deletions": 0,
       "changed_files": 0,
       "diff_hunk": "",
       "original_position": 0,
       "commit_id": "",
       "original_commit_id": "",
       "push_size": 0,
       "push_distinct_size": 0,
       "member_login": "",
       "release_tag_name": "",
       "release_name": "",
       "review_state": "none"
     }'
```

#### github_events_historic
This data source contains a simplified schema of GitHub events historical data that is used for analysis.

```bash
curl -X POST "http://127.0.0.1:8001/v0/events?name=github_events_historic" \
     -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
     -d '{
       "event_type": "WatchEvent",
       "actor_login": "user123",
       "repo_name": "organization/repository",
       "created_at": "2023-01-01 12:00:00",
       "action": "created",
       "number": 123
     }'
```

#### watch_events_by_repo
This data source stores materialized counts of GitHub Watch events (stars) by repository and date.

```bash
curl -X POST "http://127.0.0.1:8001/v0/events?name=watch_events_by_repo" \
     -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
     -d '{
       "date": "2023-01-01",
       "repo_name": "organization/repository",
       "watch_count": 0
     }'
```

#### results
This data source stores results from SQL queries, including metrics about LLM usage and performance.

```bash
curl -X POST "http://127.0.0.1:8001/v0/events?name=results" \
     -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
     -d '{
       "sql": "SELECT * FROM table",
       "sql_result_success": true,
       "sql_result_execution_time": 0.5,
       "sql_result_error": "",
       "sql_result_query_latency": 0.2,
       "sql_result_rows_read": 100,
       "sql_result_bytes_read": 1024,
       "name": "test_query",
       "question": "What is the most popular repository?",
       "model": "gpt-4",
       "provider": "openai",
       "llm_time_to_first_token": 0.1,
       "llm_total_duration": 2.5,
       "llm_prompt_tokens": 100,
       "llm_completion_tokens": 50,
       "llm_total_tokens": 150,
       "llm_error": ""
     }'
```

### Endpoints

#### Materialize Watch Events
This materialization counts GitHub Watch events (stars) by repository and date, and stores the results in the watch_events_by_repo datasource.

```bash
curl -X GET "http://127.0.0.1:8001/v0/pipes/materialize_watch_events.json?token=$TB_ADMIN_TOKEN"
```

#### Copy GitHub Events by Date Range
Filter GitHub events by a specified date range and copy them to the github_events_historic datasource.

```bash
curl -X POST "http://127.0.0.1:8001/v0/pipes/github_events_copy" \
     -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
     -d '{
       "from_date": "2023-01-01 00:00:00",
       "to_date": "2023-01-31 23:59:59"
     }'
```

Parameters:
- `from_date`: Start date for filtering events (format: YYYY-MM-DD HH:MM:SS)
- `to_date`: End date for filtering events (format: YYYY-MM-DD HH:MM:SS)

#### Top Starred Repositories
Get the most starred repositories for a given time period.

```bash
curl -X GET "http://127.0.0.1:8001/v0/pipes/top_starred_repos.json?token=$TB_ADMIN_TOKEN&start_date=2023-01-01&end_date=2023-12-31&limit=10"
```

Parameters:
- `start_date`: Start date for filtering stars (format: YYYY-MM-DD). Default: 2023-01-01
- `end_date`: End date for filtering stars (format: YYYY-MM-DD). Default: 2023-12-31
- `limit`: Number of top repositories to return. Default: 10

#### Test Endpoint
A test endpoint that checks the minimum creation date in the GitHub events Iceberg table.

```bash
curl -X GET "http://127.0.0.1:8001/v0/pipes/test.json?token=$TB_ADMIN_TOKEN"
```
