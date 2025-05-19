## Iceberg Tinybird

This repository contains a working project to synchronize the GitHub archive to Iceberg and use Tinybird for real-time analytics over Iceberg data. Read the full article [here](https://tinybird.co/blog-posts/real-time-analytics-on-apache-iceberg-with-tinybird).

Contents:
- `gharchive-to-iceberg`: an Apache Spark project to synchronize the GitHub archive to an Iceberg table in S3
- `tinybird`: a Tinybird project to synchronize Iceberg tables to Tinybird for real-time analytics

### How to use

Fork this repository and follow the next steps.

**1. Push data from [gharchive.org](https://www.gharchive.org/) to an Iceberg table in S3**

```sh
cp .env.example .env # and fill the env variables
source .env
python -m venv env
source env/bin/activate
pip install -e .
python -m src.github_archive_to_iceberg --date 2011-02-12
```

This will create an Apache Iceberg table in the S3 bucket speciefied in the `.env` file. For instance given these variables:

```
export ICEBERG_WAREHOUSE=s3a://your-bucket/iceberg/
export ICEBERG_DATABASE=your_db
export ICEBERG_TABLE=your_table
```

Your Apache Iceberg table is created in `s3://your-bucket/iceberg/your_db/your_table`

**2. Configure the Tinybird project locally**

```sh
cd tinybird
curl https://tinybird.co | sh
tb login
tb local start
```

Modify references to the `iceberg` table function in the project files to point to your Apache Iceberg table:

```sql
... 
FROM iceberg(
    's3://your-bucket/iceberg/your_db/your_table',
    {{tb_secret('aws_access_key_id')}},
    {{tb_secret('aws_secret_access_key')}}
)
...
```

Create AWS access key and secret as Tinybird environment variables:

```sh
tb secret set aws_access_key_id {{your_aws_access_key_id}}
tb secret set aws_secret_access_key {{your_aws_secret_access_key}}
```

Build locally:

```sh
tb build
```

Synchronize data and test locally:

```sh
tb copy run cp_github_events_historic --param from_date='2020-01-01 00:00:00' to_date='2025-05-15 00:00:00' --wait
tb endpoint data top_repos --event_types 'WatchEvent' --group_by 'event_type'
** Pipe: top_repos
** Query took 0.19869037 seconds
** Rows read: 1,402,882
** Bytes read: 51.92 MB
------------------------
| event_type |   count |
------------------------
| WatchEvent | 2908534 |
------------------------
```


**3. Deploy to cloud**

Deploy to cloud with the Tinybird CLI:

```sh
# create the env variables in Tinybird Cloud
tb --cloud secret set aws_access_key_id {{your_aws_access_key_id}}
tb --cloud secret set aws_secret_access_key {{your_aws_secret_access_key}}
# Deploy
tb --cloud deploy
# Sync data to cloud
tb --cloud copy run cp_github_events_historic --param from_date='2020-01-01 00:00:00' to_date='2025-05-15 00:00:00' --wait
```

Read the [Tinybird project README.md](tinybird/README.md) for a description of the project.

### GitHub Actions

The project contains GitHub actions for CI and CD. Configure these variables as secrets in your GitHub repository:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
TINYBIRD_HOST
TINYBIRD_TOKEN
```

Use `tb info` to get your `TINYBIRD_HOST` and `TINYBIRD_TOKEN`:

```sh
tb info
** New version available. 0.0.1.dev198 -> 0.0.1.dev199
** Run `tb update` to update or `export TB_VERSION_WARNING=0` to skip the check.

» Tinybird Cloud:
----------------------------------------------------------------------------------------------------------------------------------------
user: alrocar@tinybird.co
workspace_name: github_iceberg
workspace_id: 1234c2de-13cd-4e9e-804b-c17d0e44f5da
token: <-- THIS IS YOUR TINYBIRD_TOKEN
api: https://api.europe-west2.gcp.tinybird.co <-- THIS IS YOUR TINYBIRD_HOST
ui: https://cloud.tinybird.co/gcp/europe-west2/github_iceberg
```

On each pull request the CI GitHub action validates the project builds, on merge the project is automatically deployed to cloud.

There's an additional `hourly_data_ingest.yml` GitHub actions that runs hourly to synchronize new GitHub events from `gharchive.org` to the Iceberg table and to Tinybird.
