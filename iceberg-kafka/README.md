# Tinybird Iceberg Export Project

## Tinybird

### Overview
This project demonstrates how to export data from Apache Iceberg tables and PostgreSQL databases to Tinybird datasources. This allows you to integrate with existing data sources like data lakes using Iceberg format or relational databases, enabling high-performance analytics on this data within Tinybird's platform.

### Data sources
#### iceberg_export_data
This datasource stores data exported from an Apache Iceberg table. It contains structured data with id, name, value, and timestamp fields.

To ingest data into the iceberg_export_data datasource:
```bash
curl -X POST "https://api.europe-west2.gcp.tinybird.co/v0/events?name=iceberg_export_data" \
    -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
    -d '{"id":"123","name":"example","value":42.5,"timestamp":"2023-01-01 12:00:00"}'
```

#### iceberg_export_data_copy
This datasource also stores data exported from an Apache Iceberg table, with the same schema and structure as iceberg_export_data. It can be used as a backup or for different processing requirements.

To ingest data into the iceberg_export_data_copy datasource:
```bash
curl -X POST "https://api.europe-west2.gcp.tinybird.co/v0/events?name=iceberg_export_data_copy" \
    -H "Authorization: Bearer $TB_ADMIN_TOKEN" \
    -d '{"id":"123","name":"example","value":42.5,"timestamp":"2023-01-01 12:00:00"}'
```

### Endpoints
#### iceberg_export_pipe
This pipe exports data from an Apache Iceberg table stored in S3 to the iceberg_export_data datasource. It connects directly to the Iceberg table using AWS credentials stored as Tinybird secrets and filters data based on timestamp parameters.

To execute the pipe with specific date parameters:
```bash
curl -X GET "https://api.europe-west2.gcp.tinybird.co/v0/pipes/iceberg_export_pipe.json?start_date=2023-01-01%2000:00:00&end_date=2023-12-31%2023:59:59&token=$TB_ADMIN_TOKEN"
```

#### iceberg_copy_pipe
This pipe exports data from an Apache Iceberg table to the iceberg_export_data_copy datasource. It works similarly to the iceberg_export_pipe but uses a different target datasource. This can be useful for creating backups or for different processing workflows.

To execute the pipe with specific date parameters:
```bash
curl -X GET "https://api.europe-west2.gcp.tinybird.co/v0/pipes/iceberg_copy_pipe.json?start_date=2023-01-01%2000:00:00&end_date=2023-12-31%2023:59:59&token=$TB_ADMIN_TOKEN"
```

#### postgres_export_pipe
This pipe exports data from a PostgreSQL database to the iceberg_export_data datasource. It connects to a PostgreSQL database using credentials stored as Tinybird secrets and filters data based on timestamp parameters.

To execute the pipe with specific date parameters:
```bash
curl -X GET "https://api.europe-west2.gcp.tinybird.co/v0/pipes/postgres_export_pipe.json?start_date=2023-01-01%2000:00:00&end_date=2023-12-31%2023:59:59&token=$TB_ADMIN_TOKEN"
```

All pipes use the COPY type and can be scheduled to run periodically to keep the Tinybird datasources updated with the latest data from their respective sources.
