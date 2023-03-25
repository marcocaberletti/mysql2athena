# mysql2athena

Simple utility to convert a MySQL table into a AWS Athena table.

## Example

```console
./mysql2athena.py --db-host my-hostname \
    --db-user my-user \
    --db-password "super_secret_password" \
    --athena-workgroup my_athena_workgroup \
    --athena-database my_athena_db \
    --athena-query-bucket my_s3_bucket \
    --table-schema example \
    --table table_01 \
    --table-data-location my_bucket/folder \
    --dry-run
```
