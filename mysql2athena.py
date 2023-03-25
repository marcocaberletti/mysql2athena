#!/usr/bin/env python3

import argparse
import logging
import os

from utils.athena import *
from utils.mysql import *

log = logging.getLogger()
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(format)
log.addHandler(ch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", help="MySQL db hostname",
                        default="localhost")
    parser.add_argument("--db-user", help="MySQL db username",
                        default="root")
    parser.add_argument(
        "--db-password", help="MySQL db password", required=True)

    parser.add_argument("--athena-workgroup",
                        help="Athena workgroup name", required=True)
    parser.add_argument("--athena-database",
                        help="Athena database name", required=True)
    parser.add_argument("--athena-query-bucket",
                        help="Athena query bucket name", required=True)

    parser.add_argument("--table-schema", help="Table schema", required=True)
    parser.add_argument("--table", help="Table to convert", required=True)
    parser.add_argument("--table-data-location",
                        help="S3 data location", required=True)

    parser.add_argument(
        "--dry-run", help="Simulate execution", action='store_true')

    args = parser.parse_args()
    logging.info(args)

    dbhost = args.db_host
    dbuser = args.db_user
    dbpassword = args.db_password
    dbname = args.table_schema
    table = args.table
    athena_workgroup = args.athena_workgroup
    athena_database = args.athena_database
    athena_query_bucket = args.athena_query_bucket
    s3_location = args.table_data_location
    dry_run = args.dry_run

    db = MysqlUtils(dbhost, dbuser, dbpassword, dbname)
    athena = AthenaUtils(
        athena_workgroup, athena_database, athena_query_bucket)

    cols = db.get_column_types(table)
    logging.info(f"MySQL cols:\n{cols}")
    athena_cols = {}
    for k, v in cols.items():
        athena_cols[k] = athena.get_type(v)
    logging.info(f"Athena cols:\n{athena_cols}")

    ddl = athena.generate_table_ddl(table, athena_cols, s3_location)
    if dry_run:
        logging.info(f"Athena DDL to execute:\n{ddl}")
    else:
        athena.execute_query(ddl)

    db.close()


if __name__ == '__main__':
    main()
