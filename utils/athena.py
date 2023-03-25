#!/usr/bin/env python3

import logging
import boto3
import time


class AthenaUtils:
    def __init__(self, workgroup, database, query_bucket):
        self.client = boto3.client("athena")
        self.workgroup = workgroup
        self.database = database
        self.query_bucket = query_bucket

    def get_type(self, data_type):
        if data_type in ['datetime', 'date']:
            return 'timestamp'
        elif data_type in ['int', 'smallint']:
            return 'integer'
        elif data_type in ['decimal']:
            return 'decimal'
        else:
            return 'string'

    def generate_table_ddl(self, table_name, columns, s3_location):
        col_list = []
        for k, v in columns.items():
            col_list.append(f"`{k}` {v}")

        col_str = ',\n'.join(col_list)

        ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database}`.`{table_name}` (
{col_str}
)
PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
ESCAPED BY '\\\\'
LINES TERMINATED BY '\\n'
LOCATION 's3://{s3_location}/{table_name}/'
TBLPROPERTIES ('has_encrypted_data' = 'false');
        """
        return ddl

    def execute_query(self, query):
        q_start = self.client.start_query_execution(
            QueryString=f"{query}",
            QueryExecutionContext={"Database": f"{self.database}"},
            WorkGroup=self.workgroup,
            ResultConfiguration={
                'OutputLocation': f"s3://{self.query_bucket}/QueryResults/{self.database}",
            }
        )
        still_running = True
        while still_running:
            logging.info("Checking Athena query execution")
            response = self.client.get_query_execution(
                QueryExecutionId=q_start["QueryExecutionId"]
            )
            logging.info(response)
            status = response["QueryExecution"]["Status"]["State"]
            if status == 'SUCCEEDED':
                logging.info("Athena query execution done")
                still_running = False
            elif status == "FAILED":
                logging.error("Athena query failed.")
                exit(1)
            else:
                logging.info(
                    "Athena query still running: wait 10 seconds...")
                time.sleep(10)
