#!/usr/bin/env python3

import logging
import MySQLdb


class MysqlUtils:
    def __init__(self, dbhost, dbuser, dbpassword, dbname):
        self.dbhost = dbhost
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbname = dbname
        logging.info(f"Try connection to {self.dbhost}")
        self.connection = MySQLdb.connect(
            host=self.dbhost, user=self.dbuser, passwd=self.dbpassword, db=self.dbname)
        logging.info(
            f"Connected to host:[{self.dbhost}] user:[{self.dbuser}] db:[{self.dbname}]")
        if not self.connection.get_autocommit():
            self.connection.autocommit(True)
            logging.info("Set DB connection transaction autocommit.")
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()

    def get_column_types(self, table_name):
        query = f"""
        SELECT column_name,data_type
        FROM information_schema.columns
        WHERE table_schema = '{self.dbname}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        map = {}

        for row in data:
            name = row[0]
            type = row[1]
            map[name] = type
        return map
