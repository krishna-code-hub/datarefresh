import cx_Oracle
import pyodbc
import psycopg2
import logging

class OracleClient:
    def __init__(self, config):
        self.config = config

    def extract_data(self, table):
        try:
            connection = cx_Oracle.connect(
                self.config['username'],
                self.config['password'],
                f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
            )
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            data = cursor.fetchall()
            return data
        except cx_Oracle.Error as e:
            logging.error(f"Oracle extraction failed: {str(e)}")

    def load_data(self, table, data):
        try:
            # Placeholder for loading data
            logging.info(f"Loading data back to Oracle for table {table['table_name']}")
        except cx_Oracle.Error as e:
            logging.error(f"Oracle load failed: {str(e)}")

class SQLServerClient:
    def __init__(self, config):
        self.config = config

    def extract_data(self, table):
        try:
            connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.config['host']},{self.config['port']};DATABASE={self.config['database']};UID={self.config['username']};PWD={self.config['password']}"
            )
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            data = cursor.fetchall()
            return data
        except pyodbc.Error as e:
            logging.error(f"SQLServer extraction failed: {str(e)}")

    def load_data(self, table, data):
        try:
            # Placeholder for loading data
            logging.info(f"Loading data back to SQL Server for table {table['table_name']}")
        except pyodbc.Error as e:
            logging.error(f"SQLServer load failed: {str(e)}")

class PostgresClient:
    def __init__(self, config):
        self.config = config

    def extract_data(self, table):
        try:
            connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password']
            )
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            data = cursor.fetchall()
            return data
        except psycopg2.Error as e:
            logging.error(f"Postgres extraction failed: {str(e)}")

    def load_data(self, table, data):
        try:
            # Placeholder for loading data
            logging.info(f"Loading data back to Postgres for table {table['table_name']}")
        except psycopg2.Error as e:
            logging.error(f"Postgres load failed: {str(e)}")