import cx_Oracle
import pyodbc
import psycopg2
import sqlite3
import logging
from psycopg2 import pool
import cx_Oracle
from pyodbc import pooling
from sqlite3 import Connection

class OracleClient:
    def __init__(self, config):
        self.config = config
        self.pool = cx_Oracle.SessionPool(
            user=config['username'],
            password=config['password'],
            dsn=f"{config['host']}:{config['port']}/{config['service_name']}",
            min=1, max=5, increment=1, threaded=True
        )

    def get_connection(self):
        return self.pool.acquire()

    def extract_data_row_by_row(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} WHERE {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            for row in cursor:
                yield row
        except cx_Oracle.Error as e:
            logging.error(f"Oracle extraction failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def delete_unwanted_data(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            cursor.execute(delete_query)
            connection.commit()
            logging.info(f"Deleted unwanted data from Oracle for table {table['table_name']}")
        except cx_Oracle.Error as e:
            logging.error(f"Oracle delete operation failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def update_or_insert_row(self, table, row):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            primary_key = self.get_primary_key(table['table_name'])
            if primary_key in [col['column_name'] for col in table['columns'] if col.get('pii') == 'Y']:
                # If primary key is part of masking, delete and insert
                delete_query = f"DELETE FROM {table['table_name']} WHERE {primary_key} = :1"
                cursor.execute(delete_query, (row[primary_key],))
                insert_query = f"INSERT INTO {table['table_name']} VALUES ({', '.join([':{}'.format(i+1) for i in range(len(row))])})"
                cursor.execute(insert_query, row)
            else:
                # Update if primary key is not part of masking
                update_query = f"UPDATE {table['table_name']} SET ... WHERE {primary_key} = :1"
                cursor.execute(update_query, row)
            connection.commit()
            logging.info(f"Row updated/inserted in Oracle for table {table['table_name']}")
        except cx_Oracle.Error as e:
            logging.error(f"Oracle update/insert failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def get_primary_key(self, table_name):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{table_name.upper()}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name"
            cursor.execute(query)
            primary_key = cursor.fetchone()
            return primary_key[0] if primary_key else None
        except cx_Oracle.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

class SQLServerClient:
    def __init__(self, config):
        self.config = config
        self.pool = pyodbc.pooling.create_pool(
            min_conn=1, max_conn=5,
            connection_string=f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['host']},{config['port']};DATABASE={config['database']};UID={config['username']};PWD={config['password']}"
        )

    def get_connection(self):
        return self.pool.acquire()

    def extract_data_row_by_row(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} WHERE {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            for row in cursor:
                yield row
        except pyodbc.Error as e:
            logging.error(f"SQL Server extraction failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def delete_unwanted_data(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            cursor.execute(delete_query)
            connection.commit()
            logging.info(f"Deleted unwanted data from SQL Server for table {table['table_name']}")
        except pyodbc.Error as e:
            logging.error(f"SQL Server delete operation failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def update_or_insert_row(self, table, row):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            primary_key = self.get_primary_key(table['table_name'])
            if primary_key in [col['column_name'] for col in table['columns'] if col.get('pii') == 'Y']:
                # If primary key is part of masking, delete and insert
                delete_query = f"DELETE FROM {table['table_name']} WHERE {primary_key} = ?"
                cursor.execute(delete_query, (row[primary_key],))
                insert_query = f"INSERT INTO {table['table_name']} VALUES ({', '.join(['?'] * len(row))})"
                cursor.execute(insert_query, row)
            else:
                # Update if primary key is not part of masking
                update_query = f"UPDATE {table['table_name']} SET ... WHERE {primary_key} = ?"
                cursor.execute(update_query, row)
            connection.commit()
            logging.info(f"Row updated/inserted in SQL Server for table {table['table_name']}")
        except pyodbc.Error as e:
            logging.error(f"SQL Server update/insert failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def get_primary_key(self, table_name):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(query)
            primary_key = cursor.fetchone()
            return primary_key[0] if primary_key else None
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

class PostgresClient:
    def __init__(self, config):
        self.config = config
        self.pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            dbname=config['database'],
            user=config['username'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )

    def get_connection(self):
        return self.pool.getconn()

    def extract_data_row_by_row(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT * FROM {table['table_name']} WHERE {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            for row in cursor:
                yield row
        except psycopg2.Error as e:
            logging.error(f"Postgres extraction failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)

    def delete_unwanted_data(self, table):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            cursor.execute(delete_query)
            connection.commit()
            logging.info(f"Deleted unwanted data from Postgres for table {table['table_name']}")
        except psycopg2.Error as e:
            logging.error(f"Postgres delete operation failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)

    def update_or_insert_row(self, table, row):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            primary_key = self.get_primary_key(table['table_name'])
            if primary_key in [col['column_name'] for col in table['columns'] if col.get('pii') == 'Y']:
                # If primary key is part of masking, delete and insert
                delete_query = f"DELETE FROM {table['table_name']} WHERE {primary_key} = %s"
                cursor.execute(delete_query, (row[primary_key],))
                insert_query = f"INSERT INTO {table['table_name']} VALUES ({', '.join(['%s'] * len(row))})"
                cursor.execute(insert_query, row)
            else:
                # Update if primary key is not part of masking
                update_query = f"UPDATE {table['table_name']} SET ... WHERE {primary_key} = %s"
                cursor.execute(update_query, row)
            connection.commit()
            logging.info(f"Row updated/inserted in Postgres for table {table['table_name']}")
        except psycopg2.Error as e:
            logging.error(f"Postgres update/insert failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)

    def get_primary_key(self, table_name):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"
            cursor.execute(query)
            primary_key = cursor.fetchone()
            return primary_key[0] if primary_key else None
        except psycopg2.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)

class SQLiteClient:
    def __init__(self, config):
        self.config = config
        self.connection = sqlite3.connect(self.config['database'], check_same_thread=False)

    def extract_data_row_by_row(self, table):
        try:
            cursor = self.connection.cursor()
            query = f"SELECT * FROM {table['table_name']} WHERE {table['extraction_logic']['where_clause']}"
            cursor.execute(query)
            for row in cursor:
                yield row
        except sqlite3.Error as e:
            logging.error(f"SQLite extraction failed: {str(e)}")

    def delete_unwanted_data(self, table):
        try:
            cursor = self.connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            cursor.execute(delete_query)
            self.connection.commit()
            logging.info(f"Deleted unwanted data from SQLite for table {table['table_name']}")
        except sqlite3.Error as e:
            logging.error(f"SQLite delete operation failed: {str(e)}")

    def update_or_insert_row(self, table, row):
        try:
            cursor = self.connection.cursor()
            primary_key = self.get_primary_key(table['table_name'])
            if primary_key in [col['column_name'] for col in table['columns'] if col.get('pii') == 'Y']:
                # If primary key is part of masking, delete and insert
                delete_query = f"DELETE FROM {table['table_name']} WHERE {primary_key} = ?"
                cursor.execute(delete_query, (row[primary_key],))
                insert_query = f"INSERT INTO {table['table_name']} VALUES ({', '.join(['?'] * len(row))})"
                cursor.execute(insert_query, row)
            else:
                # Update if primary key is not part of masking
                update_query = f"UPDATE {table['table_name']} SET ... WHERE {primary_key} = ?"
                cursor.execute(update_query, row)
            self.connection.commit()
            logging.info(f"Row updated/inserted in SQLite for table {table['table_name']}")
        except sqlite3.Error as e:
            logging.error(f"SQLite update/insert failed: {str(e)}")

    def get_primary_key(self, table_name):
        try:
            cursor = self.connection.cursor()
            query = f"PRAGMA table_info({table_name})"
            cursor.execute(query)
            columns = cursor.fetchall()
            for column in columns:
                if column[5] == 1:  # The fifth element in the result set indicates if the column is a primary key
                    return column[1]
            return None
        except sqlite3.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")