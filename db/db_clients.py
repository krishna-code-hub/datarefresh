import sqlite3
import psycopg2
import logging
from psycopg2 import pool
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class AbstractDatabaseClient(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    async def execute_query(self, query: str, batch_size: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def delete_unwanted_data(self, table: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_primary_key(self, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def bulk_insert(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        pass

    @abstractmethod
    def bulk_update(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        pass


class SQLiteClient(AbstractDatabaseClient):
    def __init__(self, config):
        super().__init__(config)
        # Resolve the database path, supporting environment variables and relative paths
        self.db_path = os.path.expandvars(config["database_path"])
        if not os.path.isabs(self.db_path):
            # Make the path absolute based on the project base directory if it's relative
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, self.db_path)

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        logging.debug(f"SQLite db path - {self.db_path}")
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)

    def get_connection(self):
        logging.debug("Getting connection for SQLite")
        return self.connection

    async def execute_query(self, query: str, batch_size: int) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        logging.debug("Executing query: %s", query)
        try:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
                logging.debug("Fetched batch of size: %d", len(batch))
                return batch
        except sqlite3.Error as e:
            logging.error(f"SQLite query execution failed: {str(e)}")

    def delete_unwanted_data(self, table):
        try:
            cursor = self.connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            logging.debug("Executing delete query: %s", delete_query)
            cursor.execute(delete_query)
            self.connection.commit()
            logging.info(f"Deleted unwanted data from SQLite for table {table['table_name']}")
        except sqlite3.Error as e:
            logging.error(f"SQLite delete operation failed: {str(e)}")

    def get_primary_key(self, table_name):
        try:
            cursor = self.connection.cursor()
            query = f"PRAGMA table_info({table_name})"
            logging.debug("Executing query to get primary key: %s", query)
            cursor.execute(query)
            columns = cursor.fetchall()
            primary_keys = [column[1] for column in columns if column[5] == 1]  # The fifth element indicates if the column is a primary key
            return primary_keys
        except sqlite3.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")
            return []

    def bulk_insert(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        try:
            cursor = self.connection.cursor()
            # Delete existing rows based on primary key before inserting
            for row in batch:
                delete_query = f"DELETE FROM {table_name} WHERE {' AND '.join([f'{pk} = ?' for pk in primary_key])}"
                logging.debug("Executing delete query for bulk insert: %s", delete_query)
                cursor.execute(delete_query, tuple(row[pk] for pk in primary_key))
            # Insert new rows
            insert_query = (
                f"INSERT INTO {table_name} ({', '.join(batch[0].keys())}) VALUES ({', '.join(['?'] * len(batch[0]))})"
            )
            logging.debug("Executing bulk insert query: %s", insert_query)
            cursor.executemany(insert_query, [tuple(row.values()) for row in batch])
            self.connection.commit()
            logging.info(f"Bulk inserted rows into SQLite table {table_name}")
        except sqlite3.Error as e:
            logging.error(f"SQLite bulk insert failed: {str(e)}")

    def bulk_update(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        try:
            cursor = self.connection.cursor()
            for row in batch:
                update_query = f"UPDATE {table_name} SET {', '.join([f'{key} = ?' for key in row.keys() if key not in primary_key])} WHERE {' AND '.join([f'{pk} = ?' for pk in primary_key])}"
                params = [value for key, value in row.items() if key not in primary_key] + [row[pk] for pk in primary_key]
                logging.debug("Executing update query: %s with params: %s", update_query, params)
                cursor.execute(update_query, params)
            self.connection.commit()
            logging.info(f"Bulk updated rows in SQLite table {table_name}")
        except sqlite3.Error as e:
            logging.error(f"SQLite bulk update failed: {str(e)}")


class PostgresClient(AbstractDatabaseClient):
    def __init__(self, config):
        super().__init__(config)
        self.pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            dbname=config["database"],
            user=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
        )
        logging.debug("PostgresClient initialized with config: %s", config)

    def get_connection(self):
        logging.debug("Getting connection from Postgres connection pool")
        return self.pool.getconn()

    async def execute_query(self, query: str, batch_size: int) -> List[Dict[str, Any]]:
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            logging.debug("Executing query: %s", query)
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
                logging.debug("Fetched batch of size: %d", len(batch))
                yield batch
        except psycopg2.Error as e:
            logging.error(f"Postgres query execution failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)
                logging.debug("Released Postgres connection back to pool")

    def delete_unwanted_data(self, table):
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            delete_query = f"DELETE FROM {table['table_name']} WHERE NOT ({table['extraction_logic']['where_clause']})"
            logging.debug("Executing delete query: %s", delete_query)
            cursor.execute(delete_query)
            connection.commit()
            logging.info(f"Deleted unwanted data from Postgres for table {table['table_name']}")
        except psycopg2.Error as e:
            logging.error(f"Postgres delete operation failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)
                logging.debug("Released Postgres connection back to pool")

    def get_primary_key(self, table_name):
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            query = f"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"
            logging.debug("Executing query to get primary key: %s", query)
            cursor.execute(query)
            primary_keys = [row[0] for row in cursor.fetchall()]
            return primary_keys
        except psycopg2.Error as e:
            logging.error(f"Failed to retrieve primary key for table {table_name}: {str(e)}")
            return []
        finally:
            if connection:
                self.pool.putconn(connection)
                logging.debug("Released Postgres connection back to pool")

    def bulk_insert(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            # Delete existing rows based on primary key before inserting
            for row in batch:
                delete_query = f"DELETE FROM {table_name} WHERE {' AND '.join([f'{pk} = %s' for pk in primary_key])}"
                logging.debug("Executing delete query for bulk insert: %s", delete_query)
                cursor.execute(delete_query, tuple(row[pk] for pk in primary_key))
            # Insert new rows
            insert_query = (
                f"INSERT INTO {table_name} ({', '.join(batch[0].keys())}) VALUES ({', '.join(['%s'] * len(batch[0]))})"
            )
            logging.debug("Executing bulk insert query: %s", insert_query)
            cursor.executemany(insert_query, [tuple(row.values()) for row in batch])
            connection.commit()
            logging.info(f"Bulk inserted rows into Postgres table {table_name}")
        except psycopg2.Error as e:
            logging.error(f"Postgres bulk insert failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)
                logging.debug("Released Postgres connection back to pool")

    def bulk_update(self, schema: str, table_name: str, batch: List[Dict[str, Any]], primary_key: List[str]) -> None:
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            for row in batch:
                update_query = f"UPDATE {table_name} SET {', '.join([f'{key} = %s' for key in row.keys() if key not in primary_key])} WHERE {' AND '.join([f'{pk} = %s' for pk in primary_key])}"
                params = [value for key, value in row.items() if key not in primary_key] + [row[pk] for pk in primary_key]
                logging.debug("Executing update query: %s with params: %s", update_query, params)
                cursor.execute(update_query, params)
            connection.commit()
            logging.info(f"Bulk updated rows in Postgres table {table_name}")
        except psycopg2.Error as e:
            logging.error(f"Postgres bulk update failed: {str(e)}")
        finally:
            if connection:
                self.pool.putconn(connection)
                logging.debug("Released Postgres connection back to pool")
