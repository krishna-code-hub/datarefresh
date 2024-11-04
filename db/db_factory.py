#from db.db_clients import OracleClient, SQLServerClient, PostgresClient, SQLiteClient
from db.db_clients import PostgresClient, SQLiteClient

class DBFactory:
    @staticmethod
    def get_database_client(db_config, db_type, db_name):
        config = db_config.get(db_type, {}).get(db_name)
        if not config:
            raise ValueError(f"Unsupported or missing configuration for database type: {db_name}")

        if db_name == "sqlite":
            return SQLiteClient(config)
        elif db_name == "postgres":
            return PostgresClient(config)
        # elif db_name == "sqlserver":
        #     return SQLServerClient(config)
        # elif db_name == "oracle":
        #     return OracleClient(config)
        else:
            raise ValueError(f"Unsupported database type: {db_name}")
