from db_clients import OracleClient, SQLServerClient, PostgresClient


class DBFactory:
    @staticmethod
    def get_database_client(db_config):
        if 'oracle' in db_config:
            return OracleClient(db_config['oracle'])
        elif 'sqlserver' in db_config:
            return SQLServerClient(db_config['sqlserver'])
        elif 'postgres' in db_config:
            return PostgresClient(db_config['postgres'])
        else:
            raise ValueError("Unsupported database type")