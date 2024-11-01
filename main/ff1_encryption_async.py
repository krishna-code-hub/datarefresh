import asyncio
import logging
from db_factory import DBFactory
from utilities.utilities import load_pii_manifest, apply_masking
import yaml
import os

# Set up logging
logging.basicConfig(filename='../logs/masking.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load config
try:
    with open('../config/db_config.yaml') as db_file:
        db_config = yaml.safe_load(db_file)
except FileNotFoundError as e:
    logging.error(f"Database configuration file not found: {str(e)}")
    raise


async def main():
    try:
        # Load pii manifest
        pii_manifest = load_pii_manifest('../config/pii_manifest.yaml')

        # Loop over each table and mask the data
        for table in pii_manifest['tables']:
            # Create database client
            db_client = DBFactory.get_database_client(db_config)
            data = db_client.extract_data(table)

            # Apply masking to the data
            masked_data = await apply_masking(data, table['columns'], pii_manifest['metadata'])

            # Write back to the database
            db_client.load_data(table, masked_data)

    except Exception as e:
        logging.error(f"Error in masking process: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())