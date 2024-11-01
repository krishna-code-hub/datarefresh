import asyncio
import logging
from db_factory import DBFactory
from utilities.utilities import load_pii_manifest, apply_masking, replace_jinja_parameters
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

# Load extraction config
try:
    with open('../config/extraction.config') as extraction_file:
        extraction_config = yaml.safe_load(extraction_file)
except FileNotFoundError as e:
    logging.error(f"Extraction configuration file not found: {str(e)}")
    raise

async def main():
    try:
        # Load pii manifest and replace Jinja parameters
        pii_manifest = load_pii_manifest('../config/pii_manifest.yaml')
        pii_manifest = replace_jinja_parameters(pii_manifest, extraction_config)
        mode = pii_manifest['metadata'].get('mode', 'upsert')
        source_db = pii_manifest['metadata']['source_db']
        target_db = pii_manifest['metadata']['target_db']
        
        # Loop over each table and mask the data
        for table in pii_manifest['tables']:
            # Check if there are fields that require masking
            pii_columns = [col for col in table['columns'] if col.get('pii') == 'Y']
            db_client = None

            if mode == 'upsert':
                # Create database client for upsert mode (same DB)
                db_client = DBFactory.get_database_client(db_config, 'target', target_db)
                # Delete data that is not required (keep only data that matches extraction logic)
                if 'extraction_logic' in table:
                    db_client.delete_unwanted_data(table)
                
                if pii_columns:
                    # Extract and process each row
                    for row in db_client.extract_data_row_by_row(table):
                        # Apply masking to the row
                        masked_row = await apply_masking([row], table['columns'], pii_manifest['metadata'])
                        # Write back to the database
                        db_client.update_or_insert_row(table, masked_row[0])
                else:
                    logging.info(f"No PII columns to mask for table {table['table_name']}, only extraction performed.")
            elif mode == 'extract_mask_load':
                # Create source and target database clients for extract_mask_load mode
                source_db_client = DBFactory.get_database_client(db_config, 'source', source_db)
                target_db_client = DBFactory.get_database_client(db_config, 'target', target_db)
                
                if pii_columns:
                    # Extract and process each row with masking
                    for row in source_db_client.extract_data_row_by_row(table):
                        # Apply masking to the row
                        masked_row = await apply_masking([row], table['columns'], pii_manifest['metadata'])
                        # Write back to the target database
                        target_db_client.update_or_insert_row(table, masked_row[0])
                else:
                    # Extract and load without masking
                    for row in source_db_client.extract_data_row_by_row(table):
                        target_db_client.update_or_insert_row(table, row)
                    logging.info(f"No PII columns to mask for table {table['table_name']}, data loaded without masking.")
            else:
                raise ValueError(f"Unsupported mode: {mode}")
        
    except Exception as e:
        logging.error(f"Error in masking process: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())