import asyncio
import logging
import os
import traceback

import yaml
from typing import Any, Dict, List, Tuple
from db.db_factory import DBFactory
from utilities.utilities import (
    load_pii_manifest,
    replace_jinja_parameters,
)
from masking.masking_utils import apply_masking
from masking.masking_factory import MaskingFactory

# Set up base directory and logging
base_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(base_dir, "..", "logs")
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "masking.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load configurations
config_path: str = os.path.join(base_dir, "..", "config", "db_config.yaml")
extraction_config_path: str = os.path.join(base_dir, "..", "config", "extraction_config.yaml")
pii_manifest_path: str = os.path.join(base_dir, "..", "config", "pii_manifest.yaml")

BATCH_SIZE = 10  # Define batch size for processing


async def load_config(file_path: str) -> Dict[str, Any]:
    try:
        with open(file_path) as file:
            return yaml.safe_load(file)
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {file_path}, Error: {str(e)}")
        raise


# Load configurations concurrently
async def load_all_configs() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    db_config, extraction_config = await asyncio.gather(
        load_config(config_path), load_config(extraction_config_path)
    )
    return db_config, extraction_config


def process_pii_manifest(
    pii_manifest: Dict[str, Any], extraction_config: Dict[str, Any]
) -> Dict[str, Any]:
    pii_manifest = replace_jinja_parameters(pii_manifest, extraction_config)
    return pii_manifest


async def fetch_batch(
    db_client: Any, table: Dict[str, Any], schema: str, offset: int, limit: int
) -> List[Dict[str, Any]]:
    #query = f"SELECT * FROM {schema}.{table['table_name']} LIMIT {limit} OFFSET {offset}"
    query = f"SELECT * FROM {table['table_name']} LIMIT {limit} OFFSET {offset}"
    return await db_client.execute_query(query, BATCH_SIZE)


async def process_batch(
    db_client: Any, table: Dict[str, Any], batch: List[Dict[str, Any]], pii_metadata: Dict[str, Any]
) -> None:
    primary_key: List[str] = table.get("primary_key", [])
    logging.debug(f"{table['table_name']} - primary key - {', '.join(primary_key)}")
    pii_columns = [col["column_name"] for col in table["columns"] if col.get("pii") == "Y"]

    # Apply masking to the entire batch
    masked_batch = await apply_masking(batch, table["columns"], pii_metadata)

    # Decide upfront whether to insert or update based on primary key
    if any(pk in pii_columns for pk in primary_key):
        await db_client.bulk_insert(table["schema"], table["table_name"], masked_batch, primary_key)
    else:
        await db_client.bulk_update(table["schema"], table["table_name"], masked_batch, primary_key)


async def process_table_in_batches(
    db_client: Any, table: Dict[str, Any], pii_metadata: Dict[str, Any], schema: str
) -> None:
    offset = 0
    while True:
        batch = await fetch_batch(db_client, table, schema, offset, BATCH_SIZE)
        if not batch:
            break  # No more records to process

        await process_batch(db_client, table, batch, pii_metadata)
        offset += BATCH_SIZE  # Move to the next batch


async def process_table(
    table: Dict[str, Any],
    db_config: Dict[str, Any],
    pii_manifest: Dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        mode: str = table.get("strategy", pii_manifest["metadata"].get("strategy", "upsert"))
        target_db: str = pii_manifest["metadata"]["target_db"]
        source_db: str = pii_manifest["metadata"].get("source_db")

        pii_columns: List[Dict[str, Any]] = [
            col for col in table["columns"] if col.get("pii") == "Y"
        ]
        primary_key: List[str] = table.get("primary_key")

        if mode == "upsert":
            db_client = DBFactory.get_database_client(db_config, "target", target_db)
            if "extraction_logic" in table:
                db_client.delete_unwanted_data(table)
            logging.debug(f"going to process for {table['table_name']}")
            await process_table_in_batches(
                db_client, table, pii_manifest["metadata"], table["schema"]
            )
        elif mode == "extract_mask_load":
            source_db_client = DBFactory.get_database_client(db_config, "source", source_db)
            target_db_client = DBFactory.get_database_client(db_config, "target", target_db)
            await process_table_in_batches(
                source_db_client, table, pii_manifest["metadata"], table["schema"]
            )
            # In extract_mask_load, batch is fetched from source and inserted/updated in target
            async for batch in source_db_client.extract_data_batch_by_batch(
                table, table["schema"], BATCH_SIZE
            ):
                masked_batch = []
                for row in batch:
                    masked_row = await apply_masking(
                        row, table["columns"], pii_manifest["metadata"]
                    )
                    masked_batch.append(masked_row)
                await target_db_client.bulk_update_or_insert(
                    table["schema"], table["table_name"], masked_batch
                )
        else:
            raise ValueError(f"Unsupported mode: {mode}")


async def main() -> None:
    try:
        db_config, extraction_config = await load_all_configs()
        pii_manifest = load_pii_manifest(pii_manifest_path)
        pii_manifest = process_pii_manifest(pii_manifest, extraction_config)

        # Concurrency control with semaphore
        concurrency_limit: int = db_config.get("concurrency_limit", 3)
        semaphore = asyncio.Semaphore(concurrency_limit)

        tasks = [
            process_table(table, db_config, pii_manifest, semaphore)
            for table in pii_manifest["tables"]
        ]

        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Error in masking process: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    asyncio.run(main())
