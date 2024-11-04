import asyncio
import logging
from masking.masking_factory import MaskingFactory
import os
from typing import List, Dict, Any


async def apply_masking(data: List[Dict[str, Any]], columns: List[Dict[str, Any]], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    masked_data = []

    # Extract algorithm-related metadata once for the entire batch
    masking_type = metadata["fpe"].get("masking_type", "ff3").lower()
    tweak = metadata["fpe"].get("tweak", "CBD09280979564")
    key_env_var = metadata["fpe"].get("key_env_var", "FPE_KEY")
    key = os.getenv(key_env_var, "2DE79D232DF5585D68CE47882AE256D6")

    logging.debug(f"Masking Type: {masking_type}, Tweak: {tweak}, Key Environment Variable: {key_env_var}")

    masking_tasks = []

    # Prepare masking tasks for each row and each column
    for row_index, row in enumerate(data):
        logging.debug(f"Processing row {row_index + 1}/{len(data)}")
        for column in columns:
            value = row[column["column_name"]]
            if column.get("masking_algorithm"):
                # Apply custom masking algorithm based on configuration
                format_type = column["masking_algorithm"].get("format", "DIGITS").upper()
                logging.debug(f"Applying masking for column: {column['column_name']}, Format Type: {format_type}")
                masking_tasks.append(
                    fpe_encrypt_async(
                        value, masking_type, format_type, tweak, key, row, column["column_name"]
                    )
                )
            else:
                # Apply standard masking (e.g., SHA2)
                logging.debug(f"Applying standard masking for column: {column['column_name']}")
                row[column["column_name"]] = "standard_masked_value"
        masked_data.append(row)

    # Run all masking tasks concurrently
    logging.debug(f"Running {len(masking_tasks)} masking tasks concurrently")
    await asyncio.gather(*masking_tasks)
    return masked_data


async def fpe_encrypt_async(value: str, masking_type: str, format_type: str, tweak: str, key: str, masked_row: Dict[str, Any], column_name: str) -> None:
    await asyncio.sleep(0)  # Simulate async behavior
    masking_instance = MaskingFactory.get_masking_algorithm(
        algorithm_type=masking_type,
        key=key,
        tweak=tweak,
        format_type=format_type,
        alphabet="STRING",
    )

    logging.debug(f"Encrypting value for column: {column_name}")
    # Encrypt the value and update the masked row
    masked_value = await masking_instance.encrypt(value)
    masked_row[column_name] = masked_value
    logging.debug(f"Masked value for column {column_name}: {masked_value}")
