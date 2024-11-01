import yaml
import asyncio
import logging
import os
from FPE import FPE


# Load PII Manifest
def load_pii_manifest(file_path):
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        logging.error(f"PII manifest file not found: {str(e)}")
        raise


# Masking Logic using FF1 or FF3-1 FPE
async def apply_masking(data, columns, metadata):
    masked_data = []
    for row in data:
        masked_row = {}
        for column in columns:
            value = row[column['column_name']]
            if column.get('pii') == 'Y':
                if column.get('masking_algorithm'):
                    # Apply custom masking algorithm based on configuration
                    mode = metadata['fpe'].get('mode', 'FF1').upper()
                    tweak_length = metadata['fpe'].get('tweak_length', 8)
                    key_env_var = metadata['fpe'].get('key_env_var', 'FPE_KEY')
                    format_type = column['masking_algorithm'].get('format', 'DIGITS').upper()
                    masked_value = await fpe_encrypt_async(value, mode, format_type, tweak_length, key_env_var)
                else:
                    # Apply standard masking (e.g., SHA2)
                    masked_value = "standard_masked_value"
            else:
                masked_value = value
            masked_row[column['column_name']] = masked_value
        masked_data.append(masked_row)
    return masked_data


# Async function for FF1 or FF3-1 encryption with various formats
def fpe_encrypt_async(value, mode, format_type, tweak_length, key_env_var):
    try:
        tweak = FPE.generate_tweak(tweak_length)
        key = os.getenv(key_env_var)
        if not key:
            raise ValueError(f"Environment variable {key_env_var} for FPE key not found")

        if mode == 'FF1':
            cipher = FPE.New(key, tweak, FPE.Mode.FF1)
        elif mode == 'FF3-1':
            cipher = FPE.New(key, tweak, FPE.Mode.FF3_1)
        else:
            raise ValueError(f"Unsupported FPE mode: {mode}")

        # Select appropriate format
        if format_type == 'DIGITS':
            format_enum = FPE.Format.DIGITS
        elif format_type == 'CREDITCARD':
            format_enum = FPE.Format.CREDITCARD
        elif format_type == 'LETTERS':
            format_enum = FPE.Format.LETTERS
        elif format_type == 'STRING':
            format_enum = FPE.Format.STRING
        elif format_type == 'EMAIL':
            format_enum = FPE.Format.EMAIL
        elif format_type == 'CPR':
            format_enum = FPE.Format.CPR
        else:
            raise ValueError(f"Unsupported FPE format: {format_type}")

        encrypted_value = cipher.encrypt(value, format_enum)
        return asyncio.sleep(0, result=encrypted_value)
    except Exception as e:
        logging.error(f"FPE encryption failed ({mode} with {format_type}): {str(e)}")
        raise