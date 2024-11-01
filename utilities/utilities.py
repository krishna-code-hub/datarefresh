import yaml
import asyncio
import logging
import os
import jinja2
from FPE import FPE

# Load PII Manifest
def load_pii_manifest(file_path):
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        logging.error(f"PII manifest file not found: {str(e)}")
        raise

# Replace Jinja parameters in the PII Manifest
def replace_jinja_parameters(manifest, extraction_config):
    try:
        manifest_str = yaml.dump(manifest)
        template = jinja2.Template(manifest_str)
        rendered_manifest_str = template.render(**extraction_config)
        return yaml.safe_load(rendered_manifest_str)
    except Exception as e:
        logging.error(f"Failed to replace Jinja parameters in PII manifest: {str(e)}")
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

async def fpe_encrypt_async(value, mode, format_type, tweak_length, key_env_var):
    await asyncio.sleep(0)  # Simulate async behavior
    # Fetch key from environment variable
    key = os.getenv(key_env_var, 'default_key')
    tweak = os.urandom(tweak_length)
    cipher = FPE.New(key, tweak, getattr(FPE.Mode, mode))
    return cipher.encrypt(value, getattr(FPE.Format, format_type))