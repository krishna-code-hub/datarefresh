# PII Masking Framework

## Overview
This project provides a data masking solution for PII data from multiple databases. It extracts data, masks PII columns as per a provided YAML configuration, and writes masked data back to the database.

## Requirements
- Python 3.9+
- cx_Oracle (for Oracle support)
- pyodbc (for SQL Server support)
- psycopg2 (for Postgres support)
- YAML configuration for PII masking (`pii_manifest.yaml`)
- FPE library for FF1, FF3-1, and multiple formats (DIGITS, CREDITCARD, LETTERS, STRING, EMAIL, CPR)

## How to Run
1. Set up the `db_config.yaml` with appropriate database connection details.
2. Set up `pii_manifest.yaml` with your table configuration.
3. Set the encryption key in your environment (e.g., export `FPE_KEY=your_key`).
4. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
5. Run the main program:
    ```bash
    python main/ff1_encryption_async.py
    ```