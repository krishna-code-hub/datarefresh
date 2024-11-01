CREATE TABLE audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    operation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);