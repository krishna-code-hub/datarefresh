CREATE TABLE masking_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    rows_processed INT,
    error_message TEXT
);
