# create_tbl_measurement_flag.sql

-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS smartmonitoring;

-- Create the table expected by the Python script
CREATE TABLE IF NOT EXISTS smartmonitoring.tbl_measurement_flag (
    id SERIAL PRIMARY KEY,
    measurement_process_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Optional: grant permissions to the test user (already owns the DB)
GRANT ALL ON smartmonitoring.tbl_measurement_flag TO smartmonitoring_test;
