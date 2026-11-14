@echo off
set PGPASSWORD=smartmonitoring_test
"C:\SmartDeveloper\software\pgsql\bin\psql.exe" -U smartmonitoring_test -d smartmonitoring_test -f "C:\Users\hanae\.gemini\antigravity\brain\d68e89cd-0087-4768-afac-327a35f75daf\create_tbl_measurement_flag.sql"
