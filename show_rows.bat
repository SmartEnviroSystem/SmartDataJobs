@echo off
chcp 65001 > nul
set PGCLIENTENCODING=UTF8
set PGPASSWORD=smartmonitoring_test
"C:\SmartDeveloper\software\pgsql\bin\psql.exe" -U smartmonitoring_test -d smartmonitoring_test -c "SELECT * FROM smartmonitoring.tbl_measurement_flag;"
