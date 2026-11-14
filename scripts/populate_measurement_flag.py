# populate_measurement_flag.py
import subprocess
import sys
import os

# ----------------------------------------------------------------------
# Automatically locate psql.exe
# Checks:
# 1. Project local bundled PostgreSQL (C:\SmartDeveloper\software\pgsql\bin\psql.exe)
# 2. PATH
# 3. Standard Program Files installations
# Works seamlessly on Python 2.7 and Python 3.
# ----------------------------------------------------------------------

def find_psql():
    # 1. Check local SmartDeveloper bundled pgsql
    smart_dev_pgsql = r"C:\SmartDeveloper\software\pgsql\bin\psql.exe"
    if os.path.isfile(smart_dev_pgsql):
        return smart_dev_pgsql

    # 2. Check relative to this project (../../software/pgsql/bin/psql.exe)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
    rel_pgsql = os.path.join(project_root, "software", "pgsql", "bin", "psql.exe")
    if os.path.isfile(rel_pgsql):
        return rel_pgsql

    # 3. Check PATH
    paths = os.environ.get("PATH", "").split(os.pathsep)
    for p in paths:
        candidate = os.path.join(p, "psql.exe")
        if os.path.isfile(candidate):
            return candidate

    # 4. Check standard Program Files
    for base in [os.environ.get("ProgramFiles", r"C:\Program Files"), os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")]:
        if os.path.isdir(base):
            try:
                for entry in os.listdir(base):
                    if entry.lower().startswith("postgresql"):
                        candidate = os.path.join(base, entry, "bin", "psql.exe")
                        if os.path.isfile(candidate):
                            return candidate
            except Exception:
                pass
    return None

psql_exe = find_psql()
if not psql_exe:
    sys.stderr.write("Error: psql executable not found.\n")
    sys.exit(1)

# Ensure PGPASSWORD is set so psql won't prompt interactively
env = os.environ.copy()
if "PGPASSWORD" not in env:
    env["PGPASSWORD"] = "smartmonitoring_test"

# ----------------------------------------------------------------------
# Sync sequence so nextval won't collide, then insert demo rows
# ----------------------------------------------------------------------
SQL = """
SELECT setval(pg_get_serial_sequence('smartmonitoring.tbl_measurement_flag', 'id'), COALESCE(max(id), 0) + 1, false) FROM smartmonitoring.tbl_measurement_flag;
INSERT INTO smartmonitoring.tbl_measurement_flag (measurement_process_id, file_path, created_at)
VALUES
  ('demo1', '/tmp/demo1.csv', now()),
  ('demo2', '/tmp/demo2.csv', now());
"""

cmd = [
    psql_exe,
    "-h", "localhost",
    "-p", "5432",
    "-U", "smartmonitoring_test",
    "-d", "smartmonitoring_test",
    "-c", SQL
]

try:
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        sys.stderr.write("Command failed (exit {}):\n{}\n".format(proc.returncode, stderr))
        sys.exit(proc.returncode)
    else:
        print("Success! Demo rows inserted into tbl_measurement_flag.")
        if stdout:
            print(stdout)
except Exception as e:
    sys.stderr.write("Execution error: {}\n".format(e))
    sys.exit(1)
