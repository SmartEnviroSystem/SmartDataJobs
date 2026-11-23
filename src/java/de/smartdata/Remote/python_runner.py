"""Run one SmartData job on the POSIX Python host within the remaining deadline."""
import os
import signal
import subprocess
import sys
import tempfile
import time


def expire(signum, frame):
    raise TimeoutError("Python script timeout")


def run(workdir, filename, source_size, timeout):
    deadline = time.monotonic() + timeout
    child = None
    disconnected = False

    def kill_group():
        if child is not None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def disconnect(signum, frame):
        nonlocal disconnected
        disconnected = True
        kill_group()

    signal.signal(signal.SIGHUP, disconnect)
    signal.signal(signal.SIGTERM, disconnect)
    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    os.makedirs(workdir, mode=0o700, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=workdir) as directory:
        script = os.path.join(directory, filename)
        with open(script, "xb") as output:
            # Leave the remaining bytes (the JSON input) on stdin for the script.
            while source_size:
                chunk = os.read(0, min(source_size, 65536))
                if not chunk:
                    raise EOFError("Incomplete script transfer")
                output.write(chunk)
                source_size -= len(chunk)

        # wait() owns the deadline from here. An alarm during Popen could orphan the child.
        signal.setitimer(signal.ITIMER_REAL, 0)
        if time.monotonic() >= deadline:
            raise TimeoutError("Python script timeout")
        if disconnected:
            raise ConnectionAbortedError("SSH session closed")
        child = subprocess.Popen([sys.executable, "-I", "-u", script], start_new_session=True)
        try:
            if disconnected:
                kill_group()
            return child.wait(timeout=max(0, deadline - time.monotonic()))
        finally:
            kill_group()
            child.wait()


if __name__ == "__main__":
    try:
        status = run(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]) / 1000)
    except (TimeoutError, subprocess.TimeoutExpired):
        print("\nSMARTDATA_SCRIPT_TIMEOUT", file=sys.stderr)
        sys.exit(124)
    sys.exit(128 - status if status < 0 else status)
