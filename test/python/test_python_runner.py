import pathlib
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest


RUNNER = pathlib.Path(__file__).resolve().parents[2] / "src/java/de/smartdata/Remote/python_runner.py"


class PythonRunnerTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.workdir = pathlib.Path(directory.name)

    def command(self, source_size, timeout_ms):
        return [sys.executable, "-I", str(RUNNER), str(self.workdir), "daily_v1.py",
                str(source_size), str(timeout_ms)]

    def test_script_consumes_only_its_json_input(self):
        source = b"import sys\nsys.stdout.buffer.write(sys.stdin.buffer.read())\n"
        payload = '{"input":["°C",12]}'.encode()
        result = subprocess.run(self.command(len(source), 5000), input=source + payload,
                                capture_output=True, timeout=8, check=True)
        self.assertEqual(result.stdout, payload)
        self.assertEqual(list(self.workdir.iterdir()), [])

    def test_deadline_includes_incomplete_upload(self):
        with subprocess.Popen(self.command(100, 200), stdin=subprocess.PIPE, stderr=subprocess.PIPE) as run:
            self.assertEqual(run.wait(timeout=3), 124)
            self.assertIn(b"SMARTDATA_SCRIPT_TIMEOUT", run.stderr.read())
        self.assertEqual(list(self.workdir.iterdir()), [])

    def test_timeout_and_disconnect_stop_the_process_group(self):
        # Both processes inherit stdout. EOF proves that the grandchild was stopped too.
        source = (b"import subprocess, sys, time\n"
                  b"subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(20)'])\n"
                  b"print('ready', flush=True)\ntime.sleep(20)\n")
        for disconnect in (False, True):
            with self.subTest(disconnect=disconnect):
                timeout_ms = 10000 if disconnect else 1200
                with subprocess.Popen(self.command(len(source), timeout_ms), stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE) as run:
                    run.stdin.write(source)
                    run.stdin.flush()
                    with selectors.DefaultSelector() as selector:
                        selector.register(run.stdout, selectors.EVENT_READ)
                        self.assertTrue(selector.select(timeout=5))
                    self.assertEqual(run.stdout.readline(), b"ready\n")
                    if disconnect:
                        run.send_signal(signal.SIGHUP)
                    _, err = run.communicate(timeout=4)
                    if disconnect:
                        self.assertNotEqual(run.returncode, 0)
                    else:
                        self.assertEqual(run.returncode, 124)
                        self.assertIn(b"SMARTDATA_SCRIPT_TIMEOUT", err)
                self.assertEqual(list(self.workdir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
