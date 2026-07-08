from __future__ import annotations

import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from training_jobs import process


class ProcessRunnerTest(unittest.TestCase):
    def test_run_child_returns_zero_for_successful_command(self) -> None:
        self.assertEqual(process.run_child([sys.executable, "-c", "print('ok')"]), 0)

    def test_run_child_returns_non_zero_exit_status(self) -> None:
        self.assertEqual(process.run_child([sys.executable, "-c", "raise SystemExit(7)"]), 7)

    def test_sigterm_is_forwarded_to_child_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "terminated"
            script = Path(tmp) / "child.py"
            script.write_text(
                textwrap.dedent(
                    f"""
                    import signal
                    import time
                    from pathlib import Path

                    marker = Path({str(marker)!r})

                    def handle(_signum, _frame):
                        marker.write_text("terminated", encoding="utf-8")
                        raise SystemExit(0)

                    signal.signal(signal.SIGTERM, handle)
                    while True:
                        time.sleep(0.1)
                    """
                ),
                encoding="utf-8",
            )
            child = subprocess.Popen([sys.executable, str(script)], start_new_session=True)
            time.sleep(0.5)
            process._terminate_process_group(child, signal.SIGTERM)
            child.wait(timeout=5)

            self.assertTrue(marker.exists())


if __name__ == "__main__":
    unittest.main()
