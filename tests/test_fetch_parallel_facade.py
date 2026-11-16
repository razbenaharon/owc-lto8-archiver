"""The orchestrator facade must forward the stager's own call signature.

The stager calls ``host._fetch_batches_parallel(work_items, fetch_dir,
fetch_abort, session_id, streams)``. The facade used to accept only four
arguments, so ``fetch_parallel_streams > 1`` raised ``TypeError`` on the first
parallel fetch.
"""
import unittest
from unittest import mock

from src.remote_orchestrator import RemoteOrchestrator


class FetchParallelFacadeTests(unittest.TestCase):
    def test_facade_forwards_session_id_and_streams(self):
        host = mock.Mock()
        stager = host._stager.return_value
        stager._fetch_batches_parallel.return_value = True

        result = RemoteOrchestrator._fetch_batches_parallel(
            host, ["item"], "fetch-dir", "abort", 37, 3)

        self.assertIs(result, True)
        stager._fetch_batches_parallel.assert_called_once_with(
            ["item"], "fetch-dir", "abort", 37, 3)


if __name__ == "__main__":
    unittest.main()
