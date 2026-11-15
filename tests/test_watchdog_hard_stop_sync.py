"""Audit M2: the watchdog's "never auto-restart" list must be complete.

``scripts/archive_watchdog.ps1`` carried a hand-written list of six stop
reasons. Several reasons that require a human were missing (for example
``ltfs_mount_unverifiable`` — "cannot verify tape state"), and the recorded
process exit code was ignored altogether, so a run that stopped for one of the
missing reasons was relaunched ten minutes later.

Three layers:

1. the Python source of truth (``src/exit_codes.py``) is itself complete —
   checked against the ``StopResult(...)`` calls actually present in ``src/``;
2. the PowerShell copy is identical to it (a sync test, because the artefact
   is a script);
3. the PowerShell decision functions are EXECUTED on sample records. Only the
   self-contained policy block is run — it reads no process, database, file or
   device; the watchdog's main body is never executed.
"""
import ast
import base64
import json
import os
import re
import shutil
import subprocess
import unittest

from src import exit_codes
from src.exit_codes import ExitCode

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WATCHDOG = os.path.join(PROJECT_ROOT, "scripts", "archive_watchdog.ps1")
SRC_DIR = os.path.join(PROJECT_ROOT, "src")


def _watchdog_text():
    with open(WATCHDOG, encoding="utf-8-sig") as fh:
        return fh.read()


def _policy_block():
    match = re.search(r"# >>> hard-stop policy.*?# <<< hard-stop policy",
                      _watchdog_text(), re.S)
    assert match, "hard-stop policy block markers not found in the watchdog"
    return match.group(0)


def _ps_array(name):
    match = re.search(r"\$%s\s*=\s*@\((.*?)\)" % re.escape(name),
                      _policy_block(), re.S)
    assert match, f"${name} not found in the watchdog policy block"
    body = "\n".join(line.split("#", 1)[0]
                     for line in match.group(1).splitlines())
    return [item.strip().strip("'\"") for item in body.split(",")
            if item.strip()]


def _emitted_reasons_by_exit_code():
    """(exit-code name, reason value) for every StopResult(...) in src/."""
    reason_values = {name: value for name, value in vars(exit_codes).items()
                     if name.startswith("REASON_")}
    emitted, unresolved = set(), []
    for root, _dirs, files in os.walk(SRC_DIR):
        for file_name in files:
            if not file_name.endswith(".py"):
                continue
            path = os.path.join(root, file_name)
            with open(path, encoding="utf-8") as fh:
                tree = ast.parse(fh.read(), path)
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Call)
                        and getattr(node.func, "id", None) == "StopResult"):
                    continue
                kwargs = {k.arg: k.value for k in node.keywords}
                code = kwargs.get("exit_code")
                reason = kwargs.get("reason")
                if not (isinstance(code, ast.Attribute)
                        and getattr(code.value, "id", None) == "ExitCode"):
                    continue
                candidates = []
                if (isinstance(reason, ast.Constant)
                        and isinstance(reason.value, str)):
                    candidates.append(reason.value)      # a literal slug
                for sub in ast.walk(reason) if reason is not None else ():
                    if isinstance(sub, ast.Name) and sub.id in reason_values:
                        candidates.append(reason_values[sub.id])
                if not candidates:
                    unresolved.append((code.attr, file_name, node.lineno))
                for value in candidates:
                    emitted.add((code.attr, value))
    return emitted, unresolved


class PythonSourceOfTruthIsComplete(unittest.TestCase):
    def test_every_reason_constant_is_classified(self):
        all_reasons = {value for name, value in vars(exit_codes).items()
                       if name.startswith("REASON_")}
        classified = (exit_codes.NO_AUTO_RESTART_REASONS
                      | exit_codes.AUTO_RESTARTABLE_REASONS
                      | {exit_codes.REASON_COMPLETED})
        self.assertEqual(
            all_reasons - classified, set(),
            "new stop reason(s) not classified for the watchdog - add each to "
            "NO_AUTO_RESTART_REASONS or AUTO_RESTARTABLE_REASONS")
        self.assertEqual(exit_codes.NO_AUTO_RESTART_REASONS
                         & exit_codes.AUTO_RESTARTABLE_REASONS, frozenset())

    def test_every_exit_code_is_classified(self):
        classified = (exit_codes.NO_AUTO_RESTART_EXIT_CODES
                      | exit_codes.AUTO_RESTARTABLE_EXIT_CODES
                      | {ExitCode.COMPLETED})
        self.assertEqual(set(ExitCode) - classified, set())

    def test_reasons_emitted_with_a_hard_exit_code_are_all_hard(self):
        hard_codes = {c.name for c in exit_codes.NO_AUTO_RESTART_EXIT_CODES}
        emitted, unresolved = _emitted_reasons_by_exit_code()
        self.assertTrue(emitted, "found no StopResult(...) calls in src/")
        missing = sorted(reason for code, reason in emitted
                         if code in hard_codes
                         and reason not in exit_codes.NO_AUTO_RESTART_REASONS)
        self.assertEqual(missing, [],
                         "emitted with a hard exit code but restartable by "
                         "reason")
        # A hard stop whose reason the scan could not resolve would escape
        # this test; none may exist.
        self.assertEqual([u for u in unresolved if u[0] in hard_codes], [])

    def test_the_audited_gap_is_closed(self):
        # "cannot verify tape state" and friends were not in the old list.
        for reason in ("ltfs_mount_unverifiable", "ambiguous_active_sessions",
                       "scan_frontier_unavailable",
                       "sealed_batch_feature_unavailable",
                       "user_requested_stop", "bad_config"):
            self.assertIn(reason, exit_codes.NO_AUTO_RESTART_REASONS)


class PowerShellCopyIsInSync(unittest.TestCase):
    def test_hard_stop_reasons_match_python(self):
        ps = _ps_array("HardStopReasons")
        self.assertEqual(len(ps), len(set(ps)), "duplicate reason in the PS1")
        self.assertEqual(set(ps), set(exit_codes.NO_AUTO_RESTART_REASONS))

    def test_hard_stop_exit_codes_match_python(self):
        self.assertEqual(
            {int(v) for v in _ps_array("HardStopExitCodes")},
            {int(c) for c in exit_codes.NO_AUTO_RESTART_EXIT_CODES})

    def test_restartable_exit_codes_match_python(self):
        self.assertEqual(
            {int(v) for v in _ps_array("RestartableExitCodes")},
            {int(c) for c in exit_codes.AUTO_RESTARTABLE_EXIT_CODES})

    def test_the_main_body_uses_the_policy_functions(self):
        body = _watchdog_text().split("# <<< hard-stop policy", 1)[1]
        self.assertIn("Get-RecordedFailureRefusal $failure", body)
        self.assertIn("Get-TaskResultRefusal $taskInfo.LastTaskResult", body)
        self.assertNotIn("$hardReasons = @(", body)


@unittest.skipUnless(os.name == "nt" and shutil.which("powershell.exe"),
                     "needs Windows PowerShell")
class PowerShellDecisionBehaviour(unittest.TestCase):
    """Run the policy block (and nothing else) on sample failure records."""

    @classmethod
    def setUpClass(cls):
        cases = {
            # name: (last_failure.json record or None, expect_refusal)
            "transient_network": (
                {"reason": "network_retry_exhausted", "exit_code": 10}, False),
            "transient_reboot": (
                {"reason": "sccm_reboot_pending", "exit_code": 10}, False),
            "mount_unverifiable": (
                {"reason": "ltfs_mount_unverifiable", "exit_code": 20}, True),
            "user_stop": (
                {"reason": "user_requested_stop", "exit_code": 40}, True),
            "tape_write_failed": (
                {"reason": "tape_write_failed", "exit_code": 10}, True),
            "unknown_reason_hard_code": (
                {"reason": "some_future_reason", "exit_code": 20}, True),
            "unknown_reason_config_code": (
                {"reason": "some_future_reason", "exit_code": 30}, True),
            "missing_exit_code": (
                {"reason": "network_retry_exhausted"}, True),
            "garbage_exit_code": (
                {"reason": "network_retry_exhausted", "exit_code": "x"}, True),
            "unknown_exit_code": (
                {"reason": "network_retry_exhausted", "exit_code": 77}, True),
            "hard_reason_wrong_code": (
                {"reason": "ambiguous_backing_chunk", "exit_code": 10}, True),
        }
        task_cases = {"task_ok": (0, False), "task_transient": (10, False),
                      "task_running": (267009, False),
                      "task_safety_block": (20, True),
                      "task_config": (30, True), "task_user_stop": (40, True),
                      "task_unreadable": (None, True)}
        lines = [_policy_block(), "$out = @{}"]
        for name, (record, _expect) in cases.items():
            payload = json.dumps(record).replace("'", "''")
            lines.append(
                f"$out['{name}'] = [string](Get-RecordedFailureRefusal "
                f"('{payload}' | ConvertFrom-Json))")
        for name, (value, _expect) in task_cases.items():
            arg = "$null" if value is None else str(value)
            lines.append(
                f"$out['{name}'] = [string](Get-TaskResultRefusal {arg})")
        lines.append("$out | ConvertTo-Json -Compress")
        # -EncodedCommand: no temp script file and no execution-policy
        # dependency; the text is exactly the policy block plus the calls.
        encoded = base64.b64encode(
            "\n".join(lines).encode("utf-16-le")).decode("ascii")
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive",
             "-EncodedCommand", encoded],
            capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0 and proc.stdout.strip(), (
            proc.stdout, proc.stderr)
        cls.results = json.loads(proc.stdout.strip().splitlines()[-1])
        cls.expected = {name: expect
                        for name, (_r, expect) in {**cases,
                                                   **task_cases}.items()}

    def test_every_case_decides_as_expected(self):
        for name, expect_refusal in self.expected.items():
            with self.subTest(case=name):
                refusal = self.results.get(name) or ""
                self.assertEqual(bool(refusal), expect_refusal, refusal)

    def test_hard_stop_refusal_names_the_reason(self):
        self.assertIn("ltfs_mount_unverifiable",
                      self.results["mount_unverifiable"])


if __name__ == "__main__":
    unittest.main()
