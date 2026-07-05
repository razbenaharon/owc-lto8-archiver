import urllib.parse
import unittest

from src.telegram_notify import (
    TelegramNotifier,
    format_backup_summary,
    format_storage_fetch_summary,
)


class _Response:
    def __init__(self, body=b'{"ok": true}'):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.body


class TelegramNotifierTests(unittest.TestCase):
    def test_disabled_notifier_does_not_call_urlopen(self):
        calls = []
        notifier = TelegramNotifier(
            enabled=False,
            bot_token="123:secret",
            chat_id="42",
            urlopen=lambda *a, **k: calls.append((a, k)))

        self.assertFalse(notifier.send("hello"))
        self.assertEqual(calls, [])

    def test_missing_credentials_warns_without_sending(self):
        warnings = []
        calls = []
        notifier = TelegramNotifier(
            enabled=True,
            bot_token="",
            chat_id="",
            urlopen=lambda *a, **k: calls.append((a, k)),
            warn=warnings.append,
        )

        self.assertFalse(notifier.send("hello"))
        self.assertEqual(calls, [])
        self.assertIn("TELEGRAM_BOT_TOKEN", warnings[0])

    def test_send_message_posts_expected_payload(self):
        calls = []

        def fake_urlopen(request, timeout):
            calls.append((request, timeout))
            return _Response()

        notifier = TelegramNotifier(
            enabled=True,
            bot_token="123:secret",
            chat_id="42",
            timeout_seconds=7,
            urlopen=fake_urlopen,
        )

        self.assertTrue(notifier.send("hello"))
        request, timeout = calls[0]
        self.assertEqual(timeout, 7)
        self.assertIn("/bot123:secret/sendMessage", request.full_url)
        payload = urllib.parse.parse_qs(request.data.decode("utf-8"))
        self.assertEqual(payload["chat_id"], ["42"])
        self.assertEqual(payload["text"], ["hello"])

    def test_send_failure_redacts_token(self):
        warnings = []

        def fake_urlopen(request, timeout):
            raise OSError("failed at https://api.telegram.org/bot123:secret/sendMessage")

        notifier = TelegramNotifier(
            enabled=True,
            bot_token="123:secret",
            chat_id="42",
            urlopen=fake_urlopen,
            warn=warnings.append,
        )

        self.assertFalse(notifier.send("hello"))
        self.assertNotIn("123:secret", warnings[0])
        self.assertIn("<redacted>", warnings[0])


class TelegramMessageFormatTests(unittest.TestCase):
    def test_backup_summary_omits_paths_and_file_names(self):
        text = format_backup_summary({
            "status": "completed_with_skips",
            "source_host": "so01",
            "source": r"C:\secret\project\file_a.dat",
            "skipped_files_report": r"C:\secret\backup_logs\skipped.csv",
            "tape_label": "Tape_02",
            "backup_mode": "staged/packed",
            "local_chunk_index": 0,
            "copied_bytes": 1024**3,
            "skipped": 2,
            "total_time_seconds": 120,
            "record_counts": {"files_inserted": 3, "files_skipped": 4},
            "rc_sum": {
                "files_copied": 5,
                "files_skipped": 6,
                "files_failed": 0,
                "speed_mbs": 123.4,
            },
        })

        self.assertIn("Tape_02", text)
        self.assertIn("Copied: 1.00 GiB", text)
        self.assertNotIn("file_a.dat", text)
        self.assertNotIn("skipped.csv", text)
        self.assertNotIn(r"C:\secret", text)

    def test_storage_fetch_summary_is_aggregate_only(self):
        text = format_storage_fetch_summary(
            ["so01"],
            ["so02(PENDING)"],
            ["so03"],
            {"dashboard": 0},
        )

        self.assertIn("Fetched: so01", text)
        self.assertIn("Skipped: so02(PENDING)", text)
        self.assertIn("Render: dashboard=ok", text)


if __name__ == "__main__":
    unittest.main()
