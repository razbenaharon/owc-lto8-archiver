"""Best-effort Telegram notifications for operational summaries."""
import json
import urllib.error
import urllib.parse
import urllib.request


TELEGRAM_API_BASE = "https://api.telegram.org"
MAX_TELEGRAM_TEXT = 3900


def _clean_secret(value):
    value = (value or "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    return value


def _redact(value, secret):
    text = str(value)
    if secret:
        text = text.replace(secret, "<redacted>")
    return text


def _safe_float(value, default=0.0):
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _gib(value):
    return f"{_safe_float(value) / 1024**3:.2f} GiB"


def _minutes(value):
    seconds = _safe_float(value)
    return f"{seconds / 60:.1f} min" if seconds else "n/a"


def _clip(text):
    text = str(text or "")
    if len(text) <= MAX_TELEGRAM_TEXT:
        return text
    return text[:MAX_TELEGRAM_TEXT - 20].rstrip() + "\n...[truncated]"


class TelegramNotifier:
    """Small sendMessage client that never raises into operational workflows."""

    def __init__(self, enabled=False, bot_token="", chat_id="",
                 timeout_seconds=10, urlopen=None, warn=print):
        self.enabled = bool(enabled)
        self.bot_token = _clean_secret(bot_token)
        self.chat_id = _clean_secret(chat_id)
        self.timeout_seconds = max(1, int(_safe_float(timeout_seconds, 10)))
        self.urlopen = urlopen or urllib.request.urlopen
        self.warn = warn

    @classmethod
    def from_config(cls, cfg, warn=print):
        return cls(
            enabled=getattr(cfg, "telegram_enabled", False),
            bot_token=getattr(cfg, "telegram_bot_token", ""),
            chat_id=getattr(cfg, "telegram_chat_id", ""),
            timeout_seconds=getattr(cfg, "telegram_timeout_seconds", 10),
            warn=warn,
        )

    def send(self, text):
        if not self.enabled:
            return False
        if not self.bot_token or not self.chat_id:
            self._warning("enabled but TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")
            return False

        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
        payload = urllib.parse.urlencode({
            "chat_id": self.chat_id,
            "text": _clip(text),
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with self.urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8", errors="replace")
            data = json.loads(body or "{}")
            if data.get("ok") is False:
                self._warning(f"sendMessage failed: {data.get('description', 'unknown error')}")
                return False
            return True
        except (OSError, ValueError, urllib.error.URLError) as exc:
            self._warning(f"sendMessage failed: {_redact(exc, self.bot_token)}")
            return False

    def _warning(self, message):
        try:
            self.warn(f"[TELEGRAM] Warning: {_redact(message, self.bot_token)}")
        except Exception:
            pass


def send_best_effort(notifier, text):
    if notifier is None:
        return False
    try:
        return notifier.send(text)
    except Exception as exc:
        try:
            print(f"[TELEGRAM] Warning: notification failed: {exc}")
        except Exception:
            pass
        return False


def format_backup_summary(details, robocopy_result=None):
    """Build a path-free backup notification from aggregate run details."""
    details = details or {}
    rc_sum = details.get("rc_sum") or {}
    counts = details.get("record_counts") or {}
    status = details.get("status") or "unknown"
    source_host = details.get("source_host") or "local"
    tape_label = details.get("tape_label") or "unknown"
    mode = details.get("backup_mode") or "backup"
    chunk = details.get("local_chunk_index")
    chunk_text = f", chunk {_safe_int(chunk) + 1}" if chunk not in (None, "") else ""
    exit_code = "" if robocopy_result is None else robocopy_result.returncode
    inserted = sum(_safe_int(v) for k, v in counts.items() if k.endswith("_inserted"))
    updated = sum(_safe_int(v) for k, v in counts.items() if k.endswith("_updated"))
    skipped = sum(
        _safe_int(v) for k, v in counts.items()
        if k.endswith("_skipped") or k.endswith("_skipped_existing")
    )

    lines = [
        f"LTO backup {status}",
        f"Host: {source_host}",
        f"Tape: {tape_label}",
        f"Mode: {mode}{chunk_text}",
        f"Copied: {_gib(details.get('copied_bytes', 0))}",
        (
            "Files: copied {copied}, skipped {skipped_files}, failed {failed}"
            .format(
                copied=_safe_int(rc_sum.get("files_copied", 0)),
                skipped_files=(
                    _safe_int(rc_sum.get("files_skipped", 0)) +
                    _safe_int(details.get("skipped", 0))
                ),
                failed=_safe_int(rc_sum.get("files_failed", 0)),
            )
        ),
        f"Duration: {_minutes(details.get('total_time_seconds'))}",
    ]
    if exit_code != "":
        # Report the CLEAN isolated streaming rate (bytes moved while actively
        # writing), NOT robocopy's bytes-over-total-time average — the latter is
        # dragged down by the tape open/close/flush overhead on small chunks and
        # misreads as "slow". Fall back only when the profiler produced nothing.
        stream = _safe_float(details.get("tape_stream_mbs"))
        if stream > 0:
            open_s = _safe_float(details.get("tape_open_seconds"))
            close_s = _safe_float(details.get("tape_close_seconds"))
            stall_s = _safe_float(details.get("tape_stall_seconds"))
            stall_n = _safe_int(details.get("tape_stall_count"))
            peak = _safe_float(details.get("tape_stream_peak_mbs"))
            lines.append(
                f"Tape stream: {stream:.1f} MB/s (peak {peak:.0f})"
                f" | open {open_s:.0f}s, close {close_s:.0f}s,"
                f" stalls {stall_n}/{stall_s:.0f}s | robocopy exit {exit_code}"
            )
        else:
            lines.append(
                f"Tape stream: n/a (profiler unavailable) | robocopy exit {exit_code}")
    if counts:
        lines.append(f"DB: inserted {inserted}, updated {updated}, skipped {skipped}")
    source_missing = details.get("source_missing_files") or []
    if source_missing:
        lines.append(f"Source missing: {len(source_missing)}")
    return "\n".join(lines)


def notify_backup_summary(notifier, details, robocopy_result=None):
    return send_best_effort(notifier, format_backup_summary(details, robocopy_result))


def format_storage_scan_summary(launched, failed):
    lines = ["Storage Map scan launched"]
    lines.append(f"Launched: {', '.join(launched) if launched else 'none'}")
    if failed:
        lines.append(f"Failed: {', '.join(failed)}")
    return "\n".join(lines)


def format_storage_status_summary(server_name, state, started_at=None):
    lines = [f"Storage Map status: {server_name} is {state}"]
    if started_at:
        lines.append(f"Launched: {started_at}")
    return "\n".join(lines)


def format_storage_fetch_summary(fetched, skipped, failed, render_results=None):
    lines = ["Storage Map fetch complete"]
    lines.append(f"Fetched: {', '.join(fetched) if fetched else 'none'}")
    if skipped:
        lines.append(f"Skipped: {', '.join(skipped)}")
    if failed:
        lines.append(f"Failed: {', '.join(failed)}")
    if render_results:
        rendered = [
            f"{name}={'ok' if rc == 0 else 'failed'}"
            for name, rc in render_results.items()
        ]
        lines.append(f"Render: {', '.join(rendered)}")
    return "\n".join(lines)


def format_storage_dashboard_summary(servers, out_html=None, failed=False):
    label = ", ".join(servers) if servers else "none"
    if failed:
        return f"Storage Map dashboard failed\nServers: {label}"
    lines = ["Storage Map dashboard written", f"Servers: {label}"]
    if out_html:
        lines.append(f"Output: {out_html}")
    return "\n".join(lines)
