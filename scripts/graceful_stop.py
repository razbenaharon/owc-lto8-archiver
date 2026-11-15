"""Send CTRL_C to a detached run.py so it stops the way a foreground Ctrl+C does.

taskkill /F is not an option here: it would skip the pipeline's own shutdown and
leave the DB/staging state to be inferred rather than written. This attaches to
the target's console and raises CTRL_C_EVENT there, which is the same signal the
app already handles via its CANCEL event.

Usage: graceful_stop.py <pid> [--allow-active-tape-write]

The helper REFUSES while a robocopy process is running. A console CTRL_C goes
to every process attached to that console; an archiver build that predates the
process-group isolation of its tape-write child (or any other robocopy sharing
the console) is killed mid-write by it, and an interrupted tape write is the
main route to a cartridge only a human at the drive can recover. It also
refuses when the process list cannot be read: "cannot tell" is not "safe".

``--allow-active-tape-write`` overrides the refusal. Use it only when the
running archiver is known to isolate its tape write (src/robocopy.py
``_run_robocopy_tuned``), in which case the write finishes and the run stops at
the next chunk boundary.
"""
import ctypes
import subprocess
import sys
import time

CTRL_C_EVENT = 0
ALLOW_FLAG = "--allow-active-tape-write"


def robocopy_running():
    """True/False from ``tasklist``; None when the list could not be read.

    ``tasklist`` and a case-insensitive match, deliberately: the image
    registers as ``Robocopy.exe`` (capital R), which a case-sensitive WMI
    ``Name='robocopy.exe'`` filter misses.
    """
    try:
        proc = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return None
    return "robocopy" in proc.stdout.lower()


def _send_ctrl_c(pid):
    k32 = ctypes.windll.kernel32

    # Detach from our own console before attaching to the target's; a process
    # can only be attached to one at a time.
    k32.FreeConsole()
    if not k32.AttachConsole(pid):
        print(f"AttachConsole({pid}) failed: {k32.GetLastError()}")
        return 1

    # The event goes to every process on that console, this helper included.
    # Ignore it here so we survive to report the result.
    k32.SetConsoleCtrlHandler(None, True)

    ok = k32.GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)
    err = k32.GetLastError()
    time.sleep(0.5)
    k32.FreeConsole()

    if not ok:
        print(f"GenerateConsoleCtrlEvent failed: {err}")
        return 1
    print(f"CTRL_C delivered to the console of pid {pid}")
    return 0


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    allow_active = ALLOW_FLAG in args
    args = [a for a in args if a != ALLOW_FLAG]
    if len(args) != 1 or not args[0].isdigit():
        print(f"Usage: graceful_stop.py <pid> [{ALLOW_FLAG}]")
        return 2
    pid = int(args[0])

    running = robocopy_running()
    if running is None and not allow_active:
        print("REFUSED: could not read the process list (tasklist), so it is "
              "unknown whether a tape write is in flight. Nothing was sent.")
        return 3
    if running and not allow_active:
        print("REFUSED: a robocopy process is running - a tape write may be in "
              "flight, and a console CTRL_C can kill it mid-write. Nothing was "
              "sent. Wait for the chunk to finish and run this again, or pass "
              f"{ALLOW_FLAG} if the running archiver is known to isolate its "
              "tape write from console events.")
        return 3
    if running or running is None:
        print(f"WARNING: {ALLOW_FLAG} given - sending CTRL_C although "
              + ("a robocopy process is running."
                 if running else "the process list could not be read."))

    return _send_ctrl_c(pid)


if __name__ == "__main__":
    sys.exit(main())
