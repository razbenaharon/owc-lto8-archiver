"""Send CTRL_C to a detached run.py so it stops the way a foreground Ctrl+C does.

taskkill /F is not an option here: it would skip the pipeline's own shutdown and
leave the DB/staging state to be inferred rather than written. This attaches to
the target's console and raises CTRL_C_EVENT there, which is the same signal the
app already handles via its CANCEL event.

Usage: graceful_stop.py <pid>   (verify no robocopy is running first)
"""
import ctypes
import sys
import time

CTRL_C_EVENT = 0
k32 = ctypes.windll.kernel32


def main():
    pid = int(sys.argv[1])

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


if __name__ == "__main__":
    sys.exit(main())
