# 007 — A momentary DNS blip stopped the run, which then idled ~3 days

- **When:** 2026-07-17
- **Physical intervention required:** no
- **Status:** partially fixed — two lessons remain unfixed in code

## What happened

The streaming session stopped at chunk 25 with
`ssh: Could not resolve hostname so01`. It was a Technion DNS hiccup, not a real
outage: Telegram failed in the same instant with `getaddrinfo failed`, and the
machine never rebooted. Nothing relaunched the run, so it **sat idle for about
three days**.

## Root cause

`_fetch_one_batch` treated any fetch failure as fatal, so a transient name
resolution failure killed a multi-day run.

## Fix

`_fetch_one_batch` now retries transient failures with exponential backoff before
giving up (`_is_transient_fetch_error`; `[PERFORMANCE] fetch_transient_retries`
default 5, `fetch_transient_retry_base_seconds` default 5). Genuine errors —
missing file, permission denied — still fail fast.

## Two lessons NOT fixed in code (they still bite)

1. **Do not run the only monitor on the host doing the work.** When that host
   loses the network you lose the watchdog at exactly the moment you need it.
2. **There is still no auto-relaunch.** A hard stop needs a human to re-run
   `6\n1\n`. The three idle days were entirely this.

Both are directly relevant to
[the no-physical-intervention policy](000-no-physical-intervention-policy.md):
an unattended run that silently stops is only one step away from a situation
somebody has to drive out to fix.

## Related

A separate watchdog was added later for a different silent failure: a wedged
remote fetch that stalls with **no data at all** is now aborted rather than
hanging forever (`a930da6`; rationale corrected in `4ed6092` — there was no live
27-minute hang, the watchdog is precautionary).
