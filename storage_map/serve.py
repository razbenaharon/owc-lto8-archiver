"""Run the storage_map v2 interactive web dashboard (FastAPI + uvicorn)."""
import argparse
import os
import sys
import threading
import webbrowser


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import PROJECT_ROOT


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Serve the interactive Storage Map v2 dashboard.")
    parser.add_argument("--host", default=None,
                        help="Bind address (default: web_host from config, "
                             "127.0.0.1).")
    parser.add_argument("--port", type=int, default=None,
                        help="Port (default: web_port from config, 8765).")
    parser.add_argument("--open", action="store_true",
                        help="Open the dashboard in the browser once serving.")
    args = parser.parse_args(argv)

    os.chdir(PROJECT_ROOT)

    try:
        import uvicorn
    except ImportError:
        print("[WEB] uvicorn is not installed. Run: pip install fastapi uvicorn")
        return 1
    from storage_map.webapp.app import create_app

    app = create_app()
    webcfg = app.state.webcfg
    host = args.host or webcfg.host
    port = args.port or webcfg.port

    url = f"http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}/"
    print(f"[WEB] Storage Map v2 dashboard -> {url}")
    if args.open:
        threading.Timer(1.5, webbrowser.open, [url]).start()
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
