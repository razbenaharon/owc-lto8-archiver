"""Run the one local Storage Map web application."""
import argparse
import os
import shutil
import subprocess
import sys
import threading
import webbrowser


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import PROJECT_ROOT


def _chrome_path():
    """Return the installed Google Chrome executable, if available."""
    candidates = [
        shutil.which('chrome'),
        os.path.join(os.environ.get('LOCALAPPDATA', ''),
                     'Google', 'Chrome', 'Application', 'chrome.exe'),
        os.path.join(os.environ.get('PROGRAMFILES', ''),
                     'Google', 'Chrome', 'Application', 'chrome.exe'),
        os.path.join(os.environ.get('PROGRAMFILES(X86)', ''),
                     'Google', 'Chrome', 'Application', 'chrome.exe'),
    ]
    return next((path for path in candidates if path and os.path.isfile(path)),
                None)


def _open_chrome(url):
    chrome = _chrome_path()
    if not chrome:
        print('[WEB] Google Chrome was not found; opening the default browser.')
        return webbrowser.open(url)
    subprocess.Popen([chrome, url])
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Run the local Storage Map application.')
    parser.add_argument('--port', type=int, default=None,
                        help='Port (default: web_port from config, 8765).')
    browser = parser.add_mutually_exclusive_group()
    browser.add_argument('--open', action='store_true',
                         help='Open the app in the default browser.')
    browser.add_argument('--open-chrome', action='store_true',
                         help='Open the app in Google Chrome.')
    args = parser.parse_args(argv)

    os.chdir(PROJECT_ROOT)
    try:
        import uvicorn
    except ImportError:
        print('[WEB] uvicorn is not installed. Run: pip install -r requirements.txt')
        return 1

    from storage_map.webapp.app import create_app

    app = create_app()
    host = '127.0.0.1'
    port = args.port or app.state.webcfg.port
    url = f'http://{host}:{port}/'
    print(f'[WEB] Storage Map -> {url}')
    if args.open_chrome or (app.state.webcfg.open_chrome and not args.open):
        threading.Timer(1.5, _open_chrome, [url]).start()
    elif args.open:
        threading.Timer(1.5, webbrowser.open, [url]).start()
    uvicorn.run(app, host=host, port=port, log_level='info')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
