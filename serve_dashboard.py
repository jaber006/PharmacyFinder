"""
Live dashboard server - rebuilds on every page load so data is always fresh.
Runs on http://localhost:8050
"""
import http.server
import subprocess
import sys
import os
import time
import threading

PORT = 8050
BUILD_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_dashboard_v4.py')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DASHBOARD = os.path.join(OUTPUT_DIR, 'dashboard.html')

last_build = 0
build_lock = threading.Lock()
MIN_BUILD_INTERVAL = 10  # Don't rebuild more than once per 10 seconds

def rebuild_if_needed():
    global last_build
    with build_lock:
        now = time.time()
        if now - last_build < MIN_BUILD_INTERVAL:
            return
        print(f"[{time.strftime('%H:%M:%S')}] Rebuilding dashboard...")
        try:
            subprocess.run(
                [sys.executable, BUILD_SCRIPT],
                capture_output=True, timeout=30,
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
            last_build = time.time()
            print(f"[{time.strftime('%H:%M:%S')}] Dashboard rebuilt in {time.time()-now:.1f}s")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Build failed: {e}")

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)
    
    def do_GET(self):
        if self.path in ('/', '/dashboard.html', '/index.html'):
            # Rebuild dashboard on page load
            rebuild_if_needed()
            self.path = '/dashboard.html'
        super().do_GET()
    
    def log_message(self, format, *args):
        # Suppress noisy request logs, only show rebuilds
        if 'dashboard.html' in str(args):
            print(f"[{time.strftime('%H:%M:%S')}] Dashboard served")

if __name__ == '__main__':
    # Initial build
    print(f"PharmacyFinder Dashboard Server")
    print(f"================================")
    rebuild_if_needed()
    
    server = http.server.HTTPServer(('127.0.0.1', PORT), DashboardHandler)
    print(f"\nServing on http://localhost:{PORT}")
    print(f"Dashboard auto-rebuilds on each page load (max once per {MIN_BUILD_INTERVAL}s)")
    print(f"Press Ctrl+C to stop\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
        server.shutdown()
