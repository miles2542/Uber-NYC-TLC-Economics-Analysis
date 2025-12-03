"""
Simple HTTP server for testing the modular HTML structure
Run this with: python serve.py
Then open: http://localhost:8000
"""

import http.server
import socketserver
import os

PORT = 8000
DIRECTORY = r"x:\Programming\Python\[Y3S1] Year 3, Autumn semester\[Y3S1] Data preparation and Visualisation\Projects\Final term (hck)\TLC NYC filtered\ui"


class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def end_headers(self):
        # Add CORS headers for local development
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        super().end_headers()


os.chdir(DIRECTORY)

with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
    print(f"✓ Server running at http://localhost:{PORT}")
    print(f"✓ Serving directory: {DIRECTORY}")
    print(f"✓ Press Ctrl+C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n✓ Server stopped")
