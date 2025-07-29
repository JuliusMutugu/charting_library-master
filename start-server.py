#!/usr/bin/env python3
import http.server
import socketserver
import os

PORT = 8081

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def guess_type(self, path):
        mimetype = super().guess_type(path)
        if path.endswith('.js'):
            return 'application/javascript'
        return mimetype

os.chdir(os.path.dirname(os.path.abspath(__file__)))

with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
    print(f"Server running at http://localhost:{PORT}")
    print("Available demos:")
    print(f"  Main demo: http://localhost:{PORT}/index.html")
    print(f"  🦋 Gartley Scanner: http://localhost:{PORT}/gartley-scanner.html")
    print(f"  Mobile (black): http://localhost:{PORT}/mobile_black.html")
    print(f"  Mobile (white): http://localhost:{PORT}/mobile_white.html")
    print(f"  Themed demo: http://localhost:{PORT}/themed.html")
    print(f"  Test page: http://localhost:{PORT}/test.html")
    print("\n🎯 RECOMMENDED: Use the Gartley Scanner for harmonic pattern detection!")
    print("Press Ctrl+C to stop the server")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
