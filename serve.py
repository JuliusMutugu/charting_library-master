#!/usr/bin/env python3
import http.server
import socketserver
import os
import webbrowser
from urllib.parse import urljoin

PORT = 8080

class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    with socketserver.TCPServer(("", PORT), CORSHTTPRequestHandler) as httpd:
        print(f"Serving TradingView charts at http://localhost:{PORT}")
        print(f"Open: http://localhost:{PORT}/harmonic-patterns.html")
        print("Press Ctrl+C to stop")
        
        # Auto-open browser
        webbrowser.open(f'http://localhost:{PORT}/harmonic-patterns.html')
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
