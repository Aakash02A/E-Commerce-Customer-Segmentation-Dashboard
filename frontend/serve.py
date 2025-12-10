#!/usr/bin/env python3
"""
Simple HTTP Server for Frontend
Serves index.html by default for all requests to root
"""

import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

class MyHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Custom handler that serves index.html for directory requests"""
    
    def do_GET(self):
        # If root path, serve index.html
        if self.path == '/' or self.path == '':
            self.path = '/index.html'
        
        # If requesting a directory, serve index.html
        if self.path.endswith('/'):
            self.path = self.path + 'index.html'
        
        return super().do_GET()
    
    def end_headers(self):
        # Add headers to prevent caching
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        return super().end_headers()

def run_server(port=8000):
    """Run the HTTP server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, MyHTTPRequestHandler)
    
    print(f"\n{'='*70}")
    print(f"Frontend Server Running")
    print(f"{'='*70}")
    print(f"\n📱 Open browser: http://localhost:{port}")
    print(f"📂 Serving files from: {os.getcwd()}")
    print(f"\n✅ Backend API: http://localhost:5000")
    print(f"✅ Spark UI: http://localhost:4040")
    print(f"\n🛑 Press Ctrl+C to stop\n")
    print(f"{'='*70}\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped")
        sys.exit(0)

if __name__ == '__main__':
    run_server(8000)
