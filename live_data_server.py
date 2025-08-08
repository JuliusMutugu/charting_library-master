#!/usr/bin/env python3
"""
Real-time market data server using yfinance
Serves current 2025 market data for TradingView charts
"""

import json
import time
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import yfinance as yf
import pandas as pd
from typing import Dict, List, Any

class LiveDataServer(BaseHTTPRequestHandler):
    # Cache for market data
    data_cache: Dict[str, Any] = {}
    cache_expiry: Dict[str, float] = {}
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()
    
    def send_cors_headers(self):
        """Send CORS headers for all responses"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '3600')
    
    def do_GET(self):
        """Handle GET requests for market data"""
        try:
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            query_params = parse_qs(parsed_url.query)
            
            print(f"📊 Request: {path} with params: {query_params}")
            
            if path == '/config':
                self.handle_config()
            elif path == '/symbol_info':
                self.handle_symbol_info(query_params)
            elif path == '/symbols':
                self.handle_symbols(query_params)
            elif path == '/search':
                self.handle_search(query_params)
            elif path == '/history':
                self.handle_history(query_params)
            elif path == '/time':
                self.handle_time()
            else:
                self.send_error(404, f"Unknown endpoint: {path}")
                
        except Exception as e:
            print(f"❌ Server error: {e}")
            self.send_error(500, str(e))
    
    def handle_config(self):
        """Return datafeed configuration"""
        config = {
            "supported_resolutions": ["1", "5", "15", "30", "60", "240", "1D"],
            "supports_group_request": False,
            "supports_marks": False,
            "supports_search": True,
            "supports_timescale_marks": False
        }
        self.send_json_response(config)
    
    def handle_symbol_info(self, params):
        """Return symbol information"""
        group = params.get('group', [''])[0]
        symbols = []
        
        # Popular symbols with real market data
        popular_symbols = [
            {'symbol': 'AAPL', 'full_name': 'Apple Inc.', 'description': 'Apple Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'MSFT', 'full_name': 'Microsoft Corporation', 'description': 'Microsoft Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'GOOGL', 'full_name': 'Alphabet Inc.', 'description': 'Alphabet Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'AMZN', 'full_name': 'Amazon.com Inc.', 'description': 'Amazon.com Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'TSLA', 'full_name': 'Tesla Inc.', 'description': 'Tesla Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'NVDA', 'full_name': 'NVIDIA Corporation', 'description': 'NVIDIA Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'META', 'full_name': 'Meta Platforms Inc.', 'description': 'Meta Platforms Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        ]
        
        for sym in popular_symbols:
            symbols.append({
                "symbol": sym['symbol'],
                "full_name": sym['full_name'],
                "description": sym['description'],
                "exchange": sym['exchange'],
                "currency": "USD",
                "type": sym['type']
            })
        
        self.send_json_response({"symbols": symbols})
    
    def handle_symbols(self, params):
        """Return symbols for a specific symbol name"""
        symbol = params.get('symbol', ['AAPL'])[0]
        
        symbol_info = {
            "name": symbol,
            "exchange-traded": "NASDAQ",
            "exchange-listed": "NASDAQ", 
            "timezone": "America/New_York",
            "minmov": 1,
            "minmov2": 0,
            "pointvalue": 1,
            "session": "0930-1600",
            "has_intraday": True,
            "has_no_volume": False,
            "description": f"{symbol} - Live 2025 Data",
            "type": "stock",
            "supported_resolutions": ["1", "5", "15", "30", "60", "240", "1D"],
            "pricescale": 100,
            "ticker": symbol
        }
        
        self.send_json_response(symbol_info)
    
    def handle_search(self, params):
        """Handle symbol search"""
        query = params.get('query', [''])[0].upper()
        limit = int(params.get('limit', [10])[0])
        
        # Popular symbols for search
        all_symbols = [
            {'symbol': 'AAPL', 'full_name': 'Apple Inc.', 'description': 'Apple Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'MSFT', 'full_name': 'Microsoft Corporation', 'description': 'Microsoft Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'GOOGL', 'full_name': 'Alphabet Inc.', 'description': 'Alphabet Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'AMZN', 'full_name': 'Amazon.com Inc.', 'description': 'Amazon.com Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'TSLA', 'full_name': 'Tesla Inc.', 'description': 'Tesla Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'NVDA', 'full_name': 'NVIDIA Corporation', 'description': 'NVIDIA Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'META', 'full_name': 'Meta Platforms Inc.', 'description': 'Meta Platforms Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        ]
        
        # Filter symbols based on query
        if query:
            filtered = [s for s in all_symbols if query in s['symbol'] or query in s['full_name'].upper()]
        else:
            filtered = all_symbols
        
        results = []
        for sym in filtered[:limit]:
            results.append({
                "symbol": sym['symbol'],
                "full_name": sym['full_name'],
                "description": sym['description'],
                "exchange": sym['exchange'],
                "ticker": sym['symbol'],
                "type": sym['type']
            })
        
        self.send_json_response(results)
    
    def handle_history(self, params):
        """Return historical market data using yfinance"""
        try:
            symbol = params.get('symbol', ['AAPL'])[0]
            resolution = params.get('resolution', ['1D'])[0]
            from_ts = int(params.get('from', [0])[0])
            to_ts = int(params.get('to', [int(time.time())])[0])
            
            print(f"📈 Fetching live data for {symbol} from {datetime.fromtimestamp(from_ts)} to {datetime.fromtimestamp(to_ts)}")
            
            # Check cache first
            cache_key = f"{symbol}_{resolution}_{from_ts}_{to_ts}"
            current_time = time.time()
            
            if cache_key in self.data_cache and current_time < self.cache_expiry.get(cache_key, 0):
                print(f"📋 Using cached data for {symbol}")
                self.send_json_response(self.data_cache[cache_key])
                return
            
            # Fetch live data from yfinance
            ticker = yf.Ticker(symbol)
            
            # Calculate date range
            start_date = datetime.fromtimestamp(from_ts)
            end_date = datetime.fromtimestamp(to_ts)
            
            # Determine period based on resolution
            if resolution == '1D':
                period = '2y'  # Get 2 years of daily data
                interval = '1d'
            elif resolution in ['60', '240']:
                period = '60d'  # Get 60 days of hourly data
                interval = '1h'
            else:
                period = '7d'   # Get 7 days for intraday
                interval = '5m'
            
            # Fetch data
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                print(f"⚠️ No data found for {symbol}")
                self.send_json_response({"s": "no_data"})
                return
            
            # Convert to TradingView format
            bars = []
            for index, row in hist.iterrows():
                timestamp = int(index.timestamp())
                
                # Filter by requested time range
                if from_ts <= timestamp <= to_ts:
                    bars.append({
                        "time": timestamp * 1000,  # TradingView expects milliseconds
                        "open": round(float(row['Open']), 2),
                        "high": round(float(row['High']), 2),
                        "low": round(float(row['Low']), 2),
                        "close": round(float(row['Close']), 2),
                        "volume": int(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) else 0
                    })
            
            # Sort by time
            bars.sort(key=lambda x: x['time'])
            
            response = {
                "s": "ok",
                "t": [bar["time"] for bar in bars],
                "o": [bar["open"] for bar in bars],
                "h": [bar["high"] for bar in bars],
                "l": [bar["low"] for bar in bars],
                "c": [bar["close"] for bar in bars],
                "v": [bar["volume"] for bar in bars]
            }
            
            # Cache the response for 5 minutes
            self.data_cache[cache_key] = response
            self.cache_expiry[cache_key] = current_time + 300
            
            print(f"✅ Successfully fetched {len(bars)} bars for {symbol} - Latest: {datetime.fromtimestamp(bars[-1]['time']/1000) if bars else 'No data'}")
            
            self.send_json_response(response)
            
        except Exception as e:
            print(f"❌ Error fetching data for {symbol}: {e}")
            self.send_json_response({"s": "error", "errmsg": str(e)})
    
    def handle_time(self):
        """Return current server time"""
        current_time = int(time.time())
        self.send_json_response(current_time)
    
    def send_json_response(self, data):
        """Send JSON response with CORS headers"""
        self.send_response(200)
        self.send_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        json_data = json.dumps(data, indent=2)
        self.wfile.write(json_data.encode('utf-8'))

def run_server(port=8083):
    """Run the live data server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, LiveDataServer)
    
    print(f"🚀 Live Market Data Server running on http://localhost:{port}")
    print(f"📊 Serving real-time data for harmonic patterns analysis")
    print(f"📈 Data source: Yahoo Finance (yfinance)")
    print(f"⏰ Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")
        httpd.shutdown()

if __name__ == '__main__':
    # Install required packages
    try:
        import yfinance
        import pandas
    except ImportError:
        print("Installing required packages...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "pandas"])
        import yfinance
        import pandas
    
    run_server()
