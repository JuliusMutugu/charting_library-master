#!/usr/bin/env python3
"""
Real-time Yahoo Finance data server for TradingView charts
Provides live market data using yfinance library
"""

import json
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import yfinance as yf
import threading
from socketserver import ThreadingMixIn

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""
    pass

class YFinanceDataHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
        try:
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            params = parse_qs(parsed_path.query)
            
            print(f"Request: {path} with params: {params}")
            
            if path == '/config':
                # TradingView UDF datafeed configuration
                config = {
                    'supported_resolutions': ['1', '5', '15', '30', '60', '240', '1D'],
                    'supports_marks': False,
                    'supports_timescale_marks': False,
                    'supports_time': True,
                    'exchanges': [
                        {'value': '', 'name': 'All Exchanges', 'desc': ''},
                        {'value': 'NASDAQ', 'name': 'NASDAQ', 'desc': 'NASDAQ'},
                        {'value': 'NYSE', 'name': 'NYSE', 'desc': 'NYSE'}
                    ],
                    'symbols_types': [
                        {'name': 'All types', 'value': ''},
                        {'name': 'Stock', 'value': 'stock'},
                        {'name': 'Index', 'value': 'index'}
                    ]
                }
                self.wfile.write(json.dumps(config).encode())
                
            elif path == '/symbols':
                # Symbol search - UDF format
                query = params.get('query', [''])[0].upper()
                symbols = self.search_symbols(query)
                self.wfile.write(json.dumps(symbols).encode())
                
            elif path == '/symbol_info':
                # Symbol information - UDF format
                symbol = params.get('symbol', ['AAPL'])[0]
                symbol_info = self.get_symbol_info(symbol)
                self.wfile.write(json.dumps(symbol_info).encode())
                
            elif path == '/history':
                # Historical data - UDF format
                symbol = params.get('symbol', ['AAPL'])[0]
                resolution = params.get('resolution', ['1D'])[0]
                from_time = int(params.get('from', [0])[0])
                to_time = int(params.get('to', [int(time.time())])[0])
                
                print(f"Fetching history for {symbol}, resolution {resolution}")
                bars = self.get_bars(symbol, resolution, from_time, to_time)
                self.wfile.write(json.dumps(bars).encode())
                
            else:
                print(f"Unknown endpoint: {path}")
                self.wfile.write(json.dumps({'error': 'Invalid endpoint'}).encode())
                
        except Exception as e:
            print(f"Error processing request: {e}")
            import traceback
            traceback.print_exc()
            error_response = {'error': str(e), 's': 'error'}
            self.wfile.write(json.dumps(error_response).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def search_symbols(self, query):
        """Search for symbols matching the query"""
        # Popular symbols for quick search
        popular_symbols = [
            {'symbol': 'AAPL', 'full_name': 'Apple Inc.', 'description': 'AAPL', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'MSFT', 'full_name': 'Microsoft Corporation', 'description': 'MSFT', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'GOOGL', 'full_name': 'Alphabet Inc.', 'description': 'GOOGL', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'AMZN', 'full_name': 'Amazon.com Inc.', 'description': 'AMZN', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'TSLA', 'full_name': 'Tesla Inc.', 'description': 'TSLA', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'NVDA', 'full_name': 'NVIDIA Corporation', 'description': 'NVDA', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'META', 'full_name': 'Meta Platforms Inc.', 'description': 'META', 'exchange': 'NASDAQ', 'type': 'stock'},
            {'symbol': 'JPM', 'full_name': 'JPMorgan Chase & Co.', 'description': 'JPM', 'exchange': 'NYSE', 'type': 'stock'},
            {'symbol': 'JNJ', 'full_name': 'Johnson & Johnson', 'description': 'JNJ', 'exchange': 'NYSE', 'type': 'stock'},
            {'symbol': 'V', 'full_name': 'Visa Inc.', 'description': 'V', 'exchange': 'NYSE', 'type': 'stock'},
            {'symbol': 'SPY', 'full_name': 'SPDR S&P 500 ETF', 'description': 'SPY', 'exchange': 'NYSE', 'type': 'index'},
            {'symbol': 'QQQ', 'full_name': 'Invesco QQQ Trust', 'description': 'QQQ', 'exchange': 'NASDAQ', 'type': 'index'},
        ]
        
        if not query:
            return popular_symbols[:10]
        
        # Filter by query
        results = []
        for symbol in popular_symbols:
            if (query in symbol['symbol'] or 
                query.lower() in symbol['full_name'].lower() or
                query.lower() in symbol['description'].lower()):
                results.append(symbol)
        
        return results[:10]
    
    def get_symbol_info(self, symbol):
        """Get symbol information"""
        return {
            'name': symbol,
            'exchange-traded': 'NASDAQ',
            'exchange-listed': 'NASDAQ',
            'timezone': 'America/New_York',
            'minmov': 1,
            'minmov2': 0,
            'pointvalue': 1,
            'session': '0930-1600',
            'has_intraday': True,
            'has_no_volume': False,
            'description': symbol,
            'type': 'stock',
            'supported_resolutions': ['1', '5', '15', '30', '60', '240', '1D'],
            'pricescale': 100,
            'ticker': symbol
        }
    
    def get_bars(self, symbol, resolution, from_time, to_time):
        """Get historical bars using yfinance"""
        try:
            print(f"Fetching data for {symbol} from {datetime.fromtimestamp(from_time)} to {datetime.fromtimestamp(to_time)}")
            
            # Convert resolution to yfinance period
            if resolution == '1D':
                interval = '1d'
                period = '2y'  # Get more data for better charts
            elif resolution == '60':
                interval = '1h'
                period = '1mo'
            elif resolution == '30':
                interval = '30m'
                period = '1mo'
            elif resolution == '15':
                interval = '15m'
                period = '1mo'
            elif resolution == '5':
                interval = '5m'
                period = '1mo'
            else:
                interval = '1d'
                period = '2y'
            
            # Fetch data from Yahoo Finance
            ticker = yf.Ticker(symbol)
            
            # Get historical data
            start_date = datetime.fromtimestamp(from_time)
            end_date = datetime.fromtimestamp(to_time)
            
            # Ensure we get enough data
            if end_date - start_date < timedelta(days=30):
                start_date = end_date - timedelta(days=365)  # Get 1 year of data
            
            hist = ticker.history(start=start_date, end=end_date, interval=interval)
            
            if hist.empty:
                print(f"No data found for {symbol}")
                return {'s': 'no_data'}
            
            # Convert to TradingView format
            bars = []
            for index, row in hist.iterrows():
                timestamp = int(index.timestamp())
                
                # Skip weekends for daily data
                if resolution == '1D' and index.weekday() >= 5:
                    continue
                
                bar = {
                    't': timestamp,
                    'o': round(float(row['Open']), 2),
                    'h': round(float(row['High']), 2),
                    'l': round(float(row['Low']), 2),
                    'c': round(float(row['Close']), 2),
                    'v': int(row['Volume']) if 'Volume' in row and str(row['Volume']) != 'nan' else 0
                }
                bars.append(bar)
            
            print(f"Returning {len(bars)} bars for {symbol}")
            
            # Sort by time
            bars.sort(key=lambda x: x['t'])
            
            return {
                's': 'ok',
                't': [bar['t'] for bar in bars],
                'o': [bar['o'] for bar in bars],
                'h': [bar['h'] for bar in bars],
                'l': [bar['l'] for bar in bars],
                'c': [bar['c'] for bar in bars],
                'v': [bar['v'] for bar in bars]
            }
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return {'s': 'error', 'errmsg': str(e)}

def run_server():
    """Run the Yahoo Finance data server"""
    server_address = ('', 8082)
    httpd = ThreadedHTTPServer(server_address, YFinanceDataHandler)
    print("🚀 Yahoo Finance Data Server running on http://localhost:8082")
    print("📊 Providing real-time market data for TradingView charts")
    print("💡 Available endpoints:")
    print("   - /config - Datafeed configuration")
    print("   - /symbols?query=AAPL - Symbol search")
    print("   - /symbol_info?symbol=AAPL - Symbol information")
    print("   - /history?symbol=AAPL&resolution=1D&from=1609459200&to=1672531200 - Historical data")
    httpd.serve_forever()

if __name__ == '__main__':
    try:
        # Check if required modules are available
        import pandas as pd
        print("✅ All required modules available")
        run_server()
    except ImportError as e:
        print(f"❌ Missing required module: {e}")
        print("📦 Please install required packages:")
        print("   pip install yfinance pandas")
