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
    # Enhanced cache for market data with longer expiry
    data_cache: Dict[str, Any] = {}
    cache_expiry: Dict[str, float] = {}
    
    # Pre-loaded symbol data for faster responses - COMPREHENSIVE TRADING APP
    ALL_SYMBOLS = [
        # ========== MAJOR TECH STOCKS ==========
        {'symbol': 'AAPL', 'full_name': 'Apple Inc.', 'description': 'Apple Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'MSFT', 'full_name': 'Microsoft Corporation', 'description': 'Microsoft Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'GOOGL', 'full_name': 'Alphabet Inc. Class A', 'description': 'Alphabet Inc. Class A', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'GOOG', 'full_name': 'Alphabet Inc. Class C', 'description': 'Alphabet Inc. Class C', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'AMZN', 'full_name': 'Amazon.com Inc.', 'description': 'Amazon.com Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'TSLA', 'full_name': 'Tesla Inc.', 'description': 'Tesla Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'NVDA', 'full_name': 'NVIDIA Corporation', 'description': 'NVIDIA Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'META', 'full_name': 'Meta Platforms Inc.', 'description': 'Meta Platforms Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'NFLX', 'full_name': 'Netflix Inc.', 'description': 'Netflix Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'CRM', 'full_name': 'Salesforce Inc.', 'description': 'Salesforce Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'ORCL', 'full_name': 'Oracle Corporation', 'description': 'Oracle Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'ADBE', 'full_name': 'Adobe Inc.', 'description': 'Adobe Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'INTC', 'full_name': 'Intel Corporation', 'description': 'Intel Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'AMD', 'full_name': 'Advanced Micro Devices Inc.', 'description': 'Advanced Micro Devices Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'PYPL', 'full_name': 'PayPal Holdings Inc.', 'description': 'PayPal Holdings Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'SHOP', 'full_name': 'Shopify Inc.', 'description': 'Shopify Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'UBER', 'full_name': 'Uber Technologies Inc.', 'description': 'Uber Technologies Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'LYFT', 'full_name': 'Lyft Inc.', 'description': 'Lyft Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'SPOT', 'full_name': 'Spotify Technology S.A.', 'description': 'Spotify Technology S.A.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'ZOOM', 'full_name': 'Zoom Video Communications Inc.', 'description': 'Zoom Video Communications Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'PLTR', 'full_name': 'Palantir Technologies Inc.', 'description': 'Palantir Technologies Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'SNOW', 'full_name': 'Snowflake Inc.', 'description': 'Snowflake Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== FINANCIAL SECTOR ==========
        {'symbol': 'JPM', 'full_name': 'JPMorgan Chase & Co.', 'description': 'JPMorgan Chase & Co.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'BAC', 'full_name': 'Bank of America Corporation', 'description': 'Bank of America Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'WFC', 'full_name': 'Wells Fargo & Company', 'description': 'Wells Fargo & Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'GS', 'full_name': 'Goldman Sachs Group Inc.', 'description': 'Goldman Sachs Group Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'MS', 'full_name': 'Morgan Stanley', 'description': 'Morgan Stanley', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'V', 'full_name': 'Visa Inc.', 'description': 'Visa Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'MA', 'full_name': 'Mastercard Incorporated', 'description': 'Mastercard Incorporated', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'AXP', 'full_name': 'American Express Company', 'description': 'American Express Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'C', 'full_name': 'Citigroup Inc.', 'description': 'Citigroup Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'BRK-B', 'full_name': 'Berkshire Hathaway Inc. Class B', 'description': 'Berkshire Hathaway Inc. Class B', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== CONSUMER & RETAIL ==========
        {'symbol': 'JNJ', 'full_name': 'Johnson & Johnson', 'description': 'Johnson & Johnson', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'UNH', 'full_name': 'UnitedHealth Group Incorporated', 'description': 'UnitedHealth Group Incorporated', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'PG', 'full_name': 'Procter & Gamble Company', 'description': 'Procter & Gamble Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'HD', 'full_name': 'Home Depot Inc.', 'description': 'Home Depot Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'DIS', 'full_name': 'Walt Disney Company', 'description': 'Walt Disney Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'NKE', 'full_name': 'Nike Inc.', 'description': 'Nike Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'MCD', 'full_name': 'McDonald\'s Corporation', 'description': 'McDonald\'s Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'KO', 'full_name': 'Coca-Cola Company', 'description': 'Coca-Cola Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'PEP', 'full_name': 'PepsiCo Inc.', 'description': 'PepsiCo Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'WMT', 'full_name': 'Walmart Inc.', 'description': 'Walmart Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'TGT', 'full_name': 'Target Corporation', 'description': 'Target Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'COST', 'full_name': 'Costco Wholesale Corporation', 'description': 'Costco Wholesale Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'SBUX', 'full_name': 'Starbucks Corporation', 'description': 'Starbucks Corporation', 'exchange': 'NASDAQ', 'type': 'stock'},
        
        # ========== ENERGY & COMMODITIES ==========
        {'symbol': 'XOM', 'full_name': 'Exxon Mobil Corporation', 'description': 'Exxon Mobil Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'CVX', 'full_name': 'Chevron Corporation', 'description': 'Chevron Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'COP', 'full_name': 'ConocoPhillips', 'description': 'ConocoPhillips', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'SLB', 'full_name': 'Schlumberger Limited', 'description': 'Schlumberger Limited', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'HAL', 'full_name': 'Halliburton Company', 'description': 'Halliburton Company', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== AIRLINES & TRANSPORTATION ==========
        {'symbol': 'BA', 'full_name': 'Boeing Company', 'description': 'Boeing Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'AAL', 'full_name': 'American Airlines Group Inc.', 'description': 'American Airlines Group Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'DAL', 'full_name': 'Delta Air Lines Inc.', 'description': 'Delta Air Lines Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'UAL', 'full_name': 'United Airlines Holdings Inc.', 'description': 'United Airlines Holdings Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'LUV', 'full_name': 'Southwest Airlines Co.', 'description': 'Southwest Airlines Co.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'FDX', 'full_name': 'FedEx Corporation', 'description': 'FedEx Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'UPS', 'full_name': 'United Parcel Service Inc.', 'description': 'United Parcel Service Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== PHARMACEUTICALS & HEALTHCARE ==========
        {'symbol': 'PFE', 'full_name': 'Pfizer Inc.', 'description': 'Pfizer Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'MRNA', 'full_name': 'Moderna Inc.', 'description': 'Moderna Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'ABBV', 'full_name': 'AbbVie Inc.', 'description': 'AbbVie Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'BMY', 'full_name': 'Bristol-Myers Squibb Company', 'description': 'Bristol-Myers Squibb Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'LLY', 'full_name': 'Eli Lilly and Company', 'description': 'Eli Lilly and Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'MRK', 'full_name': 'Merck & Co. Inc.', 'description': 'Merck & Co. Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== POPULAR ETFs ==========
        {'symbol': 'SPY', 'full_name': 'SPDR S&P 500 ETF Trust', 'description': 'SPDR S&P 500 ETF Trust', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'QQQ', 'full_name': 'Invesco QQQ Trust', 'description': 'Invesco QQQ Trust', 'exchange': 'NASDAQ', 'type': 'etf'},
        {'symbol': 'IWM', 'full_name': 'iShares Russell 2000 ETF', 'description': 'iShares Russell 2000 ETF', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'VTI', 'full_name': 'Vanguard Total Stock Market ETF', 'description': 'Vanguard Total Stock Market ETF', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'VEA', 'full_name': 'Vanguard FTSE Developed Markets ETF', 'description': 'Vanguard FTSE Developed Markets ETF', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'VWO', 'full_name': 'Vanguard FTSE Emerging Markets ETF', 'description': 'Vanguard FTSE Emerging Markets ETF', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'EFA', 'full_name': 'iShares MSCI EAFE ETF', 'description': 'iShares MSCI EAFE ETF', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'GLD', 'full_name': 'SPDR Gold Shares', 'description': 'SPDR Gold Shares', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'SLV', 'full_name': 'iShares Silver Trust', 'description': 'iShares Silver Trust', 'exchange': 'NYSE', 'type': 'etf'},
        {'symbol': 'TLT', 'full_name': 'iShares 20+ Year Treasury Bond ETF', 'description': 'iShares 20+ Year Treasury Bond ETF', 'exchange': 'NASDAQ', 'type': 'etf'},
        
        # ========== CRYPTOCURRENCIES ==========
        {'symbol': 'BTC-USD', 'full_name': 'Bitcoin USD', 'description': 'Bitcoin USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'ETH-USD', 'full_name': 'Ethereum USD', 'description': 'Ethereum USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'BNB-USD', 'full_name': 'Binance Coin USD', 'description': 'Binance Coin USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'XRP-USD', 'full_name': 'XRP USD', 'description': 'XRP USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'ADA-USD', 'full_name': 'Cardano USD', 'description': 'Cardano USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'SOL-USD', 'full_name': 'Solana USD', 'description': 'Solana USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'DOGE-USD', 'full_name': 'Dogecoin USD', 'description': 'Dogecoin USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'AVAX-USD', 'full_name': 'Avalanche USD', 'description': 'Avalanche USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'DOT-USD', 'full_name': 'Polkadot USD', 'description': 'Polkadot USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'MATIC-USD', 'full_name': 'Polygon USD', 'description': 'Polygon USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'SHIB-USD', 'full_name': 'Shiba Inu USD', 'description': 'Shiba Inu USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'LTC-USD', 'full_name': 'Litecoin USD', 'description': 'Litecoin USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'UNI-USD', 'full_name': 'Uniswap USD', 'description': 'Uniswap USD', 'exchange': 'CCC', 'type': 'crypto'},
        {'symbol': 'LINK-USD', 'full_name': 'Chainlink USD', 'description': 'Chainlink USD', 'exchange': 'CCC', 'type': 'crypto'},
        
        # ========== FOREX MAJOR PAIRS ==========
        {'symbol': 'EURUSD=X', 'full_name': 'EUR/USD', 'description': 'Euro vs US Dollar', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'GBPUSD=X', 'full_name': 'GBP/USD', 'description': 'British Pound vs US Dollar', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'USDJPY=X', 'full_name': 'USD/JPY', 'description': 'US Dollar vs Japanese Yen', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'USDCHF=X', 'full_name': 'USD/CHF', 'description': 'US Dollar vs Swiss Franc', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'AUDUSD=X', 'full_name': 'AUD/USD', 'description': 'Australian Dollar vs US Dollar', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'USDCAD=X', 'full_name': 'USD/CAD', 'description': 'US Dollar vs Canadian Dollar', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'NZDUSD=X', 'full_name': 'NZD/USD', 'description': 'New Zealand Dollar vs US Dollar', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'EURGBP=X', 'full_name': 'EUR/GBP', 'description': 'Euro vs British Pound', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'EURJPY=X', 'full_name': 'EUR/JPY', 'description': 'Euro vs Japanese Yen', 'exchange': 'FX', 'type': 'forex'},
        {'symbol': 'GBPJPY=X', 'full_name': 'GBP/JPY', 'description': 'British Pound vs Japanese Yen', 'exchange': 'FX', 'type': 'forex'},
        
        # ========== COMMODITIES & FUTURES ==========
        {'symbol': 'GC=F', 'full_name': 'Gold Futures', 'description': 'Gold Futures', 'exchange': 'COMEX', 'type': 'futures'},
        {'symbol': 'SI=F', 'full_name': 'Silver Futures', 'description': 'Silver Futures', 'exchange': 'COMEX', 'type': 'futures'},
        {'symbol': 'CL=F', 'full_name': 'Crude Oil Futures', 'description': 'Crude Oil Futures', 'exchange': 'NYMEX', 'type': 'futures'},
        {'symbol': 'NG=F', 'full_name': 'Natural Gas Futures', 'description': 'Natural Gas Futures', 'exchange': 'NYMEX', 'type': 'futures'},
        {'symbol': 'ZC=F', 'full_name': 'Corn Futures', 'description': 'Corn Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'ZS=F', 'full_name': 'Soybean Futures', 'description': 'Soybean Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'ZW=F', 'full_name': 'Wheat Futures', 'description': 'Wheat Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'HG=F', 'full_name': 'Copper Futures', 'description': 'Copper Futures', 'exchange': 'COMEX', 'type': 'futures'},
        {'symbol': 'PL=F', 'full_name': 'Platinum Futures', 'description': 'Platinum Futures', 'exchange': 'NYMEX', 'type': 'futures'},
        {'symbol': 'PA=F', 'full_name': 'Palladium Futures', 'description': 'Palladium Futures', 'exchange': 'NYMEX', 'type': 'futures'},
        
        # ========== STOCK INDEX FUTURES ==========
        {'symbol': 'ES=F', 'full_name': 'E-mini S&P 500 Futures', 'description': 'E-mini S&P 500 Futures', 'exchange': 'CME', 'type': 'futures'},
        {'symbol': 'NQ=F', 'full_name': 'E-mini NASDAQ 100 Futures', 'description': 'E-mini NASDAQ 100 Futures', 'exchange': 'CME', 'type': 'futures'},
        {'symbol': 'YM=F', 'full_name': 'E-mini Dow Jones Futures', 'description': 'E-mini Dow Jones Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'RTY=F', 'full_name': 'E-mini Russell 2000 Futures', 'description': 'E-mini Russell 2000 Futures', 'exchange': 'CME', 'type': 'futures'},
        
        # ========== BOND FUTURES ==========
        {'symbol': 'ZB=F', 'full_name': '30-Year Treasury Bond Futures', 'description': '30-Year Treasury Bond Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'ZN=F', 'full_name': '10-Year Treasury Note Futures', 'description': '10-Year Treasury Note Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'ZF=F', 'full_name': '5-Year Treasury Note Futures', 'description': '5-Year Treasury Note Futures', 'exchange': 'CBOT', 'type': 'futures'},
        {'symbol': 'ZT=F', 'full_name': '2-Year Treasury Note Futures', 'description': '2-Year Treasury Note Futures', 'exchange': 'CBOT', 'type': 'futures'},
        
        # ========== MAJOR INDICES ==========
        {'symbol': '^GSPC', 'full_name': 'S&P 500', 'description': 'S&P 500 Index', 'exchange': 'INDEX', 'type': 'index'},
        {'symbol': '^DJI', 'full_name': 'Dow Jones', 'description': 'Dow Jones Industrial Average', 'exchange': 'INDEX', 'type': 'index'},
        {'symbol': '^IXIC', 'full_name': 'NASDAQ Composite', 'description': 'NASDAQ Composite Index', 'exchange': 'INDEX', 'type': 'index'},
        {'symbol': '^RUT', 'full_name': 'Russell 2000', 'description': 'Russell 2000 Index', 'exchange': 'INDEX', 'type': 'index'},
        {'symbol': '^VIX', 'full_name': 'CBOE Volatility Index', 'description': 'CBOE Volatility Index', 'exchange': 'INDEX', 'type': 'index'},
        {'symbol': '^TNX', 'full_name': '10-Year Treasury Yield', 'description': '10-Year Treasury Yield', 'exchange': 'INDEX', 'type': 'index'},
        
        # ========== INTERNATIONAL STOCKS ==========
        {'symbol': 'TSM', 'full_name': 'Taiwan Semiconductor Manufacturing Company Limited', 'description': 'Taiwan Semiconductor Manufacturing Company Limited', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'ASML', 'full_name': 'ASML Holding N.V.', 'description': 'ASML Holding N.V.', 'exchange': 'NASDAQ', 'type': 'stock'},
        {'symbol': 'BABA', 'full_name': 'Alibaba Group Holding Limited', 'description': 'Alibaba Group Holding Limited', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'NIO', 'full_name': 'NIO Inc.', 'description': 'NIO Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'BIDU', 'full_name': 'Baidu Inc.', 'description': 'Baidu Inc.', 'exchange': 'NASDAQ', 'type': 'stock'},
        
        # ========== EMERGING MARKETS & MEME STOCKS ==========
        {'symbol': 'GME', 'full_name': 'GameStop Corp.', 'description': 'GameStop Corp.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'AMC', 'full_name': 'AMC Entertainment Holdings Inc.', 'description': 'AMC Entertainment Holdings Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'BB', 'full_name': 'BlackBerry Limited', 'description': 'BlackBerry Limited', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'NOK', 'full_name': 'Nokia Corporation', 'description': 'Nokia Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        
        # ========== REAL ESTATE & REITS ==========
        {'symbol': 'SPG', 'full_name': 'Simon Property Group Inc.', 'description': 'Simon Property Group Inc.', 'exchange': 'NYSE', 'type': 'reit'},
        {'symbol': 'PLD', 'full_name': 'Prologis Inc.', 'description': 'Prologis Inc.', 'exchange': 'NYSE', 'type': 'reit'},
        {'symbol': 'EXR', 'full_name': 'Extended Stay America Inc.', 'description': 'Extended Stay America Inc.', 'exchange': 'NYSE', 'type': 'reit'},
        
        # ========== ADDITIONAL POPULAR STOCKS ==========
        {'symbol': 'IBM', 'full_name': 'International Business Machines Corporation', 'description': 'International Business Machines Corporation', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'GE', 'full_name': 'General Electric Company', 'description': 'General Electric Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'F', 'full_name': 'Ford Motor Company', 'description': 'Ford Motor Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'GM', 'full_name': 'General Motors Company', 'description': 'General Motors Company', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'T', 'full_name': 'AT&T Inc.', 'description': 'AT&T Inc.', 'exchange': 'NYSE', 'type': 'stock'},
        {'symbol': 'VZ', 'full_name': 'Verizon Communications Inc.', 'description': 'Verizon Communications Inc.', 'exchange': 'NYSE', 'type': 'stock'},
    ]
    
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
            elif path == '/test':
                self.handle_test()
            elif path == '/status':
                self.handle_status()
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
        """Return symbol information - optimized for speed"""
        group = params.get('group', [''])[0]
        symbols = []
        
        # Use pre-loaded symbol data for faster response
        for sym in self.ALL_SYMBOLS:
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
        """Handle symbol search - optimized for speed"""
        query = params.get('query', [''])[0].upper()
        limit = int(params.get('limit', [50])[0])  # Increased default limit
        
        # Filter symbols based on query using pre-loaded data
        if query:
            filtered = [s for s in self.ALL_SYMBOLS if query in s['symbol'] or query in s['full_name'].upper()]
        else:
            filtered = self.ALL_SYMBOLS
        
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
        """Return historical market data using yfinance - ENHANCED VERSION WITH HISTORICAL DATA"""
        try:
            symbol = params.get('symbol', ['AAPL'])[0]
            resolution = params.get('resolution', ['1D'])[0]
            from_ts = int(params.get('from', [0])[0])
            to_ts = int(params.get('to', [int(time.time())])[0])
            countback = int(params.get('countback', [300])[0])
            
            # Handle invalid/negative timestamps - use current time and countback
            current_time = int(time.time())
            
            # Fix invalid timestamps
            if from_ts <= 0 or to_ts <= 0 or from_ts > current_time or to_ts > current_time:
                print(f"⚠️ Invalid timestamps detected: from={from_ts}, to={to_ts}")
                print(f"🔧 Using current time and countback method")
                to_ts = current_time
                # Calculate reasonable from_ts based on resolution and countback
                if resolution == '1D':
                    from_ts = to_ts - (countback * 24 * 3600)  # countback days
                elif resolution in ['240', '4h']:
                    from_ts = to_ts - (countback * 4 * 3600)   # countback 4-hour periods
                elif resolution in ['60', '1h']:
                    from_ts = to_ts - (countback * 3600)       # countback hours
                elif resolution in ['30']:
                    from_ts = to_ts - (countback * 30 * 60)    # countback 30-min periods
                elif resolution in ['15']:
                    from_ts = to_ts - (countback * 15 * 60)    # countback 15-min periods
                elif resolution in ['5']:
                    from_ts = to_ts - (countback * 5 * 60)     # countback 5-min periods
                else:
                    from_ts = to_ts - (countback * 60)         # countback minutes
            
            # Convert timestamps to readable format for debugging
            from_date = datetime.fromtimestamp(from_ts) if from_ts > 0 else "Not specified"
            to_date = datetime.fromtimestamp(to_ts) if to_ts > 0 else "Not specified"
            
            print(f"📈 Fetching historical data for {symbol} from {from_date} to {to_date}")
            print(f"🔍 Resolution: {resolution}, Countback: {countback}")
            print(f"🔍 Raw timestamps: from={from_ts}, to={to_ts}")
            
            # Check cache first - cache for 60 seconds for historical data
            cache_key = f"{symbol}_{resolution}_{from_ts}_{to_ts}"
            current_cache_time = time.time()
            
            if cache_key in self.data_cache and current_cache_time < self.cache_expiry.get(cache_key, 0):
                print(f"⚡ Using cached data for {symbol}")
                self.send_json_response(self.data_cache[cache_key])
                return
            
            # Fetch historical data from yfinance - ALWAYS RETURN DATA
            ticker = yf.Ticker(symbol)
            
            # Determine period and interval - ENSURE WE GET ENOUGH HISTORICAL DATA
            try:
                print(f"🔄 Primary data fetch for {symbol}...")
                
                if resolution == '1D':
                    # For daily data, always get at least 2 years of history
                    hist = ticker.history(period='2y', interval='1d', timeout=20)
                    print(f"📊 Daily data: 2y period, 1d interval")
                    
                elif resolution in ['240', '4h']:
                    # For 4-hour data, get 3 months of hourly data
                    hist = ticker.history(period='3mo', interval='1h', timeout=20)
                    print(f"📊 4-hour data: 3mo period, 1h interval")
                    
                elif resolution in ['60', '1h']:
                    # For hourly data, get 2 months
                    hist = ticker.history(period='2mo', interval='1h', timeout=20)
                    print(f"📊 Hourly data: 2mo period, 1h interval")
                    
                elif resolution in ['30']:
                    # For 30-minute data, get 1 month
                    hist = ticker.history(period='1mo', interval='30m', timeout=20)
                    print(f"📊 30-minute data: 1mo period, 30m interval")
                    
                elif resolution in ['15']:
                    # For 15-minute data, get 2 weeks
                    hist = ticker.history(period='1mo', interval='15m', timeout=20)
                    print(f"📊 15-minute data: 1mo period, 15m interval")
                    
                elif resolution in ['5']:
                    # For 5-minute data, get 1 week
                    hist = ticker.history(period='1mo', interval='5m', timeout=20)
                    print(f"📊 5-minute data: 1mo period, 5m interval")
                    
                else:  # 1-minute or other
                    # For 1-minute data, get 5 days
                    hist = ticker.history(period='5d', interval='1m', timeout=20)
                    print(f"📊 1-minute data: 5d period, 1m interval")
                
                print(f"📋 Primary fetch result: {hist.shape}")
                
            except Exception as primary_error:
                print(f"⚠️ Primary fetch failed: {primary_error}")
                print(f"🔄 Fallback to daily data...")
                
                # Fallback: Always try to get daily data as last resort
                try:
                    hist = ticker.history(period='max', interval='1d', timeout=30)
                    print(f"📊 Fallback daily data: max period, 1d interval")
                except Exception as fallback_error:
                    print(f"❌ Fallback failed: {fallback_error}")
                    hist = pd.DataFrame()  # Empty dataframe
            
            # Log data details
            if not hist.empty:
                print(f"📋 Data shape: {hist.shape}")
                print(f"📋 Columns: {list(hist.columns)}")
                print(f"📋 Date range: {hist.index[0]} to {hist.index[-1]}")
                print(f"📋 Sample recent data:\n{hist.tail(2)}")
            else:
                print(f"⚠️ No data returned for {symbol}")
            
            # If still no data, return a helpful error
            if hist.empty:
                print(f"❌ No historical data available for {symbol}")
                error_response = {
                    "s": "no_data",
                    "errmsg": f"No historical data available for {symbol}. This symbol may not exist or may not be supported by Yahoo Finance."
                }
                self.send_json_response(error_response)
                return
            
            # Clean and validate data
            original_count = len(hist)
            hist = hist.dropna()  # Remove NaN rows
            cleaned_count = len(hist)
            
            if cleaned_count < original_count:
                print(f"🧹 Cleaned data: removed {original_count - cleaned_count} invalid rows")
            
            if hist.empty:
                print(f"⚠️ All data was invalid for {symbol}")
                self.send_json_response({"s": "no_data", "errmsg": "All historical data contained invalid values"})
                return
            
            # Convert to TradingView format - ROBUST CONVERSION
            bars = []
            processed_count = 0
            
            for index, row in hist.iterrows():
                try:
                    # Handle different timestamp formats
                    if hasattr(index, 'timestamp'):
                        timestamp = int(index.timestamp())
                    elif hasattr(index, 'value'):
                        timestamp = int(index.value // 10**9)  # Convert nanoseconds to seconds
                    else:
                        # Fallback: convert to pandas timestamp then to unix
                        ts = pd.Timestamp(index)
                        timestamp = int(ts.timestamp())
                    
                    # Validate timestamp is reasonable (not negative, not too far in future)
                    if timestamp < 0 or timestamp > current_time + 86400:  # Not more than 1 day in future
                        continue
                    
                    # Validate price data
                    if pd.isna(row['Open']) or pd.isna(row['Close']) or pd.isna(row['High']) or pd.isna(row['Low']):
                        continue
                    
                    # Ensure prices are positive
                    if row['Open'] <= 0 or row['Close'] <= 0 or row['High'] <= 0 or row['Low'] <= 0:
                        continue
                        
                    bar = {
                        "time": timestamp * 1000,  # TradingView expects milliseconds
                        "open": round(float(row['Open']), 4),
                        "high": round(float(row['High']), 4),
                        "low": round(float(row['Low']), 4),
                        "close": round(float(row['Close']), 4),
                        "volume": int(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) and row['Volume'] >= 0 else 0
                    }
                    
                    # Apply time filtering if valid timestamps
                    if from_ts > 0 and to_ts > from_ts:
                        if timestamp < from_ts or timestamp > to_ts:
                            continue
                    
                    bars.append(bar)
                    processed_count += 1
                    
                except Exception as bar_error:
                    print(f"⚠️ Error processing bar {index}: {bar_error}")
                    continue
            
            print(f"📊 Processed {processed_count} valid bars from {len(hist)} raw records")
            
            if not bars:
                print(f"❌ No valid bars after processing for {symbol}")
                # Return at least some data message instead of complete failure
                self.send_json_response({
                    "s": "no_data", 
                    "errmsg": f"No valid data in requested time range for {symbol}. Try a different time range or resolution."
                })
                return
            
            # Sort by time to ensure correct chronological order
            bars.sort(key=lambda x: x['time'])
            
            # Limit to requested count if countback is specified and reasonable
            if countback > 0 and countback < len(bars):
                bars = bars[-countback:]  # Get the most recent countback bars
                print(f"📊 Limited to most recent {countback} bars")
            
            # Create TradingView response format
            response = {
                "s": "ok",
                "t": [bar["time"] for bar in bars],
                "o": [bar["open"] for bar in bars],
                "h": [bar["high"] for bar in bars],
                "l": [bar["low"] for bar in bars],
                "c": [bar["close"] for bar in bars],
                "v": [bar["volume"] for bar in bars]
            }
            
            # Cache the response for 60 seconds
            self.data_cache[cache_key] = response
            self.cache_expiry[cache_key] = current_cache_time + 60
            
            # Log success details
            if bars:
                first_time = datetime.fromtimestamp(bars[0]['time']/1000)
                latest_time = datetime.fromtimestamp(bars[-1]['time']/1000)
                print(f"✅ Successfully fetched {len(bars)} bars for {symbol}")
                print(f"📊 Time range: {first_time} to {latest_time}")
                print(f"� Price range: ${bars[0]['close']:.2f} to ${bars[-1]['close']:.2f}")
                print(f"📈 Latest price: ${bars[-1]['close']:.2f}")
            
            self.send_json_response(response)
            
        except Exception as e:
            print(f"❌ Critical error fetching data for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            
            # Try to return a more helpful error response
            error_response = {
                "s": "error", 
                "errmsg": f"Server error while fetching {symbol}: {str(e)}. Please try again or contact support."
            }
            self.send_json_response(error_response)
    
    def handle_time(self):
        """Return current server time"""
        current_time = int(time.time())
        self.send_json_response(current_time)
    
    def handle_test(self):
        """Test endpoint to verify server functionality"""
        try:
            # Quick test with Apple stock
            ticker = yf.Ticker('AAPL')
            hist = ticker.history(period='1d', interval='1d', timeout=5)
            
            test_result = {
                "status": "ok",
                "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "yfinance_working": not hist.empty,
                "data_points": len(hist) if not hist.empty else 0,
                "symbols_available": len(self.ALL_SYMBOLS),
                "latest_price": float(hist['Close'].iloc[-1]) if not hist.empty else "No data"
            }
            
            print(f"🧪 Test endpoint called - yfinance working: {not hist.empty}")
            
        except Exception as e:
            test_result = {
                "status": "error",
                "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "error": str(e),
                "symbols_available": len(self.ALL_SYMBOLS)
            }
            
        self.send_json_response(test_result)
    
    def handle_status(self):
        """Return server status and statistics"""
        status = {
            "server": "Live Market Data Server",
            "status": "running",
            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "symbols_count": len(self.ALL_SYMBOLS),
            "cache_entries": len(self.data_cache),
            "supported_resolutions": ["1", "5", "15", "30", "60", "240", "1D"],
            "data_source": "Yahoo Finance (yfinance)",
            "version": "2.0"
        }
        self.send_json_response(status)
    
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
