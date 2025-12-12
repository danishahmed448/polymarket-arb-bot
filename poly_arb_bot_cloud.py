import time
import json
import os
import requests
import logging
import threading
import concurrent.futures
import html  # Added for dashboard security
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

# Merge function imports
from web3 import Web3
from eth_account import Account
from abi.ctf_abi import ctf_abi
from abi.safe_abi import safe_abi

# --- Configuration ---
INITIAL_BALANCE = 1000.0      # Virtual balance for dashboard display only
MIN_SPREAD_TARGET = 0.98       # ⚠️ TESTING MODE: Breakeven (CHANGE BACK TO 0.98!)
POLL_INTERVAL = 1.0           # Fast polling
BET_SIZE = 5.0                # Must be at least $5 to meet minimum 5 share requirement
MIN_SHARES = 5.0              # Polymarket minimum order size
PROFIT_THRESHOLD = 0.001
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WEB_PORT = int(os.environ.get('PORT', 8080))

# --- Merge Function Constants (Polygon Mainnet) ---
CONDITIONAL_TOKENS_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
NEG_RISK_ADAPTER_ADDRESS = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'
USDC_ADDRESS = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'
USDCE_DIGITS = 6
RPC_URL = os.environ.get('RPC_URL', 'https://polygon-rpc.com/')

# --- Multi-Timeframe Configuration ---
# Discovered from Polymarket API (Chrome Network Tab)
TIMEFRAME_CONFIG = {
    '15-min': {
        'endpoint': 'events/pagination',
        'params': {
            'limit': 100, 'active': 'true', 'archived': 'false',
            'tag_slug': '15M', 'closed': 'false',
            'order': 'volume24hr', 'ascending': 'false', 'offset': 0
        }
    },
    '1-hour': {
        'endpoint': 'events/pagination',
        'params': {
            'limit': 100, 'active': 'true', 'archived': 'false',
            'tag_slug': '1H', 'closed': 'false',
            'order': 'volume24hr', 'ascending': 'false', 'offset': 0
        }
    },
    '4-hour': {
        'endpoint': 'events',
        'params': {
            'tag_id': 102531, 'closed': 'false', 'limit': 100
        }
    },
    'Daily': {
        'endpoint': 'events/pagination',
        'params': {
            'limit': 100, 'active': 'true', 'archived': 'false',
            'tag_slug': 'daily', 'closed': 'false',
            'order': 'volume24hr', 'ascending': 'false', 'offset': 0
        }
    },
}

# Which timeframes to scan (set to True to enable)
ENABLED_TIMEFRAMES = {
    '15-min': True,
    '1-hour': True,
    '4-hour': True,
    'Daily': True,
}

COINS = ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol', 'xrp']


def detect_coin(text):
    """Detect which coin from text."""
    text = text.lower()
    if 'bitcoin' in text or 'btc' in text:
        return 'BTC'
    elif 'ethereum' in text or 'eth ' in text or text.startswith('eth'):
        return 'ETH'
    elif 'solana' in text or 'sol ' in text or text.startswith('sol'):
        return 'SOL'
    elif 'xrp' in text:
        return 'XRP'
    return None


def is_market_live(market, event):
    """Check if market is currently live (not closed, not past end date)."""
    from datetime import datetime, timezone
    
    if market.get('closed') == True:
        return False
    
    end_date = market.get('endDate') or market.get('end_date_iso') or event.get('endDate')
    if end_date:
        try:
            if isinstance(end_date, str):
                if end_date.endswith('Z'):
                    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                else:
                    end_dt = datetime.fromisoformat(end_date)
                
                now = datetime.now(timezone.utc)
                if end_dt < now:
                    return False
        except:
            pass
    
    return True


# Speed Optimization Settings
MAX_WORKERS = 10              # Parallel order book fetches
MAX_REQUESTS_PER_SECOND = 8   # Rate limit to avoid 429s

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Thread lock for safe access to shared state
status_lock = threading.Lock()

# Global state for web dashboard
bot_status = {
    'balance': 0.0,
    'total_trades': 0,
    'markets_count': 0,
    'best_spread': 1.02,
    'checks': 0,
    'last_update': '',
    'recent_checks': [],
    'running': True
}

class RateLimiter:
    """Thread-safe rate limiter to avoid API 429 errors."""
    def __init__(self, max_per_second=MAX_REQUESTS_PER_SECOND):
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0
        self.lock = threading.Lock()
    
    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

# Global rate limiter for CLOB API
clob_rate_limiter = RateLimiter(max_per_second=MAX_REQUESTS_PER_SECOND)

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        if self.path == '/api/status':
            with status_lock:
                payload = json.dumps(bot_status)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(payload.encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(self.get_dashboard_html().encode('utf-8'))
    
    def get_dashboard_html(self):
        with status_lock:
            balance = bot_status['balance']
            total_trades = bot_status['total_trades']
            markets_count = bot_status['markets_count']
            best_spread = bot_status['best_spread']
            checks = bot_status['checks']
            last_update = bot_status['last_update']
            recent_checks = list(bot_status.get('recent_checks', [])[-10:])
        
        # Coin colors mapping
        coin_colors = {
            'BTC': '#f7931a',
            'ETH': '#627eea', 
            'SOL': '#9945ff',
            'XRP': '#ffffff'
        }
        
        recent_html = ''
        for i, check in enumerate(reversed(recent_checks)):
            total = check.get('total', 1.02)
            if total < 1.00:
                badge_class, badge_text = 'badge-arb', 'ARB!'
            elif total < 1.01:
                badge_class, badge_text = 'badge-close', 'CLOSE'
            elif total <= 1.02:
                badge_class, badge_text = 'badge-ok', 'OK'
            else:
                badge_class, badge_text = 'badge-wait', 'WAIT'
            
            q = check.get('q', 'Unknown')
            coin = 'BTC' if 'Bitcoin' in q else 'ETH' if 'Ethereum' in q else 'SOL' if 'Solana' in q else 'XRP'
            coin_color = coin_colors.get(coin, '#64748b')
            safe_q = html.escape(q)
            
            # Extract timeframe from question
            timeframe = '15m' if '15' in q else '1H' if 'AM ET' in q or 'PM ET' in q else '4H' if 'AM-' in q or 'PM-' in q else 'D'
            
            recent_html += f'''
            <div class="market-card" style="animation-delay: {i * 0.05}s">
                <div class="market-header">
                    <span class="coin-badge" style="background: {coin_color}20; color: {coin_color}; border: 1px solid {coin_color}40">{coin}</span>
                    <span class="timeframe-badge">{timeframe}</span>
                </div>
                <div class="market-title">{safe_q}</div>
                <div class="market-footer">
                    <div class="spread-info">
                        <span class="spread-label">Spread</span>
                        <span class="spread-value">{total:.4f}</span>
                    </div>
                    <span class="{badge_class}">{badge_text}</span>
                </div>
            </div>'''
        
        # Determine status indicator
        status_color = '#10b981' if markets_count > 0 else '#f59e0b'
        status_text = 'LIVE' if markets_count > 0 else 'CONNECTING...'
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
    <title>Polymarket Arbitrage Bot</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        :root {{
            --bg-primary: #0a0a0f;
            --bg-card: rgba(255,255,255,0.03);
            --bg-card-hover: rgba(255,255,255,0.06);
            --border-color: rgba(255,255,255,0.08);
            --text-primary: #ffffff;
            --text-secondary: #94a3b8;
            --accent-green: #10b981;
            --accent-yellow: #f59e0b;
            --accent-blue: #3b82f6;
            --accent-red: #ef4444;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            background-image: radial-gradient(ellipse at top, #1a1a2e 0%, var(--bg-primary) 50%);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-x: hidden;
        }}
        
        .container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 16px;
            padding-bottom: 80px;
        }}
        
        /* Header */
        .header {{
            text-align: center;
            padding: 24px 0 20px;
        }}
        
        .logo {{
            font-size: 28px;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 12px;
        }}
        
        .status-pill {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background: {status_color}15;
            border: 1px solid {status_color}40;
            padding: 8px 16px;
            border-radius: 50px;
            font-size: 12px;
            font-weight: 600;
            color: {status_color};
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .status-dot {{
            width: 8px;
            height: 8px;
            background: {status_color};
            border-radius: 50%;
            animation: pulse 2s infinite;
        }}
        
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.5; transform: scale(0.8); }}
        }}
        
        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin: 20px 0;
        }}
        
        .stat-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 16px;
            text-align: center;
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
        }}
        
        .stat-card:hover {{
            background: var(--bg-card-hover);
            transform: translateY(-2px);
        }}
        
        .stat-icon {{
            font-size: 20px;
            margin-bottom: 8px;
        }}
        
        .stat-value {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 4px;
            color: var(--text-primary);
        }}
        
        .stat-value.green {{ color: var(--accent-green); }}
        .stat-value.blue {{ color: var(--accent-blue); }}
        .stat-value.yellow {{ color: var(--accent-yellow); }}
        
        .stat-label {{
            font-size: 11px;
            color: var(--text-secondary);
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        /* Section */
        .section {{
            margin: 24px 0;
        }}
        
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        
        .section-title {{
            font-size: 16px;
            font-weight: 600;
            color: var(--text-primary);
        }}
        
        .section-badge {{
            font-size: 11px;
            color: var(--text-secondary);
            background: var(--bg-card);
            padding: 4px 10px;
            border-radius: 20px;
        }}
        
        /* Market Cards */
        .markets-grid {{
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}
        
        .market-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 14px;
            padding: 14px;
            animation: fadeInUp 0.4s ease forwards;
            opacity: 0;
            transform: translateY(10px);
        }}
        
        @keyframes fadeInUp {{
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        
        .market-header {{
            display: flex;
            gap: 8px;
            margin-bottom: 8px;
        }}
        
        .coin-badge {{
            font-size: 11px;
            font-weight: 700;
            padding: 4px 8px;
            border-radius: 6px;
        }}
        
        .timeframe-badge {{
            font-size: 10px;
            font-weight: 600;
            padding: 4px 8px;
            border-radius: 6px;
            background: rgba(100,116,139,0.15);
            color: #94a3b8;
        }}
        
        .market-title {{
            font-size: 13px;
            color: var(--text-secondary);
            margin-bottom: 10px;
            line-height: 1.4;
            word-break: break-word;
        }}
        
        .market-footer {{
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .spread-info {{
            display: flex;
            flex-direction: column;
        }}
        
        .spread-label {{
            font-size: 10px;
            color: var(--text-secondary);
            text-transform: uppercase;
        }}
        
        .spread-value {{
            font-size: 16px;
            font-weight: 700;
            font-family: 'SF Mono', 'Monaco', monospace;
        }}
        
        /* Badges */
        .badge-arb {{
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 700;
            animation: glow 1.5s infinite alternate;
        }}
        
        @keyframes glow {{
            from {{ box-shadow: 0 0 5px #10b98150; }}
            to {{ box-shadow: 0 0 20px #10b98180; }}
        }}
        
        .badge-close {{
            background: rgba(245, 158, 11, 0.15);
            color: #f59e0b;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 600;
        }}
        
        .badge-ok {{
            background: rgba(59, 130, 246, 0.15);
            color: #3b82f6;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 600;
        }}
        
        .badge-wait {{
            background: rgba(100, 116, 139, 0.15);
            color: #64748b;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 600;
        }}
        
        /* Footer */
        .footer {{
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: linear-gradient(to top, var(--bg-primary) 60%, transparent);
            padding: 20px;
            text-align: center;
        }}
        
        .footer-text {{
            font-size: 11px;
            color: var(--text-secondary);
        }}
        
        .refresh-indicator {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            margin-top: 8px;
            font-size: 10px;
            color: var(--accent-green);
        }}
        
        .refresh-dot {{
            width: 6px;
            height: 6px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: blink 1s infinite;
        }}
        
        @keyframes blink {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.3; }}
        }}
        
        /* Empty State */
        .empty-state {{
            text-align: center;
            padding: 40px 20px;
            color: var(--text-secondary);
        }}
        
        .empty-icon {{
            font-size: 40px;
            margin-bottom: 12px;
            opacity: 0.5;
        }}
        
        /* Responsive */
        @media (min-width: 480px) {{
            .stats-grid {{ grid-template-columns: repeat(4, 1fr); }}
            .stat-value {{ font-size: 28px; }}
            .logo {{ font-size: 32px; }}
        }}
    </style>
    <script>
        setTimeout(() => location.reload(), 5000);
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">Poly Arb Bot</div>
            <div class="status-pill">
                <span class="status-dot"></span>
                {status_text}
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-icon">💰</div>
                <div class="stat-value green">${balance:.2f}</div>
                <div class="stat-label">Balance</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">📈</div>
                <div class="stat-value blue">{total_trades}</div>
                <div class="stat-label">Trades</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">🎯</div>
                <div class="stat-value">{markets_count}</div>
                <div class="stat-label">Markets</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">⚡</div>
                <div class="stat-value yellow">{best_spread:.3f}</div>
                <div class="stat-label">Best Spread</div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">
                <span class="section-title">Live Market Scans</span>
                <span class="section-badge">{checks} checks</span>
            </div>
            <div class="markets-grid">
                {recent_html if recent_html else '<div class="empty-state"><div class="empty-icon">🔍</div><div>Scanning markets...</div></div>'}
            </div>
        </div>
    </div>
    
    <div class="footer">
        <div class="footer-text">Last update: {last_update or 'Starting...'}</div>
        <div class="refresh-indicator">
            <span class="refresh-dot"></span>
            Auto-refresh in 5s
        </div>
    </div>
</body>
</html>'''

class RealMoneyClient:
    def __init__(self):
        self.host = "https://clob.polymarket.com"
        self.key = os.environ.get("PRIVATE_KEY") 
        self.funder = os.environ.get("FUNDER_ADDRESS")
        self.chain_id = 137  # Polygon Mainnet
        self.total_trades = 0
        self.balance = 0.0
        self.client = None
        self.traded_markets = set()  # Track markets we've already traded
        
        # Log env var status for debugging
        logger.info(f"🔑 PRIVATE_KEY set: {bool(self.key)}")
        logger.info(f"🔑 FUNDER_ADDRESS set: {bool(self.funder)}")
        
        try:
            # Initialize client (following official docs)
            self.client = ClobClient(
                self.host,
                key=self.key,
                chain_id=self.chain_id,
                signature_type=2,  # 2 for Polymarket proxy wallet
                funder=self.funder  # Use Polymarket profile address, NOT MetaMask!
            )
            
            # Derive API credentials automatically (official method)
            self.client.set_api_creds(self.client.create_or_derive_api_creds())
            logger.info("✅ Connected to Polymarket CLOB Client (Real Money)")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to CLOB: {e}")
            self.client = None
        
        self.update_balance()

    def update_balance(self):
        """Fetch real USDC balance from Polygon."""
        try:
            if not self.funder:
                self.balance = 0.0
                return
            
            # ERC20 balanceOf ABI
            erc20_abi = [{
                "inputs": [{"name": "account", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function"
            }]
            
            w3 = Web3(Web3.HTTPProvider(RPC_URL))
            usdc = w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS),
                abi=erc20_abi
            )
            
            balance_wei = usdc.functions.balanceOf(
                Web3.to_checksum_address(self.funder)
            ).call()
            
            self.balance = balance_wei / (10 ** USDCE_DIGITS)
            logger.info(f"💰 USDC Balance: ${self.balance:.2f}")
            
        except Exception as e:
            logger.warning(f"Failed to fetch USDC balance: {e}")
            self.balance = 0.0
    
    def has_traded_market(self, condition_id):
        """Check if we've already traded this market."""
        return condition_id in self.traded_markets
    
    def mark_market_traded(self, condition_id):
        """Mark a market as traded to prevent re-trading."""
        self.traded_markets.add(condition_id)
        logger.info(f"📝 Marked market {condition_id[:16]}... as traded")

    def execute_pair_buy(self, tokens, up_price, down_price, shares, condition_id=None):
        """
        Execute paired FOK buy orders using MarketOrderArgs.
        
        SAFER APPROACH:
        - Use MarketOrderArgs with cash amounts
        - Calculate cash to get exactly MIN_SHARES on each side
        - Use FOK for guaranteed fill-or-kill (no partial fills!)
        - Both orders must succeed or we don't trade
        
        This ensures EQUAL shares on both sides by:
        1. Targeting exactly 5 shares per side
        2. Calculating cash needed: 5 shares × price = cash
        3. Using FOK to guarantee the shares we want
        """
        if self.client is None:
            logger.error("❌ Cannot trade: CLOB Client not connected!")
            return False
        
        # Check if we've already traded this market
        if condition_id and self.has_traded_market(condition_id):
            logger.info(f"⏭️ Already traded this market, skipping to avoid duplicates")
            return False
            
        token_yes = tokens[0]
        token_no = tokens[1]
        
        try:
            # Calculate the cost and validate spread
            total_spread = up_price + down_price
            
            # SANITY CHECK: Spreads over 1.5 indicate dead/expired markets
            if total_spread > 1.5:
                logger.warning(f"⚠️ Spread {total_spread:.4f} > 1.5 indicates dead/expired market, skipping")
                return False
            
            # STRICTER threshold - only trade very good spreads
            if total_spread >= MIN_SPREAD_TARGET:
                logger.info(f"Spread {total_spread:.4f} >= {MIN_SPREAD_TARGET}, not profitable enough, skipping")
                return False
            
            # PRE-TRADE LIQUIDITY CHECK: Ensure enough depth exists
            try:
                ob_yes = self.client.get_order_book(token_yes)
                ob_no = self.client.get_order_book(token_no)
                
                def get_liquidity(ob, side='bids'):
                    """Get total liquidity value in USD for given side."""
                    items = []
                    if hasattr(ob, side) and getattr(ob, side):
                        items = getattr(ob, side)
                    elif isinstance(ob, dict) and ob.get(side):
                        items = ob[side]
                    
                    total_liquidity = 0
                    for item in items:
                        price = float(item.price) if hasattr(item, 'price') else float(item['price'])
                        size = float(item.size) if hasattr(item, 'size') else float(item['size'])
                        total_liquidity += price * size
                    return total_liquidity
                
                # Check ASK liquidity (needed to BUY)
                yes_ask_liquidity = get_liquidity(ob_yes, 'asks')
                no_ask_liquidity = get_liquidity(ob_no, 'asks')
                
                min_buy_liquidity = BET_SIZE * 0.8  # Need 80% of bet size in asks to buy
                
                if yes_ask_liquidity < min_buy_liquidity:
                    logger.warning(f"⚠️ Insufficient YES ask liquidity (${yes_ask_liquidity:.2f}), skipping trade")
                    return False
                
                if no_ask_liquidity < min_buy_liquidity:
                    logger.warning(f"⚠️ Insufficient NO ask liquidity (${no_ask_liquidity:.2f}), skipping trade")
                    return False
                
                # Check BID liquidity (needed for emergency exit)
                yes_bid_liquidity = get_liquidity(ob_yes, 'bids')
                no_bid_liquidity = get_liquidity(ob_no, 'bids')
                
                min_exit_liquidity = BET_SIZE * 0.5  # Need at least 50% of bet size in bids to exit
                
                if yes_bid_liquidity < min_exit_liquidity:
                    logger.warning(f"⚠️ Insufficient YES bid liquidity (${yes_bid_liquidity:.2f}), skipping trade")
                    return False
                
                if no_bid_liquidity < min_exit_liquidity:
                    logger.warning(f"⚠️ Insufficient NO bid liquidity (${no_bid_liquidity:.2f}), skipping trade")
                    return False
                    
                logger.info(f"✅ Liquidity check passed: YES asks ${yes_ask_liquidity:.2f}, NO asks ${no_ask_liquidity:.2f}")
                
            except Exception as liq_e:
                logger.warning(f"⚠️ Liquidity check failed: {liq_e}, proceeding with caution")
            
            # Target exactly MIN_SHARES on each side
            target_shares = MIN_SHARES  # 5 shares
            
            # Calculate CASH needed to buy target_shares at each price
            # Cash = shares × price (rounded to 2 decimals for API)
            cash_for_yes = round(target_shares * up_price, 2)
            cash_for_no = round(target_shares * down_price, 2)
            
            # Ensure minimum cash amounts
            if cash_for_yes < 0.10 or cash_for_no < 0.10:
                logger.warning("Cash amount too small, skipping trade")
                return False
            
            total_cost = cash_for_yes + cash_for_no
            expected_payout = target_shares * 1.00  # $1 per pair at resolution
            expected_profit = expected_payout - total_cost
            
            logger.info(f"🎯 Sending FOK Limit Orders (EXACT SHARES - OrderArgs):")
            logger.info(f"   Target: {target_shares} shares on EACH side")
            logger.info(f"   YES: ${cash_for_yes} to buy ~{target_shares} shares @ {up_price}")
            logger.info(f"   NO:  ${cash_for_no} to buy ~{target_shares} shares @ {down_price}")
            logger.info(f"   Total: ${total_cost:.2f} | Payout: ${expected_payout:.2f} | Profit: ${expected_profit:.2f}")

            # Create LIMIT Orders with EXACT share amounts (not cash!)
            # This ensures we get exactly the same number of shares on each side
            order_args_yes = OrderArgs(
                token_id=token_yes,
                size=target_shares,  # Exact number of shares
                price=up_price,      # Limit price
                side=BUY
            )
            
            order_args_no = OrderArgs(
                token_id=token_no,
                size=target_shares,  # Same exact number of shares
                price=down_price,    # Limit price
                side=BUY
            )
            
            # Execute YES order with FOK
            signed_yes = self.client.create_order(order_args_yes)
            resp_yes = self.client.post_order(signed_yes, OrderType.FOK)
            logger.info(f"YES Order Response: {resp_yes}")
            
            yes_success = resp_yes.get("success", False) if isinstance(resp_yes, dict) else False
            
            if not yes_success:
                error_msg = resp_yes.get("error", "Unknown error") if isinstance(resp_yes, dict) else str(resp_yes)
                logger.warning(f"⚠️ YES order not filled (FOK killed): {error_msg}")
                return False  # No need to cancel - FOK means nothing was filled
            
            # Execute NO order with FOK
            signed_no = self.client.create_order(order_args_no)
            resp_no = self.client.post_order(signed_no, OrderType.FOK)
            logger.info(f"NO Order Response: {resp_no}")
            
            no_success = resp_no.get("success", False) if isinstance(resp_no, dict) else False
            
            if not no_success:
                error_msg = resp_no.get("error", "Unknown error") if isinstance(resp_no, dict) else str(resp_no)
                logger.error(f"⚠️ NO order not filled but YES was! ATTEMPTING EMERGENCY EXIT...")
                logger.error(f"   Original error: {error_msg}")
                
                # EMERGENCY EXIT: Try to sell the YES position immediately
                try:
                    # Create a market SELL order for YES to exit position
                    exit_order = MarketOrderArgs(
                        token_id=token_yes,
                        amount=cash_for_yes,  # Sell back approximately what we bought
                        side=SELL
                    )
                    signed_exit = self.client.create_market_order(exit_order)
                    resp_exit = self.client.post_order(signed_exit, OrderType.FOK)
                    
                    exit_success = resp_exit.get("success", False) if isinstance(resp_exit, dict) else False
                    
                    if exit_success:
                        logger.info(f"🆘 EMERGENCY EXIT SUCCESSFUL! Sold YES position to minimize loss")
                    else:
                        exit_error = resp_exit.get("error", "Unknown") if isinstance(resp_exit, dict) else str(resp_exit)
                        logger.error(f"❌ EMERGENCY EXIT FAILED! You have an unhedged YES position!")
                        logger.error(f"   Exit error: {exit_error}")
                        logger.error(f"   Token: {token_yes}")
                        logger.error(f"   ACTION REQUIRED: Manually sell on Polymarket website!")
                except Exception as exit_e:
                    logger.error(f"❌ EMERGENCY EXIT EXCEPTION: {exit_e}")
                    logger.error(f"   You have an unhedged YES position on token: {token_yes}")
                
                return False

            # BOTH orders succeeded!
            logger.info(f"✅ ARBITRAGE EXECUTED: ~{target_shares} pairs @ spread {total_spread:.4f}")
            logger.info(f"   Expected profit at resolution: ${expected_profit:.2f}")
            
            # Mark this market as traded to prevent re-trading
            if condition_id:
                self.mark_market_traded(condition_id)
            
            self.total_trades += 1
            return True
                
        except Exception as e:
            logger.error(f"Real Trade Exception: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def merge_and_settle(self, condition_id, amount=None, neg_risk=False):
        """
        Merge conditional tokens back to USDC.
        
        Args:
            condition_id: The condition ID (bytes32 hex string)
            amount: Amount to merge in USDC (e.g., "1.5"). If None, merges all available tokens
            neg_risk: Whether to use NEG_RISK_ADAPTER (default: False)
        
        Returns:
            bool: True if merge successful, False otherwise
        """
        try:
            # Check if env vars are set
            proxy_wallet = os.environ.get("FUNDER_ADDRESS")  # Polymarket profile address
            private_key = os.environ.get("PRIVATE_KEY")
            
            if not proxy_wallet or not private_key:
                logger.info("💰 Arbed! (Manual Merge - PROXY_WALLET not configured)")
                return False
            
            # Connect to Polygon
            w3 = Web3(Web3.HTTPProvider(RPC_URL))
            
            # Inject POA middleware for Polygon
            from web3.middleware import ExtraDataToPOAMiddleware
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            
            # Load wallet and safe
            account = Account.from_key(private_key)
            safe_address = Web3.to_checksum_address(proxy_wallet)
            
            safe = w3.eth.contract(address=safe_address, abi=safe_abi)
            
            # Determine merge amount
            if amount is None:
                # Get minimum balance of both positions
                ctf_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(CONDITIONAL_TOKENS_ADDRESS),
                    abi=ctf_abi
                )
                
                parent_collection_id = bytes(32)
                collection_id_0 = ctf_contract.functions.getCollectionId(
                    parent_collection_id, bytes.fromhex(condition_id[2:] if condition_id.startswith('0x') else condition_id), 1
                ).call()
                collection_id_1 = ctf_contract.functions.getCollectionId(
                    parent_collection_id, bytes.fromhex(condition_id[2:] if condition_id.startswith('0x') else condition_id), 2
                ).call()
                
                position_id_0 = ctf_contract.functions.getPositionId(
                    Web3.to_checksum_address(USDC_ADDRESS), collection_id_0
                ).call()
                position_id_1 = ctf_contract.functions.getPositionId(
                    Web3.to_checksum_address(USDC_ADDRESS), collection_id_1
                ).call()
                
                balance_0 = ctf_contract.functions.balanceOf(safe_address, position_id_0).call()
                balance_1 = ctf_contract.functions.balanceOf(safe_address, position_id_1).call()
                
                logger.info(f"Merge check: YES balance={balance_0}, NO balance={balance_1}")
                logger.info(f"Condition ID: {condition_id}")
                
                amount_wei = min(balance_0, balance_1)
                
                if amount_wei == 0:
                    logger.warning(f"Merge failed: No tokens to merge (YES={balance_0}, NO={balance_1})")
                    return False
            else:
                amount_wei = int(float(amount) * (10 ** USDCE_DIGITS))
            
            # Encode merge transaction
            ctf_contract = w3.eth.contract(abi=ctf_abi)
            parent_collection_id = '0x0000000000000000000000000000000000000000000000000000000000000000'
            partition = [1, 2]
            
            cond_id_bytes = condition_id[2:] if condition_id.startswith('0x') else condition_id
            
            data = ctf_contract.functions.mergePositions(
                Web3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(parent_collection_id[2:]),
                bytes.fromhex(cond_id_bytes),
                partition,
                amount_wei
            )._encode_transaction_data()
            
            # Sign and execute Safe transaction
            nonce = safe.functions.nonce().call()
            to = NEG_RISK_ADAPTER_ADDRESS if neg_risk else CONDITIONAL_TOKENS_ADDRESS
            
            tx_hash = safe.functions.getTransactionHash(
                Web3.to_checksum_address(to),
                0,
                bytes.fromhex(data[2:]),
                0,  # operation: Call
                0, 0, 0,  # safeTxGas, baseGas, gasPrice
                '0x0000000000000000000000000000000000000000',  # gasToken
                '0x0000000000000000000000000000000000000000',  # refundReceiver
                nonce
            ).call()
            
            # Sign the hash
            hash_bytes = Web3.to_bytes(hexstr=tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
            signature_obj = account.unsafe_sign_hash(hash_bytes)
            
            r = signature_obj.r.to_bytes(32, byteorder='big')
            s = signature_obj.s.to_bytes(32, byteorder='big')
            v = signature_obj.v.to_bytes(1, byteorder='big')
            signature = r + s + v
            
            # Build and send transaction
            tx = safe.functions.execTransaction(
                Web3.to_checksum_address(to),
                0,
                bytes.fromhex(data[2:]),
                0,
                0, 0, 0,
                '0x0000000000000000000000000000000000000000',
                '0x0000000000000000000000000000000000000000',
                signature
            ).build_transaction({
                'from': account.address,
                'nonce': w3.eth.get_transaction_count(account.address),
                'gas': 500000,
                'gasPrice': w3.eth.gas_price,
            })
            
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for receipt
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Check if transaction succeeded
            if receipt['status'] == 1:
                logger.info(f"💰 Merge successful! Amount: {amount_wei / 10**USDCE_DIGITS} USDC")
                return True
            else:
                logger.error("Merge failed: Transaction reverted")
                return False
                
        except Exception as e:
            logger.error(f"Merge failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

class ArbitrageBot:
    def __init__(self):
        self.clob_client = ClobClient(host=CLOB_HOST, key=None, chain_id=137)
        self.real_client = RealMoneyClient()
        self.target_markets = []
        self.last_scan_time = 0
        
        with status_lock:
            bot_status['balance'] = self.real_client.balance
            bot_status['total_trades'] = self.real_client.total_trades

    def scan_markets(self):
        """Scan all enabled timeframes for LIVE crypto Up/Down markets only."""
        try:
            # Refresh USDC balance
            self.real_client.update_balance()
            
            found = []
            
            logger.info("🔄 Scanning LIVE crypto markets (4 per timeframe)...")
            
            for timeframe, config in TIMEFRAME_CONFIG.items():
                # Skip disabled timeframes
                if not ENABLED_TIMEFRAMES.get(timeframe, False):
                    continue
                
                url = f"{GAMMA_API_URL}/{config['endpoint']}"
                
                try:
                    resp = requests.get(url, params=config['params'], timeout=30)
                    
                    if resp.status_code != 200:
                        logger.warning(f"  {timeframe}: API error {resp.status_code}")
                        continue
                    
                    data = resp.json()
                    
                    # Handle different response formats
                    if isinstance(data, dict):
                        events = data.get('data', data.get('events', []))
                    else:
                        events = data
                    
                    # Collect all live markets by coin, then pick soonest ending for each
                    coin_markets = {'BTC': [], 'ETH': [], 'SOL': [], 'XRP': []}
                    
                    for event in events:
                        title = event.get('title', '')
                        slug = event.get('slug', '')
                        
                        # STRICT FILTER: Only "Up or Down" markets, not ETF flows etc
                        if 'up or down' not in title.lower() and 'updown' not in slug.lower():
                            continue
                        
                        coin = detect_coin(title + ' ' + slug)
                        if not coin:
                            continue
                        
                        # Extract markets from event
                        markets = event.get('markets', [])
                        
                        for m in markets:
                            # Skip non-live markets
                            if not is_market_live(m, event):
                                continue
                            
                            # Parse tokens
                            try:
                                tokens = m.get('clobTokenIds', [])
                                if isinstance(tokens, str):
                                    tokens = json.loads(tokens)
                            except:
                                tokens = []
                            
                            if len(tokens) < 2:
                                continue
                            
                            # CRITICAL FIX: Use outcomes field to correctly map tokens to YES/NO
                            # outcomes array corresponds to clobTokenIds array in order
                            outcomes = m.get('outcomes', [])
                            if isinstance(outcomes, str):
                                try:
                                    outcomes = json.loads(outcomes)
                                except:
                                    outcomes = []
                            
                            # Find indices for UP/YES and DOWN/NO
                            yes_idx = 0  # default
                            no_idx = 1   # default
                            for i, outcome in enumerate(outcomes):
                                outcome_lower = str(outcome).lower()
                                if 'yes' in outcome_lower or 'up' in outcome_lower:
                                    yes_idx = i
                                elif 'no' in outcome_lower or 'down' in outcome_lower:
                                    no_idx = i
                            
                            # Reorder tokens so [0] is always YES/UP and [1] is always NO/DOWN
                            if len(tokens) >= 2 and yes_idx < len(tokens) and no_idx < len(tokens):
                                ordered_tokens = [tokens[yes_idx], tokens[no_idx]]
                            else:
                                ordered_tokens = tokens[:2]
                            
                            # Add market with metadata
                            m['_tokens'] = ordered_tokens
                            m['_timeframe'] = timeframe
                            m['_event_slug'] = slug
                            m['_coin'] = coin
                            m['_end_date'] = m.get('endDate') or m.get('end_date_iso') or ''
                            
                            coin_markets[coin].append(m)
                    
                    # Select only 1 market per coin (soonest ending)
                    tf_count = 0
                    for coin in ['BTC', 'ETH', 'SOL', 'XRP']:
                        if coin_markets[coin]:
                            # Sort by end date and take first (soonest)
                            coin_markets[coin].sort(key=lambda x: x['_end_date'])
                            live_market = coin_markets[coin][0]
                            found.append(live_market)
                            tf_count += 1
                    
                    logger.info(f"  {timeframe}: {tf_count} LIVE markets")
                    
                except Exception as e:
                    logger.error(f"  {timeframe}: Error - {e}")
                
                time.sleep(0.1)  # Rate limit between timeframes
            
            self.target_markets = found
            self.last_scan_time = time.time()
            
            with status_lock:
                bot_status['markets_count'] = len(found)
            
            logger.info(f"🔍 Total: {len(found)} LIVE markets (expected: 16)")
            
        except Exception as e:
            logger.error(f"Scan error: {e}")

    def check_for_arbitrage(self):
        sorted_markets = sorted(self.target_markets, key=lambda m: m.get('_last_spread', 1.02))
        
        def process_single_market(market):
            try:
                tokens = market.get('_tokens', [])
                if len(tokens) < 2: return None

                # Fetch both sides
                clob_rate_limiter.wait()
                ob_up = self.clob_client.get_order_book(tokens[0])
                clob_rate_limiter.wait()
                ob_down = self.clob_client.get_order_book(tokens[1])

                ask_up = self._get_best_ask(ob_up)
                ask_down = self._get_best_ask(ob_down)

                if ask_up is None or ask_down is None: return None

                total = ask_up + ask_down
                q = market.get('question', '')
                market['_last_spread'] = total
                
                # Get coin and timeframe
                coin = market.get('_coin', 'UNK')
                timeframe = market.get('_timeframe', '?')
                
                return {
                    'q': q, 'up': ask_up, 'down': ask_down, 'total': total, 
                    'market': market, 'coin': coin, 'timeframe': timeframe
                }
            except Exception:
                return None

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_market, m): m for m in sorted_markets}
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res: results.append(res)
        
        # --- TERMINAL UI OUTPUT ---
        if results:
            # Group by timeframe
            by_timeframe = {}
            for r in results:
                tf = r['timeframe']
                if tf not in by_timeframe:
                    by_timeframe[tf] = []
                by_timeframe[tf].append(r)
            
            # Print header
            print("\n" + "═" * 70)
            print(f"  📊 LIVE SPREAD SCANNER | {datetime.now().strftime('%H:%M:%S')} | {len(results)} markets")
            print("═" * 70)
            
            # Coin colors (ANSI)
            COLORS = {
                'BTC': '\033[38;5;208m',  # Orange
                'ETH': '\033[38;5;99m',   # Purple
                'SOL': '\033[38;5;135m',  # Violet
                'XRP': '\033[38;5;245m',  # Gray
            }
            RESET = '\033[0m'
            GREEN = '\033[92m'
            YELLOW = '\033[93m'
            BLUE = '\033[94m'
            DIM = '\033[2m'
            
            # Print each timeframe
            for tf in ['15-min', '1-hour', '4-hour', 'Daily']:
                if tf not in by_timeframe:
                    continue
                
                tf_results = sorted(by_timeframe[tf], key=lambda x: x['total'])
                
                print(f"\n  ┌─ {tf.upper()} {'─' * (60 - len(tf))}")
                
                for r in tf_results:
                    coin = r['coin']
                    total = r['total']
                    up = r['up']
                    down = r['down']
                    
                    # Determine status
                    if total < 1.00:
                        status = f"{GREEN}★ ARB!{RESET}"
                        spread_color = GREEN
                    elif total < 1.01:
                        status = f"{YELLOW}◆ CLOSE{RESET}"
                        spread_color = YELLOW
                    elif total <= 1.02:
                        status = f"{BLUE}● OK{RESET}"
                        spread_color = BLUE
                    else:
                        status = f"{DIM}○ WAIT{RESET}"
                        spread_color = DIM
                    
                    coin_colored = f"{COLORS.get(coin, '')}{coin:>3}{RESET}"
                    spread_str = f"{spread_color}{total:.4f}{RESET}"
                    
                    print(f"  │  {coin_colored}  UP:{up:.2f}  DOWN:{down:.2f}  │  Spread: {spread_str}  {status}")
                
                print(f"  └{'─' * 67}")
            
            print()
        
        # Update status for dashboard
        for res in results:
            total = res['total']
            with status_lock:
                bot_status['checks'] += 1
                if total < bot_status['best_spread']:
                    bot_status['best_spread'] = total
                bot_status['recent_checks'].append(res)
                if len(bot_status['recent_checks']) > 10:
                    bot_status['recent_checks'].pop(0)

            # REAL TRADE EXECUTION
            if total < MIN_SPREAD_TARGET:
                profit = 1.00 - total
                if profit >= PROFIT_THRESHOLD:
                    if res['up'] <= 0 or res['down'] <= 0: continue
                    
                    shares = round(BET_SIZE / total, 2)
                    if shares <= 0: continue

                    logger.info(f"🎯 ARBITRAGE! {res['q']} | Spread: {total:.4f} | Profit: ${profit * shares:.4f}")
                    
                    cid = res['market'].get('conditionId')
                    tokens = res['market'].get('_tokens')
                    
                    # Execute with condition_id to prevent re-trading same market
                    if self.real_client.execute_pair_buy(tokens, res['up'], res['down'], shares, condition_id=cid):
                        self.real_client.merge_and_settle(cid)

    def _get_best_ask(self, ob):
        try:
            # Safety Check: Handle None or missing data
            if not ob: return None
            
            asks = []
            if hasattr(ob, 'asks') and ob.asks:
                asks = ob.asks
            elif isinstance(ob, dict) and ob.get('asks'):
                asks = ob['asks']
            else:
                return None
            
            if len(asks) == 0: return None

            parse_price = lambda x: float(x.price) if hasattr(x, 'price') else float(x['price'])
            parse_size = lambda x: float(x.size) if hasattr(x, 'size') else float(x['size'])
            
            valid_asks = [a for a in asks if parse_price(a) > 0 and parse_size(a) > 0]
            valid_asks.sort(key=parse_price)

            needed_cash = BET_SIZE / 2
            total_shares = 0
            total_cost = 0
            
            for ask in valid_asks:
                price = parse_price(ask)
                size = parse_size(ask)
                liquidity_value = price * size
                
                if total_cost + liquidity_value >= needed_cash:
                    remaining_cash = needed_cash - total_cost
                    shares_to_buy = remaining_cash / price
                    total_shares += shares_to_buy
                    total_cost += remaining_cash
                    break
                else:
                    total_shares += size
                    total_cost += liquidity_value

            if total_cost < needed_cash * 0.9: return None
            if total_shares == 0: return None
            
            return total_cost / total_shares

        except Exception:
            return None

    def update_status(self):
        with status_lock:
            bot_status['balance'] = self.real_client.balance
            bot_status['total_trades'] = self.real_client.total_trades
            # Use IST timezone (UTC+5:30)
            ist = timezone(timedelta(hours=5, minutes=30))
            bot_status['last_update'] = datetime.now(ist).strftime('%H:%M:%S')

    def run(self):
        logger.info("🚀 Starting LIVE Polymarket Bot (Real Money)")
        logger.info(f"📊 Dashboard: http://localhost:{WEB_PORT}")
        
        server = HTTPServer(('0.0.0.0', WEB_PORT), DashboardHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        
        self.scan_markets()
        
        while True:
            try:
                if time.time() - self.last_scan_time > 60:
                    self.scan_markets()
                
                self.check_for_arbitrage()
                self.update_status()
                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.info("🛑 Stopping...")
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
