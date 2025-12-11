import time
import json
import os
import requests
import logging
import threading
import concurrent.futures
import html  # Added for dashboard security
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs
from py_clob_client.order_builder.constants import BUY

# --- Configuration ---
INITIAL_BALANCE = 1000.0      # Virtual balance for dashboard display only
MIN_SPREAD_TARGET = 1.0       # ⚠️ TESTING MODE: Breakeven (CHANGE BACK TO 0.98!)
POLL_INTERVAL = 1.0           # Fast polling
BET_SIZE = 5.0                # Must be at least $5 to meet minimum 5 share requirement
MIN_SHARES = 5.0              # Polymarket minimum order size
PROFIT_THRESHOLD = 0.001
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WEB_PORT = int(os.environ.get('PORT', 8080))

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
            'XRP': '#23292f'
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
            safe_q = html.escape(q[:35])
            
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
        self.balance = 100.0  # Placeholder for dashboard

    def execute_pair_buy(self, tokens, up_price, down_price, shares):
        """
        Execute paired buy orders with EQUAL SHARES on both sides.
        
        CRITICAL for arbitrage:
        - Must buy SAME number of shares on UP and DOWN
        - Only then can we merge pairs for guaranteed $1.00 payout
        - Unequal shares = unhedged directional risk!
        
        Strategy:
        - Use OrderArgs with exact SHARES (rounded to 2 decimals)
        - Use GTC orders (FOK has precision issues per GitHub #121)
        - If first order succeeds but second fails, cancel first
        """
        if self.client is None:
            logger.error("❌ Cannot trade: CLOB Client not connected!")
            return False
            
        token_yes = tokens[0]
        token_no = tokens[1]
        
        try:
            # Calculate the cost and validate spread
            total_spread = up_price + down_price
            if total_spread >= 1.00:
                logger.info(f"Spread {total_spread:.4f} >= 1.00, no profit, skipping")
                return False
            
            # Calculate how many EQUAL shares we can buy with BET_SIZE
            # Cost per pair = up_price + down_price
            # Shares = BET_SIZE / (up_price + down_price)
            raw_shares = BET_SIZE / total_spread
            
            # CRITICAL: Round shares to exactly 2 decimal places (API requirement)
            equal_shares = round(raw_shares, 2)
            
            # Ensure minimum viable shares (Polymarket requires minimum 5 shares)
            if equal_shares < MIN_SHARES:
                logger.warning(f"Shares {equal_shares} < minimum {MIN_SHARES}, skipping trade")
                return False
            
            # Calculate expected profit
            cost = equal_shares * total_spread
            payout = equal_shares * 1.00  # Merge pays $1 per pair
            profit = payout - cost
            
            # Round prices to 2 decimals
            safe_price_yes = round(up_price, 2)
            safe_price_no = round(down_price, 2)
            
            logger.info(f"🎯 Sending EQUAL SHARE Orders (Proper Arbitrage):")
            logger.info(f"   SHARES: {equal_shares} on BOTH sides")
            logger.info(f"   YES: {equal_shares} shares @ {safe_price_yes}")
            logger.info(f"   NO:  {equal_shares} shares @ {safe_price_no}")
            logger.info(f"   Total cost: ${cost:.2f} | Payout: ${payout:.2f} | Profit: ${profit:.4f}")

            # Create Order Args with EQUAL SHARES
            order_args_yes = OrderArgs(
                token_id=token_yes,
                price=safe_price_yes,
                size=equal_shares,  # EQUAL shares on both sides!
                side=BUY
            )
            order_args_no = OrderArgs(
                token_id=token_no,
                price=safe_price_no,
                size=equal_shares,  # EQUAL shares on both sides!
                side=BUY
            )
            
            # Post YES order first (using GTC to avoid FOK precision issues)
            signed_yes = self.client.create_order(order_args_yes)
            resp_yes = self.client.post_order(signed_yes, OrderType.GTC)
            logger.info(f"YES Order Response: {resp_yes}")
            
            yes_success = resp_yes.get("success", False) if isinstance(resp_yes, dict) else False
            yes_order_id = resp_yes.get("orderID") if isinstance(resp_yes, dict) else None
            
            if not yes_success:
                logger.error(f"❌ YES order failed: {resp_yes}")
                return False
            
            # Post NO order
            signed_no = self.client.create_order(order_args_no)
            resp_no = self.client.post_order(signed_no, OrderType.GTC)
            logger.info(f"NO Order Response: {resp_no}")
            
            no_success = resp_no.get("success", False) if isinstance(resp_no, dict) else False
            
            if not no_success:
                # CRITICAL: Cancel YES order to avoid unhedged position!
                logger.error(f"❌ NO order failed: {resp_no}")
                if yes_order_id:
                    try:
                        self.client.cancel(yes_order_id)
                        logger.info(f"🔄 Cancelled YES order {yes_order_id} to avoid unhedged position")
                    except Exception as ce:
                        logger.error(f"⚠️ Failed to cancel YES order: {ce}")
                return False

            # Both orders posted successfully
            logger.info(f"✅ ARBITRAGE EXECUTED: {equal_shares} pairs @ spread {total_spread:.4f}")
            logger.info(f"   Expected profit after merge: ${profit:.4f}")
            self.total_trades += 1
            return True
                
        except Exception as e:
            logger.error(f"Real Trade Exception: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def merge_and_settle(self, condition_id):
        logger.info("💰 Arbed! (Manual Merge Required on Website)")
        pass

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
                            
                            # Add market with metadata
                            m['_tokens'] = tokens
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
                q = market.get('question', '')[:40]
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
                    
                    # Execute
                    if self.real_client.execute_pair_buy(tokens, res['up'], res['down'], shares):
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
            bot_status['last_update'] = datetime.now().strftime('%H:%M:%S')

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
