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
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

# --- Configuration ---
INITIAL_BALANCE = 1000.0      # Virtual balance for dashboard display only
MIN_SPREAD_TARGET = 1.0       # ⚠️ TESTING MODE: Breakeven (CHANGE BACK TO 0.98!)
POLL_INTERVAL = 1.0           # Fast polling
BET_SIZE = 2.0                # Reduced for better fills on both sides
PROFIT_THRESHOLD = 0.001
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
TAG_15M = 102467
WEB_PORT = int(os.environ.get('PORT', 8080))

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
            recent_checks = list(bot_status.get('recent_checks', [])[-8:])
        
        recent_html = ''
        for i, check in enumerate(reversed(recent_checks)):
            total = check.get('total', 1.02)
            if total < 1.00:
                badge_class, badge_text = 'badge-green', 'ARB'
            elif total < 1.01:
                badge_class, badge_text = 'badge-yellow', 'CLOSE'
            elif total <= 1.02:
                badge_class, badge_text = 'badge-blue', 'OK'
            else:
                badge_class, badge_text = 'badge-gray', '-'
            
            coin = 'BTC' if 'Bitcoin' in check.get('q', '') else 'ETH' if 'Ethereum' in check.get('q', '') else 'SOL' if 'Solana' in check.get('q', '') else 'XRP'
            # Added HTML escape for safety
            safe_q = html.escape(check.get('q', 'Unknown')[:30])
            
            recent_html += f'''
            <div class="check-row" style="animation-delay: {i * 0.05}s">
                <div class="check-left">
                    <span class="coin-badge">{coin}</span>
                    <span class="check-name">{safe_q}</span>
                </div>
                <div class="check-right">
                    <span class="check-spread">{total:.4f}</span>
                    <span class="{badge_class}">{badge_text}</span>
                </div>
            </div>'''
        
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Polymarket Bot</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Inter', sans-serif; background: #0a0a0f; color: #fff; min-height: 100vh; }}
        .container {{ max-width: 900px; margin: 0 auto; padding: 24px; }}
        .header {{ text-align: center; padding: 40px 0 30px; }}
        .status-badge {{ display: inline-flex; align-items: center; gap: 8px; background: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.3); padding: 8px 16px; border-radius: 100px; font-size: 13px; color: #10b981; }}
        .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 30px 0; }}
        .stat {{ background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 16px; padding: 24px; text-align: center; }}
        .stat-value {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
        .stat-label {{ font-size: 13px; color: #64748b; font-weight: 500; }}
        .card {{ background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.05); border-radius: 20px; padding: 24px; margin-bottom: 20px; }}
        .check-row {{ display: flex; justify-content: space-between; align-items: center; padding: 14px 0; border-bottom: 1px solid rgba(255,255,255,0.04); }}
        .badge-green {{ background: rgba(16, 185, 129, 0.15); color: #10b981; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-blue {{ background: rgba(59, 130, 246, 0.15); color: #3b82f6; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-gray {{ background: rgba(100, 116, 139, 0.15); color: #64748b; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-yellow {{ background: rgba(234, 179, 8, 0.15); color: #eab308; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
    </style>
    <script>setTimeout(() => location.reload(), 10000);</script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Polymarket Live Bot</h1>
            <div class="status-badge">● Running 24/7 on Cloud</div>
        </div>
        <div class="stats">
            <div class="stat"><div class="stat-value">${balance:.2f}</div><div class="stat-label">Wallet Est.</div></div>
            <div class="stat"><div class="stat-value">{total_trades}</div><div class="stat-label">Live Trades</div></div>
            <div class="stat"><div class="stat-value">{markets_count}</div><div class="stat-label">Mkts Scanned</div></div>
            <div class="stat"><div class="stat-value">{best_spread:.4f}</div><div class="stat-label">Best Spread</div></div>
        </div>
        <div class="card">
            <h3>Live Market Checks</h3>
            {recent_html if recent_html else '<p style="color:#64748b;text-align:center;">Scanning markets...</p>'}
        </div>
        <div class="footer">Auto-refreshes every 10s • Checks: {checks} • Last: {last_update}</div>
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
        if self.client is None:
            logger.error("❌ Cannot trade: CLOB Client not connected!")
            return False
            
        token_yes = tokens[0]
        token_no = tokens[1]
        
        try:
            # --- CRITICAL FIX: SANITIZE FLOATS ---
            # Python's round() leaves artifacts (e.g. 0.3000000004).
            # Use f-string formatting to strip floating point ghosts.
            safe_price_yes = float(f"{up_price:.2f}")
            safe_price_no = float(f"{down_price:.2f}")
            safe_size = float(f"{shares:.2f}")
            
            logger.info(f"🔫 Sending FOK Order: Price {safe_price_yes}/{safe_price_no}, Size {safe_size}")

            # Create Order Args with SANITIZED values
            order_args_yes = OrderArgs(
                token_id=token_yes,
                price=safe_price_yes,
                size=safe_size,
                side=BUY
            )
            order_args_no = OrderArgs(
                token_id=token_no,
                price=safe_price_no,
                size=safe_size,
                side=BUY
            )
            
            # Sign and post orders with FOK type
            signed_yes = self.client.create_order(order_args_yes)
            resp_yes = self.client.post_order(signed_yes, OrderType.FOK)
            logger.info(f"YES Order Response: {resp_yes}")
            
            signed_no = self.client.create_order(order_args_no)
            resp_no = self.client.post_order(signed_no, OrderType.FOK)
            logger.info(f"NO Order Response: {resp_no}")
            
            # Check success
            yes_success = resp_yes.get("success", False) if isinstance(resp_yes, dict) else False
            no_success = resp_no.get("success", False) if isinstance(resp_no, dict) else False

            if yes_success and no_success:
                logger.info(f"✅ REAL TRADE EXECUTED: Bought {safe_size} shares.")
                self.total_trades += 1
                return True
            else:
                logger.error(f"❌ Trade Failed. Yes:{yes_success} No:{no_success}")
                if not yes_success: logger.error(f"Err YES: {resp_yes}")
                if not no_success: logger.error(f"Err NO: {resp_no}")
                return False
                
        except Exception as e:
            logger.error(f"Real Trade Exception: {e}")
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
        try:
            resp = requests.get(f"{GAMMA_API_URL}/markets", params={'active': 'true', 'closed': 'false', 'tag_id': TAG_15M, 'limit': 50}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                # Safety check for list format
                if isinstance(data, list):
                    markets = data
                elif isinstance(data, dict) and 'markets' in data:
                    markets = data['markets']
                else:
                    return

                found = []
                for m in markets:
                    if m.get('closed') or not m.get('active', True): continue
                    
                    try:
                        clob_ids_str = m.get('clobTokenIds', '[]')
                        clob_ids = json.loads(clob_ids_str) if isinstance(clob_ids_str, str) else clob_ids_str
                    except: clob_ids = []
                    
                    if clob_ids and len(clob_ids) >= 2:
                        m['_tokens'] = clob_ids
                        found.append(m)
                
                self.target_markets = found
                self.last_scan_time = time.time()
                with status_lock:
                    bot_status['markets_count'] = len(found)
                logger.info(f"🔍 Scanned: {len(found)} markets")
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
                q = market.get('question', '')[:35]
                market['_last_spread'] = total
                return {'q': q, 'up': ask_up, 'down': ask_down, 'total': total, 'market': market}
            except Exception:
                return None

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_market, m): m for m in sorted_markets}
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res: results.append(res)
        
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
