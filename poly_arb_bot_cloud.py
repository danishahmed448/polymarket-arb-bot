import time
import json
import os
import requests
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from py_clob_client.client import ClobClient

# --- Configuration ---
INITIAL_BALANCE = 1000.0
MIN_SPREAD_TARGET = 1.00
POLL_INTERVAL = 2.0
BET_SIZE = 10.0
PROFIT_THRESHOLD = 0.001
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
TAG_15M = 102467
WEB_PORT = int(os.environ.get('PORT', 8080))

# JSONbin.io Configuration (set as environment variables on Railway)
JSONBIN_API_KEY = os.environ.get('JSONBIN_API_KEY', 'YOUR_X_MASTER_KEY_HERE')
JSONBIN_BIN_ID = os.environ.get('JSONBIN_BIN_ID', 'YOUR_BIN_ID_HERE')
JSONBIN_URL = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Thread lock for safe access to shared state
status_lock = threading.Lock()

# Global state for web dashboard
bot_status = {
    'balance': INITIAL_BALANCE,
    'initial_balance': INITIAL_BALANCE,
    'total_trades': 0,
    'markets_count': 0,
    'best_spread': 1.02,
    'checks': 0,
    'last_update': '',
    'recent_checks': [],
    'running': True
}


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(bot_status).encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(self.get_dashboard_html().encode('utf-8'))
    
    def get_dashboard_html(self):
        # Thread-safe read of shared state
        with status_lock:
            balance = bot_status['balance']
            initial_balance = bot_status['initial_balance']
            total_trades = bot_status['total_trades']
            markets_count = bot_status['markets_count']
            best_spread = bot_status['best_spread']
            checks = bot_status['checks']
            last_update = bot_status['last_update']
            recent_checks = list(bot_status.get('recent_checks', [])[-8:])
        
        profit = balance - initial_balance
        profit_pct = (profit / initial_balance) * 100 if initial_balance > 0 else 0
        profit_color = '#10b981' if profit >= 0 else '#ef4444'
        profit_icon = 'trending_up' if profit >= 0 else 'trending_down'
        
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
            recent_html += f'''
            <div class="check-row" style="animation-delay: {i * 0.05}s">
                <div class="check-left">
                    <span class="coin-badge">{coin}</span>
                    <span class="check-name">{check.get('q', 'Unknown')[:30]}</span>
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
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons+Round" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Inter', -apple-system, sans-serif;
            background: #0a0a0f;
            color: #fff;
            min-height: 100vh;
        }}
        .bg-gradient {{
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: radial-gradient(ellipse at top, #1a1a3e 0%, #0a0a0f 50%),
                        radial-gradient(ellipse at bottom right, #0f172a 0%, transparent 50%);
            z-index: -1;
        }}
        .container {{ max-width: 900px; margin: 0 auto; padding: 24px; }}
        
        /* Header */
        .header {{ text-align: center; padding: 40px 0 30px; }}
        .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 8px; background: linear-gradient(135deg, #fff 0%, #94a3b8 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        .status-badge {{ display: inline-flex; align-items: center; gap: 8px; background: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.3); padding: 8px 16px; border-radius: 100px; font-size: 13px; color: #10b981; }}
        .pulse {{ width: 8px; height: 8px; background: #10b981; border-radius: 50%; animation: pulse 2s infinite; }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; transform: scale(1); }} 50% {{ opacity: 0.5; transform: scale(0.9); }} }}
        
        /* Stats Grid */
        .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 30px 0; }}
        @media (max-width: 600px) {{ .stats {{ grid-template-columns: repeat(2, 1fr); }} }}
        .stat {{ background: rgba(255,255,255,0.03); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.06); border-radius: 16px; padding: 24px; text-align: center; transition: all 0.3s; }}
        .stat:hover {{ background: rgba(255,255,255,0.05); transform: translateY(-2px); }}
        .stat-icon {{ width: 40px; height: 40px; border-radius: 12px; display: flex; align-items: center; justify-content: center; margin: 0 auto 12px; }}
        .stat-icon .material-icons-round {{ font-size: 22px; }}
        .stat-value {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
        .stat-label {{ font-size: 13px; color: #64748b; font-weight: 500; }}
        
        /* Card */
        .card {{ background: rgba(255,255,255,0.02); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.05); border-radius: 20px; padding: 24px; margin-bottom: 20px; }}
        .card-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 20px; }}
        .card-header h2 {{ font-size: 16px; font-weight: 600; color: #e2e8f0; }}
        .card-header .material-icons-round {{ font-size: 20px; color: #64748b; }}
        
        /* Check Rows */
        .check-row {{ display: flex; justify-content: space-between; align-items: center; padding: 14px 0; border-bottom: 1px solid rgba(255,255,255,0.04); animation: fadeIn 0.3s ease; }}
        .check-row:last-child {{ border-bottom: none; }}
        @keyframes fadeIn {{ from {{ opacity: 0; transform: translateX(-10px); }} to {{ opacity: 1; transform: translateX(0); }} }}
        .check-left {{ display: flex; align-items: center; gap: 12px; }}
        .check-right {{ display: flex; align-items: center; gap: 12px; }}
        .coin-badge {{ background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%); padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .check-name {{ color: #94a3b8; font-size: 14px; }}
        .check-spread {{ font-family: 'SF Mono', monospace; font-size: 15px; font-weight: 600; }}
        
        /* Badges */
        .badge-green {{ background: rgba(16, 185, 129, 0.15); color: #10b981; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-yellow {{ background: rgba(234, 179, 8, 0.15); color: #eab308; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-blue {{ background: rgba(59, 130, 246, 0.15); color: #3b82f6; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        .badge-gray {{ background: rgba(100, 116, 139, 0.15); color: #64748b; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
        
        /* Footer */
        .footer {{ text-align: center; padding: 30px; color: #475569; font-size: 13px; }}
        .footer a {{ color: #3b82f6; text-decoration: none; }}
    </style>
    <script>setTimeout(() => location.reload(), 10000);</script>
</head>
<body>
    <div class="bg-gradient"></div>
    <div class="container">
        <div class="header">
            <h1>Polymarket Arbitrage Bot</h1>
            <div class="status-badge"><span class="pulse"></span> Running 24/7 on Cloud</div>
        </div>
        
        <div class="stats">
            <div class="stat">
                <div class="stat-icon" style="background: rgba(16, 185, 129, 0.15);"><span class="material-icons-round" style="color: #10b981;">account_balance_wallet</span></div>
                <div class="stat-value" style="color: #10b981;">${balance:.2f}</div>
                <div class="stat-label">Balance</div>
            </div>
            <div class="stat">
                <div class="stat-icon" style="background: rgba({('16, 185, 129' if profit >= 0 else '239, 68, 68')}, 0.15);"><span class="material-icons-round" style="color: {profit_color};">{profit_icon}</span></div>
                <div class="stat-value" style="color: {profit_color};">${profit:+.2f}</div>
                <div class="stat-label">Profit ({profit_pct:+.1f}%)</div>
            </div>
            <div class="stat">
                <div class="stat-icon" style="background: rgba(139, 92, 246, 0.15);"><span class="material-icons-round" style="color: #8b5cf6;">swap_horiz</span></div>
                <div class="stat-value">{total_trades}</div>
                <div class="stat-label">Trades</div>
            </div>
            <div class="stat">
                <div class="stat-icon" style="background: rgba(59, 130, 246, 0.15);"><span class="material-icons-round" style="color: #3b82f6;">speed</span></div>
                <div class="stat-value">{best_spread:.4f}</div>
                <div class="stat-label">Best Spread</div>
            </div>
        </div>
        
        <div class="card">
            <div class="card-header"><span class="material-icons-round">monitoring</span><h2>Live Market Checks</h2></div>
            {recent_html if recent_html else '<p style="color:#64748b;text-align:center;padding:20px;">Waiting for market data...</p>'}
        </div>
        
        <div class="card">
            <div class="card-header"><span class="material-icons-round">info</span><h2>Bot Statistics</h2></div>
            <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:20px;text-align:center;">
                <div><div style="font-size:24px;font-weight:600;">{markets_count}</div><div style="color:#64748b;font-size:13px;">Markets</div></div>
                <div><div style="font-size:24px;font-weight:600;">{checks}</div><div style="color:#64748b;font-size:13px;">Total Checks</div></div>
                <div><div style="font-size:24px;font-weight:600;">{last_update}</div><div style="color:#64748b;font-size:13px;">Last Update</div></div>
            </div>
        </div>
        
        <div class="footer">Auto-refreshes every 10 seconds &bull; <a href="/api/status">View API</a></div>
    </div>
</body>
</html>'''



class CloudPersistentClient:
    """Paper Trading Client with JSONbin.io cloud persistence."""
    
    def __init__(self, initial_balance=1000.0):
        self.balance = initial_balance
        self.positions = {}
        self.total_trades = 0
        self._load_from_cloud()

    def _load_from_cloud(self):
        """Load state from JSONbin.io"""
        try:
            headers = {
                'X-Master-Key': JSONBIN_API_KEY,
                'X-Bin-Meta': 'false'
            }
            resp = requests.get(JSONBIN_URL, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                record = data.get('record', {})
                self.balance = record.get('balance', self.balance)
                self.total_trades = record.get('total_trades', 0)
                logger.info(f"☁️ Loaded from cloud: Balance=${self.balance:.2f}, Trades={self.total_trades}")
            else:
                logger.warning(f"Could not load from JSONbin: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Cloud load error: {e}")

    def _save_to_cloud(self):
        """Save state to JSONbin.io"""
        try:
            headers = {
                'X-Master-Key': JSONBIN_API_KEY,
                'Content-Type': 'application/json'
            }
            data = {
                'balance': self.balance,
                'total_trades': self.total_trades,
                'last_updated': datetime.now().isoformat()
            }
            resp = requests.put(JSONBIN_URL, headers=headers, json=data, timeout=10)
            if resp.status_code == 200:
                logger.info(f"☁️ Saved to cloud: Balance=${self.balance:.2f}")
            else:
                logger.warning(f"Cloud save failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Cloud save error: {e}")

    def buy(self, market_id, outcome, price, size_shares):
        cost = price * size_shares
        if cost > self.balance:
            return False
        self.balance -= cost
        if market_id not in self.positions:
            self.positions[market_id] = {'UP': 0.0, 'DOWN': 0.0}
        self.positions[market_id][outcome] += size_shares
        logger.info(f"⚡ BOUGHT {size_shares:.2f} {outcome} @ ${price:.3f}")
        return True

    def merge_and_settle(self, market_id):
        if market_id not in self.positions:
            return
        pos = self.positions[market_id]
        mergeable = min(pos.get('UP', 0), pos.get('DOWN', 0))
        if mergeable > 0:
            pos['UP'] -= mergeable
            pos['DOWN'] -= mergeable
            self.balance += mergeable * 1.00
            self.total_trades += 1
            logger.info(f"💎 SETTLED | Balance: ${self.balance:.2f}")
            self._save_to_cloud()  # Save after each trade!


class ArbitrageBot:
    def __init__(self):
        self.clob_client = ClobClient(host=CLOB_HOST, key=None, chain_id=137)
        self.mock_client = CloudPersistentClient(initial_balance=INITIAL_BALANCE)
        self.target_markets = []
        self.last_scan_time = 0
        self.last_cloud_save = 0
        
        # Sync dashboard with cloud data immediately
        with status_lock:
            bot_status['balance'] = self.mock_client.balance
            bot_status['total_trades'] = self.mock_client.total_trades

    def scan_markets(self):
        try:
            resp = requests.get(f"{GAMMA_API_URL}/markets", params={'active': 'true', 'closed': 'false', 'tag_id': TAG_15M, 'limit': 50}, timeout=15)
            if resp.status_code == 200:
                markets = resp.json()
                found = []
                for m in markets:
                    if m.get('closed') or not m.get('active', True):
                        continue
                    clob_ids_str = m.get('clobTokenIds', '[]')
                    try:
                        clob_ids = json.loads(clob_ids_str) if isinstance(clob_ids_str, str) else clob_ids_str
                    except:
                        clob_ids = []
                    if clob_ids and len(clob_ids) >= 2:
                        m['_tokens'] = clob_ids
                        found.append(m)
                self.target_markets = found
                self.last_scan_time = time.time()
                bot_status['markets_count'] = len(found)
                logger.info(f"🔍 Scanned: {len(found)} markets")
        except Exception as e:
            logger.error(f"Scan error: {e}")

    def check_for_arbitrage(self):
        for market in self.target_markets:
            try:
                tokens = market.get('_tokens', [])
                if len(tokens) < 2:
                    continue
                ob_up = self.clob_client.get_order_book(tokens[0])
                ob_down = self.clob_client.get_order_book(tokens[1])
                ask_up = self._get_best_ask(ob_up)
                ask_down = self._get_best_ask(ob_down)
                if ask_up is None or ask_down is None:
                    continue

                bot_status['checks'] += 1
                total = ask_up + ask_down
                
                # Thread-safe updates
                with status_lock:
                    if total < bot_status['best_spread']:
                        bot_status['best_spread'] = total
                    
                    q = market.get('question', '')[:35]
                    bot_status['recent_checks'].append({'q': q, 'up': ask_up, 'down': ask_down, 'total': total})
                    if len(bot_status['recent_checks']) > 10:
                        bot_status['recent_checks'].pop(0)

                if total < MIN_SPREAD_TARGET:
                    profit = 1.00 - total
                    if profit >= PROFIT_THRESHOLD:
                        logger.info(f"🎯 ARBITRAGE! {q} | Spread: {total:.4f}")
                        shares = round(BET_SIZE / total, 2)
                        cid = market.get('conditionId')
                        if self.mock_client.buy(cid, 'UP', ask_up, shares):
                            if self.mock_client.buy(cid, 'DOWN', ask_down, shares):
                                self.mock_client.merge_and_settle(cid)

            except Exception as e:
                if "429" in str(e):
                    time.sleep(3)

    def _get_best_ask(self, ob):
        try:
            if hasattr(ob, 'asks') and ob.asks:
                return min(float(a.price) for a in ob.asks)
            elif isinstance(ob, dict) and ob.get('asks'):
                return min(float(a['price']) for a in ob['asks'])
        except:
            pass
        return None

    def update_status(self):
        bot_status['balance'] = self.mock_client.balance
        bot_status['total_trades'] = self.mock_client.total_trades
        bot_status['last_update'] = datetime.now().strftime('%H:%M:%S')

    def run(self):
        logger.info("🚀 Starting Polymarket Bot with Cloud Persistence")
        logger.info(f"📊 Dashboard: http://localhost:{WEB_PORT}")
        logger.info(f"☁️ State: JSONbin.io")
        
        server = HTTPServer(('0.0.0.0', WEB_PORT), DashboardHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        
        self.scan_markets()
        
        while True:
            try:
                if time.time() - self.last_scan_time > 60:
                    self.scan_markets()
                
                # Periodic cloud save every 5 minutes
                if time.time() - self.last_cloud_save > 300:
                    self.mock_client._save_to_cloud()
                    self.last_cloud_save = time.time()
                
                self.check_for_arbitrage()
                self.update_status()
                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.info("🛑 Stopping...")
                self.mock_client._save_to_cloud()
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(5)


if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
