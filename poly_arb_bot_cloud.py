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

STATE_FILE = "bot_state.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

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
    """Simple web dashboard"""
    
    def log_message(self, format, *args):
        pass  # Suppress HTTP logs
    
    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(bot_status).encode())
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(self.get_dashboard_html().encode())
    
    def get_dashboard_html(self):
        profit = bot_status['balance'] - bot_status['initial_balance']
        profit_pct = (profit / bot_status['initial_balance']) * 100 if bot_status['initial_balance'] > 0 else 0
        profit_color = '#22c55e' if profit >= 0 else '#ef4444'
        
        recent_html = ''
        for check in bot_status.get('recent_checks', [])[-8:]:
            total = check.get('total', 1.02)
            if total < 1.00:
                icon, color = '🔥', '#22c55e'
            elif total < 1.01:
                icon, color = '💰', '#eab308'
            elif total <= 1.02:
                icon, color = '⚡', '#06b6d4'
            else:
                icon, color = '·', '#6b7280'
            recent_html += f'''
            <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #374151;">
                <span>{icon} {check.get('q', 'Unknown')[:35]}</span>
                <span style="color:{color};font-weight:bold;">{total:.4f}</span>
            </div>'''
        
        return f'''<!DOCTYPE html>
<html>
<head>
    <title>Polymarket Arbitrage Bot</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="10">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e5e7eb;
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 800px; margin: 0 auto; }}
        .header {{ 
            text-align: center; 
            padding: 30px 0;
            border-bottom: 1px solid #374151;
            margin-bottom: 30px;
        }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .header .subtitle {{ color: #9ca3af; }}
        .stats {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat {{ 
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .stat-value {{ font-size: 28px; font-weight: bold; }}
        .stat-label {{ color: #9ca3af; font-size: 14px; margin-top: 5px; }}
        .section {{ 
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .section h2 {{ margin-bottom: 15px; font-size: 18px; }}
        .status-dot {{ 
            display: inline-block;
            width: 10px; height: 10px;
            background: #22c55e;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}
        .footer {{ text-align: center; color: #6b7280; margin-top: 30px; font-size: 14px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📈 Polymarket Arbitrage Bot</h1>
            <p class="subtitle"><span class="status-dot"></span>Running on Railway</p>
        </div>
        
        <div class="stats">
            <div class="stat">
                <div class="stat-value" style="color:#22c55e;">${bot_status['balance']:.2f}</div>
                <div class="stat-label">Balance</div>
            </div>
            <div class="stat">
                <div class="stat-value" style="color:{profit_color};">${profit:+.2f}</div>
                <div class="stat-label">Profit ({profit_pct:+.1f}%)</div>
            </div>
            <div class="stat">
                <div class="stat-value">{bot_status['total_trades']}</div>
                <div class="stat-label">Total Trades</div>
            </div>
            <div class="stat">
                <div class="stat-value">{bot_status['best_spread']:.4f}</div>
                <div class="stat-label">Best Spread</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Recent Market Checks</h2>
            {recent_html if recent_html else '<p style="color:#6b7280;">Waiting for data...</p>'}
        </div>
        
        <div class="section">
            <h2>ℹ️ Stats</h2>
            <p>Markets Monitored: <strong>{bot_status['markets_count']}</strong></p>
            <p>Total Checks: <strong>{bot_status['checks']}</strong></p>
            <p>Last Update: <strong>{bot_status['last_update']}</strong></p>
        </div>
        
        <div class="footer">
            Auto-refreshes every 10 seconds • <a href="/api/status" style="color:#06b6d4;">API</a>
        </div>
    </div>
</body>
</html>'''


class PersistentMockClient:
    def __init__(self, initial_balance=1000.0):
        self.balance = initial_balance
        self.positions = {}
        self.trade_history = []
        self.total_trades = 0
        self._load_state()

    def _load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    state = json.load(f)
                self.balance = state.get('balance', self.balance)
                self.total_trades = state.get('total_trades', 0)
                logger.info(f"📂 Restored: Balance=${self.balance:.2f}, Trades={self.total_trades}")
            except:
                pass

    def _save_state(self):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({'balance': self.balance, 'total_trades': self.total_trades, 'updated': datetime.now().isoformat()}, f)
        except:
            pass

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
            self._save_state()


class ArbitrageBot:
    def __init__(self):
        self.clob_client = ClobClient(host=CLOB_HOST, key=None, chain_id=137)
        self.mock_client = PersistentMockClient(initial_balance=INITIAL_BALANCE)
        self.target_markets = []
        self.last_scan_time = 0

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
        logger.info("🚀 Starting Polymarket Bot with Web Dashboard")
        logger.info(f"📊 Dashboard: http://localhost:{WEB_PORT}")
        
        # Start web server in background
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
                self.mock_client._save_state()
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(5)


if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
