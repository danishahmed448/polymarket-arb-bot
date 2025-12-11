import time
import json
import os
import requests
import logging
from datetime import datetime
from py_clob_client.client import ClobClient

# --- Configuration ---
INITIAL_BALANCE = 1000.0
MIN_SPREAD_TARGET = 1.00
POLL_INTERVAL = 2.0  # Slower for cloud (save API calls)
BET_SIZE = 10.0
PROFIT_THRESHOLD = 0.001
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
TAG_15M = 102467

# State file for persistence
STATE_FILE = "bot_state.json"

# Setup Logging (cloud-friendly)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class PersistentMockClient:
    """Paper Trading Client with persistent state."""
    
    def __init__(self, initial_balance=1000.0):
        self.balance = initial_balance
        self.positions = {}
        self.trade_history = []
        self.total_trades = 0
        self.total_profit = 0.0
        self._load_state()

    def _load_state(self):
        """Load state from file if exists."""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    state = json.load(f)
                self.balance = state.get('balance', self.balance)
                self.positions = state.get('positions', {})
                self.total_trades = state.get('total_trades', 0)
                self.total_profit = state.get('total_profit', 0.0)
                logger.info(f"📂 Restored state: Balance=${self.balance:.2f}, Trades={self.total_trades}")
            except Exception as e:
                logger.warning(f"Could not load state: {e}")

    def _save_state(self):
        """Save current state to file."""
        try:
            state = {
                'balance': self.balance,
                'positions': self.positions,
                'total_trades': self.total_trades,
                'total_profit': self.total_profit,
                'last_updated': datetime.now().isoformat()
            }
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save state: {e}")

    def buy(self, market_id, outcome, price, size_shares):
        cost = price * size_shares
        if cost > self.balance:
            return False
        self.balance -= cost
        if market_id not in self.positions:
            self.positions[market_id] = {'UP': 0.0, 'DOWN': 0.0}
        self.positions[market_id][outcome] += size_shares
        self.trade_history.append({
            'time': datetime.now().isoformat(),
            'action': 'BUY',
            'outcome': outcome,
            'price': price,
            'shares': size_shares
        })
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
            payout = mergeable * 1.00
            self.balance += payout
            self.total_trades += 1
            
            # Calculate profit (approx - assumes bought at ~0.50 each)
            cost = mergeable * 1.0  # Rough estimate
            profit = payout - cost
            self.total_profit += max(0, profit * 0.02)  # Rough profit estimate
            
            logger.info(f"💎 SETTLED {mergeable:.2f} pairs | Balance: ${self.balance:.2f}")
            self._save_state()


class ArbitrageBot:
    def __init__(self):
        self.clob_client = ClobClient(host=CLOB_HOST, key=None, chain_id=137)
        self.mock_client = PersistentMockClient(initial_balance=INITIAL_BALANCE)
        self.target_markets = []
        self.last_scan_time = 0
        self.last_status_time = 0
        self.stats = {
            'checks': 0,
            'best_spread': 1.02,
            'opportunities': 0
        }

    def scan_markets(self):
        try:
            resp = requests.get(
                f"{GAMMA_API_URL}/markets",
                params={'active': 'true', 'closed': 'false', 'tag_id': TAG_15M, 'limit': 50},
                timeout=15
            )
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
                logger.info(f"🔍 Scanned: {len(found)} markets active")
        except Exception as e:
            logger.error(f"Scan error: {e}")

    def check_for_arbitrage(self):
        if not self.target_markets:
            return

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

                self.stats['checks'] += 1
                total = ask_up + ask_down

                if total < self.stats['best_spread']:
                    self.stats['best_spread'] = total

                if total < MIN_SPREAD_TARGET:
                    self.stats['opportunities'] += 1
                    profit = 1.00 - total
                    
                    if profit >= PROFIT_THRESHOLD:
                        q = market.get('question', '')[:40]
                        logger.info(f"🎯 ARBITRAGE! {q}")
                        logger.info(f"   Spread: {total:.4f} | Profit: ${profit:.4f}/share")
                        
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

    def print_status(self):
        logger.info("=" * 50)
        logger.info(f"📊 STATUS REPORT")
        logger.info(f"   Balance: ${self.mock_client.balance:.2f}")
        logger.info(f"   Total Trades: {self.mock_client.total_trades}")
        logger.info(f"   Markets: {len(self.target_markets)}")
        logger.info(f"   Best Spread: {self.stats['best_spread']:.4f}")
        logger.info(f"   Checks: {self.stats['checks']}")
        logger.info("=" * 50)

    def run(self):
        logger.info("=" * 50)
        logger.info("🚀 POLYMARKET ARBITRAGE BOT (24/7 Mode)")
        logger.info("=" * 50)
        
        self.scan_markets()
        self.last_status_time = time.time()
        
        while True:
            try:
                # Rescan every 60 seconds
                if time.time() - self.last_scan_time > 60:
                    self.scan_markets()
                
                # Status every 5 minutes
                if time.time() - self.last_status_time > 300:
                    self.print_status()
                    self.last_status_time = time.time()
                
                self.check_for_arbitrage()
                time.sleep(POLL_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("🛑 Stopping bot...")
                self.mock_client._save_state()
                self.print_status()
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(5)


if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
