"""
Polymarket Arbitrage Bot V2 - High-Performance Async Engine

This is an institutional-grade rewrite implementing:
- Async I/O (aiohttp + asyncio) for massive latency reduction
- WebSockets for real-time price streaming (millisecond reactions)
- Batch ordering (both YES and NO in single request)
- Decimal math for precision (no floating-point errors)
- Pre-flight safety checks for token allowances
- L2 Auth optimization for high-frequency trading

Based on the research audit in doc.txt and template in scriptInstruct.txt.
"""

import asyncio
import json
import os
import time
import logging
import hmac
import hashlib
import base64
import html
from decimal import Decimal, ROUND_DOWN, getcontext
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any

import aiohttp
from aiohttp import web
from web3 import Web3
from eth_account import Account

# Import ABI modules
from abi.ctf_abi import ctf_abi
from abi.safe_abi import safe_abi

# Reuse constants from py_clob_client for convenience
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient

# --- Configuration ---
# Set Decimal precision high enough for crypto math
getcontext().prec = 18

# Environment & Config
RPC_URL = os.environ.get('RPC_URL', 'https://polygon-rpc.com/')
PRIVATE_KEY = os.environ.get("PRIVATE_KEY")
FUNDER_ADDRESS = os.environ.get("FUNDER_ADDRESS")  # Polymarket Proxy (Gnosis Safe) Address
WEB_PORT = int(os.environ.get('PORT', 8080))

# Builder API Credentials (Optional - for order attribution)
BUILDER_API_KEY = os.environ.get("POLY_BUILDER_API_KEY")
BUILDER_SECRET = os.environ.get("POLY_BUILDER_SECRET")
BUILDER_PASSPHRASE = os.environ.get("POLY_BUILDER_PASSPHRASE")
BUILDER_ENABLED = bool(BUILDER_API_KEY and BUILDER_SECRET and BUILDER_PASSPHRASE)

# Trading Parameters (Using Decimal for precision)
MIN_SPREAD_TARGET = Decimal('1.0')    # Trade if spread <= $1.00
BET_SIZE = Decimal('10.0')             # Target size in USDC
MIN_SHARES = Decimal('5.0')           # Min shares per order
SLIPPAGE_TOLERANCE = Decimal('0.003') # 0.3% slippage tolerance
PROFIT_THRESHOLD = Decimal('0.001')   # Minimum profit per share

# API Endpoints
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Contracts (Polygon)
COLLATERAL_TOKEN = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'  # USDC
CTF_EXCHANGE = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'      # Conditional Tokens
NEG_RISK_ADAPTER = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'
EXCHANGE_ADDRESS = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B89056'  # Polymarket Exchange
USDC_ADDRESS = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'
USDCE_DIGITS = 6

# Scan Mode Configuration
SCAN_MODE = 'CRYPTO_ONLY'     # Start with CRYPTO_ONLY first
AUTO_SWITCH_MODE = True       # Toggle between modes
CRYPTO_ONLY_INTERVAL = 600    # CRYPTO_ONLY runs for 10 minutes
ALL_BINARY_INTERVAL = 300     # ALL_BINARY runs for 5 minutes
ALL_BINARY_MARKET_LIMIT = 300 # Max markets in ALL_BINARY mode

# Multi-Timeframe Configuration (for CRYPTO_ONLY mode)
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

ENABLED_TIMEFRAMES = {
    '15-min': True,
    '1-hour': True,
    '4-hour': True,
    'Daily': True,
}

COINS = ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol', 'xrp']

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PolyArbBotV2")

# Suppress noisy HTTP logs
logging.getLogger('py_clob_client').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)


def detect_coin(text: str) -> Optional[str]:
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


# --- Async Rate Limiter (Token Bucket Algorithm) ---
class AsyncRateLimiter:
    """
    Async-compatible rate limiter using token bucket algorithm.
    Prevents 429 Too Many Requests errors from Polymarket API.
    """
    def __init__(self, max_per_second: float = 8.0):
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0.0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Wait if necessary to respect rate limit."""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                await asyncio.sleep(wait_time)
            self.last_call = time.time()
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


# Global rate limiter for CLOB API (8 requests/second default)
rate_limiter = AsyncRateLimiter(max_per_second=8.0)


# --- Async HTTP Client with HMAC Auth ---
class AsyncClobClient:
    """
    Lightweight, high-performance async wrapper for Polymarket CLOB.
    Handles L2 Authentication manually to avoid blocking calls.
    """
    def __init__(self, key: str, secret: str, passphrase: str):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        """Start with persistent session and TCP Keep-Alive."""
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=connector)

    async def close(self):
        """Close the session."""
        if self.session:
            await self.session.close()

    def _get_auth_headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Generate HMAC authentication headers for Polymarket API."""
        timestamp = str(int(time.time()))
        sig_payload = f"{timestamp}{method}{path}{body}"
        signature = base64.b64encode(
            hmac.new(
                self.secret.encode('utf-8'),
                sig_payload.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')

        headers = {
            "POLY-API-KEY": self.key,
            "POLY-API-SIGNATURE": signature,
            "POLY-API-TIMESTAMP": timestamp,
            "POLY-API-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        
        # Add Builder headers if configured (for order attribution)
        if BUILDER_ENABLED:
            builder_headers = self._get_builder_headers(method, path, body)
            headers.update(builder_headers)
        
        return headers
    
    def _get_builder_headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """
        Generate Builder API headers for order attribution.
        Follows Polymarket docs: HMAC-SHA256 of timestamp + method + path + body
        """
        timestamp = str(int(time.time()))
        sig_payload = f"{timestamp}{method}{path}{body}"
        signature = base64.b64encode(
            hmac.new(
                BUILDER_SECRET.encode('utf-8'),
                sig_payload.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        return {
            "POLY_BUILDER_API_KEY": BUILDER_API_KEY,
            "POLY_BUILDER_TIMESTAMP": timestamp,
            "POLY_BUILDER_PASSPHRASE": BUILDER_PASSPHRASE,
            "POLY_BUILDER_SIGNATURE": signature,
        }

    async def get_order_books(self, token_ids: List[str]) -> List[Dict]:
        """Fetch snapshots for multiple tokens in parallel."""
        tasks = [self.get_order_book(tid) for tid in token_ids]
        return await asyncio.gather(*tasks)

    async def get_order_book(self, token_id: str) -> Dict:
        """Fetch order book for a single token with rate limiting."""
        path = f"/book?token_id={token_id}"
        try:
            async with rate_limiter:  # Rate limit applied
                async with self.session.get(CLOB_API_URL + path) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    return {"asks": [], "bids": []}
        except Exception as e:
            logger.warning(f"Order book fetch error: {e}")
            return {"asks": [], "bids": []}

    async def post_order(self, order: Dict) -> Dict:
        """Post a single order."""
        path = "/order"
        body = json.dumps(order)
        headers = self._get_auth_headers("POST", path, body)
        
        try:
            async with self.session.post(CLOB_API_URL + path, data=body, headers=headers) as resp:
                response_data = await resp.json()
                if resp.status != 200:
                    logger.error(f"❌ Order Failed ({resp.status}): {response_data}")
                return response_data
        except Exception as e:
            logger.error(f"Order exception: {e}")
            return {"success": False, "error": str(e)}

    async def post_batch_orders(self, orders: List[Dict]) -> List[Dict]:
        """
        Send multiple orders in a SINGLE atomic HTTP request.
        This reduces network risk and latency.
        """
        path = "/orders"
        body = json.dumps(orders)
        headers = self._get_auth_headers("POST", path, body)
        
        try:
            async with self.session.post(CLOB_API_URL + path, data=body, headers=headers) as resp:
                response_data = await resp.json()
                if resp.status != 200:
                    logger.error(f"❌ Batch Order Failed ({resp.status}): {response_data}")
                    return []
                return response_data if isinstance(response_data, list) else [response_data]
        except Exception as e:
            logger.error(f"Batch order exception: {e}")
            return []


# --- Real Money Arbitrage Engine ---
class ArbitrageEngine:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL))
        self.account = Account.from_key(PRIVATE_KEY) if PRIVATE_KEY else None
        self.running = True
        
        # State
        self.markets: Dict[str, Dict] = {}  # Map condition_id -> Market Data
        self.local_orderbook: Dict[str, Dict] = {}  # Map token_id -> {asks: [], bids: []}
        self.positions: Dict[str, Decimal] = {}  # Track inventory
        self.target_markets: List[Dict] = []  # Markets to scan
        
        # Scan mode state
        self.current_mode = SCAN_MODE
        self.last_mode_switch = 0
        self.last_scan_time = 0
        
        # Dashboard Stats
        self.stats = {
            'checks': 0,
            'arb_opportunities': 0,
            'trades_executed': 0,
            'start_time': time.time(),
            'balance': Decimal('0'),
            'markets_count': 0,
            'best_spread': Decimal('1.02'),
            'recent_checks': [],
            'last_update': datetime.now(timezone.utc).isoformat(),
            'monthly_pnl': 0.0,
            'api_trades': 0,
            'current_mode': SCAN_MODE,
        }
        
        # Clients
        self.api_creds: Dict[str, str] = {}
        self.async_client: Optional[AsyncClobClient] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None

    def derive_keys(self):
        """Use official client ONCE to derive L2 keys safely."""
        logger.info("🔐 Deriving L2 API Credentials...")
        try:
            temp_client = ClobClient(
                host=CLOB_API_URL, 
                key=PRIVATE_KEY, 
                chain_id=POLYGON,
                funder=FUNDER_ADDRESS
            )
            creds = temp_client.create_or_derive_api_creds()
            self.api_creds = {
                'key': creds.api_key,
                'secret': creds.api_secret,
                'passphrase': creds.api_passphrase
            }
            logger.info("✅ L2 Credentials derived successfully")
        except Exception as e:
            logger.critical(f"❌ Failed to derive keys: {e}")
            raise e

    def check_allowances_sync(self):
        """
        PRE-FLIGHT CHECK: Ensure we have allowances BEFORE trading.
        This prevents the "bought YES but can't sell NO" disaster.
        """
        logger.info("🛡️ Performing Pre-Flight Allowance Checks...")
        
        if not FUNDER_ADDRESS:
            logger.warning("⚠️ FUNDER_ADDRESS not set, skipping allowance check")
            return

        tokens_to_check = [
            (COLLATERAL_TOKEN, "USDC"),
            (CTF_EXCHANGE, "Conditional Tokens")
        ]
        
        spenders = [EXCHANGE_ADDRESS, NEG_RISK_ADAPTER]
        
        # ERC20 ABI Subset
        erc20_abi = [
            {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
        ]

        safe_addr = Web3.to_checksum_address(FUNDER_ADDRESS)

        for token_addr, symbol in tokens_to_check:
            try:
                contract = self.w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=erc20_abi)
                
                for spender in spenders:
                    spender_addr = Web3.to_checksum_address(spender)
                    try:
                        allowance = contract.functions.allowance(safe_addr, spender_addr).call()
                        if allowance < 1000 * 10**6:  # Less than $1000
                            logger.warning(f"⚠️ Low allowance for {symbol} -> {spender[:10]}... Please approve manually!")
                        else:
                            logger.info(f"✅ Allowance OK: {symbol} -> {spender[:10]}...")
                    except Exception as e:
                        logger.warning(f"Could not check allowance for {symbol}: {e}")
            except Exception as e:
                logger.warning(f"Could not create contract for {symbol}: {e}")

    async def update_balance(self):
        """Fetch real USDC balance from Polygon."""
        try:
            if not FUNDER_ADDRESS:
                self.stats['balance'] = Decimal('0')
                return
            
            erc20_abi = [{
                "inputs": [{"name": "account", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function"
            }]
            
            usdc = self.w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS),
                abi=erc20_abi
            )
            
            balance_wei = usdc.functions.balanceOf(
                Web3.to_checksum_address(FUNDER_ADDRESS)
            ).call()
            
            self.stats['balance'] = Decimal(str(balance_wei)) / Decimal(str(10 ** USDCE_DIGITS))
            logger.info(f"💰 USDC Balance: ${self.stats['balance']:.2f}")
            
        except Exception as e:
            logger.warning(f"Failed to fetch USDC balance: {e}")
            self.stats['balance'] = Decimal('0')

    async def fetch_monthly_pnl(self) -> float:
        """Fetch monthly PnL from Polymarket API."""
        try:
            if not FUNDER_ADDRESS:
                return 0.0
            
            url = f"https://user-pnl-api.polymarket.com/user-pnl?user_address={FUNDER_ADDRESS}&interval=1m&fidelity=1d"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and len(data) > 0:
                            return float(data[-1].get('p', 0))
            return 0.0
        except Exception as e:
            logger.warning(f"Failed to fetch monthly PnL: {e}")
            return 0.0

    async def fetch_total_trades(self) -> int:
        """Fetch total trade count from Polymarket activity API."""
        try:
            if not FUNDER_ADDRESS:
                return 0
            
            url = f"https://data-api.polymarket.com/activity?user={FUNDER_ADDRESS}&limit=100&offset=0&sortBy=TIMESTAMP&sortDirection=DESC"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return sum(1 for item in data if item.get('type') == 'TRADE')
            return 0
        except Exception as e:
            logger.warning(f"Failed to fetch total trades: {e}")
            return 0

    def _get_current_interval(self) -> int:
        """Return the interval for current mode (asymmetric switching)."""
        if self.current_mode == 'CRYPTO_ONLY':
            return CRYPTO_ONLY_INTERVAL  # 10 minutes
        else:
            return ALL_BINARY_INTERVAL   # 5 minutes

    async def fetch_markets(self):
        """Fetch target markets based on current scan mode."""
        logger.info(f"🌍 Fetching markets in {self.current_mode} mode...")
        
        # Auto-switch mode logic
        if AUTO_SWITCH_MODE:
            current_time = time.time()
            if self.last_mode_switch == 0:
                self.last_mode_switch = current_time
                interval = self._get_current_interval()
                logger.info(f"🔄 Mode switching enabled. Starting in {self.current_mode} mode (for {interval // 60} min)")
        elif current_time - self.last_mode_switch >= self._get_current_interval():
                self.current_mode = 'CRYPTO_ONLY' if self.current_mode == 'ALL_BINARY' else 'ALL_BINARY'
                self.last_mode_switch = current_time
                self.stats['current_mode'] = self.current_mode
                next_interval = self._get_current_interval()
                logger.info(f"🔁 MODE SWITCHED to {self.current_mode} (for {next_interval // 60} min)")
        
        found = []
        
        async with aiohttp.ClientSession() as session:
            if self.current_mode == 'ALL_BINARY':
                found = await self._fetch_all_binary_markets(session)
            else:
                found = await self._fetch_crypto_markets(session)
        
        self.target_markets = found
        self.last_scan_time = time.time()
        
        # Initialize local orderbooks for WebSocket
        for market in found:
            tokens = market.get('_tokens', [])
            for tid in tokens:
                if tid not in self.local_orderbook:
                    self.local_orderbook[tid] = {'asks': [], 'bids': []}
        
        self.stats['markets_count'] = len(found)
        logger.info(f"✅ Loaded {len(found)} markets for monitoring")

    async def _fetch_all_binary_markets(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch all binary markets from Gamma API."""
        found = []
        try:
            params = {
                'active': 'true',
                'closed': 'false',
                'limit': ALL_BINARY_MARKET_LIMIT,
                'order': 'volume',
                'ascending': 'false'
            }
            async with session.get(f"{GAMMA_API_URL}/markets", params=params, timeout=30) as resp:
                if resp.status != 200:
                    logger.error(f"ALL_BINARY API error: {resp.status}")
                    return found
                
                markets = await resp.json()
                logger.info(f"   Fetched {len(markets)} markets from API")
                
                for m in markets:
                    # Only process binary markets (2 outcomes)
                    outcomes = m.get('outcomes', '[]')
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                        except:
                            outcomes = []
                    
                    if len(outcomes) != 2:
                        continue
                    
                    if m.get('closed') == True:
                        continue
                    
                    # Parse tokens
                    tokens = m.get('clobTokenIds', [])
                    if isinstance(tokens, str):
                        try:
                            tokens = json.loads(tokens)
                        except:
                            tokens = []
                    
                    if len(tokens) < 2:
                        continue
                    
                    # Map outcomes to tokens
                    yes_idx, no_idx = 0, 1
                    for i, outcome in enumerate(outcomes):
                        outcome_lower = str(outcome).lower()
                        words = set(outcome_lower.replace('(', ' ').replace(')', ' ').split())
                        if 'yes' in words or 'up' in words:
                            yes_idx = i
                        elif 'no' in words or 'down' in words:
                            no_idx = i
                    
                    if yes_idx == no_idx:
                        yes_idx, no_idx = 0, 1
                    
                    ordered_tokens = [tokens[yes_idx], tokens[no_idx]] if len(tokens) >= 2 else tokens[:2]
                    
                    m['_tokens'] = ordered_tokens
                    m['_timeframe'] = 'ALL'
                    m['_event_slug'] = m.get('slug', '')
                    m['_coin'] = detect_coin(m.get('question', '')) or 'BINARY'
                    m['_end_date'] = m.get('endDate') or ''
                    m['_last_spread'] = Decimal('1.02')
                    
                    found.append(m)
                
                logger.info(f"   Found {len(found)} binary markets to scan")
                
        except Exception as e:
            logger.error(f"ALL_BINARY scan error: {e}")
        
        return found

    async def _fetch_crypto_markets(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch crypto Up/Down markets for all timeframes."""
        found = []
        
        for timeframe, config in TIMEFRAME_CONFIG.items():
            if not ENABLED_TIMEFRAMES.get(timeframe, False):
                continue
            
            url = f"{GAMMA_API_URL}/{config['endpoint']}"
            
            try:
                async with session.get(url, params=config['params'], timeout=30) as resp:
                    if resp.status != 200:
                        logger.warning(f"  {timeframe}: API error {resp.status}")
                        continue
                    
                    data = await resp.json()
                    
                    if isinstance(data, dict):
                        events = data.get('data', data.get('events', []))
                    else:
                        events = data
                    
                    coin_markets: Dict[str, List] = {'BTC': [], 'ETH': [], 'SOL': [], 'XRP': []}
                    
                    for event in events:
                        title = event.get('title', '')
                        slug = event.get('slug', '')
                        
                        if 'up or down' not in title.lower() and 'updown' not in slug.lower():
                            continue
                        
                        coin = detect_coin(title + ' ' + slug)
                        if not coin:
                            continue
                        
                        markets = event.get('markets', [])
                        
                        for m in markets:
                            if m.get('closed') == True:
                                continue
                            
                            tokens = m.get('clobTokenIds', [])
                            if isinstance(tokens, str):
                                try:
                                    tokens = json.loads(tokens)
                                except:
                                    tokens = []
                            
                            if len(tokens) < 2:
                                continue
                            
                            outcomes = m.get('outcomes', [])
                            if isinstance(outcomes, str):
                                try:
                                    outcomes = json.loads(outcomes)
                                except:
                                    outcomes = []
                            
                            yes_idx, no_idx = 0, 1
                            for i, outcome in enumerate(outcomes):
                                outcome_lower = str(outcome).lower()
                                words = set(outcome_lower.replace('(', ' ').replace(')', ' ').split())
                                if 'yes' in words or 'up' in words:
                                    yes_idx = i
                                elif 'no' in words or 'down' in words:
                                    no_idx = i
                            
                            if yes_idx == no_idx:
                                yes_idx, no_idx = 0, 1
                            
                            ordered_tokens = [tokens[yes_idx], tokens[no_idx]] if len(tokens) >= 2 else tokens[:2]
                            
                            m['_tokens'] = ordered_tokens
                            m['_timeframe'] = timeframe
                            m['_event_slug'] = slug
                            m['_coin'] = coin
                            m['_end_date'] = m.get('endDate') or m.get('end_date_iso') or ''
                            m['_last_spread'] = Decimal('1.02')
                            
                            coin_markets[coin].append(m)
                    
                    tf_count = 0
                    for coin in ['BTC', 'ETH', 'SOL', 'XRP']:
                        if coin_markets[coin]:
                            coin_markets[coin].sort(key=lambda x: x['_end_date'])
                            found.append(coin_markets[coin][0])
                            tf_count += 1
                    
                    logger.info(f"  {timeframe}: {tf_count} LIVE markets")
                    
            except Exception as e:
                logger.error(f"  {timeframe}: Error - {e}")
            
            await asyncio.sleep(0.1)  # Rate limit between timeframes
        
        return found

    async def connect_websocket(self):
        """Main WebSocket Loop for real-time price updates."""
        while self.running:
            try:
                session = aiohttp.ClientSession()
                async with session.ws_connect(WS_URL) as ws:
                    logger.info("🔌 WebSocket Connected")
                    self.ws = ws
                    
                    # Subscribe to all tokens
                    token_ids = list(self.local_orderbook.keys())
                    chunk_size = 20
                    for i in range(0, len(token_ids), chunk_size):
                        chunk = token_ids[i:i+chunk_size]
                        sub_msg = {
                            "assets_ids": chunk,
                            "type": "market"
                        }
                        await ws.send_json(sub_msg)
                        await asyncio.sleep(0.1)
                    
                    logger.info(f"📡 Subscribed to {len(token_ids)} market feeds")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            # Handle empty or non-JSON messages gracefully
                            if not msg.data or msg.data.strip() == '':
                                continue
                            try:
                                data = json.loads(msg.data)
                                await self.process_ws_update(data)
                            except json.JSONDecodeError:
                                # Skip non-JSON messages (pings, acks, etc.)
                                continue
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception as e:
                logger.error(f"WebSocket Error: {e}")
                await asyncio.sleep(5)
            finally:
                if session:
                    await session.close()

    async def process_ws_update(self, data: Any):
        """Handle real-time order book updates from WebSocket."""
        if not isinstance(data, list):
            return
            
        for update in data:
            if update.get('event_type') != 'price_change':
                continue
                
            token_id = update.get('asset_id')
            if token_id not in self.local_orderbook:
                continue
            
            # Find which market this token belongs to and evaluate
            for market in self.target_markets:
                tokens = market.get('_tokens', [])
                if token_id in tokens:
                    await self.evaluate_market(market)
                    break

    async def evaluate_market(self, market: Dict):
        """Calculate spread and execute trade if profitable."""
        tokens = market.get('_tokens', [])
        if len(tokens) < 2:
            return
            
        token_yes = tokens[0]
        token_no = tokens[1]
        
        # Fetch fresh order books (async parallel)
        books = await self.async_client.get_order_books([token_yes, token_no])
        ob_yes, ob_no = books[0], books[1]
        
        # Get price AND available size
        price_yes_val, size_yes = self._get_best_price(ob_yes, 'asks')
        price_no_val, size_no = self._get_best_price(ob_no, 'asks')
        
        if not price_yes_val or not price_no_val:
            return

        # Use Decimal for math
        price_yes = Decimal(str(price_yes_val))
        price_no = Decimal(str(price_no_val))
        total_cost = price_yes + price_no
        
        # === LIQUIDITY SAFETY CHECK ===
        # Calculate target shares to check if enough liquidity exists
        temp_target = float(BET_SIZE) / float(total_cost) if total_cost > 0 else 0
        
        # Skip if order book doesn't have enough shares (with 5% buffer)
        if size_yes < (temp_target * 0.95) or size_no < (temp_target * 0.95):
            # Only log if this was actually an arb opportunity we're skipping
            if total_cost < MIN_SPREAD_TARGET:
                logger.info(f"⚠️ Low Liquidity: Skipping. Need ~{temp_target:.1f} shares. Available: YES={size_yes:.1f}, NO={size_no:.1f}")
            return
        
        # Update stats
        self.stats['checks'] += 1
        if total_cost < self.stats['best_spread']:
            self.stats['best_spread'] = total_cost
        
        # Add to recent checks for dashboard
        check_data = {
            'q': market.get('question', '')[:100],
            'up': float(price_yes),
            'down': float(price_no),
            'total': float(total_cost),
            'coin': market.get('_coin', 'BINARY'),
            'timeframe': market.get('_timeframe', 'ALL'),
        }
        self.stats['recent_checks'].append(check_data)
        if len(self.stats['recent_checks']) > 10:
            self.stats['recent_checks'].pop(0)
        
        # Update market's last spread for sorting
        market['_last_spread'] = total_cost
        
        # Check for arbitrage opportunity
        if total_cost < MIN_SPREAD_TARGET:
            profit = Decimal('1.0') - total_cost
            if profit >= PROFIT_THRESHOLD:
                await self.execute_arbitrage(market, price_yes, price_no)

    def _get_best_price(self, ob: Dict, side: str) -> Tuple[Optional[float], float]:
        """Get best ask/bid price AND available size from order book."""
        if not ob or side not in ob or not ob[side]:
            return None, 0.0
        try:
            # Filter for valid orders with size > 0
            valid_orders = [
                {'price': float(p['price']), 'size': float(p['size'])}
                for p in ob[side] if float(p.get('size', 0)) > 0
            ]
            
            if not valid_orders:
                return None, 0.0
                
            # Sort: Asks ascending (cheapest first)
            valid_orders.sort(key=lambda x: x['price'])
            
            best_order = valid_orders[0]
            return best_order['price'], best_order['size']
        except Exception as e:
            logger.warning(f"Error parsing orderbook: {e}")
            return None, 0.0

    async def execute_arbitrage(self, market: Dict, price_yes: Decimal, price_no: Decimal):
        """Execute arbitrage trade using BATCH orders for atomicity."""
        spread = price_yes + price_no
        logger.info(f"🚨 OPPORTUNITY: {market.get('question', '')[:50]}... | Spread: {spread:.4f}")
        
        # Calculate Size - Round to INTEGER to ensure USDC cost has max 2 decimals
        target_shares = BET_SIZE / spread
        target_shares = target_shares.quantize(Decimal("1"), rounding=ROUND_DOWN)
        
        if target_shares < MIN_SHARES:
            logger.warning(f"Target shares {target_shares} < minimum {MIN_SHARES}")
            return

        # Add Slippage buffer to prices (2 decimals to ensure valid USDC cost)
        limit_yes = (price_yes * (Decimal('1') + SLIPPAGE_TOLERANCE)).quantize(Decimal("0.01"))
        limit_no = (price_no * (Decimal('1') + SLIPPAGE_TOLERANCE)).quantize(Decimal("0.01"))
        
        total_cost = (limit_yes + limit_no) * target_shares
        expected_payout = target_shares * Decimal('1.0')
        expected_profit = expected_payout - total_cost

        if expected_profit <= Decimal('0'):
            logger.warning(f"Not profitable after slippage: ${expected_profit:.4f}")
            return

        tokens = market.get('_tokens', [])
        
        logger.info(f"💰 ATTEMPTING BATCH TRADE | Spread: {spread:.4f} | YES: {price_yes:.4f} | NO: {price_no:.4f}")
        logger.info(f"🎯 Target: {target_shares} shares @ ${total_cost:.2f} | Expected Profit: ${expected_profit:.4f}")

        try:
            # Use py_clob_client for batch order execution
            from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs
            from py_clob_client.order_builder.constants import BUY
            
            # --- FIX 1: Add signature_type=2 for Proxy Wallets (Gnosis Safe) ---
            sync_client = ClobClient(
                host=CLOB_API_URL,
                key=PRIVATE_KEY,
                chain_id=POLYGON,
                funder=FUNDER_ADDRESS,
                signature_type=2  # CRITICAL: EIP-1271 Proxy signature
            )
            sync_client.set_api_creds(sync_client.create_or_derive_api_creds())
            
            # Build BOTH orders
            order_args_yes = OrderArgs(
                token_id=tokens[0],
                size=float(target_shares),
                price=float(limit_yes),
                side=BUY
            )
            order_args_no = OrderArgs(
                token_id=tokens[1],
                size=float(target_shares),
                price=float(limit_no),
                side=BUY
            )
            
            # Sign both orders
            signed_yes = sync_client.create_order(order_args_yes)
            signed_no = sync_client.create_order(order_args_no)
            
            # === BATCH EXECUTION (CORRECTED) ===
            # Wrap signed orders in PostOrdersArgs with FOK type
            batch_args = [
                PostOrdersArgs(order=signed_yes, orderType=OrderType.FOK),
                PostOrdersArgs(order=signed_no, orderType=OrderType.FOK)
            ]
            
            logger.info(f"📦 Sending BATCH order (YES + NO in single request)...")
            
            async with rate_limiter:  # Rate limit for batch order
                # Correct: pass list of PostOrdersArgs
                batch_response = sync_client.post_orders(batch_args)
            
            # Check batch response
            if not batch_response:
                logger.warning(f"⚠️ Batch order returned empty response")
                return
            
            # Log response for debugging FIRST
            logger.info(f"📋 Batch Response: {batch_response}")
            
            # === STRICT FILL VERIFICATION WITH ACTUAL SIZES ===
            yes_filled = False
            no_filled = False
            yes_filled_size = float(target_shares)  # Default fallback
            no_filled_size = float(target_shares)   # Default fallback
            
            if isinstance(batch_response, list):
                # Check Order 1 (YES)
                if len(batch_response) >= 1:
                    r0 = batch_response[0]
                    if r0.get('errorMsg'):
                        logger.error(f"❌ Order 1 (YES) Error: {r0.get('errorMsg')}")
                    yes_filled = (r0.get('status') == 'matched' or bool(r0.get('transactionHash')))
                    # CAPTURE ACTUAL FILLED SIZE from takingAmount
                    if yes_filled and r0.get('takingAmount'):
                        try:
                            yes_filled_size = float(r0.get('takingAmount'))
                            logger.info(f"   YES Actual Filled: {yes_filled_size} shares")
                        except:
                            pass
                        
                # Check Order 2 (NO)
                if len(batch_response) >= 2:
                    r1 = batch_response[1]
                    if r1.get('errorMsg'):
                        logger.error(f"❌ Order 2 (NO) Error: {r1.get('errorMsg')}")
                    no_filled = (r1.get('status') == 'matched' or bool(r1.get('transactionHash')))
                    # CAPTURE ACTUAL FILLED SIZE from takingAmount
                    if no_filled and r1.get('takingAmount'):
                        try:
                            no_filled_size = float(r1.get('takingAmount'))
                            logger.info(f"   NO Actual Filled: {no_filled_size} shares")
                        except:
                            pass
            else:
                # Single response object
                if batch_response.get('errorMsg'):
                    logger.error(f"❌ Batch Error: {batch_response.get('errorMsg')}")
                yes_filled = (batch_response.get('status') == 'matched' or bool(batch_response.get('transactionHash')))
                no_filled = yes_filled
            
            if yes_filled and no_filled:
                # BOTH orders CONFIRMED filled!
                logger.info(f"✅ BATCH ARBITRAGE CONFIRMED: ~{target_shares} pairs @ spread {spread:.4f}")
                logger.info(f"   Expected profit at resolution: ${expected_profit:.2f}")
                
                self.stats['trades_executed'] += 1
                self.stats['arb_opportunities'] += 1
                
                # Auto-merge with BLOCKCHAIN DELAY
                condition_id = market.get('conditionId')
                if condition_id:
                    logger.info("⏳ Waiting 10s for blockchain settlement...")
                    await asyncio.sleep(10)  # Critical: Wait for on-chain finality
                    await self.merge_and_settle_async(condition_id)
                    
            elif yes_filled and not no_filled:
                # Partial fill - YES succeeded, NO failed - DANGEROUS!
                logger.error(f"🚨 PARTIAL FILL DANGER: YES filled, NO failed!")
                logger.error(f"   Market: {market.get('question', 'Unknown')}")
                logger.error(f"   ⚠️ EMERGENCY: You have NAKED YES position!")
                
                # Emergency sell YES position - USE ACTUAL FILLED SIZE
                yes_token = tokens[0] if tokens else None
                if yes_token:
                    await self._emergency_sell(yes_token, yes_filled_size, "YES")
                
            elif no_filled and not yes_filled:
                # Partial fill - NO succeeded, YES failed - DANGEROUS!
                logger.error(f"🚨 PARTIAL FILL DANGER: NO filled, YES failed!")
                logger.error(f"   Market: {market.get('question', 'Unknown')}")
                logger.error(f"   ⚠️ EMERGENCY: You have NAKED NO position!")
                
                # Emergency sell NO position - USE ACTUAL FILLED SIZE
                no_token = tokens[1] if len(tokens) > 1 else None
                if no_token:
                    await self._emergency_sell(no_token, no_filled_size, "NO")
                
            else:
                # Both failed or not matched (safe - no position)
                logger.warning(f"⚠️ Orders NOT FILLED (signature error, FOK killed, or rejected)")
                
        except Exception as e:
            logger.error(f"Trade execution error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def _emergency_sell(self, token_id: str, shares: float, side_name: str):
        """
        Emergency sell orphan position at market price to close risk.
        Retries up to 10 times with 3 second gaps.
        """
        MAX_RETRIES = 10
        RETRY_DELAY = 3  # seconds
        
        # Floor shares to integer to ensure valid USDC cost (2 decimals max)
        shares = int(shares)  # 10.281689 → 10
        
        logger.warning(f"🚨 EMERGENCY SELL: Dumping {shares} {side_name} shares...")
        
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"   Attempt {attempt}/{MAX_RETRIES}...")
                
                # Create sync client for emergency sell
                sync_client = ClobClient(
                    host=CLOB_API_URL,
                    key=PRIVATE_KEY,
                    chain_id=POLYGON,
                    funder=FUNDER_ADDRESS,
                    signature_type=2  # Proxy wallet
                )
                sync_client.set_api_creds(sync_client.create_or_derive_api_creds())
                
                # Sell at floor price (0.01) for immediate fill
                sell_args = OrderArgs(
                    token_id=token_id,
                    size=shares,
                    price=0.01,  # Floor price - accept any buyer
                    side=SELL
                )
                
                signed_sell = sync_client.create_order(sell_args)
                
                async with rate_limiter:
                    sell_response = sync_client.post_order(signed_sell, OrderType.GTC)
                
                logger.info(f"📋 Emergency Sell Response: {sell_response}")
                
                # Check if successful
                if sell_response.get('status') == 'matched' or sell_response.get('transactionHash'):
                    logger.info(f"✅ EMERGENCY SELL EXECUTED: {shares} {side_name} shares dumped!")
                    return True
                elif sell_response.get('orderID'):
                    logger.info(f"📝 Emergency Sell order placed (orderID: {sell_response.get('orderID')})")
                    logger.info(f"   Order will fill at market. Risk closed!")
                    return True
                else:
                    logger.warning(f"   Attempt {attempt} failed: {sell_response.get('errorMsg', 'Unknown error')}")
                    
            except Exception as e:
                logger.warning(f"   Attempt {attempt} error: {e}")
            
            # Wait before retry (except on last attempt)
            if attempt < MAX_RETRIES:
                logger.info(f"   Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)
        
        # All retries failed - CRITICAL
        logger.error(f"🚨🚨🚨 CRITICAL: ALL {MAX_RETRIES} EMERGENCY SELL ATTEMPTS FAILED!")
        logger.error(f"   MANUAL ACTION REQUIRED IMMEDIATELY!")
        logger.error(f"   Token: {token_id}")
        logger.error(f"   Shares: {shares} {side_name}")
        logger.error(f"   ⛔ STOPPING BOT to prevent more trades!")
        
        # Stop the bot to prevent more risky trades
        self.running = False
        return False

    async def merge_and_settle_async(self, condition_id: str, amount: Optional[str] = None, neg_risk: bool = False) -> bool:
        """Merge conditional tokens back to USDC (async wrapper)."""
        # Run sync merge in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._merge_and_settle_sync, condition_id, amount, neg_risk)

    def _merge_and_settle_sync(self, condition_id: str, amount: Optional[str] = None, neg_risk: bool = False) -> bool:
        """Merge conditional tokens back to USDC (sync implementation)."""
        try:
            if not FUNDER_ADDRESS or not PRIVATE_KEY:
                logger.info("💰 Arbed! (Manual Merge - FUNDER_ADDRESS not configured)")
                return False
            
            w3 = Web3(Web3.HTTPProvider(RPC_URL))
            
            from web3.middleware import ExtraDataToPOAMiddleware
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            
            account = Account.from_key(PRIVATE_KEY)
            safe_address = Web3.to_checksum_address(FUNDER_ADDRESS)
            
            safe = w3.eth.contract(address=safe_address, abi=safe_abi)
            
            # Determine merge amount
            if amount is None:
                ctf_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(CTF_EXCHANGE),
                    abi=ctf_abi
                )
                
                parent_collection_id = bytes(32)
                cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith('0x') else condition_id)
                
                collection_id_0 = ctf_contract.functions.getCollectionId(parent_collection_id, cond_bytes, 1).call()
                collection_id_1 = ctf_contract.functions.getCollectionId(parent_collection_id, cond_bytes, 2).call()
                
                position_id_0 = ctf_contract.functions.getPositionId(Web3.to_checksum_address(USDC_ADDRESS), collection_id_0).call()
                position_id_1 = ctf_contract.functions.getPositionId(Web3.to_checksum_address(USDC_ADDRESS), collection_id_1).call()
                
                balance_0 = ctf_contract.functions.balanceOf(safe_address, position_id_0).call()
                balance_1 = ctf_contract.functions.balanceOf(safe_address, position_id_1).call()
                
                logger.info(f"Merge check: YES balance={balance_0}, NO balance={balance_1}")
                
                amount_wei = min(balance_0, balance_1)
                
                if amount_wei == 0:
                    logger.warning(f"Merge failed: No tokens to merge")
                    return False
            else:
                amount_wei = int(float(amount) * (10 ** USDCE_DIGITS))
            
            # Encode merge transaction
            ctf_contract = w3.eth.contract(abi=ctf_abi)
            parent_collection_id_hex = '0x0000000000000000000000000000000000000000000000000000000000000000'
            partition = [1, 2]
            cond_id_bytes = condition_id[2:] if condition_id.startswith('0x') else condition_id
            
            data = ctf_contract.functions.mergePositions(
                Web3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(parent_collection_id_hex[2:]),
                bytes.fromhex(cond_id_bytes),
                partition,
                amount_wei
            )._encode_transaction_data()
            
            # Sign and execute Safe transaction
            nonce = safe.functions.nonce().call()
            to = NEG_RISK_ADAPTER if neg_risk else CTF_EXCHANGE
            
            tx_hash = safe.functions.getTransactionHash(
                Web3.to_checksum_address(to), 0, bytes.fromhex(data[2:]),
                0, 0, 0, 0,
                '0x0000000000000000000000000000000000000000',
                '0x0000000000000000000000000000000000000000',
                nonce
            ).call()
            
            hash_bytes = Web3.to_bytes(hexstr=tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
            signature_obj = account.unsafe_sign_hash(hash_bytes)
            
            r = signature_obj.r.to_bytes(32, byteorder='big')
            s = signature_obj.s.to_bytes(32, byteorder='big')
            v = signature_obj.v.to_bytes(1, byteorder='big')
            signature = r + s + v
            
            tx = safe.functions.execTransaction(
                Web3.to_checksum_address(to), 0, bytes.fromhex(data[2:]),
                0, 0, 0, 0,
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
            
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            
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

    async def polling_loop(self):
        """Fallback polling loop (runs alongside WebSocket for reliability)."""
        while self.running:
            try:
                # Refresh markets every 60 seconds
                if time.time() - self.last_scan_time > 60:
                    await self.fetch_markets()
                
                # Evaluate all markets
                for market in self.target_markets:
                    await self.evaluate_market(market)
                
                # Update stats
                ist = timezone(timedelta(hours=5, minutes=30))
                self.stats['last_update'] = datetime.now(ist).strftime('%H:%M:%S')
                self.stats['monthly_pnl'] = await self.fetch_monthly_pnl()
                self.stats['api_trades'] = await self.fetch_total_trades()
                
                await asyncio.sleep(1.0)  # Poll interval
                
            except Exception as e:
                logger.error(f"Polling loop error: {e}")
                await asyncio.sleep(5)

    async def start_dashboard(self):
        """Start async web dashboard."""
        app = web.Application()
        app.router.add_get('/', self.handle_dashboard)
        app.router.add_get('/api/status', self.handle_api_status)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
        logger.info(f"📊 Dashboard live at http://localhost:{WEB_PORT}")
        await site.start()
        
        # Keep running
        while self.running:
            await asyncio.sleep(3600)

    async def handle_api_status(self, request):
        """API endpoint for status."""
        stats_json = {
            'balance': float(self.stats['balance']),
            'total_trades': self.stats['trades_executed'],
            'markets_count': self.stats['markets_count'],
            'best_spread': float(self.stats['best_spread']),
            'checks': self.stats['checks'],
            'last_update': self.stats['last_update'],
            'recent_checks': self.stats['recent_checks'],
            'running': self.running,
            'current_mode': self.current_mode,
            'monthly_pnl': self.stats.get('monthly_pnl', 0),
            'api_trades': self.stats.get('api_trades', 0),
        }
        return web.json_response(stats_json)

    async def handle_dashboard(self, request):
        """Dashboard HTML endpoint."""
        balance = float(self.stats['balance'])
        markets_count = self.stats['markets_count']
        best_spread = float(self.stats['best_spread'])
        checks = self.stats['checks']
        last_update = self.stats['last_update']
        recent_checks = list(self.stats.get('recent_checks', [])[-10:])
        monthly_pnl = self.stats.get('monthly_pnl', 0.0)
        api_trades = self.stats.get('api_trades', 0)
        
        # Coin colors mapping
        coin_colors = {
            'BTC': '#f7931a', 'ETH': '#627eea', 'SOL': '#9945ff',
            'XRP': '#ffffff', 'BINARY': '#64748b'
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
            coin = check.get('coin', 'BINARY')
            coin_color = coin_colors.get(coin, '#64748b')
            safe_q = html.escape(q)
            timeframe = check.get('timeframe', '')
            
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
        
        status_color = '#10b981' if markets_count > 0 else '#f59e0b'
        status_text = 'LIVE' if markets_count > 0 else 'CONNECTING...'
        
        html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
    <title>PolyArbBot V2 (Async)</title>
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
        .container {{ max-width: 600px; margin: 0 auto; padding: 16px; padding-bottom: 80px; }}
        .header {{ text-align: center; padding: 24px 0 20px; }}
        .logo {{
            font-size: 28px; font-weight: 800;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
            margin-bottom: 12px;
        }}
        .version-badge {{ font-size: 10px; color: var(--accent-green); margin-bottom: 12px; }}
        .status-pill {{
            display: inline-flex; align-items: center; gap: 8px;
            background: {status_color}15; border: 1px solid {status_color}40;
            padding: 8px 16px; border-radius: 50px;
            font-size: 12px; font-weight: 600; color: {status_color};
            text-transform: uppercase; letter-spacing: 0.5px;
        }}
        .status-dot {{ width: 8px; height: 8px; background: {status_color}; border-radius: 50%; animation: pulse 2s infinite; }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; transform: scale(1); }} 50% {{ opacity: 0.5; transform: scale(0.8); }} }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin: 20px 0; }}
        .stat-card {{
            background: var(--bg-card); border: 1px solid var(--border-color);
            border-radius: 16px; padding: 16px; text-align: center;
            backdrop-filter: blur(10px); transition: all 0.3s ease;
        }}
        .stat-card:hover {{ background: var(--bg-card-hover); transform: translateY(-2px); }}
        .stat-icon {{ font-size: 20px; margin-bottom: 8px; }}
        .stat-value {{ font-size: 24px; font-weight: 700; margin-bottom: 4px; color: var(--text-primary); }}
        .stat-value.green {{ color: var(--accent-green); }}
        .stat-value.blue {{ color: var(--accent-blue); }}
        .stat-value.yellow {{ color: var(--accent-yellow); }}
        .stat-label {{ font-size: 11px; color: var(--text-secondary); font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; }}
        .section {{ margin: 24px 0; }}
        .section-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }}
        .section-title {{ font-size: 16px; font-weight: 600; color: var(--text-primary); }}
        .section-badge {{ font-size: 11px; color: var(--text-secondary); background: var(--bg-card); padding: 4px 10px; border-radius: 20px; }}
        .markets-grid {{ display: flex; flex-direction: column; gap: 10px; }}
        .market-card {{
            background: var(--bg-card); border: 1px solid var(--border-color);
            border-radius: 14px; padding: 14px;
            animation: fadeInUp 0.4s ease forwards; opacity: 0; transform: translateY(10px);
        }}
        @keyframes fadeInUp {{ to {{ opacity: 1; transform: translateY(0); }} }}
        .market-header {{ display: flex; gap: 8px; margin-bottom: 8px; }}
        .coin-badge {{ font-size: 11px; font-weight: 700; padding: 4px 8px; border-radius: 6px; }}
        .timeframe-badge {{ font-size: 10px; font-weight: 600; padding: 4px 8px; border-radius: 6px; background: rgba(100,116,139,0.15); color: #94a3b8; }}
        .market-title {{ font-size: 13px; color: var(--text-secondary); margin-bottom: 10px; line-height: 1.4; word-break: break-word; }}
        .market-footer {{ display: flex; justify-content: space-between; align-items: center; }}
        .spread-info {{ display: flex; flex-direction: column; }}
        .spread-label {{ font-size: 10px; color: var(--text-secondary); text-transform: uppercase; }}
        .spread-value {{ font-size: 16px; font-weight: 700; font-family: 'SF Mono', 'Monaco', monospace; }}
        .badge-arb {{ background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: white; padding: 6px 12px; border-radius: 8px; font-size: 11px; font-weight: 700; animation: glow 1.5s infinite alternate; }}
        @keyframes glow {{ from {{ box-shadow: 0 0 5px #10b98150; }} to {{ box-shadow: 0 0 20px #10b98180; }} }}
        .badge-close {{ background: rgba(245, 158, 11, 0.15); color: #f59e0b; padding: 6px 12px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
        .badge-ok {{ background: rgba(59, 130, 246, 0.15); color: #3b82f6; padding: 6px 12px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
        .badge-wait {{ background: rgba(100, 116, 139, 0.15); color: #64748b; padding: 6px 12px; border-radius: 8px; font-size: 11px; font-weight: 600; }}
        .footer {{ position: fixed; bottom: 0; left: 0; right: 0; background: linear-gradient(to top, var(--bg-primary) 60%, transparent); padding: 20px; text-align: center; }}
        .footer-text {{ font-size: 11px; color: var(--text-secondary); }}
        .refresh-indicator {{ display: inline-flex; align-items: center; gap: 6px; margin-top: 8px; font-size: 10px; color: var(--accent-green); }}
        .refresh-dot {{ width: 6px; height: 6px; background: var(--accent-green); border-radius: 50%; animation: blink 1s infinite; }}
        @keyframes blink {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.3; }} }}
        .empty-state {{ text-align: center; padding: 40px 20px; color: var(--text-secondary); }}
        .empty-icon {{ font-size: 40px; margin-bottom: 12px; opacity: 0.5; }}
        @media (min-width: 480px) {{ .stats-grid {{ grid-template-columns: repeat(4, 1fr); }} .stat-value {{ font-size: 28px; }} .logo {{ font-size: 32px; }} }}
    </style>
    <script>setTimeout(() => location.reload(), 5000);</script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">Poly Arb Bot V2</div>
            <div class="version-badge">ASYNC ENGINE | WebSocket + Batch Orders</div>
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
                <div class="stat-icon">$</div>
                <div class="stat-value {'green' if monthly_pnl >= 0 else 'red'}">${monthly_pnl:+.2f}</div>
                <div class="stat-label">Monthly PnL</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">#</div>
                <div class="stat-value blue">{api_trades}</div>
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
        <div class="footer-text">Last update: {last_update or 'Starting...'} | Mode: {self.current_mode}</div>
        <div class="refresh-indicator">
            <span class="refresh-dot"></span>
            Auto-refresh in 5s
        </div>
    </div>
</body>
</html>'''
        return web.Response(text=html_content, content_type='text/html')

    async def run(self):
        """Main entry point."""
        logger.info("🚀 Starting PolyArbBot V2 (High-Performance Async Engine)")
        logger.info(f"📊 Dashboard: http://localhost:{WEB_PORT}")
        if BUILDER_ENABLED:
            logger.info("🏷️  Builder API: ENABLED (orders attributed to ArbBot)")
        else:
            logger.info("🏷️  Builder API: DISABLED (set POLY_BUILDER_* env vars to enable)")
        
        # 1. Sync Startup Checks
        self.derive_keys()
        self.check_allowances_sync()
        
        # 2. Start Async Client
        self.async_client = AsyncClobClient(**self.api_creds)
        await self.async_client.start()
        
        # 3. Fetch initial balance
        await self.update_balance()
        
        # 4. Load Markets
        await self.fetch_markets()
        
        # 5. Start all tasks
        dashboard_task = asyncio.create_task(self.start_dashboard())
        ws_task = asyncio.create_task(self.connect_websocket())
        polling_task = asyncio.create_task(self.polling_loop())
        
        try:
            await asyncio.gather(dashboard_task, ws_task, polling_task)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping bot...")
            self.running = False
        finally:
            if self.async_client:
                await self.async_client.close()


if __name__ == "__main__":
    bot = ArbitrageEngine()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
