# Polymarket Arbitrage Bot 🤖

A real-money arbitrage bot for Polymarket crypto Up/Down markets. Exploits mispricings where `YES + NO < $1.00` for guaranteed profit.

## How It Works

```
If YES @ $0.48 + NO @ $0.49 = $0.97
→ Buy both, receive $1.00 at resolution
→ Profit: $0.03 per pair (3% risk-free!)
```

## Features

- 🎯 **Multi-timeframe scanning** — 15min, 1H, 4H, Daily markets
- ⚡ **Fast polling** — Checks spreads every 1 second
- 🛡️ **Safety features** — Liquidity checks, FOK orders, emergency exit
- 💰 **Auto-merge** — Converts tokens back to USDC on-chain
- 📊 **Live dashboard** — Monitor bot status via web UI

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
export PRIVATE_KEY=0xyour_private_key
export FUNDER_ADDRESS=0xyour_polymarket_profile_address
```

### 3. Run

```bash
python poly_arb_bot_cloud.py
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo to Railway
3. Add environment variables in Railway dashboard
4. Deploy

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PRIVATE_KEY` | ✅ | Wallet private key for signing |
| `FUNDER_ADDRESS` | ✅ | Polymarket profile address |
| `PORT` | ❌ | Dashboard port (default: 8080) |
| `RPC_URL` | ❌ | Polygon RPC (default: polygon-rpc.com) |

## Configuration

Edit these values in `poly_arb_bot_cloud.py`:

```python
MIN_SPREAD_TARGET = 0.98    # Only trade if spread < this
BET_SIZE = 5.0              # USD per trade
MIN_SHARES = 5.0            # Minimum shares per side
```

## Safety Features

- **Pre-trade liquidity check** — Skips illiquid markets
- **FOK orders** — Fill-or-kill prevents partial fills
- **Emergency exit** — Auto-sells if one leg fails
- **Duplicate prevention** — Won't re-trade same market

## Disclaimer

⚠️ **Use at your own risk.** This bot trades real money. Past performance does not guarantee future results. Always test with small amounts first.

## Support

☕ **Buy me a coffee** by sending USDC to:
```
0x06422770fda6c2f4774f445ca5354d2cf6f25cec
```
(Polygon/Ethereum/Base)

## License

MIT
