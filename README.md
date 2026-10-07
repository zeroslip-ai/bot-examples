# ZeroSlip bot examples

![ZeroSlip bot examples](assets/bot-examples-banner.png)

Python scripts for token sniping, wallet copy trading, backtesting, and selling balances. Configure a token or watched wallets and run from your terminal.

## Setup

Requires **Python 3.11+**.

```sh
git clone https://github.com/zeroslip-ai/bot-examples.git
cd bot-examples
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp config.example.toml config.toml
```

On Windows, activate with `.venv\Scripts\activate`. On Debian/Ubuntu, install `python3-venv` if needed.

Run commands from the repository folder. Edit `config.toml` for amounts and strategy settings; [config.example.toml](config.example.toml) explains each option. **Streaming bots start in paper mode; sell commands start as previews.** Neither submits trades without `--live`.

Run one streaming bot at a time and close other stream clients, including agents, to avoid the feed's connection limit.

## Run an example

Replace `TOKEN_MINT` or `WALLET_ADDRESS` with a Solana address.

### Buy a token and manage exits

```sh
python scripts/live_sniper_bot.py --mint TOKEN_MINT
```

Buys once on that token's next SOL/USDC trade, then exits on take-profit, stop-loss, or inactivity. Set `trade.buy_amount` and `sniper.take_profit`, `stop_loss`, and `idle_seconds` in `config.toml`.

### Snipe new Pump.fun launches

```sh
python scripts/live_sniper_bot.py
```

Leave `sniper.token_mint` empty. Buys launches whose initial purchase exceeds `sniper.min_initial_buy` (default 20 SOL-equivalent), then manages the same exits through Pump.fun and Pump-created PumpSwap pools. Mayhem launches are excluded.

### Copy a wallet

```sh
python scripts/copytrader_bot.py --wallet WALLET_ADDRESS
```

Copies eligible first buys at 10% of the observed amount, capped at 0.01 SOL-equivalent, and mirrors proportional sells. Adjust `copytrader.buy_fraction` and `max_buy_amount`. Repeat `--wallet` for multiple wallets, or save them in `copytrader.wallets`.

### Backtest launch sniping

```sh
python scripts/backtest_sniper_strategy.py --hours 1
# Or use a local archive:
python scripts/backtest_sniper_strategy.py --file /path/to/events.jsonl.zst
```

Replays completed UTC hours using the launch strategy and `[backtest]` latency/fee settings. It does not backtest selected-token mode. Archives can exceed hundreds of MB per hour; set `TMPDIR` if temporary storage is limited. Results are estimates; see [strategy notes](docs/strategy-notes.md) for model limitations.

### Preview or sell balances

Set `wallet.public_key` in `config.toml`, then run:

```sh
python scripts/sell_all_tokens.py --mint TOKEN_MINT
# Or select all non-quote balances:
python scripts/sell_all_tokens.py --all
```

Reads balances through Solana RPC. Frozen balances, wrapped SOL, and `trade.quote_mint` are skipped. Choose specific mints or `--all`, not both.

## Trade live

Copy `.env.example` to `.env` and set **one** credential for your funded ZeroSlip wallet:

```dotenv
ZEROSLIP_API_KEY=your_wallet_api_key
# Or: ZEROSLIP_PRIVATE_KEY=your_base58_private_key
```

Add `--live` to a sniper, copytrader, or sell command:

```sh
python scripts/live_sniper_bot.py --mint TOKEN_MINT --live
```

Credentials go to the [Lightning Trade API](https://docs.zeroslip.ai/trade-api) for signing and execution. Keep `.env` local. Live sells use the credential's wallet; their balance query incurs an API fee.

Before trading:

- Review amounts and slippage in `config.toml`: defaults are 20% for buys, 99% for streaming sells, and 100% for sell-all. Streaming bots support SOL and USDC; fund both to trade both.
- Positions exist only in memory. Ctrl+C does not sell holdings; restarting does not restore them. Use one strategy per wallet and the sell script for leftover positions.
- Uncertain orders stop the bot without retrying. Check your wallet before restarting. Stream reconnects cannot recover missed events.

## Work with a coding agent

Give the agent this repository and the [full ZeroSlip API docs](https://docs.zeroslip.ai/llms-full.txt). Adjust configuration before strategy code. Every script requires [bot_common.py](scripts/bot_common.py); keep it alongside the entry points. [Strategy notes](docs/strategy-notes.md) explain entry filters and replay assumptions.

Add `--check-config` to any example command to validate settings without network calls. Use `--help` for CLI options. For code changes, run the local test suite; it submits no real trades:

```sh
python -m unittest discover -s tests -v
```

[Bot guide](https://docs.zeroslip.ai/bot-examples) · [Upstream sources](UPSTREAM.json) · [Unlicense](LICENSE)
