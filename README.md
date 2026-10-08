# ZeroSlip bot examples

![ZeroSlip bot examples](assets/bot-examples-banner.png)

Python scripts for sniping, copy trading, backtesting, and selling balances. Choose a token or watched wallets and run from your terminal.

## How it works

Streaming bots read public ZeroSlip market events and apply their strategy rules. Paper mode logs simulated trades and PnL. With `LIVE = True`, the script sends orders and your credential to the [Lightning Trade API](https://docs.zeroslip.ai/trade-api), which signs and executes them. Backtesting reads historical events; sell previews query Solana RPC.

## Setup

Download one file from [`standalone/`](standalone), open it, and edit the **SETTINGS** section at the top. Each file contains its own runtime; no other repository files, TOML, `.env`, or arguments are required.

Use Python 3.11+ with these packages installed once in your Python environment:

```sh
python3 -m pip install 'aiohttp>=3.12,<4' 'websockets>=15,<16' 'cachetools>=5.5,<8' \
  'orjson>=3.10,<4' 'zstandard>=0.23,<1'
```

Then run `python3 file.py` as shown below. Paper mode and read-only previews are the defaults. Run one streaming bot at a time; close other feed clients, including agents.

If you prefer automatic dependency setup, install [uv](https://docs.astral.sh/uv/getting-started/installation/) and use `uv run file.py` instead. This also works when your system Python prevents package installation.

## Examples

Edit the Python settings at the top of your downloaded file. Addresses are Solana base58 addresses.

### Buy a token or snipe launches

```sh
python3 live_sniper_bot.py
```

Set `CONFIG['sniper']['token_mint']` to buy a token, or leave it empty to snipe launches. Selected-token mode buys once and manages take-profit, stop-loss, and idle exits. In launch mode, it buys non-Mayhem launches over 20 SOL-equivalent and follows Pump-created PumpSwap pools. Defaults: 0.001 SOL-equivalent per buy, 50% take-profit, 20% stop-loss, 300 seconds idle.

Paper entries wait for a subsequent trade price and count misses above buy slippage. Paper exits report realized PnL excluding fees and depth; results do not establish profitability.

### Copy a wallet

```sh
python3 copytrader_bot.py
```

Set `CONFIG['copytrader']['wallets']` to the public wallets you follow. Copies first buys in non-Mayhem Pump.fun or Pump-created PumpSwap pools at 10% of the observed amount, capped at 0.01 SOL-equivalent, and mirrors proportional sells.

### Backtest launch sniping

```sh
python3 backtest_sniper_strategy.py
```

Set `CONFIG['backtest']['hours']` (default: 1), or `REPLAY_FILE` for a downloaded archive. Uses launch rules and modeled latency/fees. Reports PnL, misses, exit reasons, win rate, price moves, entry slippage, best/worst trades, and the replay window. Zero trades can mean every entry exceeded buy slippage. The newest hour may take 20–90 seconds to appear; retry a 404 after 90 seconds. Archives can exceed hundreds of MB per hour; use `TMPDIR` for temporary storage.

### Preview balances

```sh
python3 sell_all_tokens.py
```

Set `CONFIG['wallet']['public_key']` to the public wallet to preview. `SELL_ALL = True` selects every non-quote balance. To select specific tokens, set it to `False` and fill `CONFIG['sell']['token_mints']`. Frozen balances, wrapped SOL, and USDC are skipped.

## Trade live

Set `API_KEY` or `PRIVATE_KEY` at the top of your sniper, copytrader, or sell file, then set `LIVE = True`. API keys take precedence. Keep files containing credentials private. Live sells use the credential's wallet and incur a balance-query fee.

- Review slippage: defaults are 20% for buys, 99% for streaming sells, and 100% for sell-all. Fund SOL and USDC to trade both quote markets.
- Positions stay in memory. Stopping does not sell holdings; restarting does not restore them. Use one strategy per wallet.
- Uncertain orders stop without retrying. Check the wallet before restarting. Reconnects back off up to 30 seconds but cannot recover missed events.

## Work with a coding agent

Give the agent a single-file example and the [full API docs](https://docs.zeroslip.ai/llms-full.txt). [Strategy notes](docs/strategy-notes.md) cover entry filters, proportional exits, and replay assumptions.

<details>
<summary>Repository development and CLI workflow</summary>

The original `scripts/` entry points share `bot_common.py` and support flags, optional `config.toml`, and `.env`. Clone the repository, then use `uv run scripts/file.py --help` or `--check-config` to validate settings without network calls.

The single files are generated from these same sources. After changing `scripts/`, regenerate and check:

```sh
uv run python tools/build_standalone.py
uv run --with ruff==0.16.10 ruff check scripts tests tools standalone
uv run python -m unittest discover -s tests -v
```

CI rejects stale single-file examples.

</details>

[Bot guide](https://docs.zeroslip.ai/bot-examples) · [Upstream sources](UPSTREAM.json) · [Unlicense](LICENSE)
