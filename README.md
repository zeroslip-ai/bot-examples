# ZeroSlip bot examples

![ZeroSlip bot examples](assets/bot-examples-banner.png)

Python scripts for sniping, copy trading, backtesting, and selling balances. Choose a token or watched wallets and run from your terminal.

## How it works

Streaming bots read public ZeroSlip market events and apply their strategy rules. Paper mode logs simulated trades and PnL. With `--live`, the script sends orders and your credential to the [Lightning Trade API](https://docs.zeroslip.ai/trade-api), which signs and executes them.

`scripts/bot_common.py` handles settings, stream connections, and orders. Backtesting reads historical events; sell previews query Solana RPC.

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then clone or [download the ZIP](https://github.com/zeroslip-ai/bot-examples/archive/refs/heads/main.zip) and open the repository folder:

```sh
git clone https://github.com/zeroslip-ai/bot-examples.git
cd bot-examples
```

Run an example below. `uv run` installs dependencies in a local environment and uses Python 3.11+ automatically. No activation or config file is needed for paper runs.

| File | Purpose |
| --- | --- |
| `config.toml` | Optional: copy `config.example.toml` to customize sizing, exits, or saved wallets. |
| `.env` | Live trading only: copy `.env.example` and add one credential. |
| `pyproject.toml` / `uv.lock` | Dependencies and pinned versions for `uv`. |

Run one streaming bot at a time; close other feed clients, including agents.

<details>
<summary>Use an existing Python environment</summary>

With Python 3.11+, install `requirements.txt` and replace `uv run` with `python` below:

```sh
python -m pip install -r requirements.txt
```

</details>

## Examples

Replace `TOKEN_MINT` and `WALLET_ADDRESS` with Solana addresses.

### Buy a token or snipe launches

```sh
uv run scripts/live_sniper_bot.py --mint TOKEN_MINT
# Or snipe new Pump.fun launches:
uv run scripts/live_sniper_bot.py
```

Selected-token mode buys once and manages take-profit, stop-loss, and idle exits. Without `--mint`, it buys non-Mayhem launches over 20 SOL-equivalent and follows Pump-created PumpSwap pools. Defaults: 0.001 SOL-equivalent per buy, 50% take-profit, 20% stop-loss, 300 seconds idle.

Paper entries wait for a subsequent trade price and count misses above buy slippage. Paper exits report realized PnL excluding fees and depth; results do not establish profitability.

### Copy a wallet

```sh
uv run scripts/copytrader_bot.py --wallet WALLET_ADDRESS
```

Copies first buys in non-Mayhem Pump.fun or Pump-created PumpSwap pools at 10% of the observed amount, capped at 0.01 SOL-equivalent, and mirrors proportional sells. Repeat `--wallet` to follow several wallets.

### Backtest launch sniping

```sh
uv run scripts/backtest_sniper_strategy.py --hours 1
# Or replay a downloaded archive:
uv run scripts/backtest_sniper_strategy.py --file /path/to/events.jsonl.zst
```

Uses launch rules and modeled latency/fees. Reports PnL, misses, exit reasons, win rate, price moves, entry slippage, best/worst trades, and the replay window. Zero trades can mean every entry exceeded buy slippage. The newest hour may take 20–90 seconds to appear; retry a 404 after 90 seconds. Archives can exceed hundreds of MB per hour; use `TMPDIR` for temporary storage.

### Preview balances

```sh
uv run scripts/sell_all_tokens.py --wallet WALLET_ADDRESS --all
# Or select one token:
uv run scripts/sell_all_tokens.py --wallet WALLET_ADDRESS --mint TOKEN_MINT
```

Frozen balances, wrapped SOL, and USDC are skipped. Choose specific mints or `--all`, not both.

## Trade live

Copy `.env.example` to `.env` and set one credential for your funded ZeroSlip wallet:

```dotenv
ZEROSLIP_API_KEY=your_wallet_api_key
# Or: ZEROSLIP_PRIVATE_KEY=your_base58_private_key
```

Add `--live` to a sniper, copytrader, or sell command. Keep `.env` local. Live sells use the credential's wallet and incur a balance-query fee.

- Review slippage: defaults are 20% for buys, 99% for streaming sells, and 100% for sell-all. Fund SOL and USDC to trade both quote markets.
- Positions stay in memory. Stopping does not sell holdings; restarting does not restore them. Use one strategy per wallet.
- Uncertain orders stop without retrying. Check the wallet before restarting. Reconnects back off up to 30 seconds but cannot recover missed events.

## Work with a coding agent

Give the agent this repository and the [full API docs](https://docs.zeroslip.ai/llms-full.txt). Keep `bot_common.py` alongside the scripts. [Strategy notes](docs/strategy-notes.md) cover entry filters, proportional exits, and replay assumptions.

Use `--help` for options or `--check-config` to validate without network calls. For code changes:

```sh
uv run --with ruff==0.16.10 ruff check scripts tests
uv run python -m unittest discover -s tests -v
```

[Bot guide](https://docs.zeroslip.ai/bot-examples) · [Upstream sources](UPSTREAM.json) · [Unlicense](LICENSE)
