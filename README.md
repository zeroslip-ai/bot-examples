# ZeroSlip bot examples

![ZeroSlip bot examples](assets/bot-examples-banner.png)

Four Python trading bots for Solana. Each one is a single file: settings at the top, code below. No config files or command-line arguments.

| File | What it does |
| --- | --- |
| [`live_sniper_bot.py`](live_sniper_bot.py) | Buys a token you choose, or new Pump.fun launches, then sells at take profit, stop loss, or when the token goes quiet. |
| [`copytrader_bot.py`](copytrader_bot.py) | Copies the first buy of wallets you watch, then copies their sells in the same proportion. |
| [`backtest_sniper_strategy.py`](backtest_sniper_strategy.py) | Replays past hours of market data through the sniper rules and reports PnL. Never trades. |
| [`sell_all_tokens.py`](sell_all_tokens.py) | Sells every token in a wallet, or only the ones you list. Keeps SOL and USDC. |

## Run

1. Download one file.
2. Open it and edit the **SETTINGS** section at the top.
3. Install the packages once and run it:

```sh
python3 -m pip install aiohttp websockets zstandard orjson
python3 live_sniper_bot.py
```

Python 3.11+. If your system Python blocks `pip install`, use [uv](https://docs.astral.sh/uv/) instead: `uv run live_sniper_bot.py` installs what the file needs.

Every bot starts in paper mode (`LIVE = False`). It logs what it would buy and sell, and sends nothing.

Run one streaming bot at a time. The data stream allows one connection per IP address.

## How it works

- **Market data:** the sniper and copytrader open one websocket to the free [ZeroSlip data stream](https://docs.zeroslip.ai/stream). Trades arrive as JSON, and the bot filters them on your side.
- **Orders:** with `LIVE = True`, the bot posts each order with your `API_KEY` or `PRIVATE_KEY` to the [ZeroSlip Trade API](https://docs.zeroslip.ai/trade-api), which signs and sends it. The bot then waits up to 3 seconds to see its own trade on the stream.
- **Backtest:** downloads hourly archives from the ZeroSlip replay service and runs them through the same entry and exit rules.
- **Sell all:** paper mode reads balances from public Solana RPC. Live mode asks the Trade API for the balances of your key's wallet (a small fee applies).

Nothing else is contacted. The URLs are constants near the top of each file.

## Before you trade live

- Put your key in the file only on a machine you trust, and don't share the file afterwards.
- Check the slippage settings. Defaults are 20% for buys, 99% for sells, and 100% for sell-all.
- Positions are kept in memory. Stopping the bot does not sell, and restarting does not remember open positions.
- If an order's result is unclear, the bot stops instead of retrying. Check your wallet before restarting.
- Paper and backtest results ignore some fees and liquidity. They are estimates, not a promise of profit.

Full guide: [docs.zeroslip.ai/bot-examples](https://docs.zeroslip.ai/bot-examples). Public domain ([Unlicense](LICENSE)).
