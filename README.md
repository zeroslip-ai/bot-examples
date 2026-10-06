# ZeroSlip bot examples

Standalone Python bots for ZeroSlip's Trade API, live Data Stream, and Historical Replay. No PumpApi-Agent harness, AI agent, notification service, or source edits are required. Configure a token or watched wallets, then run a script from your terminal.

[Bot examples guide](https://docs.zeroslip.ai/bot-examples) · [Quick Start](https://docs.zeroslip.ai/quick-start) · [Full docs for AI](https://docs.zeroslip.ai/llms-full.txt)

## Install once

Use **Python 3.11 or newer**.

```sh
git clone https://github.com/zeroslip-ai/bot-examples.git
cd bot-examples
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp config.example.toml config.toml
cp .env.example .env
```

On Windows, activate with `.venv\Scripts\activate` instead. On Debian/Ubuntu, install the `python3-venv` package if creating the environment fails. All commands below run from the repository folder. `--config /path/to/config.toml` works from another folder; the `.env` beside that configuration is loaded automatically. Existing environment variables take precedence.

The scripts print status and trade messages to your terminal. `config.toml` and `.env` are ignored by Git.

## Buy a specific token and manage exits

Set `trade.buy_amount` in `config.toml` (default **0.001 SOL**), then replace `TOKEN_MINT` with a real token address:

```sh
python scripts/live_sniper_bot.py --mint TOKEN_MINT
```

This starts in **paper mode**: at the next trade for that mint against the configured quote token, it simulates one buy, then manages the configured take-profit, stop-loss, and idle exit. It does not send API requests or trades. The selected-token mode can watch any supported venue; the quote mint defaults to wrapped SOL. It buys that mint once per process, rather than re-entering after an exit.

You can put the mint in `sniper.token_mint` instead of passing `--mint`. `sniper.take_profit`, `stop_loss`, and `idle_seconds` control exits.

For new Pump.fun launch sniping, leave `sniper.token_mint` empty and omit `--mint`:

```sh
python scripts/live_sniper_bot.py
```

Launch mode buys when the initial purchase exceeds `sniper.min_initial_buy` (default 20 SOL). It excludes Mayhem mode and follows Pump.fun / Pump-created PumpSwap pools. `trade.max_positions` caps simultaneously tracked positions. These filters describe strategy scope, not a guarantee about a token's legitimacy.

## Copy a wallet

Replace `WALLET_ADDRESS` with the wallet to follow:

```sh
python scripts/copytrader_bot.py --wallet WALLET_ADDRESS
```

Or set `copytrader.wallets = ["ADDRESS_1", "ADDRESS_2"]` in `config.toml` and omit `--wallet`. The CLI option is repeatable and replaces the configured list.

The bot copies `copytrader.buy_fraction` of a watched buy, up to `max_buy_amount` (defaults: 10%, capped at 0.01 SOL). It mirrors proportional exits from the same followed wallet, including partial sells. Additional buys by that wallet adjust the tracked exit baseline without buying more in your wallet. The first followed wallet to open a position owns that position's exit signals; another watched wallet cannot close it.

Optional `copytrader.token_mints` limits trading to specific mints. `first_buy_only = true` requires a matching post-trade balance showing that the followed wallet's purchase opened a new position. Copy mode uses the same Pump pool filters as launch sniping and only the configured quote mint.

## Backtest launch sniping

No credentials are needed:

```sh
python scripts/backtest_sniper_strategy.py --hours 1
```

Downloads the last completed UTC hour into a temporary file, then streams its events. Archives can be hundreds of MB per hour. Missing hours fail the run unless `backtest.allow_gaps = true`.

You can also replay an existing JSONL or Zstandard-compressed archive without network access:

```sh
python scripts/backtest_sniper_strategy.py --file /path/to/events.jsonl.zst
```

The simulator uses the same launch threshold, quote mint, sizing, pool filters, position cap, TP/SL, and idle settings. A configured `sniper.token_mint` restricts which launch is considered; **the backtester models launch entries, not the direct-token entry mode**. Latency, entry slippage, and per-side fee assumptions live in `[backtest]`. Results are printed in the configured quote token's units.

Fills use observed prices around the simulated latency deadline. Sparse events, market depth, network fees, and atomic Jito bundle execution are not fully modeled. Open positions at the end are marked to their last observed price; pending buys that cannot fill inside the window are excluded. Paper and replay results are estimates, not execution guarantees.

## Preview or sell wallet tokens

For a read-only preview, set `wallet.public_key` in `config.toml` (or `ZEROSLIP_WALLET_PUBLIC_KEY` in `.env`):

```sh
python scripts/sell_all_tokens.py --mint TOKEN_MINT
python scripts/sell_all_tokens.py --all
```

Preview mode reads balances through Solana RPC and submits **no** Trade API requests. `--all` selects all non-quote token balances; the configured quote token and wrapped SOL are preserved. Use `sell.token_mints` for a saved selection. Choose either specific mints or `--all`, not both.

With `--live`, balances are read from the **credential's own wallet** using the ZeroSlip `getBalances` action, which incurs its documented balance-query fee. The script sells each selected balance at 100%, waits for the API's confirmation response, and stops on an uncertain outcome. Frozen balances are skipped. There is no automatic burn fallback. Live balance reads cover the associated token accounts returned by the API; the RPC preview can also list non-associated token accounts.

## Enable live orders

Paper mode is the default for both streaming bots. A sell command defaults to a balance preview. To trade, put **one** Lightning credential in your local `.env`:

```dotenv
ZEROSLIP_API_KEY=your_zeroslip_wallet_api_key
# Or use ZEROSLIP_PRIVATE_KEY=your_base58_private_key instead.
```

Then add `--live` to your chosen command:

```sh
python scripts/live_sniper_bot.py --mint TOKEN_MINT --live
python scripts/copytrader_bot.py --wallet WALLET_ADDRESS --live
python scripts/sell_all_tokens.py --mint TOKEN_MINT --live
# Only when you intend to sell every selected non-quote balance:
python scripts/sell_all_tokens.py --all --live
```

These examples use [Lightning execution](https://docs.zeroslip.ai/trade-api): your API key or private key is sent to the configured HTTPS Trade API, which signs and broadcasts for you. The wallet API key is an encrypted wallet credential, not an ordinary account identifier. Local signing is not implemented in these examples. Keep secrets in local configuration, and ensure the funded wallet belongs to you. If both credential variables are set, the API key takes precedence.

Settings to review before live use:

| Setting | Meaning |
| --- | --- |
| `trade.quote_mint` | Quote asset for buys; SOL by default. Thresholds and amounts use this token's units, with no automatic SOL/USDC conversion. |
| `trade.buy_amount` | Amount for sniper entries. |
| `copytrader.max_buy_amount` | Maximum amount for each copied entry. |
| `trade.buy_slippage`, `sell_slippage` | Percentage tolerance; defaults are 20, rather than unrestricted sell slippage. |
| `trade.priority_fee` | Network priority fee in SOL. |
| `trade.max_positions` | Maximum simultaneously tracked positions in each bot process. |
| `trade.confirmation_seconds` | Time to wait for the streaming bots' own execution event. |

Streaming bots keep reading events while waiting for their own trade signature. An API rejection, timeout, missing signature, or missing confirmation **stops the bot without retrying the order**: an order may have executed even when its response was lost. Check your wallet before restarting. Stream disconnections reconnect with a delay; market events missed during the gap cannot be recovered automatically.

Position tracking lives in memory. Stopping with Ctrl+C **does not liquidate** live holdings, and restarting does not restore earlier positions. Run one strategy per trading wallet, avoid concurrent manual trades in the same tracked token, and use the sell script to handle positions left after a shutdown. Paper fills use observed prices without depth or fee modeling; use the backtester for the configured fee/latency model.

## Validate without trading

```sh
python scripts/live_sniper_bot.py --check-config
python scripts/copytrader_bot.py --wallet WALLET_ADDRESS --check-config
python scripts/backtest_sniper_strategy.py --check-config
python scripts/sell_all_tokens.py --mint TOKEN_MINT --check-config
python -m unittest discover -s tests -v
```

`--check-config` makes no network calls. Adding `--live --check-config` also checks that a credential is configured, without validating it with the server. Tests use simulated events and local/mock HTTP responses, never a real wallet or a live trade endpoint.

## Use with a coding agent

Give the agent this README, `config.example.toml`, the chosen script, [`scripts/bot_common.py`](scripts/bot_common.py), and the [full ZeroSlip docs](https://docs.zeroslip.ai/llms-full.txt). The common module is required by every entry point. Ask the agent to adjust configuration before changing strategy code; no harness or notification integration is needed.

## Source and license

Adapted from [PumpApi-io/PumpApi-Agent](https://github.com/PumpApi-io/PumpApi-Agent/tree/e395fd7fc4bcf892f6214db3508aec0ec2035cbf/scripts/pumpapi-agent/solana-pumpapi-bots/scripts), revision `e395fd7fc4bcf892f6214db3508aec0ec2035cbf`. [UPSTREAM.json](UPSTREAM.json) records the original, unmodified sources' paths and blob hashes; the current scripts include standalone configuration and execution fixes and no longer match those original hashes.

Released under [The Unlicense](LICENSE), retained from the upstream repository.
