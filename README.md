# ZeroSlip bot examples

![ZeroSlip bot examples](assets/bot-examples-banner.png)

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

Run one streaming bot at a time and close other live-stream clients first. The feed can reject a second connection, including a locally running agent's stream.

## Buy a specific token and manage exits

Set `trade.buy_amount` in `config.toml` (default **0.001 SOL**), then replace `TOKEN_MINT` with a real token address:

```sh
python scripts/live_sniper_bot.py --mint TOKEN_MINT
```

This starts in **paper mode**: at the next trade for that mint against SOL or USDC, it simulates one buy, then manages the configured take-profit, stop-loss, and idle exit. It does not send API requests or trades. The selected-token mode can watch any supported venue with SOL or USDC quotes. It buys that mint once per process, rather than re-entering after an exit.

You can put the mint in `sniper.token_mint` instead of passing `--mint`. `sniper.take_profit`, `stop_loss`, and `idle_seconds` control exits. Prices from SOL and USDC pools are converted into the entry quote before checking exits; prices in other quote assets are ignored.

For new Pump.fun launch sniping, leave `sniper.token_mint` empty and omit `--mint`:

```sh
python scripts/live_sniper_bot.py
```

Launch mode buys when the initial purchase exceeds `sniper.min_initial_buy` (default 20 SOL-equivalent). It excludes Mayhem mode and follows Pump.fun / Pump-created PumpSwap pools. Both SOL and USDC are supported, with no position cap. These filters describe strategy scope, not a guarantee about a token's legitimacy.

## Copy a wallet

Replace `WALLET_ADDRESS` with the wallet to follow:

```sh
python scripts/copytrader_bot.py --wallet WALLET_ADDRESS
```

Or set `copytrader.wallets = ["ADDRESS_1", "ADDRESS_2"]` in `config.toml` and omit `--wallet`. The CLI option is repeatable and replaces the configured list.

The bot copies `copytrader.buy_fraction` of an observed buy event, up to `max_buy_amount` (defaults: 10%, capped at 0.01 SOL). The original entry sizing uses the aggregate event amount when a transaction bundles several traders. It mirrors proportional exits from the same followed wallet, including partial sells. Additional buys by that wallet adjust the tracked exit baseline without buying more in your wallet. The first followed wallet to open a position owns that position's exit signals; another watched wallet cannot close it. Wallet matching uses `tradersInvolved` when present. Bundled exits and additional buys use that wallet's `breakdown` amounts; an ambiguous bundled exit without a breakdown is skipped.

Optional `copytrader.token_mints` limits trading to specific mints. The original first-buy gate is always applied: token balances summed across all wallets in the transaction must match the purchased amount within the upstream tolerance. Copy mode uses the same Pump pool filters as launch sniping and supports both SOL and USDC.

## Backtest launch sniping

No credentials are needed:

```sh
python scripts/backtest_sniper_strategy.py --hours 10
```

Downloads the last ten completed UTC hours by default, one temporary file at a time, then streams their events. Use `--hours 1` for a shorter run. Archives can be hundreds of MB or larger per hour; keep temporary storage available, or set `TMPDIR` to a folder with space. Missing hours fail the run unless `backtest.allow_gaps = true`.

You can also replay an existing JSONL or Zstandard-compressed archive without network access:

```sh
python scripts/backtest_sniper_strategy.py --file /path/to/events.jsonl.zst
```

The simulator restores the original launch threshold, SOL/USDC sizing, pool filters, uncapped positions, TP/SL, idle rules, and fee accounting. **It models all qualifying launch entries, not the direct-token entry mode; `sniper.token_mint` does not filter replay.** Latency, entry slippage, and per-side fee assumptions live in `[backtest]`. Results include separate WSOL/USDC totals and combined SOL-equivalent PnL.

The original replay model resolves pending fills before applying the incoming event's price, using the last known price. It processes out-of-order timestamps as recorded and force-fills pending buys at the end, then closes remaining positions at their last price. These restored assumptions can be optimistic for sparse events or an incomplete replay window. Market depth, network fees, and atomic Jito bundle execution are not fully modeled. Paper and replay results are estimates, not execution guarantees.

## Original behavior and the exit calculation

The standalone setup retains the original first-buy gate, SOL/USDC support, uncapped positions, sell tolerances, three-second stream confirmation window, and replay calculation. SOL sizing and launch thresholds are converted to USDC using the original trusted stream pool, starting at the upstream fallback of $80/SOL until a price update arrives. Fund both quote assets if you intend to trade both.

Copy exits deliberately use the unrounded fraction `tokens sold by followed wallet / its tracked remaining tokens`, applied to your remaining position. With no additional buys, this is mathematically equivalent to an exact cumulative calculation. The original rounds the cumulative percentage and incremental order percentage up to whole numbers; a 1.1% exit becomes 2%, and repeated exits can accumulate over-selling. The proportional calculation tracks the wallet more accurately and incorporates additional followed buys without increasing your own position. This mirrors its exit fraction, not its absolute token holdings. Bookkeeping changes only after a confirmed sell; exits for one mint are serialized so overlapping orders cannot corrupt that calculation.

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
| `trade.quote_mint` | Quote balance preserved by sell-all; streaming and replay strategies support both SOL and USDC. |
| `trade.buy_amount` | SOL-equivalent amount for sniper entries; converted to USDC automatically. |
| `copytrader.max_buy_amount` | Maximum SOL-equivalent amount for each copied entry. |
| `trade.buy_slippage`, `sell_slippage` | Original streaming tolerances: 20% buy, 99% sell. |
| `sell.slippage` | Original sell-all tolerance: 100%. |
| `trade.priority_fee` | Network priority fee in SOL. |
| `trade.confirmation_seconds` | Original three-second wait for the streaming bots' own execution event. |

Streaming bots execute independently across tokens while continuing to read events. Events and exits for the same token are handled in order. An API rejection, timeout, missing signature, or missing confirmation **stops the bot without retrying the order**: an order may have executed even when its response was lost. Check your wallet before restarting. Stream disconnections reconnect with a delay; market events missed during the gap cannot be recovered automatically.

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
