# ZeroSlip bot examples

Python reference scripts for building Solana trading bots with ZeroSlip's Trade API, live Data Stream, and Historical Replay. Start with an example, then adapt its configuration and strategy in your editor or with a coding agent.

[Bot examples guide](https://docs.zeroslip.ai/bot-examples) · [Quick Start](https://docs.zeroslip.ai/quick-start) · [Full docs for AI](https://docs.zeroslip.ai/llms-full.txt) · [ZeroSlip](https://zeroslip.ai)

## Examples

| Script | What it demonstrates |
| --- | --- |
| [Copytrading bot](scripts/copytrader_bot.py) | Follow selected wallets, copy purchases with a size cap, and mirror cumulative sell percentages. |
| [Live sniper bot](scripts/live_sniper_bot.py) | React to token launches, then manage take-profit, stop-loss, and idle exits. |
| [Sniper strategy backtester](scripts/backtest_sniper_strategy.py) | Simulate sniper rules using historical events with configurable latency, slippage, and fee assumptions. |
| [Sell-all-tokens script](scripts/sell_all_tokens.py) | Read a wallet's balances and sell its token holdings. |

## Adapt an example

These files are reference templates, preserved from the original PumpApi-Agent library. They need editing before they can run independently: wallet and notification assignments are unfinished, and `notify_user` belongs to the original agent environment. PumpAPI is the former name of ZeroSlip.

1. Clone this repository and choose a script:

   ```sh
   git clone https://github.com/zeroslip-ai/bot-examples.git
   cd bot-examples
   ```

2. Read its imports and install the dependencies your adapted version needs. The examples use libraries such as `aiohttp`, `websockets`, `orjson`, and `cachetools`; supply or replace `notify_user` for your own notification destination.
3. Replace unfinished assignments with your own runtime configuration. Keep wallet secrets in local configuration, outside prompts and committed source. This guidance takes precedence over the legacy key-loading comments in the scripts.
4. Update legacy endpoints and fields against the current [Trade API](https://docs.zeroslip.ai/trade-api), [Data Stream](https://docs.zeroslip.ai/stream), and [Historical Replay](https://docs.zeroslip.ai/historical-replay) docs. Use HTTPS for API requests and choose the transaction/signing mode that fits your integration.
5. Configure your watched wallets, position sizing, entry/exit rules, and pool filters. Validate with replay or paper trading before enabling live orders. Numeric settings and backtest outcomes depend on the example assumptions.

The sell-all template also needs its notification code corrected and its legacy HTTP Trade API URL updated. Its optional token-burn fallback is disabled; enable it only if you deliberately want that behavior.

## Use with a coding agent

Give your agent the [full ZeroSlip docs](https://docs.zeroslip.ai/llms-full.txt), the chosen script, and your requirements. The documentation export includes the complete source of all four examples.

```text
Adapt this ZeroSlip reference script to my strategy and preferred language.
Explain the configuration and dependencies, replace agent-only placeholders
and notifications, and use the current API endpoints and event schema.
Keep wallet secrets in local configuration. Start with replay or paper trading;
only enable live execution when I explicitly request it.
My requirements: [wallets, sizing, entry/exit rules, and runtime].
```

## Source and license

The initial scripts are unchanged snapshots from [PumpApi-io/PumpApi-Agent](https://github.com/PumpApi-io/PumpApi-Agent/tree/e395fd7fc4bcf892f6214db3508aec0ec2035cbf/scripts/pumpapi-agent/solana-pumpapi-bots/scripts), revision `e395fd7fc4bcf892f6214db3508aec0ec2035cbf`, retrieved on 6 October 2026. [UPSTREAM.json](UPSTREAM.json) records the original paths and Git blob hashes.

Released under [The Unlicense](LICENSE), retained from the upstream repository.
