# Strategy notes

Use these notes when changing the bots; setup and commands are in the [README](../README.md).

## Streaming strategies

Launch sniping and copy trading follow Pump.fun and Pump-created PumpSwap pools, excluding Mayhem mode. Selected-token sniping follows SOL/USDC trades across supported venues and buys once per process. Launch and copy modes have no position cap.

Amounts and launch thresholds are SOL-equivalent. USDC sizing uses a trusted SOL/USDC stream pool, with an $80/SOL fallback until an update arrives. Sniper exits compare prices in the entry quote; other quote assets are ignored. Paper fills use observed prices without depth or fee modeling.

## Copy trading

The first-buy gate requires token balances summed across transaction wallets to match the purchased amount within tolerance. Entry sizing uses the aggregate event amount, including bundled trades.

The wallet that opens a position controls its exits. Sells apply its unrounded fraction of remaining tokens to your remaining position. Additional followed buys update the exit baseline without another buy in your wallet. Bundled exits use per-wallet breakdowns; ambiguous exits are skipped. Live bookkeeping changes after confirmation.

## Replay

Replay models qualifying launch entries, not `sniper.token_mint`. Missing archive hours fail unless `backtest.allow_gaps = true`.

Pending fills resolve before the incoming price update using the last known price. Events remain in recorded order. At the end, pending buys are filled and open positions close at their last price. These assumptions can be optimistic with sparse events or incomplete windows. Depth, network fees, and atomic Jito execution are not fully modeled.
