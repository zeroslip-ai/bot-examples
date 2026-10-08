# Strategy notes

Use these notes when changing the bots; setup and commands are in the [README](../README.md).

## Streaming strategies

Launch sniping and copy trading follow Pump.fun and Pump-created PumpSwap pools, excluding Mayhem mode. Selected-token sniping follows SOL/USDC trades across supported venues and buys once per process. Launch and copy modes have no position cap.

Amounts and launch thresholds are SOL-equivalent. USDC sizing uses a trusted SOL/USDC stream pool, with an $80/SOL fallback until an update arrives. Sniper exits compare prices in the entry quote; other quote assets are ignored. Paper sniper entries fill at a subsequent trade price, subject to the same buy-slippage ceiling as replay. Migration events cannot fill an entry, and a rejected fill is counted as a miss. Copytrader paper entries use the observed buy price. Paper exits and running realized PnL exclude depth and fees; launch-mode paper results are not profitability estimates. Paper and replay use different fill timing and can disagree.

## Copy trading

The first-buy gate requires token balances summed across transaction wallets to match the purchased amount within tolerance. Entry sizing uses the aggregate event amount, including bundled trades.

The wallet that opens a position controls its exits. Sells apply its unrounded fraction of remaining tokens to your remaining position. Additional followed buys update the exit baseline without another buy in your wallet. Bundled exits use per-wallet breakdowns; ambiguous exits are skipped. Partial live exits round down to the token decimals reported at entry and send absolute token amounts with `denominatedInQuote: "false"`; final exits use `100%`. Missing entry decimals stops a live partial exit. Fractional percentage support is not assumed. Live bookkeeping changes after confirmation. Subunit partial exits send no order; a full exit still sells `100%`.

## Replay

Replay models qualifying launch entries, not `sniper.token_mint`. Missing archive hours fail unless `backtest.allow_gaps = true`.

Pending fills resolve before the incoming price update using the last known price. Events remain in recorded order. At the end, pending buys are filled and open positions close at their last price. These assumptions can be optimistic with sparse events or incomplete windows. Depth, network fees, and atomic Jito execution are not fully modeled.

## Sell balances

Sell previews use read-only RPC queries and report RPC errors directly. Both WSOL and USDC are always kept, even when explicitly selected. When upgrading an older configuration, `trade.quote_mint` is ignored with a warning; remove it when editing the file.
