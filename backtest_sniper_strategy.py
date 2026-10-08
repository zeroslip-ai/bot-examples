"""ZeroSlip sniper backtester.

Replays past hours of market data through the launch-sniper rules from
live_sniper_bot.py and reports what would have happened. It never trades.

How it works:
1. Downloads the last HOURS completed UTC hours from the ZeroSlip replay
   archive (or reads REPLAY_FILE), one event at a time.
2. Buys new Pump.fun launches whose first buy exceeds MIN_INITIAL_BUY, after
   BUY_LATENCY_MS, unless the price moved more than BUY_SLIPPAGE.
3. Sells at TAKE_PROFIT, STOP_LOSS, or after IDLE_SECONDS, after SELL_LATENCY_MS.
4. Reports PnL after modeled fees, win rate, exit reasons, and missed entries.
Fills use the last known price, so results are an estimate, not a promise.

Run: python3 backtest_sniper_strategy.py   (Python 3.11+, pip install aiohttp zstandard orjson)
"""

# ---------------------------------- SETTINGS ----------------------------------
HOURS = 1  # Completed UTC hours to replay. Each hour can be hundreds of MB.
REPLAY_FILE = ''  # Or a downloaded .jsonl / .jsonl.zst archive to replay instead.
ALLOW_GAPS = False  # True skips hours missing from the archive instead of stopping.

MIN_INITIAL_BUY = 20  # The creator's first buy must exceed this many SOL.
BUY_AMOUNT = 0.001  # SOL per buy. USDC pools buy the same value in USDC.
TAKE_PROFIT = 50  # Sell when the price is up this many % from entry.
STOP_LOSS = 20  # Sell when the price is down this many % from entry.
IDLE_SECONDS = 300  # Sell when the token has no activity for this long.
BUY_SLIPPAGE = 20  # %. A buy is missed if the price rises more than this before it fills.

BUY_LATENCY_MS = 400  # Delay between deciding to buy and the buy filling.
SELL_LATENCY_MS = 400
SERVICE_FEE = 0.0025  # ZeroSlip fee per trade, as a fraction (0.25%).
EXTRA_FEE = 0.0005  # Extra cost per trade you want to assume, as a fraction.
# ------------------------------------------------------------------------------

# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.12,<4", "zstandard>=0.23,<1", "orjson>=3.10,<4"]
# ///

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import io
import logging
import math
from pathlib import Path
import tempfile

import aiohttp
import zstandard

try:
    import orjson as json  # Optional: pip install orjson makes replays about 2x faster.
except ImportError:
    import json

REPLAY_URL = 'https://replay.pumpapi.io'  # Hourly archives: /YYYY/MM/DD/HH.jsonl.zst

WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
SOL_USDC_POOL = 'Gf7sXMoP8iRw4iiXmJ1nq4vxcRycbGXy5RL8a8LnTd3v'


class StopBot(Exception):
    """Bad settings or an archive problem. Stop with a clear message."""


def check_settings():
    try:
        if not (BUY_AMOUNT > 0 and TAKE_PROFIT > 0 and IDLE_SECONDS > 0 and 0 < STOP_LOSS < 100):
            raise StopBot('BUY_AMOUNT, TAKE_PROFIT and IDLE_SECONDS must be above 0; STOP_LOSS between 0 and 100')
        if not 0 <= BUY_SLIPPAGE < 100:
            raise StopBot('BUY_SLIPPAGE must be from 0 to below 100')
        if min(MIN_INITIAL_BUY, BUY_LATENCY_MS, SELL_LATENCY_MS, SERVICE_FEE, EXTRA_FEE) < 0:
            raise StopBot('MIN_INITIAL_BUY, latencies and fees cannot be negative')
        if SERVICE_FEE + EXTRA_FEE >= 1:
            raise StopBot('SERVICE_FEE and EXTRA_FEE are fractions; together they must be below 1')
    except TypeError:
        raise StopBot('Number settings must be numbers') from None
    if not REPLAY_FILE and (not isinstance(HOURS, int) or isinstance(HOURS, bool) or HOURS < 1):
        raise StopBot('HOURS must be a whole number of at least 1')
    if not isinstance(ALLOW_GAPS, bool):
        raise StopBot('ALLOW_GAPS must be True or False')


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def pump_pool(event):
    return event.get('pool') == 'pump' or (event.get('pool') == 'pump-amm' and event.get('poolCreatedBy') == 'pump')


class Backtest:
    def __init__(self):
        self.sol_price = 80.0  # USDC per SOL until the SOL/USDC pool trades.
        self.positions = {}  # mint -> position; 'state' is pending_buy, held or pending_sell
        self.trades = []
        self.last_ts = 0
        self.first_ts = self.end_ts = None
        self.seen = self.skipped = self.missed = self.buys = self.creates = 0

    def sol_to_quote(self, quote, sol_amount):
        return sol_amount if quote == WSOL else sol_amount * self.sol_price

    def fill_buy(self, mint, pos):
        decision, fill = pos['decision_price'], pos['cur_price']
        if fill > decision / (1 - min(BUY_SLIPPAGE, 99.9) / 100):
            self.missed += 1
            del self.positions[mint]
            return
        amount = pos['amount']
        pos.update(
            state='held',
            entry_price=fill,
            quote_spent=amount,
            tokens=amount * (1 - SERVICE_FEE - EXTRA_FEE - pos['entry_pool_fee']) / fill,
            sol_price=self.sol_price,
            spent_sol=amount if pos['quote_mint'] == WSOL else amount / self.sol_price,
            buy_slip=(fill / decision - 1) * 100,
            last_ts=pos['buy_fill_ts'],
        )
        self.buys += 1

    def trigger_sell(self, pos, ts, reason):
        pos.update(state='pending_sell', sell_reason=reason, sell_fill_ts=ts + SELL_LATENCY_MS)

    def close(self, mint, pos, exit_price, reason):
        fee = SERVICE_FEE + EXTRA_FEE
        gross = pos['tokens'] * exit_price
        pnl = gross * (1 - fee - pos['last_pool_fee']) - pos['quote_spent']
        fees = pos['quote_spent'] * (fee + pos['entry_pool_fee']) + gross * (fee + pos['last_pool_fee'])
        is_sol = pos['quote_mint'] == WSOL
        self.trades.append(
            {
                'mint': mint,
                'quote_mint': pos['quote_mint'],
                'reason': reason,
                'pct': (exit_price - pos['entry_price']) / pos['entry_price'] * 100,
                'pnl': pnl,
                'pnl_sol': pnl if is_sol else pnl / pos['sol_price'],
                'spent_sol': pos['spent_sol'],
                'fee_sol': fees if is_sol else fees / pos['sol_price'],
                'buy_slip': pos['buy_slip'],
            }
        )
        del self.positions[mint]

    def resolve(self, ts):
        """Fill buys and sells whose latency has passed, and start idle sells."""
        for mint, pos in list(self.positions.items()):
            if pos['state'] == 'pending_buy' and pos['buy_fill_ts'] <= ts:
                self.fill_buy(mint, pos)
            elif pos['state'] == 'pending_sell' and pos['sell_fill_ts'] <= ts:
                self.close(mint, pos, pos['cur_price'], pos['sell_reason'])
            elif pos['state'] == 'held' and ts - pos['last_ts'] > IDLE_SECONDS * 1000:
                self.trigger_sell(pos, pos['last_ts'] + IDLE_SECONDS * 1000, 'idle')

    def handle_event(self, event):
        self.seen += 1
        ts = event['timestamp']
        self.last_ts = ts
        self.first_ts = ts if self.first_ts is None else min(self.first_ts, ts)
        self.end_ts = ts if self.end_ts is None else max(self.end_ts, ts)
        # Pending fills resolve at the last known price, before this event's price applies.
        self.resolve(ts)
        if event['action'] not in ('buy', 'sell', 'add', 'remove', 'create', 'migrate', 'createPool'):
            return
        mint = event['mint']
        if event.get('poolId') == SOL_USDC_POOL:
            if finite(event.get('price')) and event['price'] > 0:
                self.sol_price = 1 / event['price']
            return
        if event['action'] == 'create' and event['pool'] == 'pump' and not event.get('mayhemMode'):
            self.creates += 1
            quote = event['quoteMint']
            big_enough = event['quoteAmount'] > self.sol_to_quote(quote, MIN_INITIAL_BUY)
            if quote in (WSOL, USDC) and big_enough and mint not in self.positions:
                self.positions[mint] = {
                    'state': 'pending_buy',
                    'quote_mint': quote,
                    'amount': self.sol_to_quote(quote, BUY_AMOUNT),
                    'decision_price': event['price'],
                    'cur_price': event['price'],
                    'entry_pool_fee': event['poolFeeRate'],
                    'last_pool_fee': event['poolFeeRate'],
                    'buy_fill_ts': ts + BUY_LATENCY_MS,
                    'buy_slip': 0.0,
                }
        elif mint in self.positions:
            pos = self.positions[mint]
            if event['action'] in ('buy', 'sell', 'add', 'remove', 'migrate') and pump_pool(event):
                price = event['price']
                pos.update(cur_price=price, last_pool_fee=event['poolFeeRate'])
                if pos['state'] == 'held':
                    change = (price - pos['entry_price']) / pos['entry_price'] * 100
                    if change > TAKE_PROFIT:
                        self.trigger_sell(pos, ts, 'tp')
                    elif change < -STOP_LOSS:
                        self.trigger_sell(pos, ts, 'sl')
            if pos['state'] == 'held':
                pos['last_ts'] = ts

    def read(self, lines):
        for line in lines:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError('Not an event')
                # Check the fields that set time or open/update a position; skip bad lines.
                ts = event.get('timestamp')
                if not finite(ts) or ts < 0:
                    raise ValueError('Invalid timestamp')
                quote, amount = event.get('quoteMint'), event.get('quoteAmount')
                entry = (
                    event.get('action') == 'create'
                    and event.get('pool') == 'pump'
                    and not event.get('mayhemMode')
                    and quote in (WSOL, USDC)
                    and finite(amount)
                    and amount > self.sol_to_quote(quote, MIN_INITIAL_BUY)
                )
                update = (
                    event.get('mint') in self.positions
                    and event.get('action') in ('buy', 'sell', 'add', 'remove', 'migrate')
                    and pump_pool(event)
                )
                if entry or update:
                    price, fee = event.get('price'), event.get('poolFeeRate')
                    if not finite(price) or price <= 0 or not finite(fee) or not 0 <= fee < 1:
                        raise ValueError('Invalid price or pool fee')
                self.handle_event(event)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                self.skipped += 1

    def read_file(self, path):
        with open(path, 'rb') as file:
            if str(path).endswith('.zst'):
                with zstandard.ZstdDecompressor().stream_reader(file) as reader:
                    self.read(io.BufferedReader(reader))
            else:
                self.read(file)

    def finish(self):
        # Buys still in flight fill; open positions close at their last price.
        for mint, pos in list(self.positions.items()):
            if pos['state'] == 'pending_buy':
                self.fill_buy(mint, pos)
        for mint, pos in list(self.positions.items()):
            if pos['state'] == 'pending_sell':
                reason = pos['sell_reason']
            else:
                reason = 'idle' if self.last_ts - pos['last_ts'] > IDLE_SECONDS * 1000 else 'end'
            self.close(mint, pos, pos['cur_price'], reason)
        self.report()

    def report(self):
        trades, count = self.trades, len(self.trades)
        pnl_sol = sum(t['pnl_sol'] for t in trades)
        volume = sum(t['spent_sol'] for t in trades)
        wins = sum(t['pnl_sol'] > 0 for t in trades)
        logging.info('Events=%s creates=%s buys=%s closed=%s', self.seen, self.creates, self.buys, count)
        logging.info('Missed entries (price moved past BUY_SLIPPAGE)=%s; bad lines=%s', self.missed, self.skipped)
        pnl_wsol = sum(t['pnl'] for t in trades if t['quote_mint'] == WSOL)
        pnl_usdc = sum(t['pnl'] for t in trades if t['quote_mint'] == USDC)
        logging.info('PnL=%+.8f SOL (WSOL %+.8f, USDC %+.8f); wins=%s/%s', pnl_sol, pnl_wsol, pnl_usdc, wins, count)
        roi = pnl_sol / volume * 100 if volume else 0
        logging.info('Volume=%s SOL; ROI=%+.2f%%; fees=%s SOL', volume, roi, sum(t['fee_sol'] for t in trades))
        if count:
            reasons = Counter(t['reason'] for t in trades)
            exits = reasons['tp'], reasons['sl'], reasons['idle'], reasons['end'], wins / count * 100
            logging.info('Exits: tp=%s sl=%s idle=%s end=%s; win rate=%.2f%%', *exits)
            average_move = sum(t['pct'] for t in trades) / count
            average_slip = sum(t['buy_slip'] for t in trades) / count
            logging.info('Average move=%+.2f%%; average entry slippage=%+.2f%%', average_move, average_slip)
            for label, pick in [('Best', max), ('Worst', min)]:
                t = pick(trades, key=lambda t: t['pnl_sol'])
                details = t['mint'], t['pnl_sol'], t['pct'], t['reason']
                logging.info('%s trade: %s pnl=%+.8f SOL move=%+.2f%% exit=%s', label, *details)
        else:
            logging.info('No trades filled. Check the missed entries and your entry and slippage settings.')
        if self.first_ts is not None:
            start = datetime.fromtimestamp(self.first_ts / 1000, timezone.utc)
            end = datetime.fromtimestamp(self.end_ts / 1000, timezone.utc)
            logging.info('Replay window: %s to %s UTC', start.isoformat(), end.isoformat())


async def download_hour(session, hour, path):
    """Save one archive hour to `path`. Returns False if ALLOW_GAPS skipped it."""
    url = f'{REPLAY_URL}/{hour:%Y/%m/%d/%H}.jsonl.zst'
    logging.info('Downloading %s UTC', hour.strftime('%Y-%m-%d %H:00'))
    try:
        async with session.get(url) as response:
            if response.status == 404 and ALLOW_GAPS:
                logging.warning('Skipping a missing archive hour')
                return False
            if response.status == 404:
                raise StopBot('Archive hour not found. The newest hour can take 90 seconds to upload; retry after that')
            if response.status != 200:
                raise StopBot(f'Archive download returned HTTP {response.status}')
            with open(path, 'wb') as file:
                async for chunk in response.content.iter_chunked(1 << 20):
                    file.write(chunk)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise StopBot(f'Archive download failed ({type(exc).__name__}: {exc}); retry or set REPLAY_FILE') from exc
    except OSError as exc:
        raise StopBot('Cannot save the archive. Free disk space, set TMPDIR, or use REPLAY_FILE') from exc
    return True


async def main():
    check_settings()
    backtest = Backtest()
    if REPLAY_FILE:
        try:
            backtest.read_file(REPLAY_FILE)
        except OSError as exc:
            raise StopBot(f'Cannot read REPLAY_FILE: {exc}') from exc
    else:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with tempfile.TemporaryDirectory(prefix='zeroslip-replay-') as folder:
                path = Path(folder) / 'hour.jsonl.zst'
                for hours_ago in range(HOURS, 0, -1):
                    if await download_hour(session, now - timedelta(hours=hours_ago), path):
                        await asyncio.to_thread(backtest.read_file, path)
    backtest.finish()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Stopped.')
    except StopBot as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from None
