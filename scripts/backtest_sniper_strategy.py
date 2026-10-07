"""Replay launch-sniper rules without credentials, signing, or broadcasting."""
import asyncio
from datetime import datetime, timedelta, timezone
import io
import logging
from pathlib import Path
import tempfile

import aiohttp
import orjson
import zstandard

from bot_common import QuoteSizing, WSOL, USDC, run, setup, StopBot


class Backtest:
    """Upstream replay ordering, fills, fees and SOL-equivalent accounting."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.quotes = QuoteSizing()
        self.positions = {}
        self.trades = []
        self.last_ts = 0
        self.seen = self.skipped = self.missed = self.buys = self.creates = 0

    def fill_buy(self, mint, pos):
        decision, fill = pos['decision_price'], pos['cur_price']
        ceiling = decision / (1 - min(self.cfg['trade']['buy_slippage'], 99.9) / 100)
        if fill > ceiling:
            self.missed += 1
            self.positions.pop(mint, None)
            return
        amount, quote = pos['amount'], pos['quote_mint']
        pos.update(state='held', entry_price=fill, quote_spent=amount,
                   tokens=amount * (1 - self.cfg['backtest']['service_fee'] - self.cfg['backtest']['extra_fee'] - pos['entry_pool_fee']) / fill,
                   sol_price=self.quotes.sol_price,
                   spent_sol=amount if quote == WSOL else amount / self.quotes.sol_price,
                   buy_slip=(fill / decision - 1) * 100, last_ts=pos['buy_fill_ts'])
        self.buys += 1

    def trigger_sell(self, pos, ts, reason):
        pos.update(state='pending_sell', sell_reason=reason,
                   sell_fill_ts=ts + self.cfg['backtest']['sell_latency_ms'])

    def close(self, mint, pos, exit_price, reason):
        gross = pos['tokens'] * exit_price
        fee = self.cfg['backtest']['service_fee'] + self.cfg['backtest']['extra_fee']
        proceeds = gross * (1 - (fee + pos['last_pool_fee']))
        pnl = proceeds - pos['quote_spent']
        fee_quote = pos['quote_spent'] * (fee + pos['entry_pool_fee']) + gross * (fee + pos['last_pool_fee'])
        is_sol = pos['quote_mint'] == WSOL
        self.trades.append({'mint': mint, 'quote_mint': pos['quote_mint'], 'reason': reason,
                            'pct': (exit_price - pos['entry_price']) / pos['entry_price'] * 100,
                            'pnl': pnl, 'pnl_sol': pnl if is_sol else pnl / pos['sol_price'],
                            'spent_sol': pos['spent_sol'],
                            'fee_sol': fee_quote if is_sol else fee_quote / pos['sol_price'],
                            'buy_slip': pos['buy_slip']})
        self.positions.pop(mint, None)

    def resolve(self, ts):
        for mint, pos in list(self.positions.items()):
            if pos['state'] == 'pending_buy' and pos['buy_fill_ts'] <= ts:
                self.fill_buy(mint, pos)
            elif pos['state'] == 'pending_sell' and pos['sell_fill_ts'] <= ts:
                self.close(mint, pos, pos['cur_price'], pos['sell_reason'])
            elif pos['state'] == 'held' and ts - pos['last_ts'] > self.cfg['sniper']['idle_seconds'] * 1000:
                self.trigger_sell(pos, pos['last_ts'] + self.cfg['sniper']['idle_seconds'] * 1000, 'idle')

    def handle_event(self, event):
        self.seen += 1
        ts = event['timestamp']
        self.last_ts = ts
        # Preserve upstream ordering: resolve BEFORE applying this event's price,
        # including timestamps that arrive out of order in the archive.
        self.resolve(ts)
        if event['action'] not in ('buy', 'sell', 'add', 'remove', 'create', 'migrate', 'createPool'):
            return
        mint = event['mint']
        if self.quotes.observe(event):
            return
        if event['action'] == 'create' and event['pool'] == 'pump' and not event.get('mayhemMode'):
            self.creates += 1
            quote = event['quoteMint']
            threshold = self.quotes.amount(quote, self.cfg['sniper']['min_initial_buy'])
            if self.quotes.supports(quote) and event['quoteAmount'] > threshold and mint not in self.positions:
                pool_fee = event['poolFeeRate']
                self.positions[mint] = {'state': 'pending_buy', 'quote_mint': quote,
                                        'amount': self.quotes.amount(quote, self.cfg['trade']['buy_amount']),
                                        'decision_price': event['price'], 'cur_price': event['price'],
                                        'entry_pool_fee': pool_fee, 'last_pool_fee': pool_fee,
                                        'buy_fill_ts': ts + self.cfg['backtest']['buy_latency_ms'], 'buy_slip': 0.0}
        elif mint in self.positions:
            pos = self.positions[mint]
            if event['action'] in ('buy', 'sell', 'add', 'remove', 'migrate') and (
                event['pool'] == 'pump' or (event['pool'] == 'pump-amm' and event.get('poolCreatedBy') == 'pump')):
                price = event['price']
                pos.update(cur_price=price, last_pool_fee=event['poolFeeRate'])
                if pos['state'] == 'held':
                    change = (price - pos['entry_price']) / pos['entry_price'] * 100
                    if change > self.cfg['sniper']['take_profit'] or change < -self.cfg['sniper']['stop_loss']:
                        self.trigger_sell(pos, ts, 'tp' if change > self.cfg['sniper']['take_profit'] else 'sl')
            if pos['state'] == 'held':
                pos['last_ts'] = ts

    def finish(self):
        # Upstream forces in-flight buys to fill, then closes at last-known price.
        for mint, pos in list(self.positions.items()):
            if pos['state'] == 'pending_buy':
                self.fill_buy(mint, pos)
        for mint, pos in list(self.positions.items()):
            reason = pos['sell_reason'] if pos['state'] == 'pending_sell' else (
                'idle' if self.last_ts - pos['last_ts'] > self.cfg['sniper']['idle_seconds'] * 1000 else 'end')
            self.close(mint, pos, pos['cur_price'], reason)
        pnl_sol = sum(trade['pnl_sol'] for trade in self.trades)
        volume = sum(trade['spent_sol'] for trade in self.trades)
        wins = sum(trade['pnl_sol'] > 0 for trade in self.trades)
        logging.info('Events=%s creates=%s buys=%s skipped=%s closed=%s missed=%s',
                     self.seen, self.creates, self.buys, self.skipped, len(self.trades), self.missed)
        logging.info('Modeled PnL=%+.8f SOL-equivalent; WSOL=%+.8f; USDC=%+.8f; wins=%s/%s',
                     pnl_sol, sum(t['pnl'] for t in self.trades if t['quote_mint'] == WSOL),
                     sum(t['pnl'] for t in self.trades if t['quote_mint'] == USDC), wins, len(self.trades))
        logging.info('Volume=%s SOL-equivalent; ROI=%+.2f%%; fees=%s SOL-equivalent',
                     volume, pnl_sol / volume * 100 if volume else 0, sum(t['fee_sol'] for t in self.trades))
        logging.info('Original replay model: last-known-price fills, end-window forced fills; not live execution guarantees.')


def replay_file(path, strategy, compressed=None):
    path = Path(path)
    if compressed is None:
        compressed = path.suffix == '.zst'
    with path.open('rb') as file:
        if compressed:
            with zstandard.ZstdDecompressor().stream_reader(file) as reader:
                read_lines(io.BufferedReader(reader), strategy)
        else:
            read_lines(file, strategy)


def read_lines(reader, strategy):
    for line in reader:
        if not line.strip():
            continue
        try:
            event = orjson.loads(line)
        except orjson.JSONDecodeError:
            strategy.skipped += 1
            continue
        if not isinstance(event, dict):
            strategy.skipped += 1
            continue
        try:
            strategy.handle_event(event)
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            strategy.skipped += 1


async def download_hour(session, cfg, hour, destination):
    url = f"{cfg['network']['replay_url'].rstrip('/')}/{hour:%Y/%m/%d/%H}.jsonl.zst"
    logging.info('Downloading archive %s UTC', hour.strftime('%Y-%m-%d %H:00'))
    async with session.get(url) as response:
        if response.status == 404 and cfg['backtest']['allow_gaps']:
            logging.warning('Skipping missing archive hour')
            return False
        if response.status != 200:
            raise StopBot(f'Archive download returned HTTP {response.status}; use --file or choose another window')
        # Download to a temporary file instead of keeping a whole hour in RAM.
        with Path(destination).open('wb') as file:
            async for chunk in response.content.iter_chunked(1 << 20):
                file.write(chunk)
    return True


async def main():
    cfg, args = setup('backtest')
    strategy = Backtest(cfg)
    if args.file:
        replay_file(args.file, strategy)
    else:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)) as session:
            with tempfile.TemporaryDirectory(prefix='zeroslip-replay-') as folder:
                path = Path(folder) / 'hour.jsonl.zst'
                for offset in range(cfg['backtest']['hours'], 0, -1):
                    if await download_hour(session, cfg, now - timedelta(hours=offset), path):
                        await asyncio.to_thread(replay_file, path, strategy, True)
    strategy.finish()


if __name__ == '__main__':
    run(main)
