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

from bot_common import positive, run, setup, trusted_pool, StopBot


class Backtest:
    def __init__(self, cfg):
        self.cfg = cfg
        self.positions = {}
        self.trades = []
        self.seen_mints = set()
        self.last_ts = 0
        self.seen = self.skipped = self.missed = 0

    def fill_buy(self, mint, position):
        price = position['price']
        ceiling = position['decision_price'] / (1 - self.cfg['trade']['buy_slippage'] / 100)
        if price > ceiling:
            self.positions.pop(mint)
            self.missed += 1
            return
        amount = self.cfg['trade']['buy_amount']
        fees = self.cfg['backtest']['service_fee'] + self.cfg['backtest']['extra_fee'] + position['pool_fee']
        position.update(state='held', entry=price, amount=amount, tokens=amount * (1 - fees) / price,
                        last=position['due'])

    def close(self, mint, position, reason):
        fees = self.cfg['backtest']['service_fee'] + self.cfg['backtest']['extra_fee'] + position['pool_fee']
        proceeds = position['tokens'] * position['price'] * (1 - fees)
        self.trades.append({'mint': mint, 'reason': reason, 'pnl': proceeds - position['amount']})
        self.positions.pop(mint)

    def trigger_sell(self, position, ts, reason):
        position.update(state='pending_sell', due=ts + self.cfg['backtest']['sell_latency_ms'], reason=reason)

    def resolve(self, ts):
        for mint, position in list(self.positions.items()):
            if position['state'] == 'pending_buy' and ts >= position['due']:
                self.fill_buy(mint, position)
            elif position['state'] == 'pending_sell' and ts >= position['due']:
                self.close(mint, position, position['reason'])
            elif position['state'] == 'held' and ts - position['last'] >= self.cfg['sniper']['idle_seconds'] * 1000:
                self.trigger_sell(position, position['last'] + self.cfg['sniper']['idle_seconds'] * 1000, 'idle')

    def handle_event(self, event):
        self.seen += 1
        ts = positive(event.get('timestamp'))
        if not ts or ts < self.last_ts:
            self.skipped += 1
            return
        self.last_ts = ts
        mint = event.get('mint')
        price = positive(event.get('price'))
        relevant = event.get('quoteMint') == self.cfg['trade']['quote_mint'] and trusted_pool(event)
        # At the first event at/after the latency deadline, use that observed
        # price, not the launch decision price. Sparse feeds remain an estimate.
        if mint in self.positions and relevant and price:
            self.positions[mint].update(price=price, pool_fee=self.pool_fee(event))
        self.resolve(ts)
        if not relevant or not mint or not price:
            return
        if event.get('action') == 'create' and event.get('pool') == 'pump':
            selected = self.cfg['sniper']['token_mint']
            if selected and mint != selected:
                return
            if mint in self.seen_mints:
                return
            self.seen_mints.add(mint)
            if positive(event.get('quoteAmount')) <= self.cfg['sniper']['min_initial_buy'] or len(self.positions) >= self.cfg['trade']['max_positions']:
                return
            self.positions[mint] = {'state': 'pending_buy', 'decision_price': price, 'price': price,
                                    'pool_fee': self.pool_fee(event),
                                    'due': ts + self.cfg['backtest']['buy_latency_ms']}
        elif mint in self.positions and event.get('action') in ('buy', 'sell', 'add', 'remove', 'migrate'):
            position = self.positions[mint]
            if position['state'] == 'held':
                change = (price / position['entry'] - 1) * 100
                if change >= self.cfg['sniper']['take_profit'] or change <= -self.cfg['sniper']['stop_loss']:
                    self.trigger_sell(position, ts, 'tp' if change >= self.cfg['sniper']['take_profit'] else 'sl')
                else:
                    position['last'] = ts

    def pool_fee(self, event):
        value = event.get('poolFeeRate', 0)
        try:
            value = float(value)
            if not 0 <= value < 1 - self.cfg['backtest']['service_fee'] - self.cfg['backtest']['extra_fee']:
                raise ValueError
            return value
        except (ValueError, TypeError):
            raise StopBot('Replay has an invalid poolFeeRate; cannot model execution costs')

    def finish(self):
        pending = 0
        for mint, position in list(self.positions.items()):
            if position['state'] == 'pending_buy':
                # A buy due after the window cannot be counted as a fill.
                pending += 1
                self.positions.pop(mint)
            else:
                self.close(mint, position, 'end-of-window mark-to-market')
        logging.info('Events=%s skipped=%s closed=%s missed=%s unfilled-at-end=%s',
                     self.seen, self.skipped, len(self.trades), self.missed, pending)
        pnl = sum(trade['pnl'] for trade in self.trades)
        wins = sum(trade['pnl'] > 0 for trade in self.trades)
        logging.info('Modeled PnL=%+.8f quote units; wins=%s/%s; quote=%s',
                     pnl, wins, len(self.trades), self.cfg['trade']['quote_mint'])
        logging.info('Model includes configured latency, entry slippage and per-side fees; it omits network fees, depth and atomic bundle execution.')


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
        strategy.handle_event(event)


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
