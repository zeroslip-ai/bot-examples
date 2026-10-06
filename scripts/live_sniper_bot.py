"""Buy a selected token or snipe Pump launches, then manage TP/SL/idle exits."""
import logging
import time

import aiohttp

from bot_common import Trader, positive, run, run_stream, setup, trusted_pool


class Sniper:
    def __init__(self, cfg, trader, clock=time.monotonic):
        self.cfg, self.trader, self.clock = cfg, trader, clock
        self.positions = {}
        self.finished = set()

    async def on_event(self, event):
        mint, quote = event.get('mint'), event.get('quoteMint')
        price = positive(event.get('price'))
        if not mint or quote != self.cfg['trade']['quote_mint'] or not price:
            return
        selected = self.cfg['sniper']['token_mint']
        if selected:
            # Direct-token mode works across supported venues. Restrict to real
            # trade events and buy once at the first observed market price.
            eligible = mint == selected and event.get('action') in ('buy', 'sell')
        else:
            eligible = event.get('action') == 'create' and event.get('pool') == 'pump' and trusted_pool(event) and (
                positive(event.get('quoteAmount')) > self.cfg['sniper']['min_initial_buy'])
        if mint not in self.positions:
            if not eligible or mint in self.finished or len(self.positions) >= self.cfg['trade']['max_positions']:
                return
            result = await self.trader.order('buy', mint, quote, self.cfg['trade']['buy_amount'], price)
            self.positions[mint] = {'entry': positive(result['price']), 'price': positive(result['price']),
                                    'tokens': positive(result['tokenAmount']), 'last': self.clock(), 'quote': quote,
                                    'opened_ts': positive(result.get('timestamp')) or positive(event.get('timestamp'))}
            logging.info('Opened %s; TP=%s%% SL=%s%%', mint, self.cfg['sniper']['take_profit'], self.cfg['sniper']['stop_loss'])
            return
        if event.get('action') not in ('buy', 'sell', 'add', 'remove', 'migrate'):
            return
        if not selected and not trusted_pool(event):
            return
        position = self.positions[mint]
        if positive(event.get('timestamp')) < position['opened_ts']:
            return
        position.update(price=price, last=self.clock())
        change = (price / position['entry'] - 1) * 100
        if change >= self.cfg['sniper']['take_profit'] or change <= -self.cfg['sniper']['stop_loss']:
            await self.exit(mint, 'take profit' if change >= self.cfg['sniper']['take_profit'] else 'stop loss')

    async def exit(self, mint, reason):
        position = self.positions[mint]
        await self.trader.order('sell', mint, position['quote'], '100%', position['price'], position['tokens'])
        self.positions.pop(mint)
        self.finished.add(mint)
        logging.info('Closed %s (%s)', mint, reason)

    async def on_tick(self):
        for mint, position in list(self.positions.items()):
            if self.clock() - position['last'] >= self.cfg['sniper']['idle_seconds']:
                await self.exit(mint, 'idle timeout')


async def main():
    cfg, args = setup('sniper')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        trader = Trader(cfg, session, args.live)
        strategy = Sniper(cfg, trader)
        await run_stream(cfg, trader, strategy.on_event, strategy.on_tick)


if __name__ == '__main__':
    run(main)
