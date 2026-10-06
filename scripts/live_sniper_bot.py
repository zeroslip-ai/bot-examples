"""Buy a selected token or snipe Pump launches, then manage TP/SL/idle exits."""
import logging
import time

import aiohttp

from bot_common import QuoteSizing, Trader, positive, run, run_stream, setup, trusted_pool


class Sniper:
    def __init__(self, cfg, trader, clock=time.monotonic):
        self.cfg, self.trader, self.clock = cfg, trader, clock
        self.positions = {}
        self.finished = set()
        self.quotes = QuoteSizing()

    async def on_event(self, event):
        if not self.accept_event(event):
            return
        mint, quote = event.get('mint'), event.get('quoteMint')
        price = positive(event.get('price'))
        if not mint:
            return
        selected = self.cfg['sniper']['token_mint']
        if selected:
            # Direct-token mode works across supported venues. Restrict to real
            # trade events and buy once at the first observed market price.
            eligible = mint == selected and event.get('action') in ('buy', 'sell')
        else:
            eligible = event.get('action') == 'create' and event.get('pool') == 'pump' and trusted_pool(event) and (
                positive(event.get('quoteAmount')) > self.quotes.amount(quote, self.cfg['sniper']['min_initial_buy']))
        if mint not in self.positions:
            if not self.quotes.supports(quote) or not price:
                return
            if not eligible or mint in self.finished:
                return
            result = await self.trader.order('buy', mint, quote, self.quotes.amount(quote, self.cfg['trade']['buy_amount']), price)
            self.positions[mint] = {'entry': positive(result['price']), 'price': positive(result['price']),
                                    'tokens': positive(result['tokenAmount']), 'last': self.clock(), 'quote': quote}
            logging.info('Opened %s; TP=%s%% SL=%s%%', mint, self.cfg['sniper']['take_profit'], self.cfg['sniper']['stop_loss'])
            return
        # As upstream, any token event resets idle; only these actions drive TP/SL.
        self.positions[mint]['last'] = self.clock()
        if event.get('action') not in ('buy', 'sell', 'add', 'remove'):
            return
        if not price:
            return
        if not selected and not trusted_pool(event):
            return
        position = self.positions[mint]
        position.update(price=price, last=self.clock())
        change = (price - position['entry']) / position['entry'] * 100
        if change > self.cfg['sniper']['take_profit'] or change < -self.cfg['sniper']['stop_loss']:
            await self.exit(mint, 'take profit' if change > self.cfg['sniper']['take_profit'] else 'stop loss')

    def accept_event(self, event):
        if self.quotes.observe(event):
            return False
        mint = event.get('mint')
        return bool(mint) and (mint in self.positions or mint == self.cfg['sniper']['token_mint'] or (
            event.get('action') == 'create' and event.get('pool') == 'pump' and trusted_pool(event)))

    async def exit(self, mint, reason):
        position = self.positions[mint]
        await self.trader.order('sell', mint, position['quote'], '100%', position['price'], position['tokens'])
        self.positions.pop(mint)
        if self.cfg['sniper']['token_mint']:
            self.finished.add(mint)
        logging.info('Closed %s (%s)', mint, reason)

    async def on_tick(self, mint=None):
        positions = list(self.positions.items()) if mint is None else (
            [(mint, self.positions[mint])] if mint in self.positions else [])
        for token, position in positions:
            if self.clock() - position['last'] > self.cfg['sniper']['idle_seconds']:
                await self.exit(token, 'idle timeout')
        return mint in self.positions


async def main():
    cfg, args = setup('sniper')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        trader = Trader(cfg, session, args.live)
        strategy = Sniper(cfg, trader)
        await run_stream(cfg, trader, strategy.on_event, strategy.on_tick, strategy.accept_event)


if __name__ == '__main__':
    run(main)
