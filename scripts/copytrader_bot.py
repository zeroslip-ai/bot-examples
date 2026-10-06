"""Copy configured wallets with capped buys and proportional exits."""
import logging
import math

import aiohttp

from bot_common import Trader, positive, run, run_stream, setup, trusted_pool


class Copytrader:
    def __init__(self, cfg, trader):
        self.cfg, self.trader = cfg, trader
        self.positions = {}

    def watched_wallet(self, event):
        watched = self.cfg['copytrader']['wallets']
        signer = event.get('txSigner')
        if signer in watched:
            return signer
        # txSigner may be a bundle payer; tradersInvolved identifies traders.
        involved = event.get('tradersInvolved') or {}
        return next((wallet for wallet in watched if wallet in involved), None)

    async def on_event(self, event):
        if event.get('action') not in ('buy', 'sell') or not trusted_pool(event):
            return
        wallet = self.watched_wallet(event)
        mint, quote = event.get('mint'), event.get('quoteMint')
        price, quantity = positive(event.get('price')), positive(event.get('tokenAmount'))
        if not wallet or not mint or not price or not quantity or quote != self.cfg['trade']['quote_mint']:
            return
        allowed = self.cfg['copytrader']['token_mints']
        if allowed and mint not in allowed:
            return
        if event['action'] == 'buy':
            if mint in self.positions:
                if wallet == self.positions[mint]['wallet']:
                    self.positions[mint]['copied_remaining'] += quantity
                return
            if len(self.positions) >= self.cfg['trade']['max_positions']:
                return
            balances = (event.get('postBalances') or {}).get(wallet, {})
            balance = positive(balances.get(mint))
            if self.cfg['copytrader']['first_buy_only'] and (not balance or not math.isclose(balance, quantity, rel_tol=1e-6, abs_tol=1e-6)):
                return
            amount = min(positive(event.get('quoteAmount')) * self.cfg['copytrader']['buy_fraction'],
                         self.cfg['copytrader']['max_buy_amount'])
            if amount <= 0:
                return
            result = await self.trader.order('buy', mint, quote, amount, price)
            self.positions[mint] = {'wallet': wallet, 'quote': quote, 'tokens': positive(result['tokenAmount']),
                                    'copied_remaining': balance or quantity}
        elif mint in self.positions:
            position = self.positions[mint]
            if wallet != position['wallet']:
                return  # A different watched wallet must not close this position.
            fraction = min(1.0, quantity / position['copied_remaining'])
            percent = min(100.0, fraction * 100)
            amount = '100%' if fraction >= 1 else f'{percent:.8f}%'
            result = await self.trader.order('sell', mint, position['quote'], amount, price, position['tokens'])
            position['tokens'] = max(0, position['tokens'] - positive(result['tokenAmount']))
            position['copied_remaining'] = max(0, position['copied_remaining'] - quantity)
            if fraction >= 1 or position['tokens'] == 0:
                self.positions.pop(mint)
            logging.info('Copied wallet exit %s mint=%s fraction=%.2f%%', wallet, mint, percent)

    async def on_tick(self):
        pass


async def main():
    cfg, args = setup('copytrader')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        trader = Trader(cfg, session, args.live)
        strategy = Copytrader(cfg, trader)
        await run_stream(cfg, trader, strategy.on_event, strategy.on_tick)


if __name__ == '__main__':
    run(main)
