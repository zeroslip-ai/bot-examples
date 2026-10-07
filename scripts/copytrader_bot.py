"""Copy configured wallets with capped buys and proportional exits."""
import logging

import aiohttp

from bot_common import QuoteSizing, Trader, positive, run, run_stream, setup, trusted_pool


class Copytrader:
    def __init__(self, cfg, trader):
        self.cfg, self.trader = cfg, trader
        self.positions = {}
        self.quotes = QuoteSizing()

    def watched_wallet(self, event):
        watched = self.cfg['copytrader']['wallets']
        signer = event.get('txSigner')
        involved = event.get('tradersInvolved')
        if isinstance(involved, dict):
            # Explicit traders take precedence over a fee payer in postBalances.
            owner = self.positions.get(event.get('mint'), {}).get('wallet')
            if owner in involved:
                return owner
            if signer in watched and signer in involved:
                return signer
            return next((wallet for wallet in watched if wallet in involved), None)
        if signer in watched:
            return signer
        # Older events without tradersInvolved retain the original fallback.
        balances = event.get('postBalances') or {}
        return next((wallet for wallet in watched if wallet in balances), None)

    @staticmethod
    def wallet_quantity(event, wallet):
        breakdown = event.get('breakdown')
        if isinstance(breakdown, list):
            return sum(positive(trade.get('tokenAmount')) for trade in breakdown
                       if isinstance(trade, dict) and trade.get('trader') == wallet
                       and trade.get('action') == event['action'])
        if len(event.get('tradersInvolved') or {}) > 1:
            return None  # An aggregate amount cannot identify this wallet's exit.
        return positive(event.get('tokenAmount'))

    async def on_event(self, event):
        if not self.accept_event(event):
            return
        wallet = self.watched_wallet(event)
        mint, quote = event.get('mint'), event.get('quoteMint')
        price, quantity = positive(event.get('price')), positive(event.get('tokenAmount'))
        if not wallet or not mint or not price or not quantity or not self.quotes.supports(quote):
            return
        allowed = self.cfg['copytrader']['token_mints']
        if allowed and mint not in allowed:
            return
        if event['action'] == 'buy':
            if mint in self.positions:
                if wallet == self.positions[mint]['wallet']:
                    owned_quantity = self.wallet_quantity(event, wallet)
                    if owned_quantity is not None:
                        self.positions[mint]['copied_remaining'] += owned_quantity
                return
            balances = (event.get('postBalances') or {}).get(wallet, {})
            balance = positive(balances.get(mint))
            # Preserve upstream's first-buy gate across ALL transaction wallets.
            post_total = sum(positive(values.get(mint)) for values in (event.get('postBalances') or {}).values())
            if abs(post_total - quantity) > max(0.000001, quantity * 0.000001):
                return
            amount = min(positive(event.get('quoteAmount')) * self.cfg['copytrader']['buy_fraction'],
                         self.quotes.amount(quote, self.cfg['copytrader']['max_buy_amount']))
            if amount <= 0:
                return
            result = await self.trader.order('buy', mint, quote, amount, price)
            self.positions[mint] = {'wallet': wallet, 'quote': quote, 'tokens': positive(result['tokenAmount']),
                                    'copied_remaining': balance or quantity}
        elif mint in self.positions:
            position = self.positions[mint]
            if wallet != position['wallet']:
                return  # A different watched wallet must not close this position.
            quantity = self.wallet_quantity(event, wallet)
            if quantity is None:
                logging.warning('Skipped ambiguous bundled exit mint=%s; no per-wallet breakdown', mint)
                return
            if quantity <= 0:
                return
            fraction = min(1.0, quantity / position['copied_remaining'])
            percent = min(100.0, fraction * 100)
            amount = '100%' if fraction >= 1 else f'{percent:.8f}%'
            result = await self.trader.order('sell', mint, position['quote'], amount, price, position['tokens'])
            position['tokens'] = max(0, position['tokens'] - positive(result['tokenAmount']))
            position['copied_remaining'] = max(0, position['copied_remaining'] - quantity)
            if fraction >= 1 or position['tokens'] == 0:
                self.positions.pop(mint)
            logging.info('Copied wallet exit %s mint=%s fraction=%.2f%%', wallet, mint, percent)

    def accept_event(self, event):
        if self.quotes.observe(event):
            return False
        return event.get('action') in ('buy', 'sell') and trusted_pool(event) and bool(self.watched_wallet(event))

    async def on_tick(self, mint=None):
        return mint in self.positions


async def main():
    cfg, args = setup('copytrader')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        trader = Trader(cfg, session, args.live)
        strategy = Copytrader(cfg, trader)
        await run_stream(cfg, trader, strategy.on_event, strategy.on_tick, strategy.accept_event)


if __name__ == '__main__':
    run(main)
