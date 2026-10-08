"""Behavior checks for mixed quotes, bundled wallet exits, and invalid replay."""

import copy
import io
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from bot_common import DEFAULTS, Trader, USDC, WSOL, SOL_USDC_POOL
from copytrader_bot import Copytrader
from live_sniper_bot import Sniper
from backtest_sniper_strategy import Backtest, read_lines
from test_bots import deliver
from test_cli_integration import market, MINT, WALLET, OTHER


class MarketRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cfg = copy.deepcopy(DEFAULTS)
        self.cfg['copytrader']['wallets'] = [WALLET, OTHER]
        self.trader = Trader(self.cfg, object())

    async def test_sniper_compares_sol_and_usdc_prices_in_entry_quote(self):
        self.cfg['sniper']['token_mint'] = MINT
        for entry_quote, entry_price, next_quote, next_price in [(WSOL, 1.0, USDC, 80.0), (USDC, 80.0, WSOL, 1.0)]:
            s = Sniper(self.cfg, self.trader)
            await deliver(s, market(quoteMint=entry_quote, price=entry_price))
            await deliver(s, market('sell', quoteMint=next_quote, price=next_price))
            self.assertIn(MINT, s.positions, 'Same economic price must not trigger an exit')
            self.assertEqual(s.positions[MINT]['price'], entry_price)
            await deliver(s, market('sell', quoteMint=next_quote, price=next_price * 1.6))
            self.assertNotIn(MINT, s.positions)

    async def test_sniper_ignores_unsupported_quote_prices(self):
        self.cfg['sniper']['token_mint'] = MINT
        s = Sniper(self.cfg, self.trader)
        await deliver(s, market())
        await deliver(s, market('sell', quoteMint=OTHER, price=500))
        self.assertIn(MINT, s.positions)
        self.assertEqual(s.positions[MINT]['price'], 1.0)

    async def test_sniper_quote_conversion_observes_latest_sol_price(self):
        self.cfg['sniper']['token_mint'] = MINT
        s = Sniper(self.cfg, self.trader)
        await deliver(s, market(poolId=SOL_USDC_POOL, price=0.01))
        await deliver(s, market())
        await deliver(s, market('sell', quoteMint=USDC, price=100))
        self.assertIn(MINT, s.positions)

    async def test_tracked_non_trade_update_cannot_become_a_copy_exit(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, market())
        await deliver(strategy, market('create'))
        self.assertEqual(strategy.positions[MINT]['copied_remaining'], 100)
        self.assertEqual(self.trader.paper_sells, 0)

    async def test_copy_bundle_sells_only_followed_wallet_fraction(self):
        s = Copytrader(self.cfg, self.trader)
        await deliver(s, market())
        initial = s.positions[MINT]['tokens']
        await deliver(
            s,
            market(
                'sell',
                txSigner=OTHER,
                tokenAmount=100,
                tradersInvolved={WALLET: {}, OTHER: {}},
                breakdown=[
                    {'action': 'sell', 'trader': WALLET, 'tokenAmount': 10},
                    {'action': 'sell', 'trader': OTHER, 'tokenAmount': 90},
                ],
            ),
        )
        self.assertIn(MINT, s.positions)
        self.assertAlmostEqual(s.positions[MINT]['tokens'], initial * 0.9)
        self.assertEqual(s.positions[MINT]['copied_remaining'], 90)

    async def test_copy_bundle_additional_buy_uses_owner_amount(self):
        s = Copytrader(self.cfg, self.trader)
        await deliver(s, market())
        await deliver(
            s,
            market(
                tokenAmount=100,
                tradersInvolved={WALLET: {}, OTHER: {}},
                breakdown=[
                    {'action': 'buy', 'trader': WALLET, 'tokenAmount': 10},
                    {'action': 'buy', 'trader': OTHER, 'tokenAmount': 90},
                ],
            ),
        )
        self.assertEqual(s.positions[MINT]['copied_remaining'], 110)

    async def test_copy_fee_payer_alone_is_not_a_followed_trade(self):
        self.cfg['copytrader']['wallets'] = [WALLET]
        s = Copytrader(self.cfg, self.trader)
        await deliver(
            s,
            market(txSigner=WALLET, tradersInvolved={OTHER: {}}, postBalances={WALLET: {MINT: 0}, OTHER: {MINT: 100}}),
        )
        self.assertFalse(s.positions)

    async def test_copy_ambiguous_multiwallet_sell_keeps_position(self):
        s = Copytrader(self.cfg, self.trader)
        await deliver(s, market())
        await deliver(s, market('sell', tokenAmount=100, tradersInvolved={WALLET: {}, OTHER: {}}))
        self.assertEqual(s.positions[MINT]['copied_remaining'], 100)


class ReplayValidationTests(unittest.TestCase):
    def test_zero_initial_buy_launch_remains_a_valid_non_entry_event(self):
        s = Backtest(copy.deepcopy(DEFAULTS))
        read_lines(io.BytesIO((json.dumps(market('create', price=0, quoteAmount=0)) + '\n').encode()), s)
        s.finish()
        self.assertEqual(s.skipped, 0)
        self.assertEqual(s.creates, 1)
        self.assertFalse(s.trades)

    def test_bad_prices_are_skipped_before_creating_pending_positions(self):
        for price in (0, -1, None, 'bad'):
            s = Backtest(copy.deepcopy(DEFAULTS))
            read_lines(io.BytesIO((json.dumps(market('create', price=price)) + '\n').encode()), s)
            s.finish()
            self.assertFalse(s.positions)
            self.assertFalse(s.trades)
            self.assertEqual(s.skipped, 1)

    def test_bad_timestamp_does_not_poison_existing_position(self):
        s = Backtest(copy.deepcopy(DEFAULTS))
        s.handle_event(market('create'))
        read_lines(io.BytesIO((json.dumps(market(timestamp='bad')) + '\n').encode()), s)
        s.finish()
        self.assertEqual(s.skipped, 1)
        self.assertEqual(len(s.trades), 1)
