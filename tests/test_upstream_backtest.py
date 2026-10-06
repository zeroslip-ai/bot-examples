"""Compare restored replay calculations against the pinned upstream engine."""
import copy
from pathlib import Path
import random
import sys
import unittest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from bot_common import DEFAULTS, WSOL, USDC, SOL_USDC_POOL
from backtest_sniper_strategy import Backtest


class UpstreamReplayTests(unittest.TestCase):
    def oracle(self):
        namespace = {}
        source = (ROOT / 'tests/fixtures/upstream_backtest.py').read_text()
        exec(compile(source, 'upstream_backtest.py', 'exec'), namespace)
        return namespace

    def assert_matches(self, events):
        original = self.oracle()
        current = Backtest(copy.deepcopy(DEFAULTS))
        for index, event in enumerate(events):
            original['handle_event'](event)
            current.handle_event(event)
            self.assertEqual(current.positions, original['positions'], f'positions after event {index}')
            self.assertEqual(current.trades, original['trades'], f'trades after event {index}')
            self.assertEqual(current.missed, original['stats']['missed'])
            self.assertEqual(current.last_ts, original['stats']['last_ts'])
        original['finish']('test window')
        current.finish()
        self.assertEqual(current.trades, original['trades'])
        self.assertEqual(current.buys, original['stats']['buys'])
        self.assertEqual(current.creates, original['stats']['creates'])

    @staticmethod
    def event(ts, action='create', mint='token-a', **extra):
        return {'timestamp': ts, 'action': action, 'mint': mint, 'pool': 'pump',
                'poolId': 'pump-pool', 'mayhemMode': False, 'quoteMint': WSOL,
                'quoteAmount': 21, 'price': 1, 'poolFeeRate': .0125, **extra}

    def test_original_latency_fees_usdc_idle_and_window_end(self):
        e = self.event
        self.assert_matches([
            e(1000, poolId=SOL_USDC_POOL, price=.01),
            e(1100, quoteMint=USDC, quoteAmount=2100),
            e(1200, 'buy', quoteMint=USDC, price=1.1, poolFeeRate=.01),
            e(1600, 'buy', quoteMint=USDC, price=1.65, poolFeeRate=.02),
            e(2000, 'sell', quoteMint=USDC, price=1.7),
            e(2100, mint='token-b'),
            e(2200, 'buy', mint='token-b', price=1.4),
            e(2600, 'buy', mint='token-b', price=1),  # Original price-before-event miss.
            e(3000, mint='token-c'),
            e(3400, 'buy', mint='token-c'),
            e(303500, 'buy', mint='unrelated'),  # Idle starts selling.
            e(304000, 'buy', mint='unrelated'),
            e(305000, mint='token-a', quoteMint=USDC, quoteAmount=2100),  # Re-entry allowed.
            e(305100, mint='token-d'),  # Both pending buys force-filled at end.
        ])

    def test_original_out_of_order_and_untrusted_pool_handling(self):
        e = self.event
        self.assert_matches([
            e(1000), e(1400, 'buy', price=1.5),  # Exact TP doesn't trigger.
            e(1300, 'buy', price=.8),  # Original keeps this out-of-order event.
            e(1600, 'buy', price=.79),
            e(1800, 'buy', pool='orca-whirlpool', price=50),
            e(2200, 'buy', price=.7),
        ])

    def test_seeded_mixed_quote_replay_matches_original(self):
        rng = random.Random(76)
        events, ts = [], 1000
        for _ in range(500):
            ts += rng.randrange(10, 1000)
            mint = f'token-{rng.randrange(6)}'
            action = rng.choice(['create', 'buy', 'sell', 'add', 'remove', 'migrate'])
            quote = rng.choice([WSOL, USDC])
            events.append(self.event(ts - rng.choice([0, 0, 500]), action, mint,
                                     quoteMint=quote, quoteAmount=2500 if quote == USDC else 25,
                                     price=rng.uniform(.5, 2), poolFeeRate=rng.choice([.01, .0125, .02])))
        self.assert_matches(events)


if __name__ == '__main__':
    unittest.main()
