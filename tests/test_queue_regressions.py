"""Exercise queued copy trades through the actual stream reader and workers."""

import asyncio
import copy
import sys
from pathlib import Path
import unittest

from aiohttp import web

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from bot_common import DEFAULTS, Trader, run_stream
from copytrader_bot import Copytrader
from test_bots import MINT, WALLET, event


class QueuedCopyTests(unittest.IsolatedAsyncioTestCase):
    async def test_queued_reentry_cannot_bypass_pool_trust_after_full_exit(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg['copytrader']['wallets'] = [WALLET]
        burst_seen, done = asyncio.Event(), asyncio.Event()
        orders, rejected_with_no_position = [], []
        unsafe = [
            {'pool': 'pump', 'mayhemMode': True},
            {'pool': 'pump-amm', 'poolCreatedBy': 'custom', 'mayhemMode': True},
            {'pool': 'pump-amm', 'poolCreatedBy': 'custom'},
            {'pool': 'pump-amm', 'poolCreatedBy': 'pump', 'mayhemMode': True},
            {'pool': 'raydium'},
        ]
        burst = [event('buy', signature='entry'), event('sell', signature='full-exit')]
        burst += [event('buy', signature=f'unsafe-{i}', **pool) for i, pool in enumerate(unsafe)]
        last = burst[-1]['signature']

        class WaitingTrader(Trader):
            def observe(self, value):
                super().observe(value)
                if value['signature'] == last:
                    burst_seen.set()

            async def order(self, action, mint, quote, amount, price, tokens=0):
                # Like live confirmation, hold the worker while the reader queues
                # the entire burst. The queue survives after the position closes.
                await burst_seen.wait()
                orders.append(action)
                return await super().order(action, mint, quote, amount, price, tokens)

        async def stream(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            for value in burst:
                await socket.send_json(value)
            async for _ in socket:
                pass
            return socket

        app = web.Application()
        app.router.add_get('/', stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        cfg['network']['stream_url'] = f'ws://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/'
        trader = WaitingTrader(cfg, object())
        strategy = Copytrader(cfg, trader)

        async def on_event(value):
            if value['signature'].startswith('unsafe-'):
                rejected_with_no_position.append(MINT not in strategy.positions)
            await strategy.on_event(value)
            if value['signature'] == last:
                done.set()

        task = asyncio.create_task(run_stream(cfg, trader, on_event, strategy.on_tick, strategy.accept_event))
        try:
            await asyncio.wait_for(done.wait(), 3)
            self.assertEqual(rejected_with_no_position, [True] * len(unsafe))
            self.assertEqual(orders, ['buy', 'sell'])
            self.assertFalse(strategy.positions)
            self.assertFalse(trader.paper_positions)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runner.cleanup()
