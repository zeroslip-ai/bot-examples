import asyncio
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web
import zstandard

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from bot_common import (
    ConfigError,
    DEFAULTS,
    StopBot,
    Trader,
    WSOL,
    USDC,
    SOL_USDC_POOL,
    credentials,
    load_config,
    run_stream,
)
from copytrader_bot import Copytrader
from live_sniper_bot import Sniper
from backtest_sniper_strategy import Backtest, replay_file
from sell_all_tokens import select_balances, sell_selected

MINT = 'GchFXs6VQHxBsmqi5mu4pGJXh99vRx7nPqTrCTSJpump'
WALLET = '11111111111111111111111111111111'
OTHER = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'


def event(action='create', **extra):
    result = {
        'action': action,
        'mint': MINT,
        'quoteMint': WSOL,
        'pool': 'pump',
        'mayhemMode': False,
        'price': 1.0,
        'quoteAmount': 21,
        'tokenAmount': 21,
        'txSigner': WALLET,
        'timestamp': 1000,
        'poolFeeRate': 0.0125,
        'poolId': 'pump-pool',
        'signature': 'launch',
        **extra,
    }
    result.setdefault('postBalances', {result['txSigner']: {result['mint']: result['tokenAmount']}})
    return result


async def deliver(strategy, value, fill=True):
    """Use the stream filter; entry tests include a subsequent trade at the same price."""
    if strategy.accept_event(value) or value.get('mint') in strategy.positions:
        await strategy.on_event(value)
    if fill and isinstance(strategy, Sniper) and value.get('mint') in strategy.pending:
        later = {
            **value,
            'action': 'buy',
            'timestamp': value['timestamp'] + 1,
            'signature': value.get('signature', '') + '-next-trade',
        }
        if strategy.accept_event(later):
            await strategy.on_event(later)


class ConfigTests(unittest.TestCase):
    def test_original_strategy_defaults_restored(self):
        cfg = load_config(ROOT / 'config.example.toml')
        self.assertEqual(cfg['trade']['sell_slippage'], 99)
        self.assertEqual(cfg['sell']['slippage'], 100)
        self.assertEqual(cfg['trade']['confirmation_seconds'], 3)
        self.assertEqual(cfg['backtest']['hours'], 10)
        self.assertNotIn('max_positions', cfg['trade'])
        self.assertNotIn('first_buy_only', cfg['copytrader'])

    def test_example_config_valid_and_secret_free(self):
        cfg = load_config(ROOT / 'config.example.toml')
        self.assertNotIn('quote_mint', cfg['trade'])
        self.assertNotIn('private_key', cfg['wallet'])

    def test_invalid_slippage_type_and_typo_fail(self):
        for text in ('[trade]\nbuy_slippage=100', '[trade]\nbuy_amount="small"', '[trade]\nbuy_amont=1'):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.toml') as file:
                file.write(text)
                file.flush()
                with self.assertRaises(ConfigError):
                    load_config(file.name)

    def test_credentials_are_optional_until_live_and_api_key_wins(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ConfigError):
                credentials()
        with patch.dict(
            os.environ, {'ZEROSLIP_API_KEY': 'test-key', 'ZEROSLIP_PRIVATE_KEY': 'test-private'}, clear=True
        ):
            self.assertEqual(credentials(), {'apiKey': 'test-key'})

    def test_all_cli_checks_and_missing_config(self):
        scripts = [
            ('live_sniper_bot.py', []),
            ('copytrader_bot.py', ['--wallet', WALLET]),
            ('backtest_sniper_strategy.py', []),
            ('sell_all_tokens.py', ['--mint', MINT]),
        ]
        for filename, options in scripts:
            env = {k: v for k, v in os.environ.items() if not k.startswith('ZEROSLIP_')}
            env['ZEROSLIP_WALLET_PUBLIC_KEY'] = OTHER
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / 'scripts' / filename),
                    '--config',
                    str(ROOT / 'config.example.toml'),
                    '--check-config',
                    *options,
                ],
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('No network calls', result.stdout)
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / 'scripts/live_sniper_bot.py'),
                '--config',
                '/nonexistent/config.toml',
                '--check-config',
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('copy config.example.toml', result.stderr)


class StrategyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cfg = copy.deepcopy(DEFAULTS)
        self.cfg['copytrader']['wallets'] = [WALLET, OTHER]
        # An object with no HTTP methods proves paper mode cannot send a request.
        self.trader = Trader(self.cfg, object())

    async def test_copy_buy_and_two_proportional_sells(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', tokenAmount=100, quoteAmount=10))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 0.01)
        await deliver(strategy, event('sell', tokenAmount=50, txSigner=OTHER))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 0.01)
        await deliver(strategy, event('sell', tokenAmount=50))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 0.005)
        await deliver(strategy, event('sell', tokenAmount=50))
        self.assertNotIn(MINT, strategy.positions)

    async def test_followed_additional_buy_updates_exit_baseline(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', tokenAmount=100))
        await deliver(strategy, event('buy', tokenAmount=100))
        await deliver(strategy, event('sell', tokenAmount=100))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 0.005)

    async def test_sell_failure_keeps_copy_position(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', tokenAmount=100))

        async def failure(*args, **kwargs):
            raise StopBot('uncertain')

        self.trader.order = failure
        with self.assertRaises(StopBot):
            await deliver(strategy, event('sell', tokenAmount=100))
        self.assertEqual(strategy.positions[MINT]['copied_remaining'], 100)

    async def test_copy_filters_and_first_buy(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', mayhemMode=True))
        self.assertFalse(strategy.positions)
        await deliver(strategy, event('buy', postBalances={WALLET: {MINT: 1000}}))
        self.assertFalse(strategy.positions)
        await deliver(strategy, event('buy', postBalances={WALLET: {MINT: 21}}))
        self.assertIn(MINT, strategy.positions)

    async def test_original_first_buy_sums_all_transaction_wallets(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', postBalances={WALLET: {MINT: 21}, OTHER: {MINT: 5}}))
        self.assertFalse(strategy.positions)  # Matched wallet alone looks like a first buy, total does not.
        await deliver(strategy, event('buy', postBalances={WALLET: {MINT: 10}, OTHER: {MINT: 11}}))
        self.assertIn(MINT, strategy.positions)

    async def test_sol_and_usdc_sizing_updates_from_original_pool(self):
        for cls in (Copytrader, Sniper):
            strategy = cls(self.cfg, self.trader)
            await deliver(strategy, event('buy', poolId=SOL_USDC_POOL, price=0.01))
            # At $100/SOL, 0.001 SOL sniper size = $0.10; copy cap = $1.
            await deliver(
                strategy,
                event(
                    'buy' if cls is Copytrader else 'create',
                    quoteMint=USDC,
                    quoteAmount=2100,
                    price=1,
                    tokenAmount=2100,
                ),
            )
            self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 1 if cls is Copytrader else 0.1)

    async def test_bots_have_no_three_position_cap(self):
        for cls in (Copytrader, Sniper):
            strategy = cls(self.cfg, self.trader)
            for mint in ('token-a', 'token-b', 'token-c', 'token-d'):
                await deliver(strategy, event('buy' if cls is Copytrader else 'create', mint=mint))
            self.assertEqual(len(strategy.positions), 4)

    async def test_proportional_exits_avoid_original_rounding_drift(self):
        strategy = Copytrader(self.cfg, self.trader)
        await deliver(strategy, event('buy', tokenAmount=100))
        initial = strategy.positions[MINT]['tokens']
        await deliver(strategy, event('sell', tokenAmount=1.1))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'] / initial, 0.989)
        await deliver(strategy, event('sell', tokenAmount=28.9))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'] / initial, 0.7)
        await deliver(strategy, event('sell', tokenAmount=20))
        self.assertAlmostEqual(strategy.positions[MINT]['tokens'] / initial, 0.5)

    async def test_sniper_take_profit_and_buy_once(self):
        self.cfg['sniper']['token_mint'] = MINT
        strategy = Sniper(self.cfg, self.trader)
        await deliver(strategy, event('buy', pool='orca-whirlpool'))
        self.assertIn(MINT, strategy.positions)
        await deliver(strategy, event('sell', pool='orca-whirlpool', price=1.6))
        self.assertFalse(strategy.positions)
        await deliver(strategy, event('buy', pool='orca-whirlpool'))
        self.assertFalse(strategy.positions)

    async def test_sniper_stop_loss_and_idle(self):
        for exit_type in ('sl', 'idle'):
            now = [0]
            strategy = Sniper(self.cfg, self.trader, clock=lambda now=now: now[0])
            await deliver(strategy, event())
            if exit_type == 'sl':
                await deliver(strategy, event('sell', price=0.7))
            else:
                now[0] = 301
                await strategy.on_tick()
            self.assertFalse(strategy.positions)

    async def test_sniper_filters_malformed_events(self):
        strategy = Sniper(self.cfg, self.trader)
        for value in ({}, event(mayhemMode=True), event(price=0), event(quoteAmount=1)):
            await deliver(strategy, value)
        self.assertFalse(strategy.positions)

    def test_backtest_original_latency_slippage_and_window_end(self):
        strategy = Backtest(self.cfg)
        strategy.handle_event(event())
        strategy.handle_event(event('buy', timestamp=1200, price=1.3))
        strategy.handle_event(event('buy', timestamp=1500, price=1.0))
        self.assertEqual(strategy.missed, 1)
        self.assertFalse(strategy.positions)
        strategy = Backtest(self.cfg)
        strategy.handle_event(event())
        strategy.finish()
        self.assertEqual(len(strategy.trades), 1)  # Upstream forces the in-flight buy to fill.
        strategy = Backtest(self.cfg)
        strategy.handle_event(event())
        strategy.handle_event(event('buy', timestamp=1500, price=1.1))
        self.assertAlmostEqual(strategy.positions[MINT]['entry_price'], 1.0)
        strategy.handle_event(event('sell', timestamp=2000, price=1.7))
        strategy.handle_event(event('sell', timestamp=2500, price=1.8))
        self.assertEqual(len(strategy.trades), 1)
        self.assertGreater(strategy.trades[0]['pnl'], 0)

    def test_local_plain_and_compressed_replay(self):
        raw = (
            b'\n'.join(
                json.dumps(e).encode()
                for e in [event(), event('buy', timestamp=1500), event('sell', timestamp=2000, price=2)]
            )
            + b'\nnot-json\n'
        )
        with tempfile.TemporaryDirectory() as folder:
            for extension, data in [('jsonl', raw), ('jsonl.zst', zstandard.ZstdCompressor().compress(raw))]:
                file = Path(folder) / ('events.' + extension)
                file.write_bytes(data)
                strategy = Backtest(self.cfg)
                replay_file(file, strategy)
                strategy.finish()
                self.assertEqual(len(strategy.trades), 1)
                self.assertEqual(strategy.skipped, 1)

    def test_sell_selection_preserves_quote_and_wsol(self):
        holdings = {MINT: {'balance': 10}, WSOL: {'balance': 20}, USDC: {'balance': 30}, OTHER: {'balance': 0}}
        self.assertEqual(list(select_balances(self.cfg, holdings, True)), [MINT])
        self.cfg['sell']['token_mints'] = [MINT]
        self.assertEqual(list(select_balances(self.cfg, holdings, False)), [MINT])


class StreamIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_other_token_executes_while_first_buy_waits(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg['copytrader']['wallets'] = [WALLET]
        first_started, second_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class WaitingTrader(Trader):
            async def order(self, action, mint, quote, amount, price, tokens=0):
                if mint == MINT:
                    first_started.set()
                    await release.wait()
                else:
                    second_started.set()
                return await super().order(action, mint, quote, amount, price, tokens)

        async def stream(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            await socket.send_json(event('buy', signature='first'))
            await socket.send_json(event('buy', mint=OTHER, signature='second'))
            async for _ in socket:
                pass
            return socket

        app = web.Application()
        app.router.add_get('/', stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        cfg['network']['stream_url'] = f'ws://127.0.0.1:{port}/'
        trader = WaitingTrader(cfg, object())
        strategy = Copytrader(cfg, trader)
        task = asyncio.create_task(run_stream(cfg, trader, strategy.on_event, strategy.on_tick, strategy.accept_event))
        try:
            await asyncio.wait_for(first_started.wait(), 2)
            await asyncio.wait_for(second_started.wait(), 2)
            self.assertFalse(release.is_set())
            self.assertIn(OTHER, strategy.positions)
            self.assertNotIn(MINT, strategy.positions)
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runner.cleanup()

    async def test_copy_fill_received_while_strategy_waits(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg['copytrader']['wallets'] = [WALLET]
        cfg['trade']['confirmation_seconds'] = 1
        sockets, orders = [], []
        completed = asyncio.Event()

        async def stream(request):
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            sockets.append(socket)
            await socket.send_json(event('buy', tokenAmount=100))
            async for _ in socket:
                pass
            return socket

        async def api(request):
            body = await request.json()
            orders.append(body)
            await sockets[0].send_json(
                event('buy', signature='own-fill', txSigner=OTHER, tokenAmount=0.01, quoteAmount=0.01)
            )
            return web.json_response({'signature': 'own-fill'})

        app = web.Application()
        app.router.add_get('/stream', stream)
        app.router.add_post('/', api)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        cfg['network']['trade_url'] = f'http://127.0.0.1:{port}/'
        cfg['network']['stream_url'] = f'ws://127.0.0.1:{port}/stream'
        try:
            with patch.dict(os.environ, {'ZEROSLIP_API_KEY': 'mock-key'}, clear=True):
                async with aiohttp.ClientSession() as session:
                    trader = Trader(cfg, session, live=True)
                    strategy = Copytrader(cfg, trader)

                    async def on_tick(mint=None):
                        if MINT in strategy.positions:
                            completed.set()
                        return mint in strategy.positions

                    task = asyncio.create_task(
                        run_stream(cfg, trader, strategy.on_event, on_tick, strategy.accept_event)
                    )
                    try:
                        await asyncio.wait_for(completed.wait(), timeout=3)
                        self.assertEqual(len(orders), 1)
                        self.assertEqual(strategy.positions[MINT]['tokens'], 0.01)
                    finally:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
        finally:
            await runner.cleanup()


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cfg = copy.deepcopy(DEFAULTS)
        self.cfg['trade']['confirmation_seconds'] = 0.1
        self.payloads = []
        self.app = web.Application()
        self.app.router.add_post('/', self.handler)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.cfg['network']['trade_url'] = f'http://127.0.0.1:{port}/'
        self.cfg['network']['rpc_url'] = self.cfg['network']['trade_url']
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2))
        self.trader = Trader(self.cfg, self.session, live=True)
        self.response = None
        self.confirm = True
        self.env = patch.dict(os.environ, {'ZEROSLIP_API_KEY': 'mock-api-key'}, clear=True)
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        await self.session.close()
        await self.runner.cleanup()

    async def handler(self, request):
        body = await request.json()
        self.payloads.append(body)
        if self.response is not None:
            return web.json_response(self.response)
        if body.get('action') == 'getBalances':
            return web.json_response({'tokenBalances': {MINT: {'balance': 10}}})
        if body.get('method') == 'getTokenAccountsByOwner':
            return web.json_response({'result': {'value': []}})
        if self.confirm:
            self.trader.observe(event(body['action'], signature='mock-signature', tokenAmount=1))
        return web.json_response({'signature': 'mock-signature', 'err': ''})

    async def test_live_request_and_early_stream_confirmation(self):
        data = await self.trader.order('buy', MINT, WSOL, 0.01, 1)
        self.assertEqual(data['tokenAmount'], 1)
        body = self.payloads[0]
        self.assertEqual(body['apiKey'], 'mock-api-key')
        self.assertNotIn('privateKey', body)
        self.assertEqual(body['denominatedInQuote'], 'true')
        self.assertNotIn('guaranteedDelivery', body)

    async def test_timeout_never_retries(self):
        self.confirm = False
        with self.assertRaises(StopBot):
            await self.trader.order('buy', MINT, WSOL, 0.01, 1)
        self.assertEqual(len(self.payloads), 1)

    async def test_rejected_and_missing_signature_stop(self):
        for response in ({'err': 'rejected'}, {}):
            self.response = response
            with self.assertRaises(StopBot):
                await self.trader.order('sell', MINT, WSOL, '100%', 1)

    async def test_sell_success_and_unconfirmed_failure(self):
        self.cfg['sell']['token_mints'] = [MINT]
        with patch('sell_all_tokens.api_post') as post:
            post.side_effect = [
                {'tokenBalances': {MINT: {'balance': 10}}},
                {'signature': 'sell-signature', 'confirmed': True, 'trades': [{'quoteMint': WSOL}]},
            ]
            await sell_selected(self.cfg, self.session, True, False)
            self.assertEqual(post.call_args_list[1].args[2]['guaranteedDelivery'], 'true')
            self.assertEqual(post.call_args_list[1].args[2]['amount'], '100%')
            self.assertEqual(post.call_args_list[1].args[2]['slippage'], 100)
            post.side_effect = [{'tokenBalances': {MINT: {'balance': 10}}}, {'confirmed': False}]
            with self.assertRaises(StopBot):
                await sell_selected(self.cfg, self.session, True, False)

    async def test_rpc_error_keeps_server_details_without_order_warning(self):
        self.cfg['wallet']['public_key'] = WALLET
        self.response = {'error': {'code': -32603, 'message': 'INTERNAL_ERROR'}}
        with self.assertRaisesRegex(StopBot, 'RPC balance query failed:.*INTERNAL_ERROR') as caught:
            await sell_selected(self.cfg, self.session, False, True)
        self.assertNotIn('order', str(caught.exception))

    async def test_preview_only_makes_read_only_rpc_calls(self):
        self.cfg['wallet']['public_key'] = WALLET
        self.cfg['sell']['token_mints'] = [MINT]
        await sell_selected(self.cfg, self.session, False, False)
        self.assertEqual(len(self.payloads), 2)
        self.assertTrue(all(p.get('method') == 'getTokenAccountsByOwner' and 'apiKey' not in p for p in self.payloads))


if __name__ == '__main__':
    unittest.main()
