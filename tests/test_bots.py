"""Regression and standalone-process tests. All network traffic uses localhost.

Run: python3 -m unittest discover -s tests -v
Uses only the packages needed by the bots; no test framework is required.
"""

import ast
import asyncio
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web
import zstandard

ROOT = Path(__file__).resolve().parents[1]
MINT = 'GchFXs6VQHxBsmqi5mu4pGJXh99vRx7nPqTrCTSJpump'
WALLET = '7npdjXpg9bpEpK5JdJh3BZtJV1QqoxNieurNGqxk4NwS'
OTHER = 'DmHRTxVzdSkv7t5UDJArFvoqKsfLTEh6Awne59uEvbAi'
WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
STREAMING = ('live_sniper_bot', 'copytrader_bot')
FILES = (*STREAMING, 'backtest_sniper_strategy', 'sell_all_tokens')


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def market(action='buy', **changes):
    event = dict(action=action, mint=MINT, quoteMint=WSOL, price=1.0,
                 quoteAmount=21.0, tokenAmount=100.0, pool='pump',
                 poolId='fixture', poolFeeRate=0.0125, decimals=6,
                 timestamp=1000, txSigner=WALLET, tradersInvolved={WALLET: {}},
                 postBalances={WALLET: {MINT: 100.0}}, signature='market-' + action)
    event.update(changes)
    return event


def settings(source, **values):
    """Edit copies as a user would, without adding arguments or shared files."""
    lines = source.splitlines(keepends=True)
    for node in reversed(ast.parse(source).body):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            name = getattr(node.targets[0], 'id', '')
            if name in values:
                lines[node.lineno - 1:node.end_lineno] = [f'{name} = {values.pop(name)!r}\n']
    if values:
        raise AssertionError('Unknown settings: ' + ', '.join(values))
    return ''.join(lines)


async def eventually(condition, seconds=6):
    deadline = asyncio.get_running_loop().time() + seconds
    while not condition():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError('Timed out waiting for bot behavior')
        await asyncio.sleep(0.02)


class SettingsTests(unittest.TestCase):
    def test_all_files_parse_as_python39_and_default_to_paper(self):
        for name in FILES:
            with self.subTest(name=name):
                ast.parse((ROOT / (name + '.py')).read_text(), feature_version=(3, 9))
                module = load(name)
                self.assertFalse(getattr(module, 'LIVE', False))

    def test_valid_and_invalid_settings(self):
        for name in FILES:
            with self.subTest(name=name):
                m = load(name)
                if name == 'copytrader_bot':
                    with self.assertRaisesRegex(m.StopBot, 'WALLETS'):
                        m.check_settings()
                    m.WALLETS = [WALLET]
                elif name == 'sell_all_tokens':
                    with self.assertRaisesRegex(m.StopBot, 'WALLET'):
                        m.check_settings()
                    m.WALLET = WALLET
                m.check_settings()
                setting = 'BUY_AMOUNT' if hasattr(m, 'BUY_AMOUNT') else 'PRIORITY_FEE'
                setattr(m, setting, -1)
                with self.assertRaises(m.StopBot):
                    m.check_settings()

    def test_keys_are_required_only_for_live_and_api_key_takes_precedence(self):
        for name in (*STREAMING, 'sell_all_tokens'):
            m = load(name)
            with self.subTest(name=name):
                with self.assertRaises(m.StopBot):
                    m.credentials()
                m.PRIVATE_KEY = ' fake-private-key '
                self.assertEqual(m.credentials(), {'privateKey': 'fake-private-key'})
                m.API_KEY = ' fake-api-key '
                self.assertEqual(m.credentials(), {'apiKey': 'fake-api-key'})

    def test_addresses_and_own_wallet_are_checked(self):
        for name in (*STREAMING, 'sell_all_tokens'):
            m = load(name)
            m.check_address(WALLET, 'wallet')
            for value in ('', '0' * 44, 'bad address', None):
                with self.subTest(name=name, value=value), self.assertRaises(m.StopBot):
                    m.check_address(value, 'wallet')
        m = load('copytrader_bot')
        m.WALLETS = [WALLET]
        m.MY_WALLET = WALLET
        with self.assertRaisesRegex(m.StopBot, 'own wallet'):
            m.check_settings()

    def test_nonfinite_and_boolean_number_settings_fail_before_network_access(self):
        for name in FILES:
            m = load(name)
            if name == 'copytrader_bot':
                m.WALLETS = [WALLET]
            if name == 'sell_all_tokens':
                m.WALLET = WALLET
            key = 'PRIORITY_FEE' if hasattr(m, 'PRIORITY_FEE') else 'MIN_INITIAL_BUY'
            for value in (float('nan'), float('inf'), float('-inf'), True):
                with self.subTest(name=name, value=value):
                    setattr(m, key, value)
                    with self.assertRaises(m.StopBot):
                        m.check_settings()


class PaperTests(unittest.IsolatedAsyncioTestCase):
    async def test_buy_partial_sell_full_sell_and_cost_basis(self):
        for name in STREAMING:
            with self.subTest(name=name):
                m = load(name)
                t = m.Trader(object(), False)  # A network request here would fail.
                fill = await t.order('buy', MINT, WSOL, 10, 2)
                self.assertEqual(fill['tokenAmount'], 5)
                await t.order('sell', MINT, WSOL, 1.25, 3, 5)
                self.assertAlmostEqual(t.pnl_sol, 1.25)
                self.assertAlmostEqual(t.paper[MINT]['cost'], 7.5)
                await t.order('sell', MINT, WSOL, '100%', 1, 3.75)
                self.assertAlmostEqual(t.pnl_sol, -2.5)
                self.assertFalse(t.paper)
                self.assertEqual((t.buys, t.sells), (1, 2))

    async def test_usdc_sizing_conversion_and_pnl(self):
        for name in STREAMING:
            m = load(name)
            t = m.Trader(object(), False)
            self.assertEqual(m.sol_to_quote(USDC, 1), 80)
            self.assertEqual(m.convert_price(88, USDC, WSOL), 1.1)
            await t.order('buy', MINT, USDC, 80, 2)
            await t.order('sell', MINT, USDC, '100%', 3, 40)
            self.assertEqual(t.pnl[USDC], 40)
            self.assertEqual(t.pnl_sol, 0.5)
            self.assertTrue(m.update_sol_price(market(poolId=m.SOL_USDC_POOL, price=0.01)))
            self.assertEqual(m.sol_price, 100)


class SniperTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.m = load('live_sniper_bot')
        self.trader = self.m.Trader(object(), False)
        self.now = 0
        self.s = self.m.Sniper(self.trader, lambda: self.now)

    async def fill(self):
        await self.s.on_event(market('create'))
        self.assertFalse(self.s.positions)
        await self.s.on_event(market(price=1.1))
        self.assertAlmostEqual(self.s.positions[MINT]['entry'], 1.1)
        self.assertEqual(self.trader.sells, 0)

    async def test_take_profit(self):
        await self.fill()
        await self.s.on_event(market('sell', price=1.8))
        self.assertFalse(self.s.positions)
        self.assertEqual(self.trader.sells, 1)
        self.assertAlmostEqual(self.trader.pnl_sol, 0.001 * (1.8 / 1.1 - 1))

    async def test_stop_loss(self):
        await self.fill()
        await self.s.on_event(market(price=0.8))
        self.assertFalse(self.s.positions)
        self.assertLess(self.trader.pnl_sol, 0)

    async def test_idle_exit(self):
        await self.fill()
        self.now = 301
        self.assertFalse(await self.s.on_tick(MINT))
        self.assertEqual(self.trader.sells, 1)

    async def test_slippage_miss_and_pending_timeout(self):
        await self.s.on_event(market('create'))
        await self.s.on_event(market(price=8))
        self.assertFalse(self.s.positions)
        self.assertEqual(self.trader.missed, 1)
        await self.s.on_event(market('create'))
        self.now = 301
        await self.s.on_tick(MINT)
        self.assertEqual(self.trader.missed, 2)
        self.assertFalse(self.s.pending)

    async def test_target_token_buys_once(self):
        self.m.TOKEN_MINT = MINT
        await self.s.on_event(market())
        await self.s.on_event(market())
        await self.s.on_event(market(price=2))
        await self.s.on_event(market())
        self.assertEqual(self.trader.buys, 1)
        self.assertFalse(self.s.positions)

    async def test_filters_and_cross_quote_fill(self):
        for changes in ({'mayhemMode': True}, {'quoteAmount': 20}, {'quoteMint': OTHER}, {'price': 0}):
            await self.s.on_event(market('create', **changes))
            self.assertFalse(self.s.pending)
        await self.s.on_event(market('create'))
        await self.s.on_event(market(quoteMint=USDC, price=88))
        self.assertAlmostEqual(self.s.positions[MINT]['entry'], 1.1)
        await self.s.on_event(market(pool='pump-amm', poolCreatedBy='pump', price=1.8))
        self.assertEqual(self.trader.sells, 1)


class CopyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.m = load('copytrader_bot')
        self.m.WALLETS = [WALLET, OTHER]
        self.t = self.m.Trader(object(), False)
        self.s = self.m.Copytrader(self.t)

    async def test_capped_buy_and_proportional_sells(self):
        await self.s.on_event(market())
        self.assertAlmostEqual(self.s.positions[MINT]['tokens'], 0.01)
        await self.s.on_event(market('sell', tokenAmount=25, price=2))
        self.assertAlmostEqual(self.s.positions[MINT]['tokens'], 0.0075)
        self.assertEqual(self.s.positions[MINT]['copied_remaining'], 75)
        await self.s.on_event(market('sell', tokenAmount=75, price=2))
        self.assertFalse(self.s.positions)
        self.assertAlmostEqual(self.t.pnl_sol, 0.01)

    async def test_only_opening_wallet_can_sell_and_topups_update_denominator(self):
        await self.s.on_event(market())
        await self.s.on_event(market('sell', txSigner=OTHER, tradersInvolved={OTHER: {}}, tokenAmount=100))
        self.assertEqual(self.t.sells, 0)
        await self.s.on_event(market())
        self.assertEqual(self.t.buys, 1)
        self.assertEqual(self.s.positions[MINT]['copied_remaining'], 200)
        await self.s.on_event(market('sell', tokenAmount=100))
        self.assertAlmostEqual(self.s.positions[MINT]['tokens'], 0.005)

    async def test_nonfirst_buy_and_unsupported_pools_are_ignored(self):
        for changes in ({'postBalances': {WALLET: {MINT: 200}}}, {'pool': 'raydium'},
                        {'mayhemMode': True}, {'quoteMint': OTHER}):
            await self.s.on_event(market(**changes))
        self.assertFalse(self.s.positions)
        self.m.TOKEN_MINTS = [OTHER]
        await self.s.on_event(market())
        self.assertFalse(self.s.positions)

    async def test_bundled_sell_requires_wallet_quantity(self):
        await self.s.on_event(market())
        bundle = dict(tradersInvolved={WALLET: {}, OTHER: {}}, tokenAmount=60)
        await self.s.on_event(market('sell', **bundle))
        self.assertEqual(self.t.sells, 0)
        await self.s.on_event(market('sell', breakdown=[
            {'trader': WALLET, 'action': 'sell', 'tokenAmount': 25},
            {'trader': OTHER, 'action': 'sell', 'tokenAmount': 35}], **bundle))
        self.assertAlmostEqual(self.s.positions[MINT]['tokens'], 0.0075)

    async def test_live_partial_sell_rounding_dust_and_missing_decimals(self):
        for decimals, tokens, expected in ((0, 49.3827156, 12.0), (6, 49.3827156, 12.345678),
                                            (8, 49.3827156, 12.3456789), (0, 0.01, None), (None, 1, 'error')):
            with self.subTest(decimals=decimals, tokens=tokens):
                t = self.m.Trader(object(), True)
                orders = []
                async def order(action, mint, quote, amount, price, held=0):
                    orders.append(amount)
                    return {'price': price, 'tokenAmount': held if amount == '100%' else amount}
                t.order = order
                s = self.m.Copytrader(t)
                s.positions[MINT] = dict(wallet=WALLET, quote=WSOL, tokens=tokens,
                                         copied_remaining=100, decimals=decimals)
                if expected == 'error':
                    with self.assertRaises(self.m.StopBot):
                        await s.on_event(market('sell', tokenAmount=25))
                    self.assertFalse(orders)
                    continue
                await s.on_event(market('sell', tokenAmount=25))
                self.assertEqual(orders, [] if expected is None else [expected])
                self.assertEqual(s.positions[MINT]['copied_remaining'], 75)
                await s.on_event(market('sell', tokenAmount=75))
                self.assertEqual(orders[-1], '100%')
                self.assertFalse(s.positions)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.m = load('backtest_sniper_strategy')

    def replay(self, events):
        b = self.m.Backtest()
        b.read([json.dumps(e).encode() for e in events])
        b.finish()
        return b

    def test_take_profit_and_stop_loss_with_latency_and_fees(self):
        for price, reason in ((2, 'tp'), (0.5, 'sl')):
            with self.subTest(reason=reason):
                b = self.replay([market('create'), market(timestamp=1400),
                                 market(price=price, timestamp=1500), market(price=price, timestamp=1900)])
                self.assertEqual(b.buys, 1)
                self.assertEqual(b.trades[0]['reason'], reason)
                fees = 0.003 + 0.0125
                expected = 0.001 * (1 - fees) * price * (1 - fees) - 0.001
                self.assertAlmostEqual(b.trades[0]['pnl_sol'], expected)

    def test_slippage_miss_explains_empty_result(self):
        with self.assertLogs(level='INFO') as logs:
            b = self.replay([market('create'), market(price=8, timestamp=1100), market(timestamp=1400)])
        self.assertEqual((b.buys, b.missed), (0, 1))
        self.assertIn('No trades filled', '\n'.join(logs.output))

    def test_pending_end_and_idle_exits(self):
        b = self.replay([market('create')])
        self.assertEqual(b.trades[0]['reason'], 'end')
        b = self.replay([market('create'), market(timestamp=1400),
                         market('transfer', timestamp=302000, mint=OTHER),
                         market('transfer', timestamp=303000, mint=OTHER)])
        self.assertEqual(b.trades[0]['reason'], 'idle')

    def test_invalid_lines_are_skipped_without_poisoning_position(self):
        b = self.m.Backtest()
        b.read([json.dumps(market('create'))])
        bad = ['not json', '[]', '{}'] + [json.dumps(market(price=p)) for p in (0, -1, 'bad', None, True)]
        b.read(bad)
        self.assertEqual(b.skipped, len(bad))
        self.assertEqual(b.positions[MINT]['cur_price'], 1)

    def test_plain_and_zstd_archives_match(self):
        raw = '\n'.join(json.dumps(e) for e in [market('create'), market(timestamp=1400), market(price=2, timestamp=1800)]).encode()
        with tempfile.TemporaryDirectory() as folder:
            results = []
            for suffix, content in (('.jsonl', raw), ('.jsonl.zst', zstandard.ZstdCompressor().compress(raw))):
                path = Path(folder) / ('hour' + suffix)
                path.write_bytes(content)
                b = self.m.Backtest()
                b.read_file(path)
                b.finish()
                results.append(b.trades)
        self.assertEqual(*results)

    def test_replay_also_works_without_optional_orjson(self):
        with patch.dict(sys.modules, {'orjson': None}):
            m = load('backtest_sniper_strategy')
        self.assertEqual(m.json.__name__, 'json')
        b = m.Backtest()
        b.read([json.dumps(market('create')).encode()])
        b.finish()
        self.assertEqual(b.buys, 1)

    def test_cross_quote_prices_do_not_create_a_false_profit_or_loss(self):
        for entry_quote, entry_price, update_quote, update_price in (
                (WSOL, 1, USDC, 80), (USDC, 80, WSOL, 1)):
            with self.subTest(entry_quote=entry_quote):
                b = self.replay([market('create', quoteMint=entry_quote, price=entry_price, quoteAmount=2000),
                                 market(timestamp=1400, quoteMint=entry_quote, price=entry_price),
                                 market(timestamp=1500, quoteMint=update_quote, price=update_price)])
                self.assertEqual(b.trades[0]['reason'], 'end')
                self.assertAlmostEqual(b.trades[0]['pct'], 0)
                self.assertLess(b.trades[0]['pnl_sol'], 0)  # Only the modeled fees.

    def test_unsupported_quote_cannot_move_a_held_price(self):
        b = self.replay([market('create'), market(timestamp=1400),
                         market(timestamp=1500, quoteMint=OTHER, price=9000)])
        self.assertEqual(b.trades[0]['reason'], 'end')
        self.assertEqual(b.trades[0]['pct'], 0)


class LocalServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests, self.sockets, self.processes = [], [], []
        self.confirm = True
        self.unconfirmed_mints = set()
        self.status = 200
        self.response = None
        self.response_delay = 0
        self.holdings = {MINT: {'balance': 10}, WSOL: {'balance': 2}, USDC: {'balance': 20},
                         OTHER: {'balance': 5, 'frozen': True}}
        self.quantity = 0
        self.price = 1
        self.archive_status = 200
        self.archive_data = zstandard.ZstdCompressor().compress(b'')
        app = web.Application()
        app.router.add_post('/api', self.api)
        app.router.add_post('/rpc', self.rpc)
        app.router.add_get('/stream', self.stream)
        app.router.add_get('/replay/{path:.*}', self.archive)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        self.url = f'http://127.0.0.1:{self.port}'
        self.folder = tempfile.TemporaryDirectory(prefix='zeroslip-bot-tests-')

    async def asyncTearDown(self):
        for process in self.processes:
            if process.returncode is None:
                process.kill()
                await process.communicate()
        for socket in self.sockets:
            await socket.close()
        await self.runner.cleanup()
        self.folder.cleanup()

    async def stream(self, request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        self.sockets.append(socket)
        async for _ in socket:
            pass
        return socket

    async def broadcast(self, event):
        for socket in self.sockets:
            if not socket.closed:
                await socket.send_str(json.dumps(event))

    async def api(self, request):
        payload = await request.json()
        self.requests.append(payload)
        if self.response_delay:
            await asyncio.sleep(self.response_delay)
        if self.response is not None:
            return web.Response(text=self.response, status=self.status, content_type='application/json')
        if self.status != 200:
            return web.json_response({'error': 'fake-secret-must-not-be-logged'}, status=self.status)
        if payload['action'] == 'getBalances':
            return web.json_response({'tokenBalances': self.holdings})
        action, amount = payload['action'], payload['amount']
        quantity = float(amount) / self.price if action == 'buy' else (
            self.quantity if amount == '100%' else float(amount))
        self.quantity += quantity if action == 'buy' else -quantity
        signature = 'local-order-' + str(len(self.requests))
        if self.confirm and payload['mint'] not in self.unconfirmed_mints:
            await self.broadcast(market(action, signature=signature, tokenAmount=quantity,
                                        price=self.price, mint=payload['mint'],
                                        quoteMint=payload.get('quoteMint', WSOL),
                                        txSigner='11111111111111111111111111111111',
                                        tradersInvolved={'11111111111111111111111111111111': {}},
                                        postBalances={}))
        return web.json_response({'signature': signature, 'confirmed': self.confirm,
                                  'trades': [{'quoteMint': WSOL}]})

    async def rpc(self, request):
        payload = await request.json()
        self.requests.append(payload)
        accounts = []
        if payload['params'][1]['programId'].startswith('Tokenkeg'):
            for mint, info in self.holdings.items():
                parsed = {'mint': mint, 'state': 'frozen' if info.get('frozen') else 'initialized',
                          'tokenAmount': {'uiAmountString': str(info['balance'])}}
                accounts.append({'account': {'data': {'parsed': {'info': parsed}}}})
        return web.json_response({'result': {'value': accounts}})

    async def archive(self, request):
        return web.Response(body=self.archive_data, status=self.archive_status)

    async def start(self, name, **changes):
        folder = Path(self.folder.name) / str(len(self.processes))
        folder.mkdir()
        source = (ROOT / (name + '.py')).read_text()
        source = source.replace('https://api.zeroslip.ai', self.url + '/api')
        source = source.replace('wss://stream.zeroslip.ai/', self.url.replace('http:', 'ws:') + '/stream')
        source = source.replace('https://api.mainnet-beta.solana.com', self.url + '/rpc')
        source = source.replace('https://replay.pumpapi.io', self.url + '/replay')
        source = settings(source, **changes)
        (folder / (name + '.py')).write_text(source)
        self.assertEqual(len(list(folder.iterdir())), 1)
        process = await asyncio.create_subprocess_exec(sys.executable, name + '.py', cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        self.processes.append(process)
        return process

    async def output(self, process, interrupt=False):
        if interrupt:
            process.send_signal(signal.SIGINT)
        output, _ = await asyncio.wait_for(process.communicate(), 8)
        text = output.decode()
        self.assertNotIn('Traceback', text)
        return text

    async def test_standalone_sniper_paper_buy_sell_and_ctrl_c_report(self):
        p = await self.start('live_sniper_bot')
        await eventually(lambda: self.sockets)
        await self.broadcast(market('create'))
        await self.broadcast(market())
        await self.broadcast(market(price=2))
        await asyncio.sleep(0.15)
        output = await self.output(p, interrupt=True)
        self.assertIn('1 buys, 1 sells', output)
        self.assertIn('Paper PnL:', output)
        self.assertFalse(self.requests)

    async def test_standalone_sniper_live_buy_and_sell_confirmed_on_stream(self):
        p = await self.start('live_sniper_bot', LIVE=True, API_KEY='local-fake-key')
        await eventually(lambda: self.sockets)
        await self.broadcast(market('create'))
        await eventually(lambda: self.quantity > 0)
        self.price = 2
        await self.broadcast(market(price=2))
        await eventually(lambda: len(self.requests) == 2)
        await asyncio.sleep(0.15)
        output = await self.output(p, interrupt=True)
        self.assertIn('Confirmed buy', output)
        self.assertIn('Confirmed sell', output)
        self.assertEqual([x['action'] for x in self.requests], ['buy', 'sell'])
        self.assertEqual([x['denominatedInQuote'] for x in self.requests], ['true', 'false'])
        self.assertEqual(self.requests[0]['amount'], 0.001)
        self.assertEqual(self.requests[1]['amount'], '100%')
        self.assertEqual([x['slippage'] for x in self.requests], [20, 99])
        self.assertEqual(self.quantity, 0)
        self.assertNotIn('local-fake-key', output)

    async def test_standalone_copytrader_paper_buy_partial_and_full_sell(self):
        p = await self.start('copytrader_bot', WALLETS=[WALLET])
        await eventually(lambda: self.sockets)
        for e in (market(), market('sell', tokenAmount=25), market('sell', tokenAmount=75)):
            await self.broadcast(e)
        await asyncio.sleep(0.15)
        output = await self.output(p, interrupt=True)
        self.assertIn('1 buys, 2 sells', output)
        self.assertFalse(self.requests)

    async def test_standalone_copytrader_live_buy_partial_and_full_sell(self):
        p = await self.start('copytrader_bot', LIVE=True, API_KEY='local-fake-key', WALLETS=[WALLET])
        await eventually(lambda: self.sockets)
        await self.broadcast(market())
        await eventually(lambda: len(self.requests) == 1)
        await self.broadcast(market('sell', tokenAmount=25))
        await eventually(lambda: len(self.requests) == 2)
        await self.broadcast(market('sell', tokenAmount=75))
        await eventually(lambda: len(self.requests) == 3)
        await asyncio.sleep(0.15)
        output = await self.output(p, interrupt=True)
        self.assertEqual([x['action'] for x in self.requests], ['buy', 'sell', 'sell'])
        self.assertEqual([x['amount'] for x in self.requests], [0.01, 0.0025, '100%'])
        self.assertEqual(self.quantity, 0)
        self.assertEqual(output.count('Confirmed sell'), 2)

    async def test_standalone_unconfirmed_buy_stops_without_retry(self):
        self.confirm = False
        p = await self.start('live_sniper_bot', LIVE=True, API_KEY='local-fake-key', CONFIRM_SECONDS=0.2)
        await eventually(lambda: self.sockets)
        await self.broadcast(market('create'))
        output = await self.output(p)
        self.assertEqual(p.returncode, 1)
        self.assertIn('not retried', output)
        self.assertEqual(len(self.requests), 1)

    async def test_standalone_unconfirmed_exits_stop_without_retry(self):
        for name in STREAMING:
            with self.subTest(name=name):
                self.requests.clear()
                self.sockets.clear()
                self.quantity = 0
                self.confirm = True
                changes = dict(LIVE=True, API_KEY='local-fake-key', CONFIRM_SECONDS=0.2)
                if name == 'copytrader_bot':
                    changes['WALLETS'] = [WALLET]
                p = await self.start(name, **changes)
                await eventually(lambda: self.sockets)
                await self.broadcast(market('create' if name == 'live_sniper_bot' else 'buy'))
                await eventually(lambda: len(self.requests) == 1)
                self.confirm = False
                await self.broadcast(market('sell', price=2))
                output = await self.output(p)
                self.assertEqual(p.returncode, 1)
                self.assertIn('not retried', output)
                self.assertEqual([r['action'] for r in self.requests], ['buy', 'sell'])

    async def test_stream_reconnect_preserves_paper_position(self):
        p = await self.start('live_sniper_bot')
        await eventually(lambda: self.sockets)
        await self.broadcast(market('create'))
        await self.broadcast(market())
        await asyncio.sleep(0.1)
        await self.sockets[0].close()
        await eventually(lambda: len(self.sockets) == 2)
        await self.broadcast(market(price=2))
        await asyncio.sleep(0.1)
        output = await self.output(p, interrupt=True)
        self.assertIn('reconnecting', output)
        self.assertIn('1 buys, 1 sells', output)
        self.assertFalse(self.requests)

    async def test_slow_confirmation_for_one_token_does_not_block_other_tokens(self):
        self.unconfirmed_mints.add(MINT)
        p = await self.start('live_sniper_bot', LIVE=True, API_KEY='local-fake-key', CONFIRM_SECONDS=0.5)
        await eventually(lambda: self.sockets)
        await self.broadcast(market('create'))
        await self.broadcast(market('create', mint=OTHER))
        await eventually(lambda: len(self.requests) == 2)
        output = await self.output(p)
        self.assertIn('Confirmed buy mint=' + OTHER, output)
        self.assertIn('not retried', output)
        self.assertEqual(p.returncode, 1)
        self.assertEqual({r['mint'] for r in self.requests}, {MINT, OTHER})

    async def test_standalone_empty_copy_settings_fail_without_connecting(self):
        p = await self.start('copytrader_bot')
        output = await self.output(p)
        self.assertEqual(p.returncode, 1)
        self.assertIn('WALLETS', output)
        self.assertFalse(self.sockets)
        self.assertFalse(self.requests)

    async def test_standalone_sell_preview_calls_both_token_programs_and_never_trades(self):
        p = await self.start('sell_all_tokens', WALLET=WALLET)
        output = await self.output(p)
        self.assertEqual(p.returncode, 0)
        self.assertIn('PREVIEW SELL 100%', output)
        self.assertIn('Skipping frozen', output)
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(x['method'] == 'getTokenAccountsByOwner' for x in self.requests))
        self.assertNotIn('mint=' + WSOL, output)
        self.assertNotIn('mint=' + USDC, output)

    async def test_standalone_sell_all_uses_key_wallet_and_preserves_quotes_and_frozen(self):
        p = await self.start('sell_all_tokens', LIVE=True, API_KEY='local-fake-key')
        output = await self.output(p)
        self.assertEqual(p.returncode, 0)
        self.assertIn('Sold mint=' + MINT, output)
        self.assertEqual([x['action'] for x in self.requests], ['getBalances', 'sell'])
        self.assertEqual(self.requests[1]['amount'], '100%')
        self.assertEqual(self.requests[1]['guaranteedDelivery'], 'true')
        self.assertEqual(self.requests[1]['mint'], MINT)
        self.assertNotIn('local-fake-key', output)

    async def test_standalone_selected_sale_and_unconfirmed_sale_stops(self):
        self.confirm = False
        p = await self.start('sell_all_tokens', LIVE=True, API_KEY='local-fake-key',
                             SELL_ALL=False, TOKEN_MINTS=[MINT])
        output = await self.output(p)
        self.assertEqual(p.returncode, 1)
        self.assertIn('Sale not confirmed', output)
        self.assertEqual(len(self.requests), 2)

    async def test_standalone_backtester_downloads_and_replays(self):
        raw = '\n'.join(json.dumps(e) for e in (market('create'), market(timestamp=1400),
                         market(price=2, timestamp=1500), market(price=2, timestamp=1900))).encode()
        self.archive_data = zstandard.ZstdCompressor().compress(raw)
        p = await self.start('backtest_sniper_strategy')
        output = await self.output(p)
        self.assertEqual(p.returncode, 0)
        self.assertIn('buys=1 closed=1', output)
        self.assertIn('Exits: tp=1', output)
        self.assertFalse(self.requests)

    async def test_standalone_backtester_reads_existing_file(self):
        path = Path(self.folder.name) / 'recorded.jsonl'
        path.write_text(json.dumps(market('create')) + '\n')
        p = await self.start('backtest_sniper_strategy', REPLAY_FILE=str(path))
        output = await self.output(p)
        self.assertEqual(p.returncode, 0)
        self.assertIn('buys=1 closed=1', output)

    async def test_api_errors_missing_signature_and_timeout_never_retry_or_leak_secrets(self):
        for name in STREAMING:
            for status, response in ((500, None), (302, None), (200, '{bad'), (200, '[]'),
                                     (200, '{"error":"fake-secret-must-not-be-logged"}'), (200, '{}')):
                with self.subTest(name=name, status=status, response=response):
                    self.status, self.response = status, response
                    m = load(name)
                    m.TRADE_URL, m.API_KEY = self.url + '/api', 'local-fake-key'
                    m.CONFIRM_SECONDS = 0.01
                    count = len(self.requests)
                    async with aiohttp.ClientSession() as session:
                        with self.assertRaises(m.StopBot) as caught:
                            await m.Trader(session, True).order('buy', MINT, WSOL, 1, 1)
                    self.assertEqual(len(self.requests), count + 1)
                    self.assertNotIn('fake-secret', str(caught.exception))
                    self.assertNotIn('local-fake-key', str(caught.exception))

    async def test_api_network_timeout_is_not_retried(self):
        self.response_delay = 0.1
        for name in (*STREAMING, 'sell_all_tokens'):
            m = load(name)
            m.TRADE_URL, m.API_KEY = self.url + '/api', 'local-fake-key'
            count = len(self.requests)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.02)) as session:
                with self.assertRaisesRegex(m.StopBot, 'No API response'):
                    payload = {'action': 'getBalances', 'apiKey': 'local-fake-key'}
                    if name == 'sell_all_tokens':
                        await m.post(session, m.TRADE_URL, payload)
                    else:
                        await m.post(session, payload)
            self.assertEqual(len(self.requests), count + 1)

    async def test_wrong_stream_confirmation_is_not_accepted(self):
        for name in STREAMING:
            for changes in ({'action': 'sell'}, {'mint': OTHER}, {'price': 0}, {'tokenAmount': 0}):
                m = load(name)
                m.TRADE_URL, m.API_KEY, m.CONFIRM_SECONDS = self.url + '/api', 'local-fake-key', 0.01
                async with aiohttp.ClientSession() as session:
                    trader = m.Trader(session, True)
                    signature = 'local-order-' + str(len(self.requests) + 1)
                    trader.remember(market(signature=signature, **changes))
                    with self.assertRaisesRegex(m.StopBot, 'not seen on the stream'):
                        await trader.order('buy', MINT, WSOL, 1, 1)

    async def test_usdc_order_payload_and_private_key_are_preserved(self):
        for name in STREAMING:
            m = load(name)
            m.TRADE_URL, m.PRIVATE_KEY = self.url + '/api', 'local-fake-private-key'
            async with aiohttp.ClientSession() as session:
                trader = m.Trader(session, True)
                signature = 'local-order-' + str(len(self.requests) + 1)
                trader.remember(market(signature=signature, quoteMint=USDC, price=80, tokenAmount=0.01))
                fill = await trader.order('buy', MINT, USDC, 0.8, 80)
                self.assertEqual(fill['tokenAmount'], 0.01)
            self.assertEqual(self.requests[-1]['quoteMint'], USDC)
            self.assertEqual(self.requests[-1]['amount'], 0.8)
            self.assertEqual(self.requests[-1]['privateKey'], 'local-fake-private-key')
            self.assertNotIn('apiKey', self.requests[-1])

    async def test_missing_archive_is_explicit_or_skipped_when_requested(self):
        m = load('backtest_sniper_strategy')
        m.REPLAY_URL = self.url + '/replay'
        self.archive_status = 404
        path = Path(self.folder.name) / 'hour.zst'
        async with aiohttp.ClientSession() as session:
            with self.assertRaisesRegex(m.StopBot, 'Archive hour not found'):
                await m.download_hour(session, datetime.now(timezone.utc), path)
            m.ALLOW_GAPS = True
            self.assertFalse(await m.download_hour(session, datetime.now(timezone.utc), path))


if __name__ == '__main__':
    unittest.main()
