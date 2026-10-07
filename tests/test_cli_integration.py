"""Run the documented entry points against local TLS stream/API/RPC fixtures.

No production credentials or endpoints are used. The only additional test
dependency is openssl, available on GitHub's Ubuntu runners.
"""
import asyncio
import json
import os
from pathlib import Path
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web
import aiohttp
import zstandard

ROOT = Path(__file__).resolve().parent.parent
WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
MINT = 'GchFXs6VQHxBsmqi5mu4pGJXh99vRx7nPqTrCTSJpump'
WALLET = '7npdjXpg9bpEpK5JdJh3BZtJV1QqoxNieurNGqxk4NwS'
OTHER = 'DmHRTxVzdSkv7t5UDJArFvoqKsfLTEh6Awne59uEvbAi'


def market(action='buy', **changes):
    e = dict(action=action, mint=MINT, quoteMint=WSOL, price=1.,
             quoteAmount=21., tokenAmount=100., pool='pump', poolId='fixture',
             poolFeeRate=.0125, timestamp=1000, txSigner=WALLET,
             tradersInvolved={WALLET: {}}, postBalances={WALLET: {MINT: 100.}},
             signature=f'fixture-{action}')
    e.update(changes)
    return e


@unittest.skipUnless(shutil.which('openssl'), 'TLS CLI fixtures need openssl')
class CliIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix='zeroslip-cli-')
        self.path = Path(self.folder.name)
        self.cert, self.key = self.path/'cert.pem', self.path/'key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(self.key), '-out', str(self.cert), '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.cert, self.key)
        self.events, self.requests, self.sockets, self.processes = [], [], [], []
        self.confirm_orders = True
        self.trade_tokens = 0.
        self.archive_status = 200
        self.rpc_accounts = {}
        self.holdings = {MINT: {'balance': 10.}}
        self.app = web.Application()
        self.app.router.add_get('/stream', self.stream)
        self.app.router.add_post('/api', self.api)
        self.app.router.add_post('/rpc', self.rpc)
        self.app.router.add_get('/replay/{path:.*}', self.archive)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0, ssl_context=ctx)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        config = (ROOT/'config.example.toml').read_text()
        for old, new in [('https://api.zeroslip.ai', f'https://127.0.0.1:{port}/api'),
                         ('wss://stream.zeroslip.ai/', f'wss://127.0.0.1:{port}/stream'),
                         ('https://replay.pumpapi.io', f'https://127.0.0.1:{port}/replay'),
                         ('https://api.mainnet-beta.solana.com', f'https://127.0.0.1:{port}/rpc')]:
            config = config.replace(old, new)
        config = config.replace('public_key = ""', f'public_key = "{OTHER}"')
        config = config.replace('idle_seconds = 300', 'idle_seconds = 0.1')
        config = config.replace('confirmation_seconds = 3', 'confirmation_seconds = 0.3')
        self.config = self.path/'config.toml'
        self.config.write_text(config)
        (self.path/'.env').write_text('ZEROSLIP_API_KEY=local-fixture-key\n')
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('ZEROSLIP_')}
        self.env['SSL_CERT_FILE'] = str(self.cert)

    async def asyncTearDown(self):
        for p in self.processes:
            if p.returncode is None:
                p.kill()
                await p.communicate()
        for socket in self.sockets:
            await socket.close()
        await self.runner.cleanup()
        self.folder.cleanup()

    async def stream(self, request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        self.sockets.append(socket)
        for event in self.events:
            await socket.send_json(event)
            await asyncio.sleep(.03)
        async for _ in socket:
            pass
        return socket

    async def api(self, request):
        body = await request.json()
        self.requests.append(body)
        if body['action'] == 'getBalances':
            return web.json_response({'tokenBalances': self.holdings})
        signature = f'order-{len(self.requests)}'
        if body.get('guaranteedDelivery'):
            return web.json_response({'confirmed': self.confirm_orders, 'signature': signature})
        if self.confirm_orders:
            if body['action'] == 'buy':
                quantity = float(body['amount'])
                self.trade_tokens += quantity
            else:
                quantity = self.trade_tokens * float(body['amount'].rstrip('%')) / 100
                self.trade_tokens -= quantity
            for socket in self.sockets:
                await socket.send_json(market(body['action'], signature=signature,
                                              txSigner=OTHER, tradersInvolved={OTHER: {}},
                                              tokenAmount=quantity, quoteAmount=quantity))
        return web.json_response({'signature': signature})

    async def archive(self, request):
        self.requests.append({'archive': request.path})
        if self.archive_status != 200:
            return web.Response(status=self.archive_status)
        data = ''.join(json.dumps(e)+'\n' for e in self.events).encode()
        return web.Response(body=zstandard.ZstdCompressor().compress(data))

    async def rpc(self, request):
        body = await request.json()
        self.requests.append(body)
        return web.json_response({'result': {'value': self.rpc_accounts.get(body['params'][1]['programId'], [])}})

    async def start(self, filename, *args):
        # A different cwd exercises config-relative .env loading and imports.
        p = await asyncio.create_subprocess_exec(sys.executable, str(ROOT/'scripts'/filename),
                                                 '--config', str(self.config), *args,
                                                 cwd=self.path, env=self.env,
                                                 stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.STDOUT)
        self.processes.append(p)
        return p

    async def wait_line(self, p, target):
        lines = []
        async with asyncio.timeout(8):
            while True:
                line = (await p.stdout.readline()).decode()
                if not line:
                    self.fail(f'CLI stopped before {target!r}: {lines}')
                lines.append(line)
                if target in line:
                    return ''.join(lines)

    async def stop(self, p):
        p.send_signal(signal.SIGINT)
        output, _ = await asyncio.wait_for(p.communicate(), 3)
        self.assertEqual(p.returncode, 0, output.decode())
        return output.decode()

    async def test_sniper_selected_token_paper_take_profit_and_buy_once(self):
        self.events = [market(pool='orca-whirlpool'),
                       market('sell', pool='orca-whirlpool', price=1.6),
                       market(pool='orca-whirlpool', signature='later-buy')]
        p = await self.start('live_sniper_bot.py', '--mint', MINT)
        output = await self.wait_line(p, '(take profit)')
        await asyncio.sleep(.15)
        output += await self.stop(p)
        self.assertEqual(output.count('PAPER BUY'), 1)
        self.assertEqual(output.count('PAPER SELL'), 1)
        self.assertFalse(self.requests)

    async def test_sniper_launch_filters_and_idle_exit(self):
        self.events = [market('create', mayhemMode=True),
                       market('create', quoteAmount=20, signature='too-small'),
                       market('create', signature='qualifying-launch')]
        p = await self.start('live_sniper_bot.py')
        output = await self.wait_line(p, '(idle timeout)')
        await self.stop(p)
        self.assertEqual(output.count('PAPER BUY'), 1)
        self.assertEqual(output.count('PAPER SELL'), 1)
        self.assertFalse(self.requests)

    async def test_sniper_live_mock_confirmation_and_stop_loss(self):
        self.events = [market(), market('sell', price=.7)]
        p = await self.start('live_sniper_bot.py', '--mint', MINT, '--live')
        await self.wait_line(p, '(stop loss)')
        await self.stop(p)
        self.assertEqual([r['action'] for r in self.requests], ['buy', 'sell'])
        self.assertEqual(self.requests[1]['amount'], '100%')
        self.assertEqual(self.requests[1]['slippage'], 99)
        self.assertEqual(self.requests[0]['apiKey'], 'local-fixture-key')

    async def test_copytrader_paper_additional_buy_and_proportional_exits(self):
        self.events = [market(), market(tokenAmount=100, signature='additional'),
                       market('sell', tokenAmount=50),
                       market('sell', tokenAmount=150, signature='remaining')]
        p = await self.start('copytrader_bot.py', '--wallet', WALLET)
        output = await self.wait_line(p, 'fraction=100.00%')
        await self.stop(p)
        self.assertEqual(output.count('PAPER BUY'), 1)
        self.assertIn('amount=25.00000000%', output)
        self.assertIn('amount=100%', output)
        self.assertFalse(self.requests)

    async def test_stream_order_timeout_stops_without_retry(self):
        self.events = [market()]
        self.confirm_orders = False
        p = await self.start('live_sniper_bot.py', '--mint', MINT, '--live')
        output, _ = await asyncio.wait_for(p.communicate(), 8)
        self.assertEqual(p.returncode, 1)
        self.assertIn('No trade event observed', output.decode())
        self.assertEqual(len(self.requests), 1)

    async def test_copytrader_live_mock_buy_and_two_partial_exit_orders(self):
        self.events = [market(), market('sell', tokenAmount=25),
                       market('sell', tokenAmount=75, signature='remaining')]
        p = await self.start('copytrader_bot.py', '--wallet', WALLET, '--live')
        await self.wait_line(p, 'fraction=100.00%')
        await self.stop(p)
        self.assertEqual([r['action'] for r in self.requests], ['buy', 'sell', 'sell'])
        self.assertEqual([r['amount'] for r in self.requests], [.01, '25.00000000%', '100%'])
        self.assertEqual(self.trade_tokens, 0)

    async def test_sell_all_preview_aggregates_accounts_and_skips_frozen(self):
        def account(mint, amount, state='initialized'):
            return {'account': {'data': {'parsed': {'info': {
                'mint': mint, 'state': state, 'tokenAmount': {'uiAmountString': str(amount)}}}}}}
        token_program = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        token_2022 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
        self.rpc_accounts = {token_program: [account(MINT, 3), account(MINT, 7), account(WSOL, 2)],
                             token_2022: [account(USDC, 4, 'frozen')]}
        p = await self.start('sell_all_tokens.py', '--all')
        output, _ = await asyncio.wait_for(p.communicate(), 8)
        self.assertEqual(p.returncode, 0, output.decode())
        self.assertIn(f'PREVIEW SELL 100% mint={MINT} balance=10.0', output.decode())
        self.assertIn('Skipping frozen balance', output.decode())
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(r.get('method') == 'getTokenAccountsByOwner' for r in self.requests))

    async def test_sell_live_mock_confirmed_and_uncertain_outcomes(self):
        for confirmed in (True, False):
            self.requests.clear()
            self.confirm_orders = confirmed
            p = await self.start('sell_all_tokens.py', '--mint', MINT, '--live')
            output, _ = await asyncio.wait_for(p.communicate(), 8)
            self.assertEqual(p.returncode, 0 if confirmed else 1, output.decode())
            self.assertEqual([r['action'] for r in self.requests], ['getBalances', 'sell'])
            self.assertNotIn('publicKey', self.requests[0])
            self.assertEqual(self.requests[1]['guaranteedDelivery'], 'true')
            self.assertEqual(self.requests[1]['slippage'], 100)

    async def test_backtest_cli_plain_and_compressed_match(self):
        events = [market('create'), market(timestamp=1400, price=1.6),
                  market('sell', timestamp=1800, price=1.7)]
        data = ''.join(json.dumps(e)+'\n' for e in events).encode()
        reports = []
        for name, body in [('events.jsonl', data), ('events.jsonl.zst', zstandard.ZstdCompressor().compress(data))]:
            file = self.path/name
            file.write_bytes(body)
            p = await self.start('backtest_sniper_strategy.py', '--file', str(file))
            output, _ = await asyncio.wait_for(p.communicate(), 8)
            self.assertEqual(p.returncode, 0, output.decode())
            reports.append(output.decode().split('Modeled PnL=')[1].split('\n')[0])
            self.assertIn('buys=1', output.decode())
        self.assertEqual(reports[0], reports[1])
        self.assertFalse(self.requests)

    async def test_backtest_download_and_missing_hour_policy(self):
        self.events = [market('create'), market(timestamp=1400, price=1.6),
                       market('sell', timestamp=1800, price=1.7)]
        for status, allow_gaps, expected_code in [(200, False, 0), (404, False, 1), (404, True, 0)]:
            self.archive_status = status
            if allow_gaps:
                self.config.write_text(self.config.read_text().replace('allow_gaps = false', 'allow_gaps = true'))
            p = await self.start('backtest_sniper_strategy.py', '--hours', '1')
            output, _ = await asyncio.wait_for(p.communicate(), 8)
            self.assertEqual(p.returncode, expected_code, output.decode())
            if status == 200:
                self.assertIn('buys=1', output.decode())
            elif allow_gaps:
                self.assertIn('Skipping missing archive hour', output.decode())
            else:
                self.assertIn('Archive download returned HTTP 404', output.decode())

    async def test_archive_storage_failure_explains_tmpdir(self):
        sys.path.insert(0, str(ROOT/'scripts'))
        from bot_common import load_config, StopBot
        from backtest_sniper_strategy import download_hour
        from datetime import datetime, timezone
        cfg = load_config(self.config)
        ctx = ssl.create_default_context(cafile=str(self.cert))
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx)) as session:
            with patch('backtest_sniper_strategy.Path.open', side_effect=OSError(122, 'Disk quota exceeded')):
                with self.assertRaisesRegex(StopBot, 'TMPDIR'):
                    await download_hour(session, cfg, datetime.now(timezone.utc), self.path/'archive.zst')
