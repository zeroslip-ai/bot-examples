"""Regressions for launch fills, preserved balances, reports, and diagnostics."""

import asyncio
import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import tomllib
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))

from bot_common import DEFAULTS, StopBot, Trader, USDC, WSOL, load_config, run, run_stream, setup
from backtest_sniper_strategy import Backtest, download_hour, read_lines
from copytrader_bot import Copytrader
from live_sniper_bot import Sniper
from sell_all_tokens import select_balances
from test_bots import deliver
from test_cli_integration import market, MINT


class PaperFillTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cfg = copy.deepcopy(DEFAULTS)
        self.trader = Trader(self.cfg, object())
        self.sniper = Sniper(self.cfg, self.trader)

    async def test_same_block_migration_jump_is_missed_not_an_instant_win(self):
        await deliver(self.sniper, market('create'), fill=False)
        self.assertFalse(self.sniper.positions)
        await deliver(self.sniper, market('migrate', pool='pump-amm', poolCreatedBy='pump'), fill=False)
        self.assertFalse(self.sniper.positions)
        await deliver(self.sniper, market(price=8, timestamp=1001, pool='pump-amm', poolCreatedBy='pump'), fill=False)
        self.assertFalse(self.sniper.positions)
        self.assertFalse(self.trader.paper_positions)
        self.assertEqual(self.trader.paper_missed, 1)
        self.assertEqual(self.trader.paper_sells, 0)

    async def test_fill_uses_subsequent_trade_and_cannot_exit_on_fill_event(self):
        await deliver(self.sniper, market('create'), fill=False)
        await deliver(self.sniper, market(price=1.1, timestamp=1001), fill=False)
        self.assertEqual(self.sniper.positions[MINT]['entry'], 1.1)
        self.assertEqual(self.trader.paper_sells, 0)
        with self.assertLogs(level='INFO') as logs:
            await deliver(self.sniper, market('sell', price=1.8, timestamp=1002), fill=False)
        self.assertFalse(self.sniper.positions)
        self.assertAlmostEqual(self.trader.paper_pnl_sol, 0.001 * (1.8 / 1.1 - 1))
        self.assertTrue(any('PAPER RESULT' in line for line in logs.output))

    async def test_pending_fill_converts_entry_quote_and_ignores_other_assets(self):
        await deliver(self.sniper, market('create'), fill=False)
        await deliver(self.sniper, market(quoteMint=MINT, price=100), fill=False)
        self.assertIn(MINT, self.sniper.pending)
        await deliver(self.sniper, market(quoteMint=USDC, price=88), fill=False)
        self.assertEqual(self.sniper.positions[MINT]['entry'], 1.1)

    async def test_pending_without_another_trade_expires_as_a_miss(self):
        clock = [0]
        self.sniper.clock = lambda: clock[0]
        await deliver(self.sniper, market('create'), fill=False)
        clock[0] = 301
        await self.sniper.on_tick()
        self.assertFalse(self.sniper.pending)
        self.assertFalse(self.trader.paper_positions)
        self.assertEqual(self.trader.paper_missed, 1)

    async def test_paper_partial_exit_pnl_preserves_remaining_cost(self):
        await self.trader.order('buy', MINT, WSOL, 10, 2)
        await self.trader.order('sell', MINT, WSOL, 1.25, 3, 5)
        self.assertAlmostEqual(self.trader.paper_pnl_sol, 1.25)
        self.assertAlmostEqual(self.trader.paper_positions[MINT]['cost'], 7.5)
        await self.trader.order('sell', MINT, WSOL, '100%', 1, 3.75)
        self.assertAlmostEqual(self.trader.paper_pnl_sol, -2.5)
        self.assertFalse(self.trader.paper_positions)

    async def test_reconnect_backoff_caps_and_logs_http_reason(self):
        delays = []

        async def pause(delay):
            delays.append(delay)
            if len(delays) == 9:
                raise StopBot('end test')

        with patch('bot_common.websockets.connect', side_effect=OSError('HTTP 429')):
            with patch('bot_common.asyncio.sleep', side_effect=pause):
                with self.assertLogs(level='WARNING') as logs:
                    with self.assertRaisesRegex(StopBot, 'end test'):
                        await run_stream(self.cfg, self.trader, None, None)
        self.assertEqual(delays, [0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 30, 30])
        self.assertTrue(all('HTTP 429' in line for line in logs.output))


class ReportAndBalanceTests(unittest.TestCase):
    def test_all_and_explicit_selections_keep_both_quotes(self):
        cfg = copy.deepcopy(DEFAULTS)
        holdings = {WSOL: {'balance': 1}, USDC: {'balance': 20}, MINT: {'balance': 3}}
        self.assertEqual(list(select_balances(cfg, holdings, True)), [MINT])
        cfg['sell']['token_mints'] = [WSOL, USDC, MINT]
        self.assertEqual(list(select_balances(cfg, holdings, False)), [MINT])

    def test_unexpected_error_includes_type_and_traceback(self):
        async def broken():
            raise ValueError('useful detail')

        with self.assertLogs(level='ERROR') as logs:
            with self.assertRaises(SystemExit) as caught:
                run(broken)
        self.assertEqual(caught.exception.code, 1)
        self.assertIn('ValueError', '\n'.join(logs.output))
        self.assertIn('Traceback', '\n'.join(logs.output))
        self.assertIn('useful detail', '\n'.join(logs.output))

    def test_report_restores_reasons_window_and_trade_statistics(self):
        cfg = copy.deepcopy(DEFAULTS)
        strategy = Backtest(cfg)
        for e in [market('create'), market(timestamp=1400), market('sell', timestamp=1800, price=2)]:
            strategy.handle_event(e)
        with self.assertLogs(level='INFO') as logs:
            strategy.finish()
        text = '\n'.join(logs.output)
        for label in (
            'Exits: tp=',
            'win_rate=',
            'Average move=',
            'average entry slippage=',
            'Best trade:',
            'Worst trade:',
            'Replay window:',
        ):
            self.assertIn(label, text)

    def test_invalid_held_price_cannot_poison_replay_state(self):
        strategy = Backtest(copy.deepcopy(DEFAULTS))
        strategy.handle_event(market('create'))
        for price in (0, -1, 'bad', True, None):
            raw = json.dumps(market(timestamp=1100, price=price)) + '\n'
            read_lines(io.BytesIO(raw.encode()), strategy)
        self.assertEqual(strategy.skipped, 5)
        self.assertEqual(strategy.positions[MINT]['cur_price'], 1)


class ArchiveErrors(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_during_read_is_not_a_storage_error(self):
        class Content:
            async def iter_chunked(self, size):
                yield b'first chunk'
                raise asyncio.TimeoutError('read timed out')

        class Response:
            status, content = 200, Content()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        class Session:
            def get(self, url):
                return Response()

        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(StopBot, 'Archive network failure: TimeoutError') as caught:
                await download_hour(
                    Session(), copy.deepcopy(DEFAULTS), datetime.now(timezone.utc), Path(folder) / 'hour'
                )
        self.assertNotIn('temporary storage', str(caught.exception))


class SetupCompatibilityTests(unittest.TestCase):
    def test_uv_and_pip_use_the_same_runtime_dependencies(self):
        project = tomllib.loads((ROOT / 'pyproject.toml').read_text())
        self.assertEqual(
            sorted(project['project']['dependencies']), sorted((ROOT / 'requirements.txt').read_text().splitlines())
        )

    def test_legacy_quote_setting_is_ignored_with_a_warning(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'config.toml'
            path.write_text('[trade]\nquote_mint = "' + USDC + '"\nbuy_amount = 0.002\n')
            with self.assertLogs('bot_common', level='WARNING') as logs:
                cfg = load_config(path)
        self.assertNotIn('quote_mint', cfg['trade'])
        self.assertEqual(cfg['trade']['buy_amount'], 0.002)
        self.assertIn('deprecated trade.quote_mint', logs.output[0])

    def test_no_config_file_uses_example_defaults_and_never_creates_a_file(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'config.example.toml').write_text((ROOT / 'config.example.toml').read_text())
            with patch('bot_common.ROOT', root), patch('sys.argv', ['sniper', '--check-config']):
                with self.assertRaises(SystemExit) as caught:
                    setup('sniper')
            self.assertEqual(caught.exception.code, 0)
            self.assertFalse((root / 'config.toml').exists())

    def test_existing_config_still_overrides_defaults(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'config.toml').write_text('[trade]\nbuy_amount = 0.002\n')
            with patch('bot_common.ROOT', root), patch('sys.argv', ['sniper']):
                cfg, args = setup('sniper')
            self.assertEqual(args.config, root / 'config.toml')
            self.assertEqual(cfg['trade']['buy_amount'], 0.002)


class RoundedExitTests(unittest.IsolatedAsyncioTestCase):
    async def make_strategy(self, tokens, decimals):
        cfg = copy.deepcopy(DEFAULTS)
        cfg['copytrader']['wallets'] = [market()['txSigner']]
        trader = Trader(cfg, object(), live=True)
        orders = []

        async def order(action, mint, quote, amount, price, tokens_held=0):
            orders.append(amount)
            quantity = tokens if action == 'buy' else (tokens_held if amount == '100%' else amount)
            return {'price': price, 'tokenAmount': quantity}

        trader.order = order
        strategy = Copytrader(cfg, trader)
        await deliver(strategy, market(decimals=decimals))
        return strategy, orders

    async def test_live_partial_exits_round_down_to_mint_decimals(self):
        for decimals, expected in [(0, 12.0), (6, 12.345678), (8, 12.3456789)]:
            with self.subTest(decimals=decimals):
                strategy, orders = await self.make_strategy(49.382715600000004, decimals)
                await deliver(strategy, market('sell', tokenAmount=25))
                self.assertEqual(orders[1], expected)
                self.assertEqual(strategy.positions[MINT]['copied_remaining'], 75)
                self.assertAlmostEqual(strategy.positions[MINT]['tokens'], 49.382715600000004 - expected)
                await deliver(strategy, market('sell', tokenAmount=75))
                self.assertEqual(orders[2], '100%')
                self.assertFalse(strategy.positions)

    async def test_subunit_exit_updates_followed_balance_without_a_zero_order(self):
        strategy, orders = await self.make_strategy(0.01, 0)
        await deliver(strategy, market('sell', tokenAmount=25))
        self.assertEqual(len(orders), 1)
        self.assertEqual(strategy.positions[MINT]['copied_remaining'], 75)
        await deliver(strategy, market('sell', tokenAmount=75))
        self.assertEqual(orders[-1], '100%')
        self.assertFalse(strategy.positions)

    async def test_missing_decimals_never_submits_an_unrounded_live_partial(self):
        strategy, orders = await self.make_strategy(1, None)
        with self.assertRaisesRegex(StopBot, 'Token decimals unavailable'):
            await deliver(strategy, market('sell', tokenAmount=25))
        self.assertEqual(len(orders), 1)
        self.assertEqual(strategy.positions[MINT]['copied_remaining'], 100)


class ArchiveCloseTests(unittest.IsolatedAsyncioTestCase):
    async def download(self, file, read_error=None):
        class Content:
            async def iter_chunked(self, size):
                yield b'first chunk'
                if read_error:
                    raise read_error

        class Response:
            status, content = 200, Content()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        class Session:
            def get(self, url):
                return Response()

        with patch('backtest_sniper_strategy.Path.open', return_value=file):
            await download_hour(Session(), copy.deepcopy(DEFAULTS), datetime.now(timezone.utc), 'archive')

    async def test_close_failure_does_not_hide_download_timeout(self):
        file = Mock()
        file.close.side_effect = OSError('close failed')
        with self.assertRaisesRegex(StopBot, 'Archive network failure: TimeoutError'):
            await self.download(file, asyncio.TimeoutError('read timed out'))
        file.close.assert_called_once()

    async def test_write_failure_survives_a_second_close_failure(self):
        file = Mock()
        file.write.side_effect = OSError('disk full')
        file.close.side_effect = OSError('close failed')
        with self.assertRaisesRegex(StopBot, 'Cannot write archive') as caught:
            await self.download(file)
        self.assertEqual(str(caught.exception.__cause__), 'disk full')

    async def test_close_failure_after_success_is_a_storage_error(self):
        file = Mock()
        file.close.side_effect = OSError('flush failed')
        with self.assertRaisesRegex(StopBot, 'Cannot write archive') as caught:
            await self.download(file)
        self.assertEqual(str(caught.exception.__cause__), 'flush failed')
