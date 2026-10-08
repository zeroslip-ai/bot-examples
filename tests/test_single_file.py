"""Run copied single files outside the repo, with settings edited instead of CLI flags."""

import ast
import asyncio
from pathlib import Path
import sys
import tomllib

import test_cli_integration as fixtures

ROOT = Path(__file__).resolve().parent.parent


def edit_setting(source, name, value):
    lines = source.splitlines(keepends=True)
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign) and any(getattr(target, 'id', None) == name for target in node.targets)
    )
    lines[node.lineno - 1 : node.end_lineno] = [f'{name} = {value!r}\n']
    return ''.join(lines)


class SingleFileIntegrationTests(fixtures.CliIntegrationTests):
    async def start(self, filename, *options):
        # Reuse the end-to-end scenarios, translating their settings into Python.
        # The launched process receives no arguments and has only one local file.
        cfg = tomllib.loads(self.config.read_text())
        live, all_tokens, replay_file = False, False, ''
        options = iter(options)
        for option in options:
            if option == '--live':
                live = True
            elif option == '--all':
                all_tokens = True
            elif option == '--wallet':
                wallet = next(options)
                if filename == 'copytrader_bot.py':
                    cfg['copytrader']['wallets'] = [wallet]
                else:
                    cfg['wallet']['public_key'] = wallet
            elif option == '--mint':
                mint = next(options)
                if filename == 'live_sniper_bot.py':
                    cfg['sniper']['token_mint'] = mint
                else:
                    cfg['sell']['token_mints'] = [mint]
            elif option == '--hours':
                cfg['backtest']['hours'] = int(next(options))
            elif option == '--file':
                replay_file = next(options)
            else:
                self.fail(f'Unsupported fixture option: {option}')
        source = (ROOT / 'standalone' / filename).read_text()
        settings = [('CONFIG', cfg)]
        if filename != 'backtest_sniper_strategy.py':
            settings.extend([('LIVE', live), ('API_KEY', 'local-fixture-key')])
        for name, value in settings:
            source = edit_setting(source, name, value)
        if filename == 'sell_all_tokens.py':
            source = edit_setting(source, 'SELL_ALL', all_tokens)
        if filename == 'backtest_sniper_strategy.py':
            source = edit_setting(source, 'REPLAY_FILE', replay_file)
        folder = self.path / str(len(self.processes))
        folder.mkdir()
        (folder / filename).write_text(source)
        self.assertEqual([path.name for path in folder.iterdir()], [filename])
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            filename,
            cwd=folder,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        self.processes.append(process)
        return process

    async def test_empty_copy_settings_fail_before_connecting(self):
        process = await self.start('copytrader_bot.py')
        output, _ = await asyncio.wait_for(process.communicate(), 8)
        self.assertEqual(process.returncode, 1)
        self.assertIn('copytrader.wallets', output.decode())
        self.assertFalse(self.sockets)
        self.assertFalse(self.requests)
