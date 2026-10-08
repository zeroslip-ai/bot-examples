"""Bundle each strategy with the shared runtime; users edit only the top settings."""

import argparse
import ast
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = {
    'copytrader_bot.py': """# Add the public wallets you want to follow. An empty list stops before connecting.
CONFIG = {
    'copytrader': {
        'wallets': [],
        'token_mints': [],  # Empty: follow any qualifying token.
        'buy_fraction': 0.1,
        'max_buy_amount': 0.01,  # SOL-equivalent.
    },
    'trade': {'buy_slippage': 20.0, 'sell_slippage': 99.0, 'priority_fee': 0.0001},
    'wallet': {'public_key': ''},  # Your own wallet, to prevent copying yourself.
}
""",
    'live_sniper_bot.py': """CONFIG = {
    'sniper': {
        'token_mint': '',  # Paste a mint; empty means qualifying new launches.
        'min_initial_buy': 20.0,  # SOL-equivalent launch threshold.
        'take_profit': 50.0,  # Percent.
        'stop_loss': 20.0,  # Percent.
        'idle_seconds': 300.0,
    },
    'trade': {
        'buy_amount': 0.001,  # SOL-equivalent.
        'buy_slippage': 20.0,
        'sell_slippage': 99.0,
        'priority_fee': 0.0001,
    },
}
""",
    'backtest_sniper_strategy.py': """REPLAY_FILE = ''  # Optional path to a downloaded .jsonl or .jsonl.zst archive.
CONFIG = {
    'backtest': {
        'hours': 1,  # Completed UTC hours; ignored when REPLAY_FILE is set.
        'allow_gaps': False,
        'buy_latency_ms': 400,
        'sell_latency_ms': 400,
        'service_fee': 0.0025,
        'extra_fee': 0.0005,
    },
    'sniper': {'min_initial_buy': 20.0, 'take_profit': 50.0, 'stop_loss': 20.0, 'idle_seconds': 300.0},
    'trade': {'buy_amount': 0.001, 'buy_slippage': 20.0, 'sell_slippage': 99.0},
}
""",
    'sell_all_tokens.py': """SELL_ALL = True  # False: sell only the token_mints listed below.
CONFIG = {
    'wallet': {'public_key': ''},  # Public wallet to preview; live sells use the credential wallet.
    'sell': {'token_mints': [], 'slippage': 100.0},  # 100 accepts any price; review before live use.
    'trade': {'priority_fee': 0.0001},
}
""",
}
ADAPTER = """
def credentials():
    api_key = API_KEY.strip()
    private_key = PRIVATE_KEY.strip()
    if not api_key and not private_key:
        raise ConfigError('LIVE = True needs API_KEY or PRIVATE_KEY in the settings at the top')
    return {'apiKey': api_key} if api_key else {'privateKey': private_key}


def setup(kind):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if not isinstance(LIVE, bool) or not isinstance(SELL_ALL, bool):
        raise ConfigError('Run switches must be True or False')
    cfg = validate_config(CONFIG)
    args = SimpleNamespace(live=LIVE, all=SELL_ALL, mint=None, file=Path(REPLAY_FILE) if REPLAY_FILE else None)
    validate_setup(cfg, args, kind)
    return cfg, args
"""


def nodes(path, omitted):
    """Keep complete original functions/classes, removing only file/CLI adapters."""
    source = path.read_text()
    lines = source.splitlines(keepends=True)
    result = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            continue
        if getattr(node, 'name', None) in omitted:
            continue
        if isinstance(node, ast.ImportFrom) and node.module in ('bot_common', 'dotenv'):
            continue
        if isinstance(node, ast.Import) and any(name.name in omitted for name in node.names):
            continue
        if isinstance(node, ast.Assign) and any(getattr(target, 'id', None) in omitted for target in node.targets):
            continue
        result.append(''.join(lines[node.lineno - 1 : node.end_lineno]).rstrip())
    return result


def referenced_names(source):
    return {
        node.id for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }


def used_runtime(common, strategy, adapter):
    providers = {}
    for part in common:
        node = ast.parse(part).body[0]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names = [node.name]
        elif isinstance(node, ast.Assign):
            names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        else:
            names = [name.asname or name.name.split('.')[0] for name in node.names]
        for name in names:
            providers[name] = part
    needed = referenced_names('\n\n'.join(strategy) + '\n' + adapter)
    selected = set()
    while pending := {providers[name] for name in needed if name in providers} - selected:
        selected.update(pending)
        for part in pending:
            needed.update(referenced_names(part))
    return [part for part in common if part in selected]


def generate():
    dependencies = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']['dependencies']
    dependencies = [dep for dep in dependencies if not dep.startswith('python-dotenv')]
    metadata = '\n'.join(f'#   "{dep}",' for dep in dependencies)
    common = nodes(
        ROOT / 'scripts/bot_common.py', {'setup', 'load_config', 'credentials', 'ROOT', 'argparse', 'os', 'tomllib'}
    )
    for filename, settings in EXAMPLES.items():
        header = f'''"""ZeroSlip {filename}: edit settings below, then run python3 {filename}.

Python 3.11+. Install the packages listed below once in your Python environment.
Alternatively, uv run {filename} handles them automatically. No config files or flags.
Generated by tools/build_standalone.py from scripts/; edit strategy logic there.
"""
# /// script
# requires-python = ">=3.11"
# dependencies = [
{metadata}
# ]
# ///

# SETTINGS — edit this section. Nothing below sends orders unless LIVE is True.
LIVE = False  # Paper trades / read-only previews. Backtesting never submits orders.
API_KEY = ''  # Live only: your wallet API key. Keep this file private if filled.
PRIVATE_KEY = ''  # Or a base58 private key; API_KEY takes precedence.
{settings}
# IMPLEMENTATION — the same runtime and strategy as scripts/, bundled into this file.
from types import SimpleNamespace
'''
        adapter = ADAPTER
        if filename != 'sell_all_tokens.py':
            adapter = adapter.replace('SELL_ALL', 'False')
        if filename != 'backtest_sniper_strategy.py':
            adapter = adapter.replace('REPLAY_FILE', "''")
        if filename == 'backtest_sniper_strategy.py':
            header = header.replace(
                '# SETTINGS — edit this section. Nothing below sends orders unless LIVE is True.\n'
                'LIVE = False  # Paper trades / read-only previews. Backtesting never submits orders.\n'
                "API_KEY = ''  # Live only: your wallet API key. Keep this file private if filled.\n"
                "PRIVATE_KEY = ''  # Or a base58 private key; API_KEY takes precedence.\n",
                '# SETTINGS — edit this section. Backtesting never submits orders.\n',
            )
            adapter = adapter[adapter.index('def setup') :].replace('LIVE', 'False')
            adapter = "def credentials():\n    raise ConfigError('Backtesting never submits orders')\n\n\n" + adapter
        body = nodes(ROOT / 'scripts' / filename, set())
        imports = []
        code = []
        for part in used_runtime(common, body, adapter) + body:
            node = ast.parse(part).body[0]
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if part not in imports:
                    imports.append(part)
            else:
                code.append(part)
        # Main resolves the settings adapter when called, after all definitions are loaded.
        source = header + '\n'.join(imports) + '\n\n\n' + '\n\n\n'.join(code[:-1])
        source += '\n\n\n' + adapter.strip() + '\n\n\n' + code[-1] + '\n'
        yield filename, source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Fail if a bundle is stale instead of writing it')
    args = parser.parse_args()
    for filename, source in generate():
        path = ROOT / 'standalone' / filename
        if args.check:
            if not path.exists() or path.read_text() != source:
                parser.error(f'{path.relative_to(ROOT)} is stale; run python tools/build_standalone.py')
        else:
            path.parent.mkdir(exist_ok=True)
            path.write_text(source)
    print('Single-file examples match the shared strategy sources.')


if __name__ == '__main__':
    main()
