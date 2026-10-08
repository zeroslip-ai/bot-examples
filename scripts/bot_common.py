"""Shared standalone configuration, execution, and stream handling (Python 3.11+)."""

import argparse
import asyncio
import copy
import logging
import math
import os
from pathlib import Path
import tomllib
from urllib.parse import urlparse

import aiohttp
import orjson
from cachetools import TTLCache
from dotenv import load_dotenv
import websockets

BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
ROOT = Path(__file__).resolve().parent.parent
WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
SOL_USDC_POOL = 'Gf7sXMoP8iRw4iiXmJ1nq4vxcRycbGXy5RL8a8LnTd3v'
DEFAULTS = {
    'network': {
        'trade_url': 'https://api.zeroslip.ai',
        'stream_url': 'wss://stream.zeroslip.ai/',
        'replay_url': 'https://replay.pumpapi.io',
        'rpc_url': 'https://api.mainnet-beta.solana.com',
    },
    'trade': {
        'buy_amount': 0.001,
        'buy_slippage': 20.0,
        'sell_slippage': 99.0,
        'priority_fee': 0.0001,
        'confirmation_seconds': 3.0,
    },
    'wallet': {'public_key': ''},
    'copytrader': {'wallets': [], 'token_mints': [], 'buy_fraction': 0.1, 'max_buy_amount': 0.01},
    'sniper': {
        'token_mint': '',
        'min_initial_buy': 20.0,
        'take_profit': 50.0,
        'stop_loss': 20.0,
        'idle_seconds': 300.0,
    },
    'backtest': {
        'hours': 10,
        'allow_gaps': False,
        'buy_latency_ms': 400,
        'sell_latency_ms': 400,
        'service_fee': 0.0025,
        'extra_fee': 0.0005,
    },
    'sell': {'token_mints': [], 'slippage': 100.0},
}


class ConfigError(ValueError):
    pass


class StopBot(RuntimeError):
    """Execution is rejected or uncertain: stop without sending another order."""


def address(value, label):
    if not isinstance(value, str) or not 32 <= len(value) <= 44 or any(c not in BASE58_ALPHABET for c in value):
        raise ConfigError(f'{label} must be a Solana base58 address')
    # Verify decoded length, not just the alphabet.
    number = 0
    for char in value:
        number = number * 58 + BASE58_ALPHABET.index(char)
    size = (number.bit_length() + 7) // 8 + len(value) - len(value.lstrip('1'))
    if size != 32:
        raise ConfigError(f'{label} must decode to 32 bytes')
    return value


def load_config(path):
    cfg = copy.deepcopy(DEFAULTS)
    try:
        with Path(path).open('rb') as file:
            overrides = tomllib.load(file)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError('Cannot read config; copy config.example.toml to config.toml and check TOML syntax') from exc
    for section, values in overrides.items():
        if section not in cfg or not isinstance(values, dict):
            raise ConfigError(f'Unknown or invalid configuration section: {section}')
        for key, value in values.items():
            if section == 'trade' and key == 'quote_mint':
                logging.getLogger(__name__).warning(
                    'Ignoring deprecated trade.quote_mint; SOL/USDC quotes come from each event. '
                    'Remove this key from your config'
                )
                continue
            if key not in cfg[section]:
                raise ConfigError(f'Unknown configuration option: {section}.{key}')
            default = cfg[section][key]
            if isinstance(default, bool):
                valid = isinstance(value, bool)
            elif isinstance(default, (float, int)):
                valid = isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)
            else:
                valid = isinstance(value, type(default))
            if not valid:
                raise ConfigError(f'Invalid value type: {section}.{key}')
            cfg[section][key] = value
    for section, key in [
        ('trade', 'buy_amount'),
        ('trade', 'confirmation_seconds'),
        ('copytrader', 'max_buy_amount'),
        ('sniper', 'idle_seconds'),
        ('sniper', 'take_profit'),
        ('sniper', 'stop_loss'),
    ]:
        if cfg[section][key] <= 0:
            raise ConfigError(f'{section}.{key} must be positive')
    for section, key in [('backtest', 'hours')]:
        if not isinstance(cfg[section][key], int) or cfg[section][key] < 1:
            raise ConfigError(f'{section}.{key} must be a positive integer')
    for key in ('buy_slippage', 'sell_slippage'):
        if not 0 <= cfg['trade'][key] < 100:
            raise ConfigError(f'trade.{key} must be between 0 and 100 (exclusive)')
    if not 0 <= cfg['sell']['slippage'] <= 100:
        raise ConfigError('sell.slippage must be between 0 and 100 (inclusive)')
    if not 0 < cfg['copytrader']['buy_fraction'] <= 1:
        raise ConfigError('copytrader.buy_fraction must be between 0 and 1')
    if not 0 < cfg['sniper']['stop_loss'] < 100:
        raise ConfigError('sniper.stop_loss must be between 0 and 100 (exclusive)')
    for section, key in [
        ('trade', 'priority_fee'),
        ('sniper', 'min_initial_buy'),
        ('backtest', 'buy_latency_ms'),
        ('backtest', 'sell_latency_ms'),
        ('backtest', 'service_fee'),
        ('backtest', 'extra_fee'),
    ]:
        if cfg[section][key] < 0:
            raise ConfigError(f'{section}.{key} cannot be negative')
    if cfg['backtest']['service_fee'] + cfg['backtest']['extra_fee'] >= 1:
        raise ConfigError('Backtest fee fractions must total less than 1')
    for key, url in cfg['network'].items():
        parsed = urlparse(url)
        if (
            parsed.scheme != ('wss' if key == 'stream_url' else 'https')
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
        ):
            raise ConfigError(f'network.{key} must be a secure URL without credentials or query parameters')
    for section, key in [('copytrader', 'wallets'), ('copytrader', 'token_mints'), ('sell', 'token_mints')]:
        for item in cfg[section][key]:
            address(item, f'{section}.{key}')
    if cfg['sniper']['token_mint']:
        address(cfg['sniper']['token_mint'], 'sniper.token_mint')
    return cfg


def credentials():
    api_key = os.getenv('ZEROSLIP_API_KEY', '').strip()
    private_key = os.getenv('ZEROSLIP_PRIVATE_KEY', '').strip()
    if not api_key and not private_key:
        raise ConfigError('Live mode needs ZEROSLIP_API_KEY or ZEROSLIP_PRIVATE_KEY in .env or your environment')
    return {'apiKey': api_key} if api_key else {'privateKey': private_key}


def setup(kind):
    parser = argparse.ArgumentParser(description=f'ZeroSlip standalone {kind}; paper mode unless --live')
    parser.add_argument('--config', type=Path, help='Settings file; defaults to config.toml or config.example.toml')
    parser.add_argument('--check-config', action='store_true', help='Validate settings and exit without network calls')
    if kind != 'backtest':
        parser.add_argument('--live', action='store_true', help='Enable real Lightning trades using local credentials')
    if kind in ('sniper', 'sell'):
        parser.add_argument('--mint', help='Token mint to buy/watch or sell (overrides config)')
    if kind == 'copytrader':
        parser.add_argument('--wallet', action='append', help='Wallet to copy; repeat for several wallets')
    if kind == 'sell':
        parser.add_argument('--wallet', help='Public wallet address for previews; live sells use the credential wallet')
        parser.add_argument('--all', action='store_true', help='Select every non-quote token balance')
    if kind == 'backtest':
        parser.add_argument('--hours', type=int, help='Number of completed UTC archive hours')
        parser.add_argument('--file', type=Path, help='Replay a local JSONL or JSONL.zst file instead of downloading')
    args = parser.parse_args()
    if args.config is None:
        args.config = ROOT / ('config.toml' if (ROOT / 'config.toml').exists() else 'config.example.toml')
    load_dotenv(args.config.resolve().parent / '.env', override=False)
    try:
        cfg = load_config(args.config)
        if getattr(args, 'mint', None):
            address(args.mint, '--mint')
            if kind == 'sniper':
                cfg['sniper']['token_mint'] = args.mint
            else:
                cfg['sell']['token_mints'] = [args.mint]
        if kind == 'copytrader' and args.wallet:
            cfg['copytrader']['wallets'] = [address(w, '--wallet') for w in args.wallet]
        if kind == 'sell' and args.wallet:
            cfg['wallet']['public_key'] = address(args.wallet, '--wallet')
        if getattr(args, 'hours', None) is not None:
            if args.hours < 1:
                raise ConfigError('--hours must be positive')
            cfg['backtest']['hours'] = args.hours
        if not (kind == 'sell' and args.wallet):
            cfg['wallet']['public_key'] = (
                os.getenv('ZEROSLIP_WALLET_PUBLIC_KEY', '').strip() or cfg['wallet']['public_key']
            )
        if cfg['wallet']['public_key']:
            address(cfg['wallet']['public_key'], 'wallet.public_key')
        if kind == 'copytrader' and not cfg['copytrader']['wallets']:
            raise ConfigError('Set copytrader.wallets or pass --wallet ADDRESS')
        if kind == 'copytrader' and cfg['wallet']['public_key'] in cfg['copytrader']['wallets']:
            raise ConfigError('Do not copy your own trading wallet')
        if kind == 'sell':
            if not args.live and not cfg['wallet']['public_key']:
                raise ConfigError(
                    'Sell preview needs --wallet ADDRESS, wallet.public_key, or ZEROSLIP_WALLET_PUBLIC_KEY'
                )
            if args.all and (args.mint or cfg['sell']['token_mints']):
                raise ConfigError('Choose --all or specific token mints, not both')
            if not args.all and not cfg['sell']['token_mints']:
                raise ConfigError('Choose --mint ADDRESS, sell.token_mints, or --all')
        if getattr(args, 'live', False):
            credentials()
    except ConfigError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.check_config:
        print('Configuration valid. No network calls or transactions made.')
        raise SystemExit(0)
    return cfg, args


def trusted_pool(event):
    return not event.get('mayhemMode') and (
        event.get('pool') == 'pump' or (event.get('pool') == 'pump-amm' and event.get('poolCreatedBy') == 'pump')
    )


def positive(value):
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else 0.0
    except (ValueError, TypeError):
        return 0.0


class QuoteSizing:
    """Original two-quote strategy: SOL sizes, converted to USDC at stream price."""

    def __init__(self):
        self.sol_price = 80.0  # Original fallback until the trusted pool updates.

    def observe(self, event):
        if event.get('poolId') != SOL_USDC_POOL:
            return False
        price = positive(event.get('price'))
        if price:
            self.sol_price = 1 / price
        return True

    def amount(self, quote, sol_amount):
        return sol_amount if quote == WSOL else sol_amount * self.sol_price

    @staticmethod
    def supports(quote):
        return quote in (WSOL, USDC)


async def api_post(session, url, payload, *, rpc=False):
    # Never log the credential-bearing payload or a server body that may echo it.
    try:
        async with session.post(url, json=payload, allow_redirects=False) as response:
            if response.status != 200:
                if rpc:
                    raise StopBot(f'RPC returned HTTP {response.status}; retry the balance preview')
                raise StopBot(f'API returned HTTP {response.status}; check the order outcome before retrying')
            data = await response.json()
        if not isinstance(data, dict) or data.get('err') or data.get('error'):
            if rpc:
                error = data.get('error') if isinstance(data, dict) else 'Invalid response'
                raise StopBot(f'RPC balance query failed: {error}')
            raise StopBot('API rejected the request; check the order outcome before retrying')
        return data
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        if rpc:
            raise StopBot(f'RPC balance query unavailable: {type(exc).__name__}: {exc}') from exc
        raise StopBot('API response unavailable; order outcome is uncertain. Check the wallet before retrying') from exc


class Trader:
    def __init__(self, cfg, session, live=False):
        self.cfg, self.session, self.live = cfg, session, live
        self.events = TTLCache(maxsize=10000, ttl=300)
        self.paper_positions = {}
        self.paper_totals = {WSOL: 0.0, USDC: 0.0}
        self.paper_pnl_sol = 0.0
        self.paper_missed = self.paper_buys = self.paper_sells = 0
        self.quotes = QuoteSizing()

    def observe(self, event):
        self.quotes.observe(event)
        signature = event.get('signature')
        if signature:
            self.events.setdefault(signature, []).append(event)

    async def order(self, action, mint, quote, amount, price, tokens=0):
        if not self.live:
            logging.info('PAPER %s mint=%s amount=%s quote=%s', action.upper(), mint, amount, quote)
            if action == 'buy':
                quantity = float(amount) / price
                self.paper_positions[mint] = {
                    'tokens': quantity,
                    'cost': float(amount),
                    'sol_price': self.quotes.sol_price,
                }
                self.paper_buys += 1
            else:
                quantity = (
                    tokens * float(amount[:-1]) / 100
                    if isinstance(amount, str) and amount.endswith('%')
                    else float(amount)
                )
                quantity = min(tokens, quantity)
                position = self.paper_positions[mint]
                cost = position['cost'] * quantity / position['tokens']
                pnl = quantity * price - cost
                self.paper_totals[quote] += pnl
                self.paper_pnl_sol += pnl if quote == WSOL else pnl / position['sol_price']
                self.paper_sells += 1
                logging.info(
                    'PAPER RESULT mint=%s move=%+.2f%% pnl=%+.8f quote=%s total_sol=%+.8f',
                    mint,
                    (quantity * price / cost - 1) * 100,
                    pnl,
                    quote,
                    self.paper_pnl_sol,
                )
                position['tokens'] -= quantity
                position['cost'] -= cost
                if position['tokens'] <= 0:
                    self.paper_positions.pop(mint)
            return {'price': price, 'tokenAmount': quantity, 'quoteAmount': quantity * price}
        settings = self.cfg['trade']
        payload = {
            **credentials(),
            'action': action,
            'mint': mint,
            'quoteMint': quote,
            'amount': amount,
            'denominatedInQuote': 'true' if action == 'buy' else 'false',
            'slippage': settings['buy_slippage' if action == 'buy' else 'sell_slippage'],
            'priorityFee': settings['priority_fee'],
        }
        response = await api_post(self.session, self.cfg['network']['trade_url'], payload)
        signature = response.get('signature')
        if not isinstance(signature, str) or not signature:
            raise StopBot('API did not return a signature. Check the wallet before retrying')
        logging.info('Submitted %s mint=%s signature=%s', action, mint, signature)
        deadline = asyncio.get_running_loop().time() + settings['confirmation_seconds']
        while asyncio.get_running_loop().time() < deadline:
            for event in self.events.get(signature, []):
                if (
                    event.get('action') == action
                    and event.get('mint') == mint
                    and positive(event.get('price'))
                    and positive(event.get('tokenAmount'))
                ):
                    logging.info('Confirmed %s mint=%s signature=%s', action, mint, signature)
                    return event
            await asyncio.sleep(0.1)
        raise StopBot(
            f'No trade event observed for {signature}. Check the wallet before restarting; no order was retried'
        )

    def report(self):
        if not self.live:
            logging.info(
                'Paper summary: buys=%s sells=%s missed=%s open=%s '
                'realized_pnl=%+.8f SOL-equivalent; WSOL=%+.8f USDC=%+.8f (fees excluded)',
                self.paper_buys,
                self.paper_sells,
                self.paper_missed,
                len(self.paper_positions),
                self.paper_pnl_sol,
                self.paper_totals[WSOL],
                self.paper_totals[USDC],
            )


async def run_stream(cfg, trader, on_event, on_tick, accept_event=None):
    # Independent token workers restore the original cross-token concurrency.
    # A token's exits stay in stream order so proportional bookkeeping is stable.
    queues = {}
    workers = set()
    failure = asyncio.get_running_loop().create_future()

    def completed(task):
        workers.discard(task)
        if not task.cancelled() and task.exception() is not None and not failure.done():
            failure.set_exception(task.exception())

    async def token_worker(mint, queue):
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1)
                await on_event(event)
            except asyncio.TimeoutError:
                pass
            tracking = await on_tick(mint)
            if not tracking and queue.empty():
                # No await between checking/removing: reader cannot enqueue into
                # an orphaned queue. Untracked tokens don't retain idle workers.
                queues.pop(mint, None)
                return

    async def reader():
        delay = 0.4
        while True:
            connected_at = None
            try:
                async with websockets.connect(cfg['network']['stream_url'], max_size=8 << 20) as socket:
                    connected_at = asyncio.get_running_loop().time()
                    logging.info('Connected to market stream (%s)', 'LIVE' if trader.live else 'PAPER')
                    async for message in socket:
                        try:
                            event = orjson.loads(message)
                        except (ValueError, TypeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        trader.observe(event)
                        mint = event.get('mint') or '__market__'
                        # Once a token worker exists, retain its market updates
                        # even while its buy hasn't registered a position yet.
                        if accept_event is not None and not accept_event(event) and mint not in queues:
                            continue
                        if mint not in queues:
                            queues[mint] = asyncio.Queue(maxsize=10000)
                            task = asyncio.create_task(token_worker(mint, queues[mint]))
                            workers.add(task)
                            task.add_done_callback(completed)
                        try:
                            queues[mint].put_nowait(event)
                        except asyncio.QueueFull as exc:
                            raise StopBot('Token event queue overflowed; reconcile wallet positions') from exc
                    reason = f'close={socket.close_code} {socket.close_reason}'
            except (OSError, websockets.exceptions.WebSocketException) as exc:
                reason = f'{type(exc).__name__}: {exc}'
            if connected_at is not None and asyncio.get_running_loop().time() - connected_at >= 30:
                delay = 0.4
            logging.warning('Stream disconnected (%s); reconnecting in %.1f seconds', reason, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    stream = asyncio.create_task(reader())
    try:
        done, _ = await asyncio.wait([stream, failure], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        stream.cancel()
        for task in workers:
            task.cancel()
        await asyncio.gather(stream, *workers, return_exceptions=True)
        if not failure.done():
            failure.cancel()
        trader.report()


def run(main):
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Stopped. Existing live positions remain in the wallet.')
    except (StopBot, ConfigError) as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from exc
    except Exception as exc:
        logging.exception('Unexpected runtime failure (%s); stopped', type(exc).__name__)
        raise SystemExit(1) from exc
