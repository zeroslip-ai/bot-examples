"""ZeroSlip copytrader bot.

Copies the first buy that a watched wallet makes in a token, then copies its
sells in the same proportion.

How it works:
1. Reads live trades from the ZeroSlip data stream (one websocket).
2. When a wallet in WALLETS makes its first buy of a token, buys BUY_FRACTION
   of that amount, capped at MAX_BUY_AMOUNT.
3. When that wallet sells 25% of its tokens, sells 25% of yours.
Only Pump.fun and Pump-created PumpSwap pools are copied (Mayhem mode skipped).

LIVE = False (default) only logs paper trades; nothing is sent.
LIVE = True sends orders with your key to the ZeroSlip Trade API, which signs them.

Run: python3 copytrader_bot.py   (Python 3.11+, pip install aiohttp websockets)
"""

# ---------------------------------- SETTINGS ----------------------------------
LIVE = False  # True sends real orders.
API_KEY = ''  # Your ZeroSlip wallet API key. Needed only when LIVE = True.
PRIVATE_KEY = ''  # Or your base58 private key. API_KEY is used if both are set.

WALLETS = []  # Wallet addresses to copy, e.g. ['address1', 'address2']
TOKEN_MINTS = []  # Only copy these tokens. Empty = any token.
BUY_FRACTION = 0.1  # Copy 10% of the watched wallet's buy...
MAX_BUY_AMOUNT = 0.01  # ...up to this many SOL. USDC pools use the same value in USDC.
MY_WALLET = ''  # Optional: your own address, so you can't copy yourself by mistake.
BUY_SLIPPAGE = 20  # %
SELL_SLIPPAGE = 99  # %
PRIORITY_FEE = 0.0001  # SOL
# ------------------------------------------------------------------------------

# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.12,<4", "websockets>=15,<16"]
# ///

import asyncio
from decimal import Decimal, ROUND_DOWN, localcontext
import json
import logging
import math
import time

import aiohttp
import websockets

TRADE_URL = 'https://api.zeroslip.ai'
STREAM_URL = 'wss://stream.zeroslip.ai/'
CONFIRM_SECONDS = 3  # How long to wait for our own trade to appear on the stream.

WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
SOL_USDC_POOL = 'Gf7sXMoP8iRw4iiXmJ1nq4vxcRycbGXy5RL8a8LnTd3v'
BASE58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

sol_price = 80.0  # USDC per SOL until the SOL/USDC pool trades on the stream.


class StopBot(Exception):
    """Bad settings, or an order outcome we can't confirm. Stop instead of retrying."""


def positive(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) and value > 0 else 0.0


def check_address(value, name):
    # Base58 that decodes to 32 bytes, so a typo fails before the bot starts.
    if not isinstance(value, str) or not 32 <= len(value) <= 44 or any(c not in BASE58 for c in value):
        raise StopBot(f'{name} must be a Solana address')
    number = 0
    for char in value:
        number = number * 58 + BASE58.index(char)
    if (number.bit_length() + 7) // 8 + len(value) - len(value.lstrip('1')) != 32:
        raise StopBot(f'{name} must be a Solana address')


def credentials():
    if API_KEY.strip():
        return {'apiKey': API_KEY.strip()}
    if PRIVATE_KEY.strip():
        return {'privateKey': PRIVATE_KEY.strip()}
    raise StopBot('LIVE = True needs API_KEY or PRIVATE_KEY in the settings')


def check_settings():
    if not isinstance(WALLETS, list) or not WALLETS:
        raise StopBot('Add the wallets you want to copy to WALLETS')
    for wallet in WALLETS:
        check_address(wallet, 'WALLETS')
    for mint in TOKEN_MINTS:
        check_address(mint, 'TOKEN_MINTS')
    if MY_WALLET:
        check_address(MY_WALLET, 'MY_WALLET')
        if MY_WALLET in WALLETS:
            raise StopBot('Do not copy your own wallet')
    try:
        if not (0 < BUY_FRACTION <= 1 and MAX_BUY_AMOUNT > 0):
            raise StopBot('BUY_FRACTION must be above 0 and at most 1; MAX_BUY_AMOUNT above 0')
        if not (0 <= BUY_SLIPPAGE < 100 and 0 <= SELL_SLIPPAGE < 100):
            raise StopBot('BUY_SLIPPAGE and SELL_SLIPPAGE must be from 0 to below 100')
        if PRIORITY_FEE < 0:
            raise StopBot('PRIORITY_FEE cannot be negative')
    except TypeError:
        raise StopBot('Number settings must be numbers') from None
    if not isinstance(LIVE, bool):
        raise StopBot('LIVE must be True or False')
    if LIVE:
        credentials()


def update_sol_price(event):
    """Track SOL's USDC price from the SOL/USDC pool. Returns True for that pool's events."""
    global sol_price
    if event.get('poolId') != SOL_USDC_POOL:
        return False
    price = positive(event.get('price'))
    if price:
        sol_price = 1 / price
    return True


def sol_to_quote(quote, sol_amount):
    return sol_amount if quote == WSOL else sol_amount * sol_price


def convert_price(price, from_quote, to_quote):
    if from_quote == to_quote:
        return price
    return price / sol_price if from_quote == USDC else price * sol_price


def pump_pool(event):
    # Pump.fun bonding curve, or a PumpSwap pool that Pump created. Mayhem mode is skipped.
    return not event.get('mayhemMode') and (
        event.get('pool') == 'pump' or (event.get('pool') == 'pump-amm' and event.get('poolCreatedBy') == 'pump')
    )


async def post(session, payload):
    # Never log the payload or response body: they can contain your key.
    try:
        async with session.post(TRADE_URL, json=payload, allow_redirects=False) as response:
            if response.status != 200:
                raise StopBot(f'API returned HTTP {response.status}; check the wallet before retrying')
            data = await response.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        raise StopBot('No API response; the order may or may not have gone through. Check the wallet') from exc
    if not isinstance(data, dict) or data.get('err') or data.get('error'):
        raise StopBot('API rejected the order; check the wallet before retrying')
    return data


class Trader:
    """Sends orders (LIVE) or simulates them (paper), and keeps the paper PnL."""

    def __init__(self, session, live):
        self.session, self.live = session, live
        self.recent = {}  # signature -> stream events, to confirm our own trades
        self.paper = {}  # mint -> {'tokens', 'cost', 'sol_price'}
        self.pnl = {WSOL: 0.0, USDC: 0.0}
        self.pnl_sol = 0.0
        self.buys = self.sells = self.missed = 0

    def remember(self, event):
        signature = event.get('signature')
        if signature:
            self.recent.setdefault(signature, []).append(event)
            if len(self.recent) > 10000:
                del self.recent[next(iter(self.recent))]

    async def order(self, action, mint, quote, amount, price, tokens=0):
        """Buy `amount` of quote, or sell `amount` tokens ('100%' sells all). Returns the fill."""
        if not self.live:
            return self.paper_order(action, mint, quote, amount, price, tokens)
        payload = {
            **credentials(),
            'action': action,
            'mint': mint,
            'quoteMint': quote,
            'amount': amount,
            'denominatedInQuote': 'true' if action == 'buy' else 'false',
            'slippage': BUY_SLIPPAGE if action == 'buy' else SELL_SLIPPAGE,
            'priorityFee': PRIORITY_FEE,
        }
        signature = (await post(self.session, payload)).get('signature')
        if not isinstance(signature, str) or not signature:
            raise StopBot('API did not return a signature; check the wallet before retrying')
        logging.info('Sent %s mint=%s signature=%s', action, mint, signature)
        # Our own trade shows up on the stream with the real price and token amount.
        deadline = time.monotonic() + CONFIRM_SECONDS
        while time.monotonic() < deadline:
            for event in self.recent.get(signature, []):
                if (
                    event.get('action') == action
                    and event.get('mint') == mint
                    and positive(event.get('price'))
                    and positive(event.get('tokenAmount'))
                ):
                    logging.info('Confirmed %s mint=%s', action, mint)
                    return event
            await asyncio.sleep(0.1)
        raise StopBot(f'{signature} not seen on the stream; not retried. Check the wallet before restarting')

    def paper_order(self, action, mint, quote, amount, price, tokens):
        logging.info('PAPER %s mint=%s amount=%s quote=%s', action.upper(), mint, amount, quote)
        if action == 'buy':
            quantity = amount / price
            self.paper[mint] = {'tokens': quantity, 'cost': amount, 'sol_price': sol_price}
            self.buys += 1
        else:
            quantity = tokens * float(amount[:-1]) / 100 if str(amount).endswith('%') else float(amount)
            quantity = min(tokens, quantity)
            position = self.paper[mint]
            cost = position['cost'] * quantity / position['tokens']
            pnl = quantity * price - cost
            self.pnl[quote] += pnl
            self.pnl_sol += pnl if quote == WSOL else pnl / position['sol_price']
            self.sells += 1
            move = (quantity * price / cost - 1) * 100
            logging.info('PAPER RESULT mint=%s move=%+.2f%% pnl=%+.8f %s', mint, move, pnl, quote)
            position['tokens'] -= quantity
            position['cost'] -= cost
            if position['tokens'] <= 0:
                del self.paper[mint]
        return {'price': price, 'tokenAmount': quantity, 'quoteAmount': quantity * price}

    def report(self):
        if self.live:
            return
        counts = self.buys, self.sells, self.missed, len(self.paper)
        logging.info('Paper trades: %s buys, %s sells, %s missed, %s open', *counts)
        logging.info('Paper PnL: %+.8f SOL (WSOL %+.8f, USDC %+.8f), fees excluded', self.pnl_sol, *self.pnl.values())


class Copytrader:
    def __init__(self, trader):
        self.trader = trader
        # mint -> {'wallet', 'quote', 'tokens' (ours), 'copied_remaining' (theirs), 'decimals'}
        self.positions = {}

    def watched_wallet(self, event):
        """The watched wallet that traded in this event, if any."""
        signer = event.get('txSigner')
        involved = event.get('tradersInvolved')
        if isinstance(involved, dict):
            # The wallet that opened a position comes first, so its exits are found.
            owner = self.positions.get(event.get('mint'), {}).get('wallet')
            if owner in involved:
                return owner
            if signer in WALLETS and signer in involved:
                return signer
            return next((wallet for wallet in WALLETS if wallet in involved), None)
        if signer in WALLETS:
            return signer
        balances = event.get('postBalances') or {}
        return next((wallet for wallet in WALLETS if wallet in balances), None)

    @staticmethod
    def wallet_quantity(event, wallet):
        """Tokens this wallet traded. None when a bundled trade doesn't say."""
        breakdown = event.get('breakdown')
        if isinstance(breakdown, list):
            return sum(
                positive(trade.get('tokenAmount'))
                for trade in breakdown
                if isinstance(trade, dict) and trade.get('trader') == wallet and trade.get('action') == event['action']
            )
        if len(event.get('tradersInvolved') or {}) > 1:
            return None
        return positive(event.get('tokenAmount'))

    def wants(self, event):
        """Which stream events need a worker: trades by watched wallets in Pump pools."""
        if update_sol_price(event):
            return False
        return event.get('action') in ('buy', 'sell') and pump_pool(event) and bool(self.watched_wallet(event))

    async def on_event(self, event):
        if event.get('action') not in ('buy', 'sell') or not pump_pool(event):
            return
        wallet = self.watched_wallet(event)
        mint, quote = event.get('mint'), event.get('quoteMint')
        price, quantity = positive(event.get('price')), positive(event.get('tokenAmount'))
        if not wallet or not mint or not price or not quantity or quote not in (WSOL, USDC):
            return
        if TOKEN_MINTS and mint not in TOKEN_MINTS:
            return
        if event['action'] == 'buy':
            await self.copy_buy(event, wallet, mint, quote, price, quantity)
        elif mint in self.positions:
            await self.copy_sell(event, wallet, mint, quote, price)

    async def copy_buy(self, event, wallet, mint, quote, price, quantity):
        if mint in self.positions:
            # More buys by the same wallet raise the amount its later sells are measured against.
            if wallet == self.positions[mint]['wallet']:
                bought = self.wallet_quantity(event, wallet)
                if bought is not None:
                    self.positions[mint]['copied_remaining'] += bought
            return
        # First buy only: nobody in the transaction held the token before it.
        balances = event.get('postBalances') or {}
        held_after = sum(positive(values.get(mint)) for values in balances.values())
        if abs(held_after - quantity) > max(0.000001, quantity * 0.000001):
            return
        amount = min(positive(event.get('quoteAmount')) * BUY_FRACTION, sol_to_quote(quote, MAX_BUY_AMOUNT))
        if amount <= 0:
            return
        fill = await self.trader.order('buy', mint, quote, amount, price)
        self.positions[mint] = {
            'wallet': wallet,
            'quote': quote,
            'tokens': positive(fill['tokenAmount']),
            'copied_remaining': positive(balances.get(wallet, {}).get(mint)) or quantity,
            'decimals': event.get('decimals'),
        }

    async def copy_sell(self, event, wallet, mint, quote, price):
        position = self.positions[mint]
        if wallet != position['wallet']:
            return  # Only the wallet that opened the position closes it.
        sold = self.wallet_quantity(event, wallet)
        if sold is None:
            logging.warning('Skipped a bundled sell mint=%s; it does not say how much this wallet sold', mint)
            return
        if sold <= 0:
            return
        fraction = min(1.0, sold / position['copied_remaining'])
        amount = '100%' if fraction >= 1 else position['tokens'] * fraction
        if self.trader.live and fraction < 1:
            # Live partial sells send an exact token amount, rounded down to the token's decimals.
            decimals = position['decimals']
            if not isinstance(decimals, int) or isinstance(decimals, bool) or not 0 <= decimals <= 255:
                raise StopBot('Token decimals unknown; cannot size a partial sell. Check the wallet')
            with localcontext() as context:
                context.prec = max(28, decimals + 20)
                ours, theirs = Decimal(str(position['tokens'])), Decimal(str(position['copied_remaining']))
                exact = ours * Decimal(str(sold)) / theirs
                amount = float(exact.quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_DOWN))
            if amount == 0:
                position['copied_remaining'] = max(0, position['copied_remaining'] - sold)
                logging.info('Skipped a sell smaller than one token unit mint=%s', mint)
                return
        price = convert_price(price, quote, position['quote'])
        fill = await self.trader.order('sell', mint, position['quote'], amount, price, position['tokens'])
        position['tokens'] = max(0, position['tokens'] - positive(fill['tokenAmount']))
        position['copied_remaining'] = max(0, position['copied_remaining'] - sold)
        if fraction >= 1 or position['tokens'] == 0:
            del self.positions[mint]
        logging.info('Copied sell by %s mint=%s fraction=%.2f%%', wallet, mint, fraction * 100)

    async def on_tick(self, mint):
        """Returns False when the token's worker can stop."""
        return mint in self.positions


async def run_stream(trader, strategy):
    """Read the stream and give each token its own worker.

    Tokens are handled in parallel, so a slow order on one token doesn't delay
    the others. Each token's events stay in order.
    """
    queues, workers = {}, set()
    failure = asyncio.get_running_loop().create_future()

    def worker_done(task):
        workers.discard(task)
        if not task.cancelled() and task.exception() and not failure.done():
            failure.set_exception(task.exception())

    async def token_worker(mint, queue):
        while True:
            try:
                await strategy.on_event(await asyncio.wait_for(queue.get(), timeout=1))
            except asyncio.TimeoutError:
                pass
            if not await strategy.on_tick(mint) and queue.empty():
                del queues[mint]  # No await since the check, so nothing can be queued in between.
                return

    async def reader():
        delay = 0.4
        while True:
            connected_at = None
            try:
                async with websockets.connect(STREAM_URL, max_size=8 << 20) as socket:
                    connected_at = time.monotonic()
                    logging.info('Connected to the stream (%s)', 'LIVE' if trader.live else 'PAPER')
                    async for message in socket:
                        try:
                            event = json.loads(message)
                        except ValueError:
                            continue
                        if not isinstance(event, dict):
                            continue
                        trader.remember(event)
                        mint = event.get('mint') or 'market'
                        if not strategy.wants(event) and mint not in queues:
                            continue
                        if mint not in queues:
                            queues[mint] = asyncio.Queue(maxsize=10000)
                            task = asyncio.create_task(token_worker(mint, queues[mint]))
                            workers.add(task)
                            task.add_done_callback(worker_done)
                        try:
                            queues[mint].put_nowait(event)
                        except asyncio.QueueFull:
                            raise StopBot('Too many queued events for one token; check your positions') from None
                    reason = f'closed {socket.close_code} {socket.close_reason}'
            except (OSError, websockets.exceptions.WebSocketException) as exc:
                reason = f'{type(exc).__name__}: {exc}'
            if connected_at is not None and time.monotonic() - connected_at >= 30:
                delay = 0.4
            logging.warning('Stream disconnected (%s); reconnecting in %.1f s', reason, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    stream = asyncio.create_task(reader())
    try:
        done, _ = await asyncio.wait([stream, failure], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in [stream, *workers]:
            task.cancel()
        await asyncio.gather(stream, *workers, return_exceptions=True)
        failure.cancel()
        trader.report()


async def main():
    check_settings()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        trader = Trader(session, LIVE)
        await run_stream(trader, Copytrader(trader))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Stopped. Open positions stay in your wallet; restarting does not restore them.')
    except StopBot as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from None
