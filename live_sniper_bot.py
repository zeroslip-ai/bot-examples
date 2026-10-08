"""ZeroSlip sniper bot.

Buys one token you choose, or snipes new Pump.fun launches, then sells at
take profit, stop loss, or after a quiet period.

How it works:
1. Reads live trades from the ZeroSlip data stream (one websocket).
2. Buys when your entry rule matches.
3. Sells 100% when price hits TAKE_PROFIT or STOP_LOSS, or the token goes quiet.

LIVE = False (default) only logs paper trades; nothing is sent.
LIVE = True sends orders with your key to the ZeroSlip Trade API, which signs them.

Run: python3 live_sniper_bot.py   (Python 3.11+, pip install aiohttp websockets)
"""

# ---------------------------------- SETTINGS ----------------------------------
LIVE = False  # True sends real orders.
API_KEY = ''  # Your ZeroSlip wallet API key. Needed only when LIVE = True.
PRIVATE_KEY = ''  # Or your base58 private key. API_KEY is used if both are set.

TOKEN_MINT = ''  # Token to buy once. Empty = snipe new Pump.fun launches.
MIN_INITIAL_BUY = 20  # Launch mode: the creator's first buy must exceed this many SOL.
BUY_AMOUNT = 0.001  # SOL per buy. USDC pools buy the same value in USDC.
TAKE_PROFIT = 50  # Sell when the price is up this many % from entry.
STOP_LOSS = 20  # Sell when the price is down this many % from entry.
IDLE_SECONDS = 300  # Sell when the token has no activity for this long.
BUY_SLIPPAGE = 20  # %
SELL_SLIPPAGE = 99  # %
PRIORITY_FEE = 0.0001  # SOL
# ------------------------------------------------------------------------------

# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.12,<4", "websockets>=15,<16"]
# ///

import asyncio
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
    try:
        if not (BUY_AMOUNT > 0 and TAKE_PROFIT > 0 and IDLE_SECONDS > 0 and 0 < STOP_LOSS < 100):
            raise StopBot('BUY_AMOUNT, TAKE_PROFIT and IDLE_SECONDS must be above 0; STOP_LOSS between 0 and 100')
        if not (0 <= BUY_SLIPPAGE < 100 and 0 <= SELL_SLIPPAGE < 100):
            raise StopBot('BUY_SLIPPAGE and SELL_SLIPPAGE must be from 0 to below 100')
        if MIN_INITIAL_BUY < 0 or PRIORITY_FEE < 0:
            raise StopBot('MIN_INITIAL_BUY and PRIORITY_FEE cannot be negative')
    except TypeError:
        raise StopBot('Number settings must be numbers') from None
    if not isinstance(LIVE, bool):
        raise StopBot('LIVE must be True or False')
    if TOKEN_MINT:
        check_address(TOKEN_MINT, 'TOKEN_MINT')
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


class Sniper:
    def __init__(self, trader, clock=time.monotonic):
        self.trader, self.clock = trader, clock
        self.positions = {}  # mint -> {'entry', 'price', 'tokens', 'last', 'quote'}
        self.pending = {}  # paper buys waiting for the next trade price
        self.finished = set()  # TOKEN_MINT mode buys once

    def is_launch(self, event):
        return event.get('action') == 'create' and event.get('pool') == 'pump' and pump_pool(event)

    def wants(self, event):
        """Which stream events need a worker: launches, TOKEN_MINT, and tokens we hold."""
        if update_sol_price(event):
            return False
        mint = event.get('mint')
        return bool(mint) and (
            mint in self.positions or mint in self.pending or mint == TOKEN_MINT or self.is_launch(event)
        )

    async def on_event(self, event):
        mint, quote = event.get('mint'), event.get('quoteMint')
        price = positive(event.get('price'))
        if not mint:
            return
        if TOKEN_MINT:
            eligible = mint == TOKEN_MINT and event.get('action') in ('buy', 'sell')
        else:
            big_enough = positive(event.get('quoteAmount')) > sol_to_quote(quote, MIN_INITIAL_BUY)
            eligible = self.is_launch(event) and big_enough

        if mint in self.pending:
            # Paper buys fill at the next trade price, like a real order that lands a moment later.
            pending = self.pending[mint]
            if event.get('action') not in ('buy', 'sell') or not price or quote not in (WSOL, USDC):
                return
            if not TOKEN_MINT and not pump_pool(event):
                return
            price = convert_price(price, quote, pending['quote'])
            del self.pending[mint]
            ceiling = pending['decision'] / (1 - BUY_SLIPPAGE / 100)
            if price > ceiling:
                self.trader.missed += 1
                logging.info('PAPER MISS mint=%s price=%s above slippage limit %s', mint, price, ceiling)
                if TOKEN_MINT:
                    self.finished.add(mint)
                return
            await self.buy(mint, pending['quote'], price)
            return  # The fill event can't also trigger take profit.

        if mint not in self.positions:
            if not eligible or mint in self.finished or quote not in (WSOL, USDC) or not price:
                return
            if self.trader.live:
                await self.buy(mint, quote, price)
            else:
                self.pending[mint] = {'decision': price, 'quote': quote, 'last': self.clock()}
                logging.info('PAPER PENDING BUY mint=%s; waiting for the next trade price', mint)
            return

        # Any event for a held token resets the idle timer; only trades and liquidity moves set the price.
        position = self.positions[mint]
        position['last'] = self.clock()
        if event.get('action') not in ('buy', 'sell', 'add', 'remove') or not price or quote not in (WSOL, USDC):
            return
        if not TOKEN_MINT and not pump_pool(event):
            return
        position['price'] = convert_price(price, quote, position['quote'])
        change = (position['price'] - position['entry']) / position['entry'] * 100
        if change > TAKE_PROFIT:
            await self.sell(mint, 'take profit')
        elif change < -STOP_LOSS:
            await self.sell(mint, 'stop loss')

    async def buy(self, mint, quote, price):
        fill = await self.trader.order('buy', mint, quote, sol_to_quote(quote, BUY_AMOUNT), price)
        entry = positive(fill['price'])
        self.positions[mint] = {
            'entry': entry,
            'price': entry,
            'tokens': positive(fill['tokenAmount']),
            'last': self.clock(),
            'quote': quote,
        }
        logging.info('Opened %s; TP=%s%% SL=%s%%', mint, TAKE_PROFIT, STOP_LOSS)

    async def sell(self, mint, reason):
        position = self.positions[mint]
        await self.trader.order('sell', mint, position['quote'], '100%', position['price'], position['tokens'])
        del self.positions[mint]
        if TOKEN_MINT:
            self.finished.add(mint)
        logging.info('Closed %s (%s)', mint, reason)

    async def on_tick(self, mint):
        """Runs about once a second per token. Returns False when the token's worker can stop."""
        now = self.clock()
        if mint in self.pending and now - self.pending[mint]['last'] > IDLE_SECONDS:
            del self.pending[mint]
            self.trader.missed += 1
            if TOKEN_MINT:
                self.finished.add(mint)
            logging.info('PAPER MISS mint=%s; no trade before the idle timeout', mint)
        if mint in self.positions and now - self.positions[mint]['last'] > IDLE_SECONDS:
            await self.sell(mint, 'idle timeout')
        return mint in self.positions or mint in self.pending


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
        await run_stream(trader, Sniper(trader))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Stopped. Open positions stay in your wallet; restarting does not restore them.')
    except StopBot as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from None
