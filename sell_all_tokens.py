"""ZeroSlip sell-all-tokens script.

Sells every token in a wallet (or only the ones you list), keeping SOL and USDC.

How it works:
1. LIVE = False (default): reads WALLET's token balances from Solana RPC and
   logs what it would sell. Nothing is sent.
2. LIVE = True: asks the ZeroSlip Trade API for your key's wallet balances,
   then sells 100% of each selected token and waits for confirmation.
Frozen balances are skipped.

Run: python3 sell_all_tokens.py   (Python 3.11+, pip install aiohttp)
"""

# ---------------------------------- SETTINGS ----------------------------------
LIVE = False  # True sends real sell orders.
API_KEY = ''  # Your ZeroSlip wallet API key. Needed only when LIVE = True.
PRIVATE_KEY = ''  # Or your base58 private key. API_KEY is used if both are set.

WALLET = ''  # Wallet address to preview. LIVE sells from your key's wallet instead.
SELL_ALL = True  # True = every token. False = only TOKEN_MINTS.
TOKEN_MINTS = []  # Tokens to sell when SELL_ALL = False.
SLIPPAGE = 100  # %. 100 accepts any price.
PRIORITY_FEE = 0.0001  # SOL
# ------------------------------------------------------------------------------

# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.12,<4"]
# ///

import asyncio
import logging
import math

import aiohttp

TRADE_URL = 'https://api.zeroslip.ai'
RPC_URL = 'https://api.mainnet-beta.solana.com'  # Public Solana RPC. Use your own if rate limited.

WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
TOKEN_PROGRAMS = ('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb')
BASE58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


class StopBot(Exception):
    """Bad settings, or a sale we can't confirm. Stop instead of retrying."""


def positive(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) and value > 0 else 0.0


def check_address(value, name):
    # Base58 that decodes to 32 bytes, so a typo fails before anything runs.
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
    if not isinstance(LIVE, bool) or not isinstance(SELL_ALL, bool):
        raise StopBot('LIVE and SELL_ALL must be True or False')
    if WALLET:
        check_address(WALLET, 'WALLET')
    elif not LIVE:
        raise StopBot('Set WALLET to the wallet address you want to preview')
    for mint in TOKEN_MINTS:
        check_address(mint, 'TOKEN_MINTS')
    if SELL_ALL and TOKEN_MINTS:
        raise StopBot('Use SELL_ALL = True or list TOKEN_MINTS, not both')
    if not SELL_ALL and not TOKEN_MINTS:
        raise StopBot('List TOKEN_MINTS, or set SELL_ALL = True')
    try:
        if not 0 <= SLIPPAGE <= 100 or PRIORITY_FEE < 0:
            raise StopBot('SLIPPAGE must be from 0 to 100; PRIORITY_FEE cannot be negative')
    except TypeError:
        raise StopBot('Number settings must be numbers') from None
    if LIVE:
        credentials()


async def post(session, url, payload, service='API'):
    # Never log the payload or response body: they can contain your key.
    try:
        async with session.post(url, json=payload, allow_redirects=False) as response:
            if response.status != 200:
                raise StopBot(f'{service} returned HTTP {response.status}; check the wallet before retrying')
            data = await response.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        raise StopBot(f'No {service} response ({type(exc).__name__}); check the wallet before retrying') from exc
    if not isinstance(data, dict) or data.get('err') or data.get('error'):
        detail = data.get('error') if service == 'RPC' and isinstance(data, dict) else 'request rejected'
        raise StopBot(f'{service} error: {detail}; check the wallet before retrying')
    return data


async def balances(session):
    """mint -> {'balance', 'frozen'}"""
    if LIVE:
        # The balances of the wallet that will sell. The API charges a small fee for this call.
        data = await post(session, TRADE_URL, {**credentials(), 'action': 'getBalances'})
        if not isinstance(data.get('tokenBalances'), dict):
            raise StopBot('API did not return token balances')
        return data['tokenBalances']
    result = {}
    for program in TOKEN_PROGRAMS:
        params = [WALLET, {'programId': program}, {'encoding': 'jsonParsed', 'commitment': 'confirmed'}]
        request = {'jsonrpc': '2.0', 'id': 1, 'method': 'getTokenAccountsByOwner', 'params': params}
        data = await post(session, RPC_URL, request, 'RPC')
        accounts = data.get('result', {}).get('value')
        if not isinstance(accounts, list):
            raise StopBot('RPC did not return token accounts')
        for account in accounts:
            info = account['account']['data']['parsed']['info']
            entry = result.setdefault(info['mint'], {'balance': 0, 'frozen': False})
            entry['balance'] += positive(info['tokenAmount'].get('uiAmountString'))
            entry['frozen'] = entry['frozen'] or info.get('state') == 'frozen'
    return result


def select(holdings):
    # SOL and USDC are what you sell into, so they are always kept.
    return {
        mint: info
        for mint, info in holdings.items()
        if mint not in (WSOL, USDC) and (SELL_ALL or mint in TOKEN_MINTS) and positive(info.get('balance'))
    }


async def main():
    check_settings()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        selected = select(await balances(session))
        if not selected:
            logging.info('No matching token balances to sell.')
            return
        for mint, info in selected.items():
            if info.get('frozen'):
                logging.warning('Skipping frozen balance mint=%s', mint)
                continue
            logging.info('%s SELL 100%% mint=%s balance=%s', 'LIVE' if LIVE else 'PREVIEW', mint, info['balance'])
            if not LIVE:
                continue
            order = {
                **credentials(),
                'action': 'sell',
                'mint': mint,
                'amount': '100%',
                'denominatedInQuote': 'false',
                'slippage': SLIPPAGE,
                'priorityFee': PRIORITY_FEE,
                'guaranteedDelivery': 'true',
            }
            data = await post(session, TRADE_URL, order)
            if data.get('confirmed') is not True or not data.get('signature'):
                raise StopBot('Sale not confirmed; check the wallet before retrying')
            trades = [trade for trade in data.get('trades', []) if isinstance(trade, dict)]
            quote = trades[0].get('quoteMint') if trades else 'auto-selected'
            logging.info('Sold mint=%s for %s signature=%s', mint, quote, data['signature'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Stopped.')
    except StopBot as exc:
        logging.error('%s', exc)
        raise SystemExit(1) from None
