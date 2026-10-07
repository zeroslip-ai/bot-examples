"""Preview or sell selected token balances; --all explicitly selects every token."""
import logging

import aiohttp

from bot_common import WSOL, api_post, credentials, positive, run, setup, StopBot

TOKEN_PROGRAMS = (
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
)


async def balances(cfg, session, live):
    if live:
        # Defaults to the credential's wallet; never scan one wallet then sell
        # from a different one. This documented read incurs an API balance fee.
        data = await api_post(session, cfg['network']['trade_url'], {**credentials(), 'action': 'getBalances'})
        result = data.get('tokenBalances')
        if not isinstance(result, dict):
            raise StopBot('API did not return token balances')
        return result
    result = {}
    for program in TOKEN_PROGRAMS:
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': 'getTokenAccountsByOwner',
                   'params': [cfg['wallet']['public_key'], {'programId': program},
                              {'encoding': 'jsonParsed', 'commitment': 'confirmed'}]}
        data = await api_post(session, cfg['network']['rpc_url'], payload)
        accounts = data.get('result', {}).get('value')
        if not isinstance(accounts, list):
            raise StopBot('RPC did not return token accounts')
        for account in accounts:
            info = account['account']['data']['parsed']['info']
            amount = info['tokenAmount']
            quantity = positive(amount.get('uiAmountString'))
            mint = info['mint']
            entry = result.setdefault(mint, {'balance': 0, 'frozen': False})
            entry['balance'] += quantity
            entry['frozen'] = entry['frozen'] or info.get('state') == 'frozen'
    return result


def select_balances(cfg, holdings, all_tokens):
    wanted = cfg['sell']['token_mints']
    # Do not turn the configured quote balance or WSOL back into itself.
    excluded = {cfg['trade']['quote_mint'], WSOL}
    return {mint: info for mint, info in holdings.items()
            if mint not in excluded and (all_tokens or mint in wanted) and positive(info.get('balance'))}


async def sell_selected(cfg, session, live, all_tokens):
    selected = select_balances(cfg, await balances(cfg, session, live), all_tokens)
    if not selected:
        logging.info('No matching token balances to sell.')
        return
    for mint, info in selected.items():
        if info.get('frozen'):
            logging.warning('Skipping frozen balance mint=%s; no burn is attempted', mint)
            continue
        logging.info('%s SELL 100%% mint=%s balance=%s', 'LIVE' if live else 'PREVIEW', mint, info['balance'])
        if not live:
            continue
        data = await api_post(session, cfg['network']['trade_url'], {
            **credentials(), 'action': 'sell', 'mint': mint, 'amount': '100%',
            'denominatedInQuote': 'false', 'slippage': cfg['sell']['slippage'],
            'priorityFee': cfg['trade']['priority_fee'], 'guaranteedDelivery': 'true',
        })
        if data.get('confirmed') is not True or not data.get('signature'):
            raise StopBot('Sale not confirmed; check the wallet before retrying. No burn was attempted')
        quote = next((trade.get('quoteMint') for trade in data.get('trades', []) if isinstance(trade, dict)), 'auto-selected')
        logging.info('Sale confirmed mint=%s quote=%s signature=%s', mint, quote, data['signature'])


async def main():
    cfg, args = setup('sell')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        await sell_selected(cfg, session, args.live, args.all)


if __name__ == '__main__':
    run(main)
