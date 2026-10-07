"""Unmodified calculation from PumpApi-Agent e395fd7 (Unlicense).
Only imports, notification wiring, downloads and reporting were omitted.
Used as a regression oracle; never connects to the network.
"""
import logging
import traceback

HOURS = 10
ALLOW_GAPS = False
WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
SOL_USDC_POOL = 'Gf7sXMoP8iRw4iiXmJ1nq4vxcRycbGXy5RL8a8LnTd3v'

TP = 50
SL = 20
IDLE_MS = 5 * 60 * 1000
BUY_LATENCY_MS = 400   # delay between deciding to buy and the tx landing
SELL_LATENCY_MS = 400  # same for sells
SLIPPAGE_BUY = 20      # buy reverts if price moved past decision_price/(1-slippage/100) while landing
PUMPAPI_FEE = 0.0025   # 0.25% pumpapi fee per side
FORCE_MAJEURE = 0.0005 # 0.05% per side for different uncounted risks

global_dict = {'sol_price': 80}
SOL_BUY_THRESHOLD = 20 # pump fee is 1.25%, so it’s disadvantageous for any dev to sell right after creation because the loss will be around 0.5 SOL.
SOL_AMOUNT_TO_BUY = 0.001
strategy_dict = {WSOL: {'buy_threshold': SOL_BUY_THRESHOLD, 'amount_to_buy': SOL_AMOUNT_TO_BUY, 'ui_name': 'sol'},
                 USDC: {'buy_threshold': SOL_BUY_THRESHOLD*global_dict['sol_price'], 'amount_to_buy': SOL_AMOUNT_TO_BUY*global_dict['sol_price'], 'ui_name': 'USDC'}}

positions = {}   # mint -> dict, state: pending_buy / held / pending_sell
trades = []
stats = {'seen': 0, 'creates': 0, 'buys': 0, 'missed': 0, 'skipped': 0, 'last_ts': 0}

def fill_buy(mint, pos):
    try:
        dec, fill = pos['decision_price'], pos['cur_price']
        max_price = dec / (1 - min(SLIPPAGE_BUY, 99.9) / 100)
        if fill > max_price:
            stats['missed'] += 1
            positions.pop(mint, None)
            logging.info(f"MISS {mint} ran +{(fill/dec-1)*100:.0f}% in {BUY_LATENCY_MS}ms, past {SLIPPAGE_BUY}% slippage")
            return
        amount, pool_fee, q = pos['amount'], pos['entry_pool_fee'], pos['quote_mint']
        pos.update(state='held', entry_price=fill, quote_spent=amount,
                   tokens=amount * (1 - PUMPAPI_FEE - FORCE_MAJEURE - pool_fee) / fill, sol_price=global_dict['sol_price'],
                   spent_sol=amount if q == WSOL else amount / global_dict['sol_price'],
                   buy_slip=(fill / dec - 1) * 100, last_ts=pos['buy_fill_ts'])
        stats['buys'] += 1
        logging.info(f"BUY  {mint} q={strategy_dict[q]['ui_name']} dec={dec:.3e} fill={fill:.3e} ({(fill/dec-1)*100:+.1f}% slip) spent={amount:.6f}")
    except:
        logging.info(f'ERROR! traceback in fill_buy --> {traceback.format_exc()}')

def trigger_sell(pos, decision_ts, reason):
    pos['state'] = 'pending_sell'
    pos['sell_reason'] = reason
    pos['sell_fill_ts'] = decision_ts + SELL_LATENCY_MS


def close(mint, pos, exit_price, reason):
    try:
        gross = pos['tokens'] * exit_price
        cost_in = PUMPAPI_FEE + FORCE_MAJEURE + pos['entry_pool_fee']
        cost_out = PUMPAPI_FEE + FORCE_MAJEURE + pos['last_pool_fee']
        proceeds = gross * (1 - cost_out)
        pnl = proceeds - pos['quote_spent']
        fee_quote = pos['quote_spent'] * cost_in + gross * cost_out
        is_sol = pos['quote_mint'] == WSOL
        pnl_sol = pnl if is_sol else pnl / pos['sol_price']
        pct = (exit_price - pos['entry_price']) / pos['entry_price'] * 100
        trades.append({'mint': mint, 'quote_mint': pos['quote_mint'], 'reason': reason, 'pct': pct,
                       'pnl': pnl, 'pnl_sol': pnl_sol, 'spent_sol': pos['spent_sol'],
                       'fee_sol': fee_quote if is_sol else fee_quote / pos['sol_price'], 'buy_slip': pos['buy_slip']})
        positions.pop(mint, None)
        logging.info(f"SELL {reason:4} {mint} exit={exit_price:.3e} move={pct:+.1f}% pnl={pnl:+.6f} {strategy_dict[pos['quote_mint']]['ui_name']} ({pnl_sol:+.6f} SOL)")
    except:
        logging.info(f'ERROR! traceback in close --> {traceback.format_exc()}')

def resolve_pending(ts):
    for mint in list(positions):
        pos = positions[mint]
        st = pos['state']
        if st == 'pending_buy' and pos['buy_fill_ts'] <= ts:
            fill_buy(mint, pos)
        elif st == 'pending_sell' and pos['sell_fill_ts'] <= ts:
            close(mint, pos, pos['cur_price'], pos['sell_reason'])
        elif st == 'held' and ts - pos['last_ts'] > IDLE_MS:
            trigger_sell(pos, pos['last_ts'] + IDLE_MS, 'idle')


def handle_event(event):
    stats['seen'] += 1
    ts = event['timestamp']
    stats['last_ts'] = ts
    resolve_pending(ts)
    if event['action'] not in ('buy', 'sell', 'add', 'remove', 'create', 'migrate', 'createPool'): # only trade  events
        return
    mint = event['mint']
    if event['poolId'] == SOL_USDC_POOL:
        global_dict['sol_price'] = 1 / event['price']
        strategy_dict[USDC]['buy_threshold'] = strategy_dict[WSOL]['buy_threshold'] * global_dict['sol_price']
        strategy_dict[USDC]['amount_to_buy'] = strategy_dict[WSOL]['amount_to_buy'] * global_dict['sol_price']
    elif event['action'] == 'create' and event['pool'] == 'pump' and not event['mayhemMode']: # mayhemMode is present only in pump and pump-amm pool events.  Skipping it to be able always to sell
        stats['creates'] += 1
        q = event['quoteMint']
        if (q == WSOL or q == USDC) and event['quoteAmount'] > strategy_dict[q]['buy_threshold'] and mint not in positions:
            pool_fee = event['poolFeeRate']
            positions[mint] = {'state': 'pending_buy', 'quote_mint': q, 'amount': strategy_dict[q]['amount_to_buy'],
                               'decision_price': event['price'], 'cur_price': event['price'],
                               'entry_pool_fee': pool_fee, 'last_pool_fee': pool_fee,
                               'buy_fill_ts': ts + BUY_LATENCY_MS, 'buy_slip': 0.0}
    elif mint in positions:
        pos = positions[mint]
        if event['action'] in ('buy', 'sell', 'add', 'remove', 'migrate') and \
           (event['pool'] == 'pump' or (event['pool'] == 'pump-amm' and event['poolCreatedBy'] == 'pump')): # avoiding scam pools
            price = event['price']
            pos['cur_price'] = price
            pos['last_pool_fee'] = event['poolFeeRate']
            if pos['state'] == 'held':
                pct = (price - pos['entry_price']) / pos['entry_price'] * 100
                if pct > TP or pct < -SL:
                    trigger_sell(pos, ts, 'tp' if pct > TP else 'sl')
        if pos['state'] == 'held':
            pos['last_ts'] = ts


def finish(window):
    for mint in list(positions):           # any buy mid-flight would have landed -> fill it
        if positions[mint]['state'] == 'pending_buy':
            fill_buy(mint, positions[mint])
    for mint in list(positions):           # close the rest at last known price
        pos = positions[mint]
        if pos['state'] == 'pending_sell':
            close(mint, pos, pos['cur_price'], pos['sell_reason'])
        else:
            reason = 'idle' if stats['last_ts'] - pos['last_ts'] > IDLE_MS else 'end'
            close(mint, pos, pos['cur_price'], reason)

