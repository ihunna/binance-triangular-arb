import ccxt.async_support as ccxt
import pandas as pd
import time
import logging
import itertools
import numpy as np
import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime
import random
import math
import csv
import traceback
from rich.console import Console
from rich.table import Table
from rich import box

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler for bot.log
log_file = 'bot.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

console = Console()

# Thread-safe locks
balance_lock = asyncio.Lock()
history_lock = asyncio.Lock()

# Load .env
parent_folder = os.path.abspath(os.path.dirname(__file__))
env_path = os.path.join(parent_folder, '.env')
logger.info(f"Attempting to load .env file from: {env_path}")

if not os.path.exists(env_path):
    logger.warning(f".env file not found at {env_path}. Creating empty .env file.")
    with open(env_path, 'w') as f:
        pass
    os.chmod(env_path, 0o600)

load_dotenv(env_path)

# Validate .env content
with open(env_path, 'r') as f:
    env_content = f.read().strip()
    logger.info(f".env content (sanitized): {'Non-empty' if env_content else 'Empty'}")

# Load API keys
EXCHANGES = ['bybit', 'kucoin', 'binance']
API_KEYS = {}
for exchange in EXCHANGES:
    API_KEYS[exchange] = {
        'apiKey': os.getenv(f'{exchange.upper()}_API_KEY'),
        'secret': os.getenv(f'{exchange.upper()}_SECRET'),
        'passphrase': os.getenv(f'{exchange.upper()}_PASSPHRASE') if exchange == 'kucoin' else None
    }
    logger.info(f"{exchange.upper()}_API_KEY loaded: {'True' if API_KEYS[exchange]['apiKey'] else 'False'}")
    logger.info(f"{exchange.upper()}_SECRET loaded: {'True' if API_KEYS[exchange]['secret'] else 'False'}")
    if exchange == 'kucoin':
        passphrase = API_KEYS[exchange]['passphrase']
        logger.info(f"{exchange.upper()}_PASSPHRASE loaded: {'True' if passphrase else 'False'}")
        if passphrase:
            logger.info(f"KuCoin passphrase characteristics: length={len(passphrase)}")

# Configuration
CONFIG = {
    'exchanges': {
        'bybit': {
            'active': False,
            'apiKey': API_KEYS['bybit']['apiKey'],
            'secret': API_KEYS['bybit']['secret'],
            'enableRateLimit': True
        },
        'kucoin': {
            'active': True,
            'apiKey': API_KEYS['kucoin']['apiKey'],
            'secret': API_KEYS['kucoin']['secret'],
            'passphrase': API_KEYS['kucoin']['passphrase'],
            'enableRateLimit': True
        },
        'binance': {
            'active': False,
            'apiKey': API_KEYS['binance']['apiKey'],
            'secret': API_KEYS['binance']['secret'],
            'enableRateLimit': True
        }
    },
    'coin_count': 10,
    'quote_currencies': ['USDT', 'USDC', 'KCS', 'BTC', 'ETH', 'BNB'],
    'avg_trades': 5,
    'fee_rate': {},
    'default_fee_rate': {
        'bybit': {'maker': 0.0002, 'taker': 0.001},
        'kucoin': {'maker': 0.001, 'taker': 0.001},
        'binance': {'maker': 0.001, 'taker': 0.001}
    },
    'use_kcs_discount': False,
    'use_bnb_discount': False,
    'min_profit_threshold': 0.005,
    'max_profit_cap': 0.1,
    'max_exposure': 0.1,
    'max_volatility': 0.02,
    'stop_loss_threshold': 0.8,
    'sleep_interval': 5,
    'initial_balance': 10000.0,
    'slippage_tolerance': 0.005,
    'trade_log_file': 'trades.csv',
    'order_book_type': 'market',
    'stake_amount': 1000.0
}

# Initialize exchanges
async def initialize_exchange(name, config):
    exchange = None
    try:
        if name == 'bybit':
            exchange = ccxt.bybit(config)
        elif name == 'kucoin':
            if not config.get('passphrase'):
                logger.warning(f"KuCoin passphrase missing in .env. Prompting for manual input.")
                config['passphrase'] = input(f"Enter {name} Passphrase: ").strip()
                API_KEYS[name]['passphrase'] = config['passphrase']
                logger.info(f"KuCoin passphrase set: length={len(config['passphrase'])}")
            exchange = ccxt.kucoin(config)
            await exchange.load_markets()
            logger.info(f"KuCoin authentication successful")
        elif name == 'binance':
            exchange = ccxt.binance(config)
        logger.info(f"Initialized {name} exchange")
        return exchange
    except ccxt.AuthenticationError as e:
        logger.error(f"Authentication failed for {name}: {e}\n{traceback.format_exc()}")
        if exchange:
            await exchange.close()
        return None
    except Exception as e:
        logger.error(f"Failed to initialize {name}: {e}\n{traceback.format_exc()}")
        if exchange:
            await exchange.close()
        return None

exchanges = {}
for name in EXCHANGES:
    if CONFIG['exchanges'][name]['active'] and API_KEYS[name]['apiKey'] and API_KEYS[name]['secret']:
        exchange = asyncio.run(initialize_exchange(name, CONFIG['exchanges'][name]))
        if exchange:
            exchanges[name] = exchange
    elif CONFIG['exchanges'][name]['active']:
        logger.warning(f"{name} is active but missing API keys. Prompting for manual input.")
        API_KEYS[name]['apiKey'] = input(f"Enter {name} API Key: ").strip()
        API_KEYS[name]['secret'] = input(f"Enter {name} Secret Key: ").strip()
        if name == 'kucoin':
            API_KEYS[name]['passphrase'] = input(f"Enter {name} Passphrase: ").strip()
            CONFIG['exchanges']['kucoin']['password'] = API_KEYS[name]['passphrase']
            logger.info(f"KuCoin passphrase set: length={len(API_KEYS[name]['passphrase'])}")
        if API_KEYS[name]['apiKey'] and API_KEYS[name]['secret']:
            exchange = asyncio.run(initialize_exchange(name, CONFIG['exchanges'][name]))
            if exchange:
                exchanges[name] = exchange
        else:
            logger.warning(f"Skipping {name} due to missing API keys.")

exchanges = {k: v for k, v in exchanges.items() if v is not None}

if not exchanges:
    raise ValueError("No valid exchanges initialized. Please set 'active': True and provide valid API keys for at least one exchange.")

# Mock funds
mock_balance = {
    'USDT': CONFIG['initial_balance']
}
initial_balance = CONFIG['initial_balance']
trade_history = []
recent_profits = []

# Initialize trades.csv
with open(CONFIG['trade_log_file'], 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Timestamp', 'Exchange', 'Pair1', 'Pair2', 'Pair3', 'Initial_Amount', 'Final_Amount', 'Profit', 'Profit_Percentage', 'Balance_USDT'])

async def log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    async with history_lock:
        with open(CONFIG['trade_log_file'], 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp,
                exchange.id,
                pair1,
                pair2,
                pair3,
                trade_details['initial_amount'],
                trade_details['final_amount'],
                trade_details['avg_profit'],
                trade_details['avg_profit_percentage'],
                mock_balance['USDT']
            ])

def print_trade_update(exchange, pair1, pair2, pair3, trade_details):
    table = Table(title=f"Trade Update on {exchange.id}", box=box.MINIMAL, style="cyan")
    table.add_column("Field", style="magenta")
    table.add_column("Value", justify="right")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    profit_color = "green" if trade_details['avg_profit'] > 0 else "red"
    table.add_row("Timestamp", timestamp)
    table.add_row("Exchange", exchange.id)
    table.add_row("Pair 1", pair1)
    table.add_row("Pair 2", pair2)
    table.add_row("Pair 3", pair3)
    table.add_row("Initial Amount", f"{trade_details['initial_amount']:.2f} USDT")
    table.add_row("Final Amount", f"{trade_details['final_amount']:.2f} USDT")
    table.add_row("Profit", f"[{profit_color}]{trade_details['avg_profit']:.2f} USDT[/{profit_color}]")
    table.add_row("Profit %", f"[{profit_color}]{trade_details['avg_profit_percentage']:.2f}%[/{profit_color}]")
    table.add_row("Current Balance", f"{mock_balance['USDT']:.2f} USDT")
    console.print(table)

async def print_summary():
    async with history_lock:
        total_trades = len(trade_history)
        wins = len([t for t in trade_history if t['avg_profit'] > 0])
        losses = len([t for t in trade_history if t['avg_profit'] <= 0])
        win_loss_ratio = wins / losses if losses > 0 else wins
        total_profit_loss = mock_balance['USDT'] - initial_balance
    table = Table(title="Trading Summary", box=box.MINIMAL, style="cyan")
    table.add_column("Field", style="magenta")
    table.add_column("Value", justify="right")
    profit_color = "green" if total_profit_loss > 0 else "red"
    table.add_row("Current Balance", f"{mock_balance['USDT']:.2f} USDT")
    table.add_row("Total Profit/Loss", f"[{profit_color}]{total_profit_loss:.2f} USDT[/{profit_color}]")
    table.add_row("Total Trades", str(total_trades))
    table.add_row("Wins", str(wins))
    table.add_row("Losses", str(losses))
    table.add_row("Win/Loss Ratio", f"{win_loss_ratio:.2f}")
    table.add_row("Recent Profits", f"{[f'{p:.2f}%' for p in recent_profits]}")
    console.print(table)

async def fetch_trading_fees(exchange, valid_pairs):
    try:
        fee_dict = {}
        fee_type = 'maker' if CONFIG['order_book_type'] == 'limit' else 'taker'
        default_fee = CONFIG['default_fee_rate'][exchange.id][fee_type]
        
        if exchange.id == 'kucoin':
            liquid_pair = 'BTC/USDT'
            if liquid_pair in valid_pairs:
                try:
                    fee_data = await exchange.fetch_trading_fee(liquid_pair)
                    fee_rate = fee_data[fee_type]
                    if CONFIG['use_kcs_discount']:
                        fee_rate *= 0.8
                    fee_dict = {pair: fee_rate for pair in valid_pairs}
                    logger.info(f"Fetched fee for {liquid_pair}: {fee_type}={fee_rate:.4f}, applying to {len(valid_pairs)} pairs on {exchange.id}")
                except Exception as e:
                    logger.warning(f"Error fetching fee for {liquid_pair} on {exchange.id}: {e}. Using default fee {default_fee:.4f} for {len(valid_pairs)} pairs")
                    fee_dict = {pair: default_fee for pair in valid_pairs}
            else:
                logger.warning(f"Liquid pair {liquid_pair} not in valid pairs. Using default fee {default_fee:.4f} for {len(valid_pairs)} pairs")
                fee_dict = {pair: default_fee for pair in valid_pairs}
        else:
            fees = await exchange.fetch_trading_fees()
            for pair in valid_pairs:
                if pair in fees:
                    fee_rate = fees[pair][fee_type]
                    if exchange.id == 'binance' and CONFIG['use_bnb_discount']:
                        fee_rate *= 0.75
                    fee_dict[pair] = fee_rate
                    logger.debug(f"Fetched fee for {pair}: {fee_type}={fee_rate:.4f}")
                else:
                    logger.warning(f"Fee not found for {pair} on {exchange.id}. Using default fee {default_fee:.4f}")
                    fee_dict[pair] = default_fee
            logger.info(f"Fetched trading fees for {len(fee_dict)} pairs on {exchange.id}")
        
        return fee_dict
    except Exception as e:
        logger.error(f"Error fetching trading fees for {exchange.id}: {e}\n{traceback.format_exc()}")
        fee_dict = {pair: CONFIG['default_fee_rate'][exchange.id][fee_type] for pair in valid_pairs}
        logger.info(f"Using default {fee_type} fees for {len(fee_dict)} pairs on {exchange.id}")
        return fee_dict

async def fetch_market_data(exchange, symbol):
    try:
        base, quote = symbol.split('/')
        if CONFIG['order_book_type'] == 'market':
            ticker = await exchange.fetch_ticker(symbol)
            bid = ticker['bid'] if 'bid' in ticker else None
            ask = ticker['ask'] if 'ask' in ticker else None
            quote_volume = ticker.get('quoteVolume', 0)
            usdt_volume = quote_volume
            if quote != 'USDT':
                usdt_pair = f"{quote}/USDT"
                try:
                    usdt_ticker = await exchange.fetch_ticker(usdt_pair)
                    bid_price = usdt_ticker['bid'] if 'bid' in usdt_ticker else None
                    if bid_price:
                        usdt_volume = quote_volume * bid_price
                    else:
                        logger.warning(f"No bid price for {usdt_pair}. Assuming zero USDT volume.")
                        usdt_volume = 0
                except Exception as e:
                    logger.warning(f"Error fetching {usdt_pair} for volume conversion: {e}. Assuming zero USDT volume.")
                    usdt_volume = 0
            return bid, ask, quote_volume, quote, usdt_volume, None, None
        else:
            order_book = await exchange.fetch_order_book(symbol, limit=1)
            bid = order_book['bids'][0][0] if order_book['bids'] else None
            ask = order_book['asks'][0][0] if order_book['asks'] else None
            bid_volume = order_book['bids'][0][1] if order_book['bids'] else 0
            ask_volume = order_book['asks'][0][1] if order_book['asks'] else 0
            usdt_bid_volume = bid_volume
            usdt_ask_volume = ask_volume
            if quote != 'USDT':
                usdt_pair = f"{quote}/USDT"
                try:
                    usdt_ticker = await exchange.fetch_ticker(usdt_pair)
                    bid_price = usdt_ticker['bid'] if 'bid' in usdt_ticker else None
                    if bid_price:
                        usdt_bid_volume = bid_volume * bid_price
                        usdt_ask_volume = ask_volume * bid_price
                    else:
                        logger.warning(f"No bid price for {usdt_pair}. Assuming zero USDT volume.")
                        usdt_bid_volume = 0
                        usdt_ask_volume = 0
                except Exception as e:
                    logger.warning(f"Error fetching {usdt_pair} for volume conversion: {e}. Assuming zero USDT volume.")
                    usdt_bid_volume = 0
                    usdt_ask_volume = 0
            return bid, ask, None, quote, usdt_ask_volume, bid_volume, ask_volume
    except ccxt.RateLimitExceeded as e:
        logger.warning(f"Rate limit exceeded on {exchange.id}. Backing off...")
        logger.error(f"Rate limit error details: {e}\n{traceback.format_exc()}")
        await asyncio.sleep(2 ** int(math.log2(random.randint(1, 5))))
        return None, None, 0, None, 0, None, None
    except Exception as e:
        logger.error(f"Error fetching market data for {symbol} on {exchange.id}: {e}\n{traceback.format_exc()}")
        return None, None, 0, None, 0, None, None

async def calculate_volatility(exchange, symbol, periods=5, interval='1m'):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, interval, limit=periods)
        prices = [candle[4] for candle in ohlcv]
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        volatility = np.std(returns) if returns else 0
        return volatility
    except Exception as e:
        logger.error(f"Error calculating volatility for {symbol} on {exchange.id}: {e}\n{traceback.format_exc()}")
        return float('inf')

async def fetch_top_coins(exchange, coin_count):
    try:
        markets = await exchange.load_markets()
        tickers = await exchange.fetch_tickers()
        quote_currencies = [q for q in CONFIG['quote_currencies'] if any(f"/{q}" in symbol for symbol in markets)]
        valid_pairs = []
        for symbol in tickers:
            if markets[symbol].get('active', True):
                base, quote = symbol.split('/')
                if quote in quote_currencies:
                    valid_pairs.append((symbol, tickers[symbol].get('quoteVolume', 0)))
        valid_pairs.sort(key=lambda x: x[1], reverse=True)
        base_coins = []
        for symbol, _ in valid_pairs:
            base = symbol.split('/')[0]
            if base not in base_coins and base not in quote_currencies:
                base_coins.append(base)
                if len(base_coins) >= coin_count:
                    break
        coins = list(set(base_coins + quote_currencies))
        for coin in coins:
            if coin not in mock_balance:
                mock_balance[coin] = 0.0
        logger.info(f"Fetched {len(coins)} coins on {exchange.id}: {coins}")
        return coins
    except Exception as e:
        logger.error(f"Error fetching top coins on {exchange.id}: {e}\n{traceback.format_exc()}")
        return CONFIG['quote_currencies']

async def generate_triplets(coins, exchange):
    try:
        markets = await exchange.load_markets()
        valid_pairs = set(markets.keys())
        logger.info(f"Valid pairs on {exchange.id}: {sorted(valid_pairs)}")
        triplets = []
        for base, inter, quote in itertools.permutations(coins, 3):
            # Forward path: e.g., SOL/USDT, BTC/SOL, BTC/USDT
            pair1 = f"{base}/{quote}"
            pair2 = f"{inter}/{base}"
            pair3 = f"{inter}/{quote}"
            if pair1 in valid_pairs and pair2 in valid_pairs and pair3 in valid_pairs:
                base1, quote1 = pair1.split('/')
                base2, quote2 = pair2.split('/')
                base3, quote3 = pair3.split('/')
                if quote1 == quote3 and base2 == base1 and base3 == inter:
                    triplets.append(('forward', pair1, pair2, pair3))
            # Reverse path: e.g., SOL/USDT, BTC/USDT, SOL/BTC
            pair1_r = f"{base}/{quote}"
            pair2_r = f"{inter}/{quote}"
            pair3_r = f"{base}/{inter}"
            if pair1_r in valid_pairs and pair2_r in valid_pairs and pair3_r in valid_pairs:
                base1_r, quote1_r = pair1_r.split('/')
                base2_r, quote2_r = pair2_r.split('/')
                base3_r, quote3_r = pair3_r.split('/')
                if quote1_r == quote2_r and base3_r == base1_r and quote3_r == base2_r:
                    triplets.append(('reverse', pair1_r, pair2_r, pair3_r))
        logger.info(f"Generated {len(triplets)} triplets on {exchange.id}")
        return triplets, valid_pairs
    except Exception as e:
        logger.error(f"Error generating triplets on {exchange.id}: {e}\n{traceback.format_exc()}")
        return [], set()

async def calculate_triangular_arbitrage(exchange, triplet_type, pair1, pair2, pair3, amount, avg_trades):
    try:
        prices = []
        reasons = []
        fees = CONFIG['fee_rate'].get(exchange.id, {})
        fee_type = 'maker' if CONFIG['order_book_type'] == 'limit' else 'taker'
        default_fee = CONFIG['default_fee_rate'][exchange.id][fee_type]
        fee1 = fees.get(pair1, default_fee)
        fee2 = fees.get(pair2, default_fee)
        fee3 = fees.get(pair3, default_fee)
        for _ in range(avg_trades):
            bid1, ask1, vol1, quote_curr1, usdt_vol1, bid_vol1, ask_vol1 = await fetch_market_data(exchange, pair1)
            bid2, ask2, vol2, quote_curr2, usdt_vol2, bid_vol2, ask_vol2 = await fetch_market_data(exchange, pair2)
            bid3, ask3, vol3, quote_curr3, usdt_vol3, bid_vol3, ask_vol3 = await fetch_market_data(exchange, pair3)
            if not all([bid1, ask1, bid2, ask2, bid3, ask3]):
                reasons.append(f"Missing price data: {pair1} (bid={bid1}, ask={ask1}), "
                              f"{pair2} (bid={bid2}, ask={ask2}), {pair3} (bid={bid3}, ask={ask3})")
                logger.error(f"Missing price data for {pair1}, {pair2}, {pair3} on {exchange.id}: {reasons[-1]}")
                return None, 0.0
            adjusted_amount = amount
            if CONFIG['order_book_type'] == 'limit' and ask_vol1 is not None:
                usdt_ask_vol1 = ask_vol1 * ask1 if quote_curr1 == 'USDT' else ask_vol1 * (await exchange.fetch_ticker(f"{quote_curr1}/USDT"))['bid']
                if usdt_ask_vol1 < amount:
                    adjusted_amount = usdt_ask_vol1
                    logger.info(f"Adjusted stake amount from {amount:.2f} to {adjusted_amount:.2f} USDT due to low ask volume on {pair1}")
            min_usdt_volume = min(usdt_vol1, usdt_vol2, usdt_vol3)
            if min_usdt_volume < 1000:
                volume_log = (
                    f"{pair1} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
                    f"{vol1 if vol1 is not None else ask_vol1:.2f} {quote_curr1}, {usdt_vol1:.2f} USDT @ bid {bid1:.6f}, ask {ask1:.6f}), "
                    f"{pair2} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
                    f"{vol2 if vol2 is not None else ask_vol2:.2f} {quote_curr2}, {usdt_vol2:.2f} USDT @ bid {bid2:.6f}, ask {ask2:.6f}), "
                    f"{pair3} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
                    f"{vol3 if vol3 is not None else ask_vol3:.2f} {quote_curr3}, {usdt_vol3:.2f} USDT @ bid {bid3:.6f}, ask {ask3:.6f})"
                )
                reasons.append(f"Low {'24h' if CONFIG['order_book_type'] == 'market' else 'order book'} volume: {min_usdt_volume:.2f} USDT < 1000")
                logger.warning(f"Low volume for {pair1}, {pair2}, {pair3} on {exchange.id}. Trade amount: {adjusted_amount:.2f}, Volumes: {volume_log}")
                return None, 0.0
            volatility = max(
                await calculate_volatility(exchange, pair1),
                await calculate_volatility(exchange, pair2),
                await calculate_volatility(exchange, pair3)
            )
            if volatility > CONFIG['max_volatility']:
                reasons.append(f"Volatility too high: {volatility:.4f} > {CONFIG['max_volatility']}")
                logger.info(f"Volatility too high ({volatility:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0
            slippage = max(
                abs((ask1 - bid1) / bid1),
                abs((ask2 - bid2) / bid2),
                abs((ask3 - bid3) / bid3)
            )
            if slippage > CONFIG['slippage_tolerance']:
                reasons.append(f"Slippage too high: {slippage:.4f} > {CONFIG['slippage_tolerance']}")
                logger.info(f"Slippage too high ({slippage:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0
            # Calculate arbitrage
            base1, quote1 = pair1.split('/')
            base2, quote2 = pair2.split('/')
            base3, quote3 = pair3.split('/')
            amount1 = adjusted_amount / ask1  # Buy base1 with quote1
            amount1_after_fee = amount1 * (1 - fee1)
            if triplet_type == 'forward':
                # Forward: base1/quote1, base2/base1, base2/quote1
                if quote2 != base1:
                    reasons.append(f"Invalid forward triplet: quote2 ({quote2}) != base1 ({base1})")
                    logger.error(f"Invalid forward triplet for {pair1}, {pair2}, {pair3}")
                    return None, 0.0
                amount2 = amount1_after_fee / ask2  # Buy base2 with base1
                amount2_after_fee = amount2 * (1 - fee2)
                if base3 != base2 or quote3 != quote1:
                    reasons.append(f"Invalid forward triplet: base3 ({base3}) != base2 ({base2}) or quote3 ({quote3}) != quote1 ({quote1})")
                    logger.error(f"Invalid forward triplet for {pair1}, {pair2}, {pair3}")
                    return None, 0.0
                final_amount = amount2_after_fee * bid3  # Sell base2 for quote1
                final_currency = quote1
            else:
                # Reverse: base1/quote1, base2/quote1, base1/base2
                if quote2 != quote1:
                    reasons.append(f"Invalid reverse triplet: quote2 ({quote2}) != quote1 ({quote1})")
                    logger.error(f"Invalid reverse triplet for {pair1}, {pair2}, {pair3}")
                    return None, 0.0
                amount2 = amount1_after_fee * (1 / bid2)  # Sell base1 for base2 (bid2 is base2/quote1, invert to quote1/base2)
                amount2_after_fee = amount2 * (1 - fee2)
                if base3 != base1 or quote3 != base2:
                    reasons.append(f"Invalid reverse triplet: base3 ({base3}) != base1 ({base1}) or quote3 ({quote3}) != base2 ({base2})")
                    logger.error(f"Invalid reverse triplet for {pair1}, {pair2}, {pair3}")
                    return None, 0.0
                usdt_pair = f"{base2}/USDT"
                try:
                    usdt_ticker = await exchange.fetch_ticker(usdt_pair)
                    bid_usdt = usdt_ticker['bid']
                    final_amount = amount2_after_fee * bid_usdt  # Sell base2 for USDT
                    final_currency = 'USDT'
                    logger.info(f"Step 3: Sell {base2} for USDT using {usdt_pair} bid={bid_usdt:.6f}")
                except Exception as e:
                    reasons.append(f"Cannot fetch {usdt_pair} for final conversion: {e}")
                    logger.error(f"Error fetching {usdt_pair} for {pair1}, {pair2}, {pair3}: {e}")
                    return None, 0.0
            final_amount_after_fee = final_amount * (1 - fee3)
            if final_currency != 'USDT':
                try:
                    usdt_pair = f"{final_currency}/USDT"
                    usdt_ticker = await exchange.fetch_ticker(usdt_pair)
                    bid_usdt = usdt_ticker['bid']
                    final_amount_after_fee *= bid_usdt
                    logger.info(f"Converted final amount from {final_currency} to USDT using {usdt_pair} bid={bid_usdt:.6f}")
                except Exception as e:
                    reasons.append(f"Cannot convert {final_currency} to USDT: {e}")
                    logger.error(f"Error converting {final_currency} to USDT for {pair1}, {pair2}, {pair3}: {e}")
                    return None, 0.0
            profit = final_amount_after_fee - adjusted_amount
            profit_percentage = (profit / adjusted_amount) * 100
            if profit_percentage > CONFIG['max_profit_cap'] * 100:
                reasons.append(f"Profit too high: {profit_percentage:.2f}% > {CONFIG['max_profit_cap'] * 100:.2f}%")
                logger.warning(f"Unrealistic profit for {pair1}, {pair2}, {pair3}: {profit_percentage:.2f}%")
                return None, 0.0
            logger.info(
                f"Calculation for {pair1}, {pair2}, {pair3} ({triplet_type}): "
                f"Step 1: {adjusted_amount:.2f} {quote1} -> {amount1:.6f} {base1} @ ask {ask1:.6f}, after fee {amount1_after_fee:.6f}; "
                f"Step 2: {amount1_after_fee:.6f} {base1} -> {amount2:.6f} {base2} @ {'ask' if triplet_type == 'forward' else 'bid (inverted)'} {ask2 if triplet_type == 'forward' else (1/bid2):.6f}, after fee {amount2_after_fee:.6f}; "
                f"Step 3: {amount2_after_fee:.6f} {base2} -> {final_amount:.6f} {final_currency} @ bid {bid3 if triplet_type == 'forward' else bid_usdt:.6f}, after fee {final_amount_after_fee:.6f} USDT"
            )
            prices.append({
                'initial_amount': adjusted_amount,
                'final_amount': final_amount_after_fee,
                'profit': profit,
                'profit_percentage': profit_percentage
            })
            await asyncio.sleep(0.1)
        avg_profit = np.mean([p['profit'] for p in prices])
        avg_profit_percentage = np.mean([p['profit_percentage'] for p in prices])
        trade_details = {
            'initial_amount': adjusted_amount,
            'final_amount': prices[-1]['final_amount'],
            'avg_profit': avg_profit,
            'avg_profit_percentage': avg_profit_percentage,
            'prices': prices
        }
        dynamic_threshold = CONFIG['min_profit_threshold']
        async with history_lock:
            if recent_profits:
                valid_profits = [p for p in recent_profits if p <= CONFIG['max_profit_cap'] * 100]
                if valid_profits:
                    dynamic_threshold = max(CONFIG['min_profit_threshold'], np.mean(valid_profits) * 0.8)
                else:
                    dynamic_threshold = CONFIG['min_profit_threshold']
                    recent_profits.clear()
        if avg_profit_percentage < dynamic_threshold * 100:
            reasons.append(f"Profit too low: {avg_profit_percentage:.2f}% < {dynamic_threshold * 100:.2f}%")
            logger.info(f"No profitable trade for {pair1}, {pair2}, {pair3}: {', '.join(reasons)}")
            return None, 0.0
        volume_log = (
            f"{pair1} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
            f"{vol1 if vol1 is not None else ask_vol1:.2f} {quote_curr1}, {usdt_vol1:.2f} USDT @ bid {bid1:.6f}, ask {ask1:.6f}), "
            f"{pair2} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
            f"{vol2 if vol2 is not None else ask_vol2:.2f} {quote_curr2}, {usdt_vol2:.2f} USDT @ bid {bid2:.6f}, ask {ask2:.6f}), "
            f"{pair3} ({'24h' if CONFIG['order_book_type'] == 'market' else 'ask'}: "
            f"{vol3 if vol3 is not None else ask_vol3:.2f} {quote_curr3}, {usdt_vol3:.2f} USDT @ bid {bid3:.6f}, ask {ask3:.6f})"
        )
        logger.info(
            f"Arbitrage opportunity found for {pair1}, {pair2}, {pair3} on {exchange.id}: "
            f"Profit {avg_profit_percentage:.2f}% >= {dynamic_threshold * 100:.2f}%, "
            f"Initial amount {adjusted_amount:.2f} USDT, Final amount {final_amount_after_fee:.2f} USDT, "
            f"Fees: {pair1}={fee1:.4f}, {pair2}={fee2:.4f}, {pair3}={fee3:.4f}, "
            f"Volume: {volume_log}, "
            f"Volatility: {volatility:.4f} <= {CONFIG['max_volatility']}, "
            f"Slippage: {slippage:.4f} <= {CONFIG['slippage_tolerance']}"
        )
        if CONFIG['order_book_type'] == 'limit' and adjusted_amount != amount:
            logger.info(f"Stake amount reset to {CONFIG['stake_amount']:.2f} USDT for next triplet")
        return trade_details, prices[-1]['final_amount']
    except Exception as e:
        reasons.append(f"Calculation error: {str(e)}")
        logger.error(f"Error calculating arbitrage on {exchange.id} for {pair1}, {pair2}, {pair3}: {', '.join(reasons)}\n{traceback.format_exc()}")
        return None, 0.0

async def simulate_trade(exchange, pair1, pair2, pair3, trade_details):
    if not trade_details:
        return False
    try:
        initial_amount = trade_details['initial_amount']
        final_amount = trade_details['final_amount']
        profit = trade_details['avg_profit']
        profit_percentage = trade_details['avg_profit_percentage']
        async with balance_lock:
            mock_balance['USDT'] = mock_balance['USDT'] - initial_amount + final_amount
        async with history_lock:
            if profit_percentage <= CONFIG['max_profit_cap'] * 100:
                recent_profits.append(profit_percentage)
            trade_history.append(trade_details)
            if len(recent_profits) > 10:
                recent_profits.pop(0)
        await log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details)
        print_trade_update(exchange, pair1, pair2, pair3, trade_details)
        async with balance_lock:
            if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                logger.error(f"Stop-loss triggered: Balance below threshold")
                await print_summary()
                raise Exception("Stop-loss triggered")
        return True
    except Exception as e:
        logger.error(f"Error simulating trade on {exchange.id}: {e}\n{traceback.format_exc()}")
        return False

async def run_exchange(exchange_name, exchange):
    console.print(f"[yellow]Loading markets for {exchange_name}[/yellow]")
    logger.info(f"Loading markets for {exchange_name}")
    try:
        coins = await fetch_top_coins(exchange, CONFIG['coin_count'])
        if len(coins) < 3:
            logger.error(f"Not enough coins ({len(coins)}) to form triplets on {exchange_name}")
            return
        triplets, valid_pairs = await generate_triplets(coins, exchange)
        console.print(f"[cyan]Found {len(triplets)} valid triplets on {exchange_name}[/cyan]")
        logger.info(f"Found {len(triplets)} valid triplets on {exchange_name}")
        CONFIG['fee_rate'][exchange_name] = await fetch_trading_fees(exchange, valid_pairs)
        while True:
            async with balance_lock:
                if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                    console.print(f"[bold red]Trading halted on {exchange_name}: Balance below stop-loss threshold[/bold red]")
                    logger.error(f"Trading halted on {exchange_name}: Balance below stop-loss threshold")
                    await print_summary()
                    break
            for triplet_type, pair1, pair2, pair3 in triplets:
                logger.info(f"Checking arbitrage on {exchange_name} for {pair1}, {pair2}, {pair3} ({triplet_type})")
                async with balance_lock:
                    max_trade_amount = mock_balance['USDT'] * CONFIG['max_exposure']
                    trade_amount = min(max_trade_amount, CONFIG['stake_amount'])
                trade_details, final_amount = await calculate_triangular_arbitrage(
                    exchange, triplet_type, pair1, pair2, pair3, trade_amount, CONFIG['avg_trades']
                )
                if trade_details and final_amount > trade_details['initial_amount']:
                    await simulate_trade(exchange, pair1, pair2, pair3, trade_details)
                else:
                    logger.info(f"No arbitrage opportunity on {exchange_name} for {pair1}, {pair2}, {pair3}: No trade details or unprofitable")
                await asyncio.sleep(CONFIG['sleep_interval'])
    except Exception as e:
        console.print(f"[bold red]Fatal error on {exchange_name}: {e}[/bold red]")
        logger.error(f"Fatal error on {exchange_name}: {e}\n{traceback.format_exc()}")
        await print_summary()
    finally:
        await exchange.close()

async def main():
    console.print(f"[bold green]Bot started with active exchanges: {[name for name in exchanges]}[/bold green]")
    logger.info(f"Bot started with active exchanges: {[name for name in exchanges]}")
    tasks = [run_exchange(name, exchange) for name, exchange in exchanges.items()]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        console.print("[bold yellow]Bot stopped by user.[/bold yellow]")
        logger.info("Bot stopped by user")
        await print_summary()
    finally:
        console.print("[bold green]Bot stopped[/bold green]")
        logger.info("Bot stopped")
        for exchange in exchanges.values():
            await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())