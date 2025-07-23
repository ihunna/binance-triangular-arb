import ccxt
import pandas as pd
import time
import logging
import itertools
import numpy as np
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
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
        logger.info(f"{exchange.upper()}_PASSPHRASE loaded: {'True' if API_KEYS[exchange]['passphrase'] else 'False'}")

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
    'coins': ['BTC', 'ETH', 'USDT', 'LTC', 'XRP'],
    'trace_back': 5,
    'fee_rate': {'bybit': 0.001, 'kucoin': 0.001, 'binance': 0.001},
    'min_profit_threshold': 0.01,
    'max_exposure': 0.1,
    'max_volatility': 0.02,
    'stop_loss_threshold': 0.8,
    'sleep_interval': 5,
    'initial_balance': 10000.0,
    'slippage_tolerance': 0.005,
    'trade_log_file': 'trades.csv'
}

# Initialize exchanges
exchanges = {}
for name in EXCHANGES:
    if CONFIG['exchanges'][name]['active'] and API_KEYS[name] and API_KEYS[name]['apiKey'] and API_KEYS[name]['secret']:
        try:
            if name == 'bybit':
                exchanges[name] = ccxt.bybit(CONFIG['exchanges'][name])
            elif name == 'kucoin':
                exchanges[name] = ccxt.kucoin(CONFIG['exchanges'][name])
            elif name == 'binance':
                exchanges[name] = ccxt.binance(CONFIG['exchanges'][name])
            logger.info(f"Initialized {name} exchange")
        except Exception as e:
            logger.error(f"Failed to initialize {name}: {e}\n{traceback.format_exc()}")
            exchanges[name] = None
    elif CONFIG['exchanges'][name]['active']:
        logger.warning(f"{name} is active but missing API keys. Prompting for manual input.")
        API_KEYS[name]['apiKey'] = input(f"Enter {name} API Key: ")
        API_KEYS[name]['secret'] = input(f"Enter {name} Secret Key: ")
        if name == 'kucoin':
            API_KEYS[name]['passphrase'] = input(f"Enter {name} Passphrase: ")
        if API_KEYS[name]['apiKey'] and API_KEYS[name]['secret']:
            try:
                if name == 'bybit':
                    exchanges[name] = ccxt.bybit(CONFIG['exchanges'][name])
                elif name == 'kucoin':
                    exchanges[name] = ccxt.kucoin(CONFIG['exchanges'][name])
                elif name == 'binance':
                    exchanges[name] = ccxt.binance(CONFIG['exchanges'][name])
                logger.info(f"Initialized {name} exchange")
            except Exception as e:
                logger.error(f"Failed to initialize {name}: {e}\n{traceback.format_exc()}")
                exchanges[name] = None
        else:
            logger.warning(f"Skipping {name} due to missing API keys.")
exchanges = {k: v for k, v in exchanges.items() if v is not None}

if not exchanges:
    raise ValueError("No valid exchanges initialized. Please set 'active': True and provide valid API keys for at least one exchange.")

# Mock funds
mock_balance = {
    'USDT': CONFIG['initial_balance'],
    'BTC': 0.0,
    'ETH': 0.0,
    'LTC': 0.0,
    'XRP': 0.0
}
initial_balance = CONFIG['initial_balance']
trade_history = []
recent_profits = []

# Initialize trades.csv
with open(CONFIG['trade_log_file'], 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Timestamp', 'Exchange', 'Pair1', 'Pair2', 'Pair3', 'Initial_Amount', 'Final_Amount', 'Profit', 'Profit_Percentage', 'Balance_USDT'])

def log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with history_lock:
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

def print_summary():
    with history_lock:
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

def fetch_order_book(exchange, symbol, limit=20):
    try:
        order_book = exchange.fetch_order_book(symbol, limit=limit)
        bid = order_book['bids'][0][0] if order_book['bids'] else None
        ask = order_book['asks'][0][0] if order_book['asks'] else None
        bid_volume = order_book['bids'][0][1] if order_book['bids'] else 0
        ask_volume = order_book['asks'][0][1] if order_book['asks'] else 0
        return bid, ask, bid_volume, ask_volume
    except ccxt.RateLimitExceeded as e:
        logger.warning(f"Rate limit exceeded on {exchange.id}. Backing off...")
        logger.error(f"Rate limit error details: {e}\n{traceback.format_exc()}")
        time.sleep(2 ** int(math.log2(random.randint(1, 5))))
        return None, None, 0, 0
    except Exception as e:
        logger.error(f"Error fetching order book for {symbol} on {exchange.id}: {e}\n{traceback.format_exc()}")
        return None, None, 0, 0

def calculate_volatility(exchange, symbol, periods=5, interval='1m'):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, interval, limit=periods)
        prices = [candle[4] for candle in ohlcv]
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        volatility = np.std(returns) if returns else 0
        return volatility
    except Exception as e:
        logger.error(f"Error calculating volatility for {symbol} on {exchange.id}: {e}\n{traceback.format_exc()}")
        return float('inf')

def generate_triplets(coins, exchange):
    try:
        markets = exchange.load_markets()
        valid_pairs = set(markets.keys())
        triplets = []
        for base, quote, third in itertools.permutations(coins, 3):
            pair1 = f"{base}/{quote}"
            pair2 = f"{third}/{base}"
            pair3 = f"{third}/{quote}"
            if pair1 in valid_pairs and pair2 in valid_pairs and pair3 in valid_pairs:
                triplets.append((pair1, pair2, pair3))
        logger.info(f"Generated {len(triplets)} triplets on {exchange.id}: {triplets}")
        return triplets
    except Exception as e:
        logger.error(f"Error generating triplets on {exchange.id}: {e}\n{traceback.format_exc()}")
        return []

def calculate_triangular_arbitrage(exchange, pair1, pair2, pair3, amount, trace_back):
    try:
        prices = []
        for _ in range(trace_back):
            bid1, ask1, bid_vol1, ask_vol1 = fetch_order_book(exchange, pair1)
            bid2, ask2, bid_vol2, ask_vol2 = fetch_order_book(exchange, pair2)
            bid3, ask3, bid_vol3, ask_vol3 = fetch_order_book(exchange, pair3)
            
            if not all([bid1, ask1, bid2, ask2, bid3, ask3]):
                return None, 0.0

            if min(bid_vol1, ask_vol1, bid_vol2, ask_vol2, bid_vol3, ask_vol3) < amount:
                logger.info(f"Insufficient liquidity for {pair1}, {pair2}, {pair3} on {exchange.id}")
                return None, 0.0

            volatility = max(
                calculate_volatility(exchange, pair1),
                calculate_volatility(exchange, pair2),
                calculate_volatility(exchange, pair3)
            )
            if volatility > CONFIG['max_volatility']:
                logger.info(f"Volatility too high ({volatility:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0

            slippage = max(
                abs((ask1 - bid1) / bid1),
                abs((ask2 - bid2) / bid2),
                abs((ask3 - bid3) / bid3)
            )
            if slippage > CONFIG['slippage_tolerance']:
                logger.info(f"Slippage too high ({slippage:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0

            amount1 = amount / ask1
            amount1_after_fee = amount1 * (1 - CONFIG['fee_rate'][exchange.id])
            amount2 = amount1_after_fee / ask2
            amount2_after_fee = amount2 * (1 - CONFIG['fee_rate'][exchange.id])
            final_amount = amount2_after_fee * bid3
            final_amount_after_fee = final_amount * (1 - CONFIG['fee_rate'][exchange.id])

            profit = final_amount_after_fee - amount
            profit_percentage = (profit / amount) * 100

            prices.append({
                'initial_amount': amount,
                'final_amount': final_amount_after_fee,
                'profit': profit,
                'profit_percentage': profit_percentage
            })

            time.sleep(0.1)

        avg_profit = np.mean([p['profit'] for p in prices])
        avg_profit_percentage = np.mean([p['profit_percentage'] for p in prices])
        return {
            'initial_amount': amount,
            'final_amount': prices[-1]['final_amount'],
            'avg_profit': avg_profit,
            'avg_profit_percentage': avg_profit_percentage,
            'prices': prices
        }, prices[-1]['final_amount']
    except Exception as e:
        logger.error(f"Error calculating arbitrage on {exchange.id}: {e}\n{traceback.format_exc()}")
        return None, 0.0

def simulate_trade(exchange, pair1, pair2, pair3, trade_details):
    if not trade_details:
        return False
    try:
        initial_amount = trade_details['initial_amount']
        final_amount = trade_details['final_amount']
        profit = trade_details['avg_profit']
        profit_percentage = trade_details['avg_profit_percentage']
        dynamic_threshold = CONFIG['min_profit_threshold']
        with history_lock:
            if recent_profits:
                dynamic_threshold = max(CONFIG['min_profit_threshold'], np.mean(recent_profits) * 0.8)
        if profit_percentage >= dynamic_threshold:
            with balance_lock:
                mock_balance['USDT'] = mock_balance['USDT'] - initial_amount + final_amount
            with history_lock:
                recent_profits.append(profit_percentage)
                trade_history.append(trade_details)
                if len(recent_profits) > 10:
                    recent_profits.pop(0)
            print_trade_update(exchange, pair1, pair2, pair3, trade_details)
            log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details)
            with balance_lock:
                if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                    logger.error(f"Stop-loss triggered: Balance below threshold\n{traceback.format_exc()}")
                    print_summary()
                    raise Exception("Stop-loss triggered")
            return True
        else:
            logger.info(f"No profitable trade. Profit {profit_percentage:.2f}% below threshold {dynamic_threshold}%")
            return False
    except Exception as e:
        logger.error(f"Error simulating trade on {exchange.id}: {e}\n{traceback.format_exc()}")
        return False

async def log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    async with history_lock:  # Use async with
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

async def simulate_trade(exchange, pair1, pair2, pair3, trade_details):
    if not trade_details:
        return False
    try:
        initial_amount = trade_details['initial_amount']
        final_amount = trade_details['final_amount']
        profit = trade_details['avg_profit']
        profit_percentage = trade_details['avg_profit_percentage']
        dynamic_threshold = CONFIG['min_profit_threshold']
        async with history_lock:  # Use async with
            if recent_profits:
                dynamic_threshold = max(CONFIG['min_profit_threshold'], np.mean(recent_profits) * 0.8)
        if profit_percentage >= dynamic_threshold:
            async with balance_lock:  # Use async with
                mock_balance['USDT'] = mock_balance['USDT'] - initial_amount + final_amount
            async with history_lock:  # Use async with
                recent_profits.append(profit_percentage)
                trade_history.append(trade_details)
                if len(recent_profits) > 10:
                    recent_profits.pop(0)
            print_trade_update(exchange, pair1, pair2, pair3, trade_details)
            await log_trade_to_csv(exchange, pair1, pair2, pair3, trade_details)  # Await async function
            async with balance_lock:  # Use async with
                if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                    logger.error(f"Stop-loss triggered: Balance below threshold\n{traceback.format_exc()}")
                    print_summary()
                    raise Exception("Stop-loss triggered")
            return True
        else:
            logger.info(f"No profitable trade. Profit {profit_percentage:.2f}% below threshold {dynamic_threshold}%")
            return False
    except Exception as e:
        logger.error(f"Error simulating trade on {exchange.id}: {e}\n{traceback.format_exc()}")
        return False

async def run_exchange(exchange_name, exchange):
    console.print(f"[yellow]Loading markets for {exchange_name}[/yellow]")
    logger.info(f"Loading markets for {exchange_name}")
    try:
        triplets = generate_triplets(CONFIG['coins'], exchange)
        console.print(f"[cyan]Found {len(triplets)} valid triplets on {exchange_name}: {triplets}[/cyan]")
        logger.info(f"Found {len(triplets)} valid triplets on {exchange_name}")
        while True:
            async with balance_lock:  # Use async with
                if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                    console.print(f"[bold red]Trading halted on {exchange_name}: Balance below stop-loss threshold[/bold red]")
                    logger.error(f"Trading halted on {exchange_name}: Balance below stop-loss threshold")
                    print_summary()
                    break
            for pair1, pair2, pair3 in triplets:
                logger.info(f"Checking arbitrage on {exchange_name} for {pair1}, {pair2}, {pair3}")
                async with balance_lock:  # Use async with
                    max_trade_amount = mock_balance['USDT'] * CONFIG['max_exposure']
                    trade_amount = min(max_trade_amount, CONFIG['initial_balance'] * 0.1)
                trade_details, final_amount = await asyncio.get_event_loop().run_in_executor(
                    None, calculate_triangular_arbitrage, exchange, pair1, pair2, pair3, trade_amount, CONFIG['trace_back']
                )
                if trade_details and final_amount > trade_amount:
                    await simulate_trade(exchange, pair1, pair2, pair3, trade_details)
                else:
                    logger.info(f"No arbitrage opportunity on {exchange_name} for {pair1}, {pair2}, {pair3}")
                await asyncio.sleep(CONFIG['sleep_interval'])
    except Exception as e:
        console.print(f"[bold red]Fatal error on {exchange_name}: {e}[/bold red]")
        logger.error(f"Fatal error on {exchange_name}: {e}\n{traceback.format_exc()}")
        print_summary()

async def main():
    console.print(f"[bold green]Bot started with active exchanges: {[name for name in exchanges]}[/bold green]")
    logger.info(f"Bot started with active exchanges: {[name for name in exchanges]}")
    tasks = [run_exchange(name, exchange) for name, exchange in exchanges.items()]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        console.print("[bold yellow]Bot stopped by user.[/bold yellow]")
        logger.info("Bot stopped by user")
        print_summary()
    finally:
        console.print("[bold green]Bot stopped[/bold green]")
        logger.info("Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())