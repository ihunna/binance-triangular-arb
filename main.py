import ccxt
import pandas as pd
import time
import logging
import itertools
import numpy as np
from datetime import datetime, timedelta
import random
import math,os
from dotenv import load_dotenv


parent_folder = os.path.abspath(os.path.dirname(__file__))
env_path = os.path.join(parent_folder, '.env')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not os.path.exists(env_path):
    logger.error(f".env file not found at {env_path}")
    raise FileNotFoundError(f".env file not found at {env_path}")

load_dotenv(env_path)

BINANCE_API_KEY, BINANCE_SECRET = os.getenv('BINANCE_API_KEY'),os.getenv('BINANCE_SECRET')
if not BINANCE_API_KEY or not BINANCE_SECRET:
    raise Exception('Binance creds are missing')

# Configuration
CONFIG = {
    'exchanges': {
        'binance': {
            'apiKey': BINANCE_API_KEY,  # Replace with your Binance API key
            'secret': BINANCE_SECRET,   # Replace with your Binance secret
            'enableRateLimit': True
        },
        'coinbase': {
            'apiKey': 'YOUR_COINBASE_API_KEY',  # Replace with your Coinbase API key
            'secret': 'YOUR_COINBASE_SECRET',   # Replace with your Coinbase secret
            'enableRateLimit': True
        }
    },
    'coins': ['BTC', 'ETH', 'USDT', 'LTC', 'BNB'],  # List of coins to generate triplets
    'trace_back': 5,  # Number of recent price points to analyze per triplet
    'fee_rate': 0.001,  # Trading fee per trade (0.1%)
    'min_profit_threshold': 0.01,  # Minimum profit percentage (1%)
    'max_exposure': 0.1,  # Max % of balance per trade (10%)
    'max_volatility': 0.02,  # Max allowed volatility (2% price change)
    'stop_loss_threshold': 0.8,  # Stop if balance drops below 80% of initial
    'sleep_interval': 5,  # Seconds between checks
    'initial_balance': 10000.0,  # Starting mock balance in USDT
    'slippage_tolerance': 0.005  # Max allowed slippage (0.5%)
}

# Initialize exchanges
exchanges = {
    'binance': ccxt.binance(CONFIG['exchanges']['binance']),
    'coinbase': ccxt.coinbase(CONFIG['exchanges']['coinbase'])
}

# Mock funds (paper trading)
mock_balance = {
    'USDT': CONFIG['initial_balance'],
    'BTC': 0.0,
    'ETH': 0.0,
    'LTC': 0.0,
    'BNB': 0.0
}
initial_balance = CONFIG['initial_balance']
recent_profits = []  # Track recent trade profits for dynamic threshold

def fetch_order_book(exchange, symbol, limit=10):
    """Fetch order book data for a given symbol."""
    try:
        order_book = exchange.fetch_order_book(symbol, limit=limit)
        bid = order_book['bids'][0][0] if order_book['bids'] else None
        ask = order_book['asks'][0][0] if order_book['asks'] else None
        bid_volume = order_book['bids'][0][1] if order_book['bids'] else 0
        ask_volume = order_book['asks'][0][1] if order_book['asks'] else 0
        return bid, ask, bid_volume, ask_volume
    except ccxt.RateLimitExceeded:
        logger.warning(f"Rate limit exceeded on {exchange.id}. Backing off...")
        time.sleep(2 ** int(math.log2(random.randint(1, 5))))
        return None, None, 0, 0
    except Exception as e:
        logger.error(f"Error fetching order book for {symbol} on {exchange.id}: {e}")
        return None, None, 0, 0

def calculate_volatility(exchange, symbol, periods=5, interval='1m'):
    """Calculate price volatility over recent periods."""
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, interval, limit=periods)
        prices = [candle[4] for candle in ohlcv]  # Closing prices
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        volatility = np.std(returns) if returns else 0
        return volatility
    except Exception as e:
        logger.error(f"Error calculating volatility for {symbol} on {exchange.id}: {e}")
        return float('inf')

def generate_triplets(coins, exchange):
    """Generate all possible trading pair triplets from a list of coins."""
    markets = exchange.load_markets()
    valid_pairs = set(markets.keys())
    triplets = []
    for base, quote, third in itertools.permutations(coins, 3):
        pair1 = f"{base}/{quote}"
        pair2 = f"{third}/{base}"
        pair3 = f"{third}/{quote}"
        if pair1 in valid_pairs and pair2 in valid_pairs and pair3 in valid_pairs:
            triplets.append((pair1, pair2, pair3))
    logger.info(f"Generated triplets: {triplets}")
    return triplets

def calculate_triangular_arbitrage(exchange, pair1, pair2, pair3, amount, trace_back):
    """Calculate potential profit from triangular arbitrage over trace_back periods."""
    try:
        prices = []
        for _ in range(trace_back):
            bid1, ask1, bid_vol1, ask_vol1 = fetch_order_book(exchange, pair1)
            bid2, ask2, bid_vol2, ask_vol2 = fetch_order_book(exchange, pair2)
            bid3, ask3, bid_vol3, ask_vol3 = fetch_order_book(exchange, pair3)
            
            if not all([bid1, ask1, bid2, ask2, bid3, ask3]):
                return None, 0.0

            # Check liquidity
            if min(bid_vol1, ask_vol1, bid_vol2, ask_vol2, bid_vol3, ask_vol3) < amount:
                logger.info(f"Insufficient liquidity for {pair1}, {pair2}, {pair3} on {exchange.id}")
                return None, 0.0

            # Check volatility
            volatility = max(
                calculate_volatility(exchange, pair1),
                calculate_volatility(exchange, pair2),
                calculate_volatility(exchange, pair3)
            )
            if volatility > CONFIG['max_volatility']:
                logger.info(f"Volatility too high ({volatility:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0

            # Check slippage
            slippage = max(
                abs((ask1 - bid1) / bid1),
                abs((ask2 - bid2) / bid2),
                abs((ask3 - bid3) / bid3)
            )
            if slippage > CONFIG['slippage_tolerance']:
                logger.info(f"Slippage too high ({slippage:.4f}) for {pair1}, {pair2}, {pair3}")
                return None, 0.0

            # Step 1: Buy base with quote (e.g., USDT -> BTC)
            amount1 = amount / ask1
            amount1_after_fee = amount1 * (1 - CONFIG['fee_rate'])

            # Step 2: Buy third with base (e.g., BTC -> ETH)
            amount2 = amount1_after_fee / ask2
            amount2_after_fee = amount2 * (1 - CONFIG['fee_rate'])

            # Step 3: Sell third for quote (e.g., ETH -> USDT)
            final_amount = amount2_after_fee * bid3
            final_amount_after_fee = final_amount * (1 - CONFIG['fee_rate'])

            profit = final_amount_after_fee - amount
            profit_percentage = (profit / amount) * 100

            prices.append({
                'initial_amount': amount,
                'final_amount': final_amount_after_fee,
                'profit': profit,
                'profit_percentage': profit_percentage
            })

            time.sleep(0.1)  # Small delay to avoid rate limits

        # Average profit across trace_back periods
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
        logger.error(f"Error calculating arbitrage on {exchange.id}: {e}")
        return None, 0.0

def simulate_trade(exchange, trade_details):
    """Simulate a trade with mock funds and log results."""
    if not trade_details:
        return False

    try:
        initial_amount = trade_details['initial_amount']
        final_amount = trade_details['final_amount']
        profit = trade_details['avg_profit']
        profit_percentage = trade_details['avg_profit_percentage']

        # Dynamic profit threshold
        dynamic_threshold = CONFIG['min_profit_threshold']
        if recent_profits:
            dynamic_threshold = max(CONFIG['min_profit_threshold'], np.mean(recent_profits) * 0.8)

        if profit_percentage >= dynamic_threshold:
            mock_balance['USDT'] = mock_balance['USDT'] - initial_amount + final_amount
            recent_profits.append(profit_percentage)
            if len(recent_profits) > 10:
                recent_profits.pop(0)
            
            logger.info(f"Simulated Trade on {exchange.id}:")
            logger.info(f"Initial USDT: {initial_amount:.2f}")
            logger.info(f"Final USDT: {final_amount:.2f}")
            logger.info(f"Average Profit: {profit:.2f} USDT ({profit_percentage:.2f}%)")
            logger.info(f"New Mock Balance: {mock_balance}")
            
            if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                logger.error("Stop-loss triggered: Balance below threshold.")
                raise Exception("Stop-loss triggered")
            
            return True
        else:
            logger.info(f"No profitable trade. Profit {profit_percentage:.2f}% below threshold {dynamic_threshold}%")
            return False
    except Exception as e:
        logger.error(f"Error simulating trade on {exchange.id}: {e}")
        return False

def main():
    """Main loop to monitor and execute triangular arbitrage."""
    for exchange_name, exchange in exchanges.items():
        logger.info(f"Loading markets for {exchange_name}")
        try:
            triplets = generate_triplets(CONFIG['coins'], exchange)
            logger.info(f"Found {len(triplets)} valid triplets on {exchange_name}: {triplets}")
            
            while True:
                if mock_balance['USDT'] < initial_balance * CONFIG['stop_loss_threshold']:
                    logger.error("Trading halted: Balance below stop-loss threshold")
                    break

                for pair1, pair2, pair3 in triplets:
                    logger.info(f"Checking arbitrage on {exchange_name} for {pair1}, {pair2}, {pair3}")
                    max_trade_amount = mock_balance['USDT'] * CONFIG['max_exposure']
                    trade_amount = min(max_trade_amount, CONFIG['initial_balance'] * 0.1)
                    
                    trade_details, final_amount = calculate_triangular_arbitrage(
                        exchange, pair1, pair2, pair3, trade_amount, CONFIG['trace_back']
                    )
                    if trade_details and final_amount > trade_amount:
                        simulate_trade(exchange, trade_details)
                    else:
                        logger.info(f"No arbitrage opportunity on {exchange_name} for {pair1}, {pair2}, {pair3}")
                
                time.sleep(CONFIG['sleep_interval'])
        except KeyboardInterrupt:
            logger.info("Bot stopped by user.")
            break
        except Exception as e:
            logger.error(f"Fatal error on {exchange_name}: {e}")
            break

if __name__ == "__main__":
    main()