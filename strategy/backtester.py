import pandas as pd
from logger import logger
from brokers import BrokerGateway

class BacktesterStrategy:
    """
    A strategy for backtesting a trading algorithm based on historical data.
    """

    def __init__(self, broker: BrokerGateway, config: dict):
        """
        Initializes the backtesting strategy.

        Args:
            broker: An instance of the broker gateway.
            config: A dictionary containing the strategy configuration.
        """
        self.broker = broker
        self.config = config
        self.results = {}
        self.historical_data = None

    def run_backtest(self, symbol: str, start_date: str, end_date: str, interval: str):
        """
        Runs the backtest for the given symbol and date range.

        Args:
            symbol: The trading symbol to backtest.
            start_date: The start date of the backtest in 'YYYY-MM-DD' format.
            end_date: The end date of the backtest in 'YYYY-MM-DD' format.
            interval: The data interval (e.g., '15minute', 'day').
        """
        logger.info(f"Running backtest for {symbol} from {start_date} to {end_date} with interval {interval}")

        # Download instruments to get lot size and other details
        self.broker.download_instruments()

        # Dynamically generate futures symbol
        base_symbol = symbol.split(':')[-1]
        start_month = pd.to_datetime(start_date).strftime('%b').upper()
        start_year = pd.to_datetime(start_date).strftime('%y')
        futures_symbol = f"NSE:{base_symbol}{start_year}{start_month}FUT"

        logger.info(f"Dynamically generated futures symbol: {futures_symbol}")

        # Fetch historical data
        try:
            data = self.broker.get_history(futures_symbol, interval, start_date, end_date)
            if not data:
                logger.error(f"No historical data found for {futures_symbol}.")
                return

            self.historical_data = pd.DataFrame(data)
            self.historical_data['ts'] = pd.to_datetime(self.historical_data['ts'], unit='s')
            self.historical_data.set_index('ts', inplace=True)
            logger.info(f"Successfully fetched {len(self.historical_data)} data points.")

        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return

        # The logic for implementing the trading strategy and generating results will be added in the subsequent steps.
        self._calculate_indicators()
        self._run_simulation()

        return self.results

    def _calculate_indicators(self):
        """Calculates technical indicators required for the strategy."""
        if self.historical_data is None:
            return

        self.historical_data['ema_20'] = self.historical_data['close'].ewm(span=20, adjust=False).mean()
        self.historical_data['ema_50'] = self.historical_data['close'].ewm(span=50, adjust=False).mean()

        # Calculate Previous Day High and Low
        daily_data = self.historical_data.resample('D').agg({
            'high': 'max',
            'low': 'min'
        })
        daily_data['pdh'] = daily_data['high'].shift(1)
        daily_data['pdl'] = daily_data['low'].shift(1)

        # Convert daily_data index to date for proper joining
        daily_data.index = daily_data.index.date

        self.historical_data = self.historical_data.join(daily_data[['pdh', 'pdl']], on=self.historical_data.index.date)
        self.historical_data.dropna(inplace=True)

    def _run_simulation(self):
        """Runs the trading simulation based on the 3-candle theory."""
        if self.historical_data is None or len(self.historical_data) < 3:
            return

        trades = []
        position = None
        capital = 100000  # Starting capital
        risk_per_trade = 0.01 # 1% of capital

        # Get lot size for the instrument
        instrument = self.broker.get_instruments()
        lot_size = instrument[instrument['symbol'] == self.historical_data.iloc[0]['symbol']]['lot_size'].iloc[0]

        # Iterate through the data, leaving room for the 3-candle pattern
        for i in range(2, len(self.historical_data) - 1):
            c1 = self.historical_data.iloc[i-2]
            c2 = self.historical_data.iloc[i-1]
            c3 = self.historical_data.iloc[i]

            entry_candle = self.historical_data.iloc[i+1]

            # If a position is open, check for exit conditions
            if position:
                if position['trade_type'] == 'bullish':
                    # Check for stop loss or take profit
                    if entry_candle['low'] <= position['stop_loss']:
                        position['exit_price'] = position['stop_loss']
                        position['pnl'] = (position['exit_price'] - position['entry_price']) * position['quantity']
                        trades.append(position)
                        position = None
                        continue
                    elif entry_candle['high'] >= position['take_profit']:
                        position['exit_price'] = position['take_profit']
                        position['pnl'] = (position['exit_price'] - position['entry_price']) * position['quantity']
                        trades.append(position)
                        position = None
                        continue
                elif position['trade_type'] == 'bearish':
                     if entry_candle['high'] >= position['stop_loss']:
                        position['exit_price'] = position['stop_loss']
                        position['pnl'] = (position['entry_price'] - position['exit_price']) * position['quantity']
                        trades.append(position)
                        position = None
                        continue
                     elif entry_candle['low'] <= position['take_profit']:
                        position['exit_price'] = position['take_profit']
                        position['pnl'] = (position['entry_price'] - position['exit_price']) * position['quantity']
                        trades.append(position)
                        position = None
                        continue

                # Time-based exit
                if entry_candle.name.time() >= pd.to_datetime('15:14').time():
                    position['exit_price'] = entry_candle['close']
                    if position['trade_type'] == 'bullish':
                        position['pnl'] = (position['exit_price'] - position['entry_price']) * position['quantity']
                    else:
                        position['pnl'] = (position['entry_price'] - position['exit_price']) * position['quantity']
                    trades.append(position)
                    position = None
                continue

            # --- Check for new trade setups ---
            is_bullish_sweep = c2['low'] < c1['low'] and c1['low'] < c2['close'] < c1['high']
            is_bearish_sweep = c2['high'] > c1['high'] and c1['low'] < c2['close'] < c1['high']

            # Bullish Setup
            if is_bullish_sweep and c3['low'] > c2['low']:
                if c3['close'] > c3['pdh'] and c3['ema_20'] > c3['ema_50']:
                    entry_price = c2['high']
                    stop_loss = c2['low']
                    take_profit = entry_price + 2 * (entry_price - stop_loss)

                    # Risk Management
                    trade_risk = entry_price - stop_loss
                    if trade_risk > (capital * risk_per_trade) or trade_risk <= 0:
                        continue # Skip trade if risk is too high or invalid

                    # Calculate quantity in lots
                    num_lots = int((capital * risk_per_trade) / (trade_risk * lot_size))
                    if num_lots < 1:
                        continue # Skip trade if not enough capital for one lot

                    quantity = num_lots * lot_size

                    position = {
                        'entry_time': entry_candle.name,
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'quantity': quantity,
                        'trade_type': 'bullish'
                    }

            # Bearish Setup
            elif is_bearish_sweep and c3['high'] < c2['high']:
                 if c3['close'] < c3['pdl'] and c3['ema_20'] < c3['ema_50']:
                    entry_price = c2['low']
                    stop_loss = c2['high']
                    take_profit = entry_price - 2 * (stop_loss - entry_price)

                     # Risk Management
                    trade_risk = stop_loss - entry_price
                    if trade_risk > (capital * risk_per_trade) or trade_risk <= 0:
                        continue # Skip trade if risk is too high or invalid

                    # Calculate quantity in lots
                    num_lots = int((capital * risk_per_trade) / (trade_risk * lot_size))
                    if num_lots < 1:
                        continue # Skip trade if not enough capital for one lot

                    quantity = num_lots * lot_size

                    position = {
                        'entry_time': entry_candle.name,
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'quantity': quantity,
                        'trade_type': 'bearish'
                    }

        # After the loop, check if a position is still open
        if position:
            last_candle = self.historical_data.iloc[-1]
            position['exit_price'] = last_candle['close']
            if position['trade_type'] == 'bullish':
                position['pnl'] = (position['exit_price'] - position['entry_price']) * position['quantity']
            else:
                position['pnl'] = (position['entry_price'] - position['exit_price']) * position['quantity']
            trades.append(position)

        # Store results
        if trades:
            trade_df = pd.DataFrame(trades)
            total_pnl = trade_df['pnl'].sum()
            win_rate = (trade_df['pnl'] > 0).mean() * 100

            self.results = {
                'total_pnl': total_pnl,
                'win_rate': win_rate,
                'number_of_trades': len(trades),
                'trades': trade_df.to_dict('records')
            }
        else:
            self.results = {
                'total_pnl': 0,
                'win_rate': 0,
                'number_of_trades': 0,
                'trades': []
            }
