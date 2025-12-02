import os
import yaml
from logger import logger
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
import pandas as pd
import pandas_ta as ta
import time
from datetime import datetime, timedelta
from termcolor import colored
import argparse

class FVGStrategy:
    """
    FVG (Fair Value Gap) Trading Strategy
    """

    def __init__(self, broker, config, order_tracker):
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)
        self.broker = broker
        self.order_tracker = order_tracker

        self.symbols = self.strat_var_symbols
        self.positions = {}

        logger.info("FVG Strategy initialized")

    def run(self):
        logger.info("Running FVG Strategy")
        while True:
            now = datetime.now()
            next_run = (now - timedelta(minutes=now.minute % 15, seconds=now.second, microseconds=now.microsecond)) + timedelta(minutes=15)

            self.check_open_orders()
            self.analyze_and_trade()
            self.manage_positions()
            self.display_table()

            sleep_time = (next_run - datetime.now()).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)

    def check_open_orders(self):
        for symbol, pos in self.positions.items():
            if pos['status'] == 'PENDING_ENTRY':
                order_status = self.broker.get_order_status(pos['order_id'])
                if order_status == 'FILLED':
                    pos['status'] = 'OPEN'
                    self.place_exit_orders(symbol, pos)

    def place_exit_orders(self, symbol, pos):
        # Place SL order
        sl_req = OrderRequest(
            symbol=symbol.split(':')[1],
            exchange=Exchange[symbol.split(':')[0]],
            transaction_type=TransactionType.SELL if pos['type'] == 'LONG' else TransactionType.BUY,
            quantity=1,
            product_type=ProductType.MARGIN,
            order_type=OrderType.STOP,
            price=pos['stop_loss'],
            stop_price=pos['stop_loss']
        )
        sl_resp = self.broker.place_order(sl_req)
        if sl_resp.status == 'ok':
            pos['sl_order_id'] = sl_resp.order_id

        # Place Target order
        tgt_req = OrderRequest(
            symbol=symbol.split(':')[1],
            exchange=Exchange[symbol.split(':')[0]],
            transaction_type=TransactionType.SELL if pos['type'] == 'LONG' else TransactionType.BUY,
            quantity=1,
            product_type=ProductType.MARGIN,
            order_type=OrderType.LIMIT,
            price=pos['target']
        )
        tgt_resp = self.broker.place_order(tgt_req)
        if tgt_resp.status == 'ok':
            pos['target_order_id'] = tgt_resp.order_id

    def analyze_and_trade(self):
        logger.info("Analyzing for FVG patterns...")

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        for symbol in self.symbols:
            if symbol in self.positions:
                continue

            try:
                df = pd.DataFrame(self.broker.get_history(symbol, "15m", start_date, end_date))
                if df.empty:
                    continue

                df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])
                df['ema200'] = ta.ema(df['close'], length=self.strat_var_ema_length)
                df['bullish_fvg'] = self.is_bullish_fvg(df)
                df['bearish_fvg'] = self.is_bearish_fvg(df)
                self.check_long_entry_conditions(df, symbol)
                self.check_short_entry_conditions(df, symbol)

            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")

    def is_bullish_fvg(self, df):
        return df['low'] > df['high'].shift(2)

    def is_bearish_fvg(self, df):
        return df['high'] < df['low'].shift(2)

    def check_long_entry_conditions(self, df, symbol):
        last_candle = df.iloc[-1]
        second_last_candle = df.iloc[-2]

        if last_candle['bullish_fvg'] and (last_candle['close'] > last_candle['vwap'] or last_candle['close'] > last_candle['ema200']) and self.is_near_support(df):
            entry_price = last_candle['high']
            stop_loss = second_last_candle['low']
            target = entry_price + 2 * (entry_price - stop_loss)

            logger.info(f"Long entry condition met for {symbol}: Entry at {entry_price}, SL at {stop_loss}, Target at {target}")
            self._place_order(symbol, 1, TransactionType.BUY, entry_price, stop_loss, target, "LONG")

    def check_short_entry_conditions(self, df, symbol):
        last_candle = df.iloc[-1]
        second_last_candle = df.iloc[-2]

        if last_candle['bearish_fvg'] and (last_candle['close'] < last_candle['vwap'] or last_candle['close'] < last_candle['ema200']) and self.is_near_resistance(df):
            entry_price = last_candle['low']
            stop_loss = second_last_candle['high']
            target = entry_price - 2 * (stop_loss - entry_price)

            logger.info(f"Short entry condition met for {symbol}: Entry at {entry_price}, SL at {stop_loss}, Target at {target}")
            self._place_order(symbol, 1, TransactionType.SELL, entry_price, stop_loss, target, "SHORT")

    def is_near_support(self, df):
        recent_low = df['low'].tail(20).min()
        last_close = df.iloc[-1]['close']
        return (last_close - recent_low) / recent_low < self.strat_var_support_proximity_threshold

    def is_near_resistance(self, df):
        recent_high = df['high'].tail(20).max()
        last_close = df.iloc[-1]['close']
        return (recent_high - last_close) / last_close < self.strat_var_support_proximity_threshold

    def _place_order(self, symbol, quantity, transaction_type, entry_price, stop_loss, target, position_type):
        exchange_str, _ = symbol.split(':')
        exchange = Exchange[exchange_str]

        req = OrderRequest(
            symbol=symbol.split(':')[1],
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
            product_type=ProductType.MARGIN,
            order_type=OrderType.STOP,
            price=entry_price,
            stop_price=entry_price
        )

        order_resp = self.broker.place_order(req)

        if order_resp.status == "ok":
            self.positions[symbol] = {
                "order_id": order_resp.order_id,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target": target,
                "status": "PENDING_ENTRY",
                "type": position_type
            }
            logger.info(f"Order placed for {symbol}: {order_resp.order_id}")
        else:
            logger.error(f"Failed to place order for {symbol}: {order_resp.message}")

    def manage_positions(self):
        for symbol, pos in list(self.positions.items()):
            if pos['status'] == "OPEN":
                quote = self.broker.get_quote(symbol)
                if pos['type'] == 'LONG':
                    if quote.last_price >= pos['entry_price'] + (pos['target'] - pos['entry_price']) / 2:
                        if pos['stop_loss'] < pos['entry_price']:
                            self.broker.cancel_order(pos['sl_order_id'])
                            self.broker.cancel_order(pos['target_order_id'])
                            pos['stop_loss'] = pos['entry_price']
                            self.place_exit_orders(symbol, pos)
                            logger.info(f"Moved SL to breakeven for {symbol}")
                else: # SHORT
                    if quote.last_price <= pos['entry_price'] - (pos['entry_price'] - pos['target']) / 2:
                        if pos['stop_loss'] > pos['entry_price']:
                            self.broker.cancel_order(pos['sl_order_id'])
                            self.broker.cancel_order(pos['target_order_id'])
                            pos['stop_loss'] = pos['entry_price']
                            self.place_exit_orders(symbol, pos)
                            logger.info(f"Moved SL to breakeven for {symbol}")

                sl_status = self.broker.get_order_status(pos['sl_order_id'])
                if sl_status == 'FILLED':
                    self.broker.cancel_order(pos['target_order_id'])
                    pos['status'] = 'CLOSED_SL'
                    logger.info(f"SL hit for {symbol}")

                tgt_status = self.broker.get_order_status(pos['target_order_id'])
                if tgt_status == 'FILLED':
                    self.broker.cancel_order(pos['sl_order_id'])
                    pos['status'] = 'CLOSED_TARGET'
                    logger.info(f"Target hit for {symbol}")

    def display_table(self):
        os.system('cls' if os.name == 'nt' else 'clear')

        table_data = []
        for symbol, pos in self.positions.items():
            quote = self.broker.get_quote(symbol)

            row = {
                "stock_name": symbol.split(':')[1],
                "symbol": symbol,
                "future_price": quote.last_price,
                "entry_price": pos['entry_price'],
                "target_price": pos['target'],
                "stoploss_price": pos['stop_loss'],
                "order_filled": pos['status'],
                "type": pos['type']
            }
            table_data.append(row)

        df = pd.DataFrame(table_data)

        print("--- FVG Strategy Live Positions ---")
        if not df.empty:
            for index, row in df.iterrows():
                color = None
                if row['order_filled'] == 'CLOSED_SL':
                    color = 'red'
                elif row['order_filled'] == 'CLOSED_TARGET':
                    color = 'green'

                print(colored(row.to_string(), color))
        else:
            print("No open positions.")
        print("-----------------------------------")


if __name__ == "__main__":
    import yaml
    import sys
    from orders import OrderTracker
    from logger import logger
    from dotenv import load_dotenv
    load_dotenv()
    import logging
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description="FVG Strategy")
    parser.add_argument('--symbols', type=str, nargs='+', help='List of symbols to trade')
    parser.add_argument('--ema-length', type=int, help='Length for EMA calculation')
    parser.add_argument('--support-proximity-threshold', type=float, help='Proximity threshold for support zone')
    args = parser.parse_args()

    config_file = os.path.join(os.path.dirname(__file__), "configs/fvg_strategy.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']

    if args.symbols:
        config['symbols'] = args.symbols
    if args.ema_length:
        config['ema_length'] = args.ema_length
    if args.support_proximity_threshold:
        config['support_proximity_threshold'] = args.support_proximity_threshold

    broker = BrokerGateway.from_name(os.getenv("BROKER_NAME"))
    order_tracker = OrderTracker()

    strategy = FVGStrategy(broker, config, order_tracker)
    strategy.run()
