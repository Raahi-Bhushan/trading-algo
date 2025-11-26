import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import yaml
from logger import logger
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
from tabulate import tabulate
import pandas as pd
from datetime import datetime, timedelta
from playsound import playsound
from termcolor import colored

class OITrackerStrategy:
    """
    OI Tracker Strategy

    This strategy tracks the change in Open Interest (OI) for ATM and 2 slightly ITM and 2 slightly OTM options
    both call and put. It creates a live table in python which updates every minute.
    """

    def __init__(self, broker, config):
        # Assign config values as instance variables with 'strat_var_' prefix
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)
        # External dependencies
        self.broker = broker
        self.broker.download_instruments()
        self.instruments = self.broker.get_instruments()

        self._initialize_state()

    def _initialize_state(self):
        """Initializes the state of the strategy."""
        logger.info("Initializing OI Tracker Strategy...")
        # Initialize any necessary variables here
        pass

    def on_ticks_update(self, ticks):
        """
        Main strategy execution method called on each tick update
        """
        # This strategy will be updated every minute, not on every tick.
        pass

    def run(self):
        """
        Main loop for the strategy.
        """
        while True:
            self.update_tables()
            time.sleep(60) # Update every minute

    def get_atm_strike(self):
        """
        Get the current ATM strike price for NIFTY.
        """
        nifty_quote = self.broker.get_quote(self.strat_var_index_symbol)
        current_price = nifty_quote.last_price
        strike_difference = self.strat_var_strike_difference
        atm_strike = round(current_price / strike_difference) * strike_difference
        return atm_strike

    def get_strikes(self, atm_strike):
        """
        Get the 5 strike prices to track (ATM, 2 ITM, 2 OTM).
        """
        strike_difference = self.strat_var_strike_difference
        strikes = [atm_strike - 2 * strike_difference,
                   atm_strike - 1 * strike_difference,
                   atm_strike,
                   atm_strike + 1 * strike_difference,
                   atm_strike + 2 * strike_difference]
        return strikes

    def get_oi_data(self, strikes, option_type):
        """
        Fetch OI data for the given strikes and option type.
        """
        oi_data = {}
        now = datetime.now()
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(hours=3)).strftime("%Y-%m-%d")

        for strike in strikes:
            symbol = self.get_option_symbol(strike, option_type)
            if symbol is None:
                logger.error(f"Could not find symbol for strike {strike} and option type {option_type}")
                continue

            history = self.broker.get_history(symbol, "1m", start_date, end_date, oi=True)
            if not history:
                continue

            oi_data[strike] = {'history': history}
            oi_data[strike]['current_oi'] = history[-1]['oi']

            # Historical OI for different intervals
            for interval, minutes in self.strat_var_intervals.items():
                target_time = now - timedelta(minutes=minutes)
                for record in reversed(history):
                    record_time = datetime.fromtimestamp(record['ts'])
                    if record_time <= target_time:
                        oi_data[strike][interval] = record['oi']
                        break
        return oi_data

    def get_option_symbol(self, strike, option_type):
        """
        Get the option symbol for a given strike and option type.
        """
        df = self.instruments

        # Find the next expiry date
        today = datetime.now().date()
        expiries = pd.to_datetime(df['expiry'], errors='coerce').dropna().dt.date.unique()
        expiries = sorted([e for e in expiries if e >= today])
        if not expiries:
            return None
        next_expiry = expiries[0]

        # Filter for the specific strike and option type on the next expiry
        instrument = df[
            (df['strike'] == strike) &
            (df['instrument_type'] == option_type) &
            (pd.to_datetime(df['expiry']).dt.date == next_expiry)
        ]

        if not instrument.empty:
            return instrument.iloc[0]['symbol']
        return None

    def calculate_oi_change(self, current_oi, historical_oi):
        """
        Calculate the percentage and absolute change in OI.
        """
        if historical_oi is None or historical_oi == 0:
            return 0, 0

        absolute_change = current_oi - historical_oi
        percentage_change = (absolute_change / historical_oi) * 100
        return percentage_change, absolute_change

    def update_tables(self):
        """
        Update the Put, Call, and NIFTY tables.
        """
        atm_strike = self.get_atm_strike()
        strikes = self.get_strikes(atm_strike)

        # Update Call and Put tables
        alert_triggered = False
        for option_type in ["CE", "PE"]:
            oi_data = self.get_oi_data(strikes, option_type)
            table_data = []
            headers = ["Strike", "Current OI"] + list(self.strat_var_intervals.keys())
            red_cell_count = 0

            for strike in strikes:
                row = [strike, oi_data.get(strike, {}).get('current_oi', 'N/A')]
                current_oi = oi_data.get(strike, {}).get('current_oi')

                for interval in self.strat_var_intervals.keys():
                    historical_oi = oi_data.get(strike, {}).get(interval)
                    if current_oi is not None and historical_oi is not None:
                        percentage_change, absolute_change = self.calculate_oi_change(current_oi, historical_oi)
                        formatted_cell, is_red = self.format_and_color_cell(percentage_change, absolute_change, interval)
                        if is_red:
                            red_cell_count += 1
                        row.append(formatted_cell)
                    else:
                        row.append("N/A")
                table_data.append(row)

            print(f"\n--- {option_type} Table ---")
            print(tabulate(table_data, headers=headers))

            total_cells = len(strikes) * len(['3m', '5m', '10m', '15m', '30m', '3h'])
            if not alert_triggered and (red_cell_count / total_cells > 0.3):
                self.trigger_alert()
                alert_triggered = True

        # Update NIFTY table
        now = datetime.now()
        nifty_table_data = []
        nifty_headers = ["", "Current Value", "3m", "5m", "10m", "15m", "30m", "3h"]

        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(hours=3)).strftime("%Y-%m-%d")
        history = self.broker.get_history(self.strat_var_index_symbol, "1m", start_date, end_date)

        if history:
            current_price = history[-1]['close']
            nifty_row = ["NIFTY", f"{current_price:.2f}"]
            for interval, minutes in self.strat_var_intervals.items():
                target_time = now - timedelta(minutes=minutes)
                historical_price = None
                for record in reversed(history):
                    record_time = datetime.fromtimestamp(record['ts'])
                    if record_time <= target_time:
                        historical_price = record['close']
                        break

                if historical_price is not None:
                    percentage_change = ((current_price - historical_price) / historical_price) * 100
                    absolute_change = current_price - historical_price
                    nifty_row.append(f"{percentage_change:.2f}% ({absolute_change:.2f})")
                else:
                    nifty_row.append("N/A")
            nifty_table_data.append(nifty_row)
        print("\n--- NIFTY Table ---")
        print(tabulate(nifty_table_data, headers=nifty_headers))


    def format_and_color_cell(self, percentage_change, absolute_change, column_name):
        """
        Formats the cell text and applies color if the threshold is met.
        """
        text = f"{percentage_change:.2f}% ({absolute_change})"
        threshold = self.strat_var_color_thresholds.get(column_name)
        if threshold and percentage_change > threshold:
            return colored(text, 'red'), True
        return text, False

    def trigger_alert(self):
        """
        Trigger an alert sound if more than 30% of the cells in any table are color-coded.
        """
        logger.info("ALERT: More than 30% of cells are color-coded!")
        try:
            playsound(self.strat_var_alert_sound_path)
        except Exception as e:
            logger.error(f"Error playing alert sound: {e}")

if __name__ == "__main__":
    import argparse
    import warnings
    from dotenv import load_dotenv
    load_dotenv()
    warnings.filterwarnings("ignore")

    config_file = os.path.join(os.path.dirname(__file__), "configs/oi_tracker.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']

    broker = BrokerGateway.from_name(os.getenv("BROKER_NAME"))

    strategy = OITrackerStrategy(broker, config)
    strategy.run()
