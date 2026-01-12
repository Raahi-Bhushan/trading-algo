import os
import sys
import yaml
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Add project root to path to access brokers
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from brokers.integrations.zerodha.driver import ZerodhaDriver
from logger import logger

def load_config(config_path="config.yaml"):
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found.")
        sys.exit(1)

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def set_env_credentials(config):
    """
    Sets environment variables from config for ZerodhaDriver to pick up.
    """
    if 'api_key' in config:
        os.environ['BROKER_API_KEY'] = config['api_key']
    if 'access_token' in config:
        os.environ['BROKER_ACCESS_TOKEN'] = config['access_token']
    if 'api_secret' in config:
        os.environ['BROKER_API_SECRET'] = config['api_secret']
    if 'user_id' in config:
        os.environ['BROKER_ID'] = config['user_id']
    if 'password' in config:
        os.environ['BROKER_PASSWORD'] = config['password']
    if 'totp_key' in config:
        os.environ['BROKER_TOTP_KEY'] = config['totp_key']
        os.environ['BROKER_LOGIN_MODE'] = 'totp'

def download_data(driver, symbol, start_date, end_date, interval, output_file):
    print(f"Downloading {interval} data for {symbol} from {start_date} to {end_date}...")

    all_data = []
    current_date = start_date

    # Batch size: 60 days to be safe for 15m/60m candles (well within 2000 candle limit)
    # 60m: 7 * 60 = 420 candles
    # 15m: 25 * 60 = 1500 candles (Limit is typically 2000)
    BATCH_DAYS = 60

    while current_date < end_date:
        next_date = min(current_date + timedelta(days=BATCH_DAYS), end_date)

        # Format dates as strings for driver
        start_str = current_date.strftime("%Y-%m-%d")
        end_str = next_date.strftime("%Y-%m-%d")

        print(f"  Fetching {start_str} to {end_str}...")

        try:
            batch = driver.get_history(symbol, interval, start_str, end_str)
            if batch:
                # Convert to DataFrame to clean up overlaps if any
                df_batch = pd.DataFrame(batch)
                all_data.append(df_batch)
            else:
                print(f"    No data found for this batch.")
        except Exception as e:
            print(f"    Error fetching batch: {e}")

        # Move to next batch
        current_date = next_date + timedelta(days=1)

    if not all_data:
        print("No data downloaded.")
        return

    # Merge all batches
    final_df = pd.concat(all_data, ignore_index=True)

    # Deduplicate by timestamp
    final_df.drop_duplicates(subset=['ts'], keep='first', inplace=True)

    # Sort by timestamp
    final_df.sort_values(by='ts', inplace=True)

    # Add human readable date
    final_df['date'] = pd.to_datetime(final_df['ts'], unit='s')

    # Save to CSV
    final_df.to_csv(output_file, index=False)
    print(f"Saved {len(final_df)} records to {output_file}")

def main():
    # Load Configuration
    config = load_config("config.yaml")
    set_env_credentials(config)

    # Initialize Driver
    driver = ZerodhaDriver()

    # Check if connected
    try:
        profile = driver.get_profile()
        if "error" in profile:
            print(f"Connection Failed: {profile['error']}")
            # In a real run, we might exit, but for testing script logic we continue if mocking is involved?
            # But here we assume user provides valid creds.
            if "unauthenticated" in profile.get("error", ""):
                 print("Please check your credentials in config.yaml")
                 sys.exit(1)
        else:
             print(f"Connected as {profile.get('user_id', 'User')}")
    except Exception as e:
        print(f"Connection Check Error: {e}")
        # sys.exit(1) # Commented out to allow dry run/testing of script structure

    # Parameters
    symbol = "NSE:NIFTY 50"
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)

    # Adjust end date to now if future
    if end_date > datetime.now():
        print(f"Adjusting end date from {end_date.date()} to today {datetime.now().date()}")
        end_date = datetime.now()

    # Timeframes to download
    timeframes = ["15m", "60m"]

    for tf in timeframes:
        output_file = f"nifty_{tf}_2020_2025.csv"
        download_data(driver, symbol, start_date, end_date, tf, output_file)

if __name__ == "__main__":
    main()
