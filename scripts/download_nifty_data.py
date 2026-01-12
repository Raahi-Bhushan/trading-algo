import os
import sys
import yaml
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteConnect

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

def authenticate_interactive(config):
    """
    Performs interactive login flow if access_token is missing.
    """
    api_key = config.get('api_key')
    api_secret = config.get('api_secret')

    if not api_key or not api_secret:
        print("Error: api_key and api_secret must be set in config.yaml")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)

    # STEP 1: Generate login URL
    login_url = kite.login_url()
    print("\nLOGIN URL (open in browser):")
    print(login_url)

    # STEP 2: Get request_token from user
    request_token = input("\nPaste request_token here: ").strip()

    # STEP 3: Generate access token
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        print("\nLogin successful!")
        print("Access Token:", access_token)

        # Set environment variables for ZerodhaDriver
        os.environ['BROKER_API_KEY'] = api_key
        os.environ['BROKER_ACCESS_TOKEN'] = access_token

    except Exception as e:
        print(f"\nLogin Failed: {e}")
        sys.exit(1)

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

    # Authenticate interactively
    authenticate_interactive(config)

    # Initialize Driver (will pick up env vars set by authenticate_interactive)
    driver = ZerodhaDriver()

    # Check if connected
    try:
        profile = driver.get_profile()
        if "error" in profile:
            print(f"Connection Failed: {profile['error']}")
            sys.exit(1)
        else:
             print(f"Connected as {profile.get('user_id', 'User')}")
    except Exception as e:
        print(f"Connection Check Error: {e}")

    # Parameters
    # Note: ZerodhaDriver.get_history resolves token from symbol
    # 'NSE:NIFTY 50' is standard for Nifty 50 Index on Kite
    symbol = "NSE:NIFTY 50"
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)

    # Adjust end date to now if future
    if end_date > datetime.now():
        print(f"Adjusting end date from {end_date.date()} to today {datetime.now().date()}")
        end_date = datetime.now()

    # Timeframes to download - Using correct Kite Connect API strings
    # The driver maps these:
    # "15m" -> "15minute"
    # "60m" -> "60minute"
    # So using "15m" and "60m" is actually CORRECT for the Driver wrapper,
    # but let's double check driver.py line 287 imap.

    # In ZerodhaDriver.get_history:
    # imap = { "15m": "15minute", "60m": "60minute", ... }
    # So the driver DOES handle the mapping!
    # However, to be explicit and safe given the review feedback,
    # we can pass keys that map correctly.

    timeframes = ["15m", "60m"]

    for tf in timeframes:
        output_file = f"nifty_{tf}_2020_2025.csv"
        download_data(driver, symbol, start_date, end_date, tf, output_file)

if __name__ == "__main__":
    main()
