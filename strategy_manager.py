import multiprocessing
import yaml
import os
import importlib
from brokers import BrokerGateway
from dispatcher import DataDispatcher
from orders import OrderTracker
from logger import logger
from queue import Queue
import time
from dotenv import load_dotenv

def run_strategy(strategy_name, config, log_queue):
    """
    This function will be run in a separate process.
    """
    try:
        # Dynamically import the strategy class
        strategy_module = importlib.import_module(f"strategy.{strategy_name}")
        strategy_class_name = f"{strategy_name.capitalize()}Strategy"
        strategy_class = getattr(strategy_module, strategy_class_name)

        load_dotenv()
        broker_name = os.getenv("BROKER_NAME")
        if not broker_name:
            log_queue.put("Error: BROKER_NAME not found in environment variables. "
                          "Please ensure your .env file is created and correctly configured.")
            return

        broker = BrokerGateway.from_name(broker_name)
        if broker is None:
            log_queue.put(f"Error: Failed to initialize broker '{broker_name}'. "
                          "Please check your broker credentials in the .env file.")
            return

        order_tracker = OrderTracker()

        try:
            instrument_token = config['index_symbol']
            logger.info(f"✓ Index instrument token obtained: {instrument_token}")
        except Exception as e:
            logger.error(f"Failed to get instrument token for {config['index_symbol']}: {e}")
            return

        dispatcher = DataDispatcher()
        dispatcher.register_main_queue(Queue())

        def on_ticks(ws, ticks):
            if isinstance(ticks, list):
                dispatcher.dispatch(ticks)
            else:
                if "symbol" in ticks:
                    dispatcher.dispatch(ticks)

        def on_connect(ws, response):
            broker.symbols_to_subscribe([instrument_token])

        def on_order_update(ws, data):
            log_queue.put(f"Order update received: {data}")

        broker.connect_websocket(on_ticks=on_ticks, on_connect=on_connect)
        broker.connect_order_websocket(on_order_update=on_order_update)
        time.sleep(10)

        strategy = strategy_class(broker, config, order_tracker)

        while True:
            try:
                tick_data = dispatcher._main_queue.get()
                if isinstance(tick_data, list):
                    symbol_data = tick_data[0]
                else:
                    symbol_data = tick_data

                if isinstance(symbol_data, dict) and ('last_price' in symbol_data or 'ltp' in symbol_data):
                    strategy.on_ticks_update(symbol_data)
            except KeyboardInterrupt:
                break
            except Exception as e:
                log_queue.put(f"Error in strategy loop: {e}")

    except Exception as e:
        log_queue.put(f"Error initializing strategy: {e}")


class StrategyManager:
    def __init__(self):
        self.process = None
        self.log_queue = multiprocessing.Queue()

    def start(self, strategy_name, config_overrides=None):
        if self.process and self.process.is_alive():
            return "Strategy is already running."

        config_file = os.path.join(os.path.dirname(__file__), f"strategy/configs/{strategy_name}.yml")
        if not os.path.exists(config_file):
            return f"Error: Configuration file for strategy '{strategy_name}' not found."

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)['default']

        if config_overrides:
            config.update(config_overrides)

        self.process = multiprocessing.Process(target=run_strategy, args=(strategy_name, config, self.log_queue))
        self.process.start()
        return f"Strategy '{strategy_name}' started."

    def stop(self):
        if not self.process or not self.process.is_alive():
            return "Strategy is not running."

        self.process.terminate()
        self.process.join()
        return "Strategy stopped."

    def get_status(self):
        if self.process and self.process.is_alive():
            return "Running"
        return "Stopped"

    def get_logs(self):
        logs = []
        while not self.log_queue.empty():
            logs.append(self.log_queue.get())
        return logs

if __name__ == '__main__':
    manager = StrategyManager()
    manager.start()
    time.sleep(60)
    manager.stop()
