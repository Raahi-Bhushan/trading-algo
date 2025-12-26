from flask import Flask, render_template, jsonify, request
from strategy_manager import StrategyManager
from strategy.backtester import BacktesterStrategy
from strategy.nifty50 import NIFTY_50_STOCKS
from brokers import BrokerGateway
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
manager = StrategyManager()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start_strategy():
    data = request.json
    strategy_name = data.get('strategy_name')
    config_overrides = data.get('config')

    if not strategy_name:
        return jsonify({"message": "Error: strategy_name is required."}), 400

    message = manager.start(strategy_name, config_overrides)
    return jsonify({"message": message})

@app.route('/stop', methods=['POST'])
def stop_strategy():
    message = manager.stop()
    return jsonify({"message": message})

@app.route('/status')
def status():
    status = manager.get_status()
    return jsonify({"status": status})

@app.route('/logs')
def logs():
    logs = manager.get_logs()
    return jsonify({"logs": logs})

@app.route('/backtester')
def backtester():
    return render_template('backtester.html', nifty50_stocks=NIFTY_50_STOCKS)

@app.route('/backtest', methods=['POST'])
def run_backtest():
    data = request.json
    symbol = data.get('symbol')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    interval = data.get('interval', '15m') # Default to 15 minutes

    if not all([symbol, start_date, end_date]):
        return jsonify({"message": "Error: symbol, start_date, and end_date are required."}), 400

    broker = BrokerGateway.from_name('zerodha')

    config = {}

    strategy = BacktesterStrategy(broker, config)
    try:
        results = strategy.run_backtest(symbol, start_date, end_date, interval)
        return jsonify(results)
    except PermissionError as e:
        return jsonify({"message": f"Error: {e}"}), 401
    except Exception as e:
        return jsonify({"message": f"An unexpected error occurred: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)