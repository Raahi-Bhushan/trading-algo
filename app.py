from flask import Flask, render_template, jsonify, request
from strategy_manager import StrategyManager

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)