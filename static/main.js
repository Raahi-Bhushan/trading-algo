document.addEventListener('DOMContentLoaded', () => {
    const startBtn = document.getElementById('start-btn');
    const stopBtn = document.getElementById('stop-btn');
    const statusEl = document.getElementById('status');
    const logsEl = document.getElementById('logs');
    const configForm = document.getElementById('config-form');

    let logInterval;
    let statusInterval;

    function getStatus() {
        fetch('/status')
            .then(response => response.json())
            .then(data => {
                statusEl.textContent = data.status;
                if (data.status === 'Running') {
                    statusEl.style.backgroundColor = '#28a745'; // Green
                } else {
                    statusEl.style.backgroundColor = '#dc3545'; // Red
                }
            });
    }

    function getLogs() {
        fetch('/logs')
            .then(response => response.json())
            .then(data => {
                if (data.logs.length > 0) {
                    logsEl.textContent += data.logs.join('\n') + '\n';
                    logsEl.scrollTop = logsEl.scrollHeight;
                }
            });
    }

    startBtn.addEventListener('click', () => {
        const formData = new FormData(configForm);
        const strategyName = formData.get('strategy_name');
        const config = {};
        formData.forEach((value, key) => {
            if (key !== 'strategy_name') {
                config[key] = isNaN(value) ? value : Number(value);
            }
        });

        fetch('/start', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                strategy_name: strategyName,
                config: config
            })
        })
        .then(response => response.json())
        .then(data => {
            alert(data.message);
            getStatus();
            logInterval = setInterval(getLogs, 2000);
            statusInterval = setInterval(getStatus, 5000);
        });
    });

    stopBtn.addEventListener('click', () => {
        fetch('/stop', {
            method: 'POST'
        })
        .then(response => response.json())
        .then(data => {
            alert(data.message);
            getStatus();
            clearInterval(logInterval);
            clearInterval(statusInterval);
        });
    });

    // Initial status check
    getStatus();
});